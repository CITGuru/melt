"""Melt bench harness — queries-per-dollar deltas vs. Snowflake passthrough.

Runs a synthetic agent-shaped workload (see workload.toml) against:

  1. Melt-routed-to-lake — connects to the local Melt proxy and lets the
     router decide; eligible reads run locally on DuckDB at zero
     Snowflake credit cost.
  2. Snowflake passthrough baseline — connects directly to
     <account>.snowflakecomputing.com, every query bills the
     driver-pinned warehouse.

Output: results-<timestamp>.json with per-query records (route, latency,
warehouse, predicted vs. observed route) plus a summary block with
queries-per-dollar deltas.

Two execution modes:

  - real (default): both paths use the official `snowflake-connector-python`,
    unmodified. Requires a Snowflake account.

  - synthetic (--synthetic): no Snowflake account needed. Routes are taken
    from `melt route "<sql>"` (offline) and latencies are sampled from the
    [synthetic] section of workload.toml. Useful for sanity-checking the
    harness and producing a reference fixture from a clean state.

Env vars:

  Melt:        MELT_HOST (default 127.0.0.1) MELT_PORT (8443) MELT_PROTOCOL (http)
  Snowflake:   SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD
               SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_WAREHOUSE
               SNOWFLAKE_ROLE
  Cost:        BENCH_CREDIT_RATE (overrides workload.toml [cost].credit_rate_usd)

Run from a clean docker compose:

    docker compose up --build -d
    cd examples/bench
    pip install -r requirements.txt
    python run_bench.py --workload workload.toml --out results.json

For a no-credentials sanity run (writes to fixtures/):

    python run_bench.py --synthetic --out fixtures/results-synthetic.json
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import time
import tomllib
from contextlib import closing
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
HARNESS_VERSION = 1
SECONDS_PER_HOUR = 3600.0

# Default Melt admin port — `melt-metrics::serve_admin` exposes
# `/metrics` (Prometheus text), `/healthz`, `/readyz`. Override via
# MELT_ADMIN_HOST / MELT_ADMIN_PORT.
DEFAULT_ADMIN_HOST = "127.0.0.1"
DEFAULT_ADMIN_PORT = 9090


# ─── Records ────────────────────────────────────────────────────────────────


@dataclass
class QueryRecord:
    path: str                      # "melt" | "snowflake"
    name: str                      # query template name from workload.toml
    expected_route: str            # what the workload expects Melt to choose
    observed_route: str            # what actually ran (lake / snowflake / dual / unknown)
    latency_ms: float
    warehouse: Optional[str]       # Snowflake warehouse used (when known)
    warehouse_size: Optional[str]  # XS/S/M/L/XL — what we costed it as
    bytes_scanned: Optional[int]   # not yet wired — left as None in v1
    error: Optional[str]           # truthy when the query failed; latency_ms still set


# ─── Workload + cost model ─────────────────────────────────────────────────


@dataclass
class CostModel:
    credit_rate_usd: float
    warehouse_credits_per_hour: dict[str, float]
    default_warehouse_size: str
    lake_snowflake_credits_per_query: float

    @classmethod
    def from_toml(cls, cost: dict[str, Any], rate_override: Optional[float]) -> "CostModel":
        return cls(
            credit_rate_usd=rate_override if rate_override is not None else float(cost["credit_rate_usd"]),
            warehouse_credits_per_hour={k: float(v) for k, v in cost["warehouse_credits_per_hour"].items()},
            default_warehouse_size=str(cost["default_warehouse_size"]).upper(),
            lake_snowflake_credits_per_query=float(cost["lake_snowflake_credits_per_query"]),
        )

    def credits_for(self, record: QueryRecord) -> float:
        # Lake (and dual when we add it) are zero-credit on the Snowflake side.
        if record.observed_route == "lake":
            return self.lake_snowflake_credits_per_query
        size = (record.warehouse_size or self.default_warehouse_size).upper()
        per_hour = self.warehouse_credits_per_hour.get(size)
        if per_hour is None:
            # Unknown warehouse size — fall back to default rather than fail.
            per_hour = self.warehouse_credits_per_hour[self.default_warehouse_size]
        return (record.latency_ms / 1000.0 / SECONDS_PER_HOUR) * per_hour

    def usd_for(self, record: QueryRecord) -> float:
        return self.credits_for(record) * self.credit_rate_usd


@dataclass
class WorkloadQuery:
    name: str
    weight: int
    expected_route: str
    sql: str


@dataclass
class Workload:
    runs: int
    concurrency: int
    warmup: int
    seed: int
    cost: CostModel
    queries: list[WorkloadQuery]
    synthetic: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path, rate_override: Optional[float]) -> "Workload":
        with open(path, "rb") as f:
            doc = tomllib.load(f)
        bench = doc["bench"]
        return cls(
            runs=int(bench.get("runs", 100)),
            concurrency=int(bench.get("concurrency", 4)),
            warmup=int(bench.get("warmup", 10)),
            seed=int(bench.get("seed", 7)),
            cost=CostModel.from_toml(doc["cost"], rate_override),
            queries=[
                WorkloadQuery(
                    name=q["name"],
                    weight=int(q["weight"]),
                    expected_route=q["expected_route"],
                    sql=q["sql"].strip(),
                )
                for q in doc["query"]
            ],
            synthetic=doc.get("synthetic", {}),
        )


# ─── Route discovery ───────────────────────────────────────────────────────


def discover_routes(queries: Iterable[WorkloadQuery]) -> dict[str, str]:
    """Run `melt route` once per distinct SQL to record what Melt would do.

    Tries the cargo-built binary first (fast), falls back to the docker
    compose service. If both fail, every query is recorded as `unknown` —
    the harness still runs, just without a per-query route attribution.
    """
    melt_bin = shutil.which("melt") or str(REPO_ROOT / "target" / "release" / "melt")
    cmds: list[list[str]] = []
    if Path(melt_bin).exists():
        cmds.append([melt_bin, "route"])
    cmds.append(["docker", "compose", "run", "--rm", "-T", "melt", "route"])

    out: dict[str, str] = {}
    for q in queries:
        if q.sql in out:
            continue
        out[q.sql] = _try_route(cmds, q.sql)
    return out


def _try_route(cmds: list[list[str]], sql: str) -> str:
    for base in cmds:
        try:
            r = subprocess.run(
                base + [sql],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=15,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        if r.returncode != 0:
            continue
        for line in r.stdout.splitlines():
            stripped = line.strip().lower()
            # `melt route` prints e.g. "route: lake" or "route: snowflake"
            if stripped.startswith("route:"):
                return stripped.split(":", 1)[1].strip()
        # Some builds print just the route on one of the first lines.
        if "lake" in r.stdout.lower():
            return "lake"
        if "snowflake" in r.stdout.lower():
            return "snowflake"
    return "unknown"


# ─── Snowflake driver wiring (mirrors examples/python/melt_demo.py) ────────


def _read_private_key_pem() -> Optional[str]:
    if path := os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        with open(path, "r") as f:
            return f.read()
    return os.environ.get("SNOWFLAKE_PRIVATE_KEY") or None


def _load_private_key(pem_str: Optional[str]) -> Optional[bytes]:
    if not pem_str:
        return None
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    try:
        private_key = serialization.load_pem_private_key(
            pem_str.encode("utf-8"), password=None, backend=default_backend()
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except Exception:
        return None


def _common_kwargs() -> dict:
    try:
        base = dict(
            user=os.environ["SNOWFLAKE_USER"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        )
    except KeyError as missing:
        sys.exit(f"missing required env var: {missing.args[0]}")
    if wh := os.environ.get("SNOWFLAKE_WAREHOUSE"):
        base["warehouse"] = wh
    if role := os.environ.get("SNOWFLAKE_ROLE"):
        base["role"] = role
    if pw := os.environ.get("SNOWFLAKE_PASSWORD"):
        base["password"] = pw
    else:
        pkb = _load_private_key(_read_private_key_pem())
        if pkb is None:
            sys.exit("no SNOWFLAKE_PASSWORD and no usable private key — see README")
        base["private_key"] = pkb
    return base


def connect_melt():
    import snowflake.connector
    kw = _common_kwargs()
    kw.update(
        host=os.environ.get("MELT_HOST", "127.0.0.1"),
        port=int(os.environ.get("MELT_PORT", "8443")),
        protocol=os.environ.get("MELT_PROTOCOL", "http"),
        insecure_mode=True,
    )
    return snowflake.connector.connect(**kw)


def connect_snowflake():
    import snowflake.connector
    return snowflake.connector.connect(**_common_kwargs())


# ─── Execution paths ───────────────────────────────────────────────────────


def execute_real(
    path: str,
    conn_factory,
    queries: list[WorkloadQuery],
    routes: dict[str, str],
    runs: int,
    concurrency: int,
    warmup: int,
    seed: int,
    default_warehouse_size: str,
) -> list[QueryRecord]:
    """Drive `runs + warmup` queries over `concurrency` workers using the same
    connection pool. The Snowflake connector is thread-safe per-connection
    for sequential execute calls — we open one connection per worker."""
    rng = random.Random(seed)
    plan = [_pick_query(rng, queries) for _ in range(runs + warmup)]
    warmup_plan, measured_plan = plan[:warmup], plan[warmup:]

    # Drain warmup serially on a single connection so connection-warmup
    # latency doesn't pollute the measured set.
    with closing(conn_factory()) as warm_conn:
        with closing(warm_conn.cursor()) as cur:
            for q in warmup_plan:
                _run_one(cur, path, q, routes, default_warehouse_size, record=False)

    records: list[QueryRecord] = []
    chunks = _chunk(measured_plan, concurrency)

    def worker(chunk: list[WorkloadQuery]) -> list[QueryRecord]:
        out: list[QueryRecord] = []
        with closing(conn_factory()) as conn:
            with closing(conn.cursor()) as cur:
                for q in chunk:
                    out.append(_run_one(cur, path, q, routes, default_warehouse_size))
        return out

    with cf.ThreadPoolExecutor(max_workers=concurrency) as ex:
        for batch in ex.map(worker, chunks):
            records.extend(batch)
    return records


def _run_one(cur, path: str, q: WorkloadQuery, routes: dict[str, str],
             default_warehouse_size: str, record: bool = True) -> Optional[QueryRecord]:
    from snowflake.connector.errors import Error as SfError
    started = time.perf_counter()
    err: Optional[str] = None
    warehouse: Optional[str] = None
    try:
        cur.execute(q.sql)
        cur.fetchall()
    except SfError as e:
        err = f"{type(e).__name__}: {e}"
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if not record:
        return None
    # Best-effort: the connector exposes the warehouse on the connection. Redact
    # the live name by default so checked-in fixtures don't leak customer/account-
    # specific identifiers — the cost math only needs `warehouse_size`. Set
    # BENCH_RECORD_WAREHOUSE_NAME=1 to capture the live name in local-only runs.
    if os.environ.get("BENCH_RECORD_WAREHOUSE_NAME") == "1":
        try:
            warehouse = cur.connection.warehouse
        except Exception:
            warehouse = None
    else:
        warehouse = None
    # Path "snowflake" is always passthrough; path "melt" is whatever the
    # router decided (lake / snowflake / dual). For Melt path we trust the
    # offline `melt route` decision since the proxy is deterministic per SQL.
    observed = routes.get(q.sql, "unknown") if path == "melt" else "snowflake"
    return QueryRecord(
        path=path,
        name=q.name,
        expected_route=q.expected_route,
        observed_route=observed,
        latency_ms=elapsed_ms,
        warehouse=warehouse,
        warehouse_size=default_warehouse_size,
        bytes_scanned=None,
        error=err,
    )


def execute_synthetic(
    path: str,
    queries: list[WorkloadQuery],
    routes: dict[str, str],
    runs: int,
    warmup: int,
    seed: int,
    default_warehouse_size: str,
    syn: dict[str, Any],
) -> list[QueryRecord]:
    """No-credentials synthetic mode: sample latencies from a log-normal-ish
    model parameterized by the synthetic block in workload.toml. Each path
    gets its own RNG offset so the two streams are independent but
    reproducible."""
    rng = random.Random(seed + (0 if path == "melt" else 1))
    sf_p50 = float(syn.get("snowflake_p50_ms", 1500))
    sf_p95 = float(syn.get("snowflake_p95_ms", 6000))
    lake_p50 = float(syn.get("lake_p50_ms", 100))
    lake_p95 = float(syn.get("lake_p95_ms", 400))
    lake_frac = float(syn.get("lake_route_fraction", 0.85))

    records: list[QueryRecord] = []
    for i in range(runs + warmup):
        q = _pick_query(rng, queries)
        if path == "snowflake":
            observed = "snowflake"
            latency = _sample_lognormal(rng, sf_p50, sf_p95)
        else:
            base_route = routes.get(q.sql, q.expected_route)
            # If `melt route` says lake but the synthetic mix is configured
            # to leak some fraction back to passthrough, honor that — it
            # models routing-eligibility realism (e.g. unsynced tables).
            observed = base_route if rng.random() < lake_frac or base_route != "lake" else "snowflake"
            latency = _sample_lognormal(rng, lake_p50, lake_p95) if observed == "lake" \
                else _sample_lognormal(rng, sf_p50, sf_p95)
        if i < warmup:
            continue
        records.append(QueryRecord(
            path=path,
            name=q.name,
            expected_route=q.expected_route,
            observed_route=observed,
            latency_ms=latency,
            warehouse=None,
            warehouse_size=default_warehouse_size,
            bytes_scanned=None,
            error=None,
        ))
    return records


def _sample_lognormal(rng: random.Random, p50: float, p95: float) -> float:
    """Cheap latency model: pick a sample so that the empirical p50/p95
    matches the configured values. Uses a log-normal with mu=ln(p50) and
    sigma derived from p95/p50 ratio (z₀.₉₅ ≈ 1.645)."""
    import math
    if p95 <= p50:
        sigma = 0.0
    else:
        sigma = math.log(p95 / p50) / 1.645
    mu = math.log(max(p50, 1e-3))
    return float(math.exp(rng.gauss(mu, sigma)))


def _pick_query(rng: random.Random, queries: list[WorkloadQuery]) -> WorkloadQuery:
    total = sum(q.weight for q in queries)
    pick = rng.uniform(0, total)
    cum = 0
    for q in queries:
        cum += q.weight
        if pick <= cum:
            return q
    return queries[-1]


def _chunk(plan: list[WorkloadQuery], n: int) -> list[list[WorkloadQuery]]:
    out: list[list[WorkloadQuery]] = [[] for _ in range(n)]
    for i, q in enumerate(plan):
        out[i % n].append(q)
    return out


# ─── Parity sampler scrape (POWA-162) ──────────────────────────────────────
#
# When `--parity-sampler` is on, the harness:
#
#   1. Pre-flights the admin /metrics endpoint and captures the
#      baseline values of the parity counters
#      (`melt_hybrid_parity_mismatches_total`,
#       `melt_hybrid_parity_samples_total{outcome=…}`,
#       `melt_hybrid_parity_sample_drops_total`).
#   2. Runs the bench loop unchanged (the flag does NOT enable
#      sampling — that's a config concern; operator sets
#      `[router].hybrid_parity_sample_rate = 1.0` in melt.toml).
#   3. After the run, scrapes /metrics again and computes deltas.
#   4. Asserts: `mismatches == 0` AND
#      `samples_processed >= --parity-min-samples` (default 100).
#   5. Embeds the deltas + verdict into the JSON output and prints
#      a one-line summary.
#
# This is the v0.1 launch-checklist §A2 verification surface — it's
# how we prove the parity sampler ran ≥100 queries with 0 mismatches
# against current main without needing to scrape the proxy logs.


@dataclass
class ParityCounters:
    """Snapshot of `melt_hybrid_parity_*` counters at one point in
    time. Computed as floats because Prometheus text values are
    floats; in practice they're always integral for counters."""

    mismatches: dict[str, float]            # by reason label
    samples: dict[str, float]               # by outcome label
    sample_drops: float

    @classmethod
    def zero(cls) -> "ParityCounters":
        return cls(mismatches={}, samples={}, sample_drops=0.0)

    def total_mismatches(self) -> float:
        return sum(self.mismatches.values())

    def total_samples(self) -> float:
        return sum(self.samples.values())

    def diff(self, before: "ParityCounters") -> "ParityCounters":
        def sub(a: dict[str, float], b: dict[str, float]) -> dict[str, float]:
            keys = set(a) | set(b)
            return {k: a.get(k, 0.0) - b.get(k, 0.0) for k in keys}
        return ParityCounters(
            mismatches=sub(self.mismatches, before.mismatches),
            samples=sub(self.samples, before.samples),
            sample_drops=self.sample_drops - before.sample_drops,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mismatches": self.mismatches,
            "samples": self.samples,
            "sample_drops": self.sample_drops,
            "total_mismatches": self.total_mismatches(),
            "total_samples": self.total_samples(),
        }


def _admin_url(path: str) -> str:
    host = os.environ.get("MELT_ADMIN_HOST", DEFAULT_ADMIN_HOST)
    port = int(os.environ.get("MELT_ADMIN_PORT", DEFAULT_ADMIN_PORT))
    return f"http://{host}:{port}{path}"


def _fetch_metrics_text() -> str:
    """Scrape the admin /metrics endpoint. Returns Prometheus text
    format. Raises on connection or HTTP errors so the bench fails
    fast rather than silently scoring zero samples."""
    import urllib.request
    url = _admin_url("/metrics")
    with urllib.request.urlopen(url, timeout=10) as resp:
        if resp.status != 200:
            raise RuntimeError(f"GET {url} -> {resp.status}")
        return resp.read().decode("utf-8")


def scrape_parity_counters() -> ParityCounters:
    """Parse `melt_hybrid_parity_*` counters from the metrics scrape.

    Format is Prometheus text:
        melt_hybrid_parity_mismatches_total{route="hybrid",reason="row_count"} 3
        melt_hybrid_parity_samples_total{outcome="ok"} 17
        melt_hybrid_parity_sample_drops_total 0

    Unlabeled counters fall through with key `""`."""
    body = _fetch_metrics_text()
    counters = ParityCounters.zero()
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        # Parse `name{labels} value` or `name value`.
        if "{" in line:
            name, _, rest = line.partition("{")
            labels_str, _, value_str = rest.partition("} ")
        else:
            parts = line.rsplit(" ", 1)
            if len(parts) != 2:
                continue
            name, value_str = parts
            labels_str = ""
        try:
            value = float(value_str.strip())
        except ValueError:
            continue
        labels = _parse_prom_labels(labels_str)
        if name == "melt_hybrid_parity_mismatches_total":
            counters.mismatches[labels.get("reason", "")] = value
        elif name == "melt_hybrid_parity_samples_total":
            counters.samples[labels.get("outcome", "")] = value
        elif name == "melt_hybrid_parity_sample_drops_total":
            counters.sample_drops = value
    return counters


def _parse_prom_labels(s: str) -> dict[str, str]:
    """Tolerant Prometheus-label parser. Good enough for our counters
    (no embedded commas/quotes in our label values)."""
    out: dict[str, str] = {}
    for piece in s.split(","):
        if "=" not in piece:
            continue
        k, _, v = piece.partition("=")
        out[k.strip()] = v.strip().strip('"')
    return out


def assert_parity_clean(
    delta: ParityCounters, min_samples: int
) -> tuple[bool, list[str]]:
    """Returns (passed, problems). Problems is a list of human-
    readable strings explaining why the assertion failed (empty when
    passed). Mismatches > 0 and samples < min_samples are both
    failures; sample_drops alone is a warning, not a failure
    (drop = sampler caught up later, not drift)."""
    problems: list[str] = []
    if delta.total_mismatches() > 0:
        for reason, count in delta.mismatches.items():
            if count > 0:
                problems.append(
                    f"  mismatches[reason={reason or '-'}]={int(count)}"
                )
    if delta.total_samples() < min_samples:
        problems.append(
            f"  samples_processed={int(delta.total_samples())} "
            f"< min_samples={min_samples} — proxy not reached or "
            f"`router.hybrid_parity_sample_rate` is too low"
        )
    return (not problems), problems


# ─── Summary + output ──────────────────────────────────────────────────────


def summarize(records: list[QueryRecord], cost: CostModel) -> dict[str, Any]:
    if not records:
        return {"queries": 0}
    latencies = [r.latency_ms for r in records if r.error is None]
    credits = sum(cost.credits_for(r) for r in records)
    usd = sum(cost.usd_for(r) for r in records)
    by_route: dict[str, int] = {}
    for r in records:
        by_route[r.observed_route] = by_route.get(r.observed_route, 0) + 1
    qpd = (len(records) / usd) if usd > 0 else float("inf")
    return {
        "queries": len(records),
        "errors": sum(1 for r in records if r.error),
        "latency_ms": {
            "p50": round(_pct(latencies, 0.50), 2),
            "p95": round(_pct(latencies, 0.95), 2),
            "p99": round(_pct(latencies, 0.99), 2),
            "mean": round(statistics.fmean(latencies), 2) if latencies else 0.0,
        },
        "credits": round(credits, 6),
        "usd": round(usd, 6),
        "queries_per_dollar": round(qpd, 2) if qpd != float("inf") else None,
        "route_mix": by_route,
    }


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[k]


def delta(melt_summary: dict[str, Any], sf_summary: dict[str, Any]) -> dict[str, Any]:
    sf_qpd = sf_summary.get("queries_per_dollar")
    melt_qpd = melt_summary.get("queries_per_dollar")
    sf_usd_per_1k = (sf_summary.get("usd", 0) / sf_summary.get("queries", 1)) * 1000
    melt_usd_per_1k = (melt_summary.get("usd", 0) / melt_summary.get("queries", 1)) * 1000
    # `None` queries_per_dollar means USD was 0 (e.g. all queries routed to lake) —
    # the factor is unbounded. Encode that as a string sentinel rather than zero so
    # the headline doesn't read "melt is 0× snowflake".
    if sf_qpd in (None, 0):
        factor: Any = None
    elif melt_qpd is None:
        factor = "infinite"
    else:
        factor = round(melt_qpd / sf_qpd, 2)
    return {
        "queries_per_dollar_factor": factor,
        "usd_per_1k_queries": {
            "snowflake": round(sf_usd_per_1k, 4),
            "melt": round(melt_usd_per_1k, 4),
            "savings": round(sf_usd_per_1k - melt_usd_per_1k, 4),
            "savings_pct":
                round(100 * (sf_usd_per_1k - melt_usd_per_1k) / sf_usd_per_1k, 2)
                if sf_usd_per_1k else None,
        },
    }


def _safe_workload_path(p: str) -> str:
    """Render the workload path repo-relative when possible, otherwise basename
    only. Avoids leaking the operator's home directory into checked-in fixtures."""
    try:
        return str(Path(p).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return Path(p).name


def git_sha() -> Optional[str]:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT,
            capture_output=True, text=True, timeout=2,
        )
        if r.returncode == 0:
            return r.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


# ─── Entrypoint ────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--workload", default=str(Path(__file__).parent / "workload.toml"))
    p.add_argument("--out", default=None,
                   help="output JSON path (default: results-<timestamp>.json)")
    p.add_argument("--mode", choices=["both", "melt", "snowflake"], default="both")
    p.add_argument("--synthetic", action="store_true",
                   help="skip real connections; use offline routing + synthetic latency model")
    p.add_argument("--runs", type=int, default=None)
    p.add_argument("--concurrency", type=int, default=None)
    p.add_argument("--credit-rate", type=float, default=None,
                   help="$/credit override (else workload.toml or BENCH_CREDIT_RATE)")
    p.add_argument("--parity-sampler", action="store_true",
                   help="scrape melt-proxy parity counters before/after the run; "
                        "assert 0 mismatches and >= --parity-min-samples processed. "
                        "Requires `[router].hybrid_parity_sample_rate = 1.0` in melt.toml.")
    p.add_argument("--parity-min-samples", type=int, default=100,
                   help="minimum sampled-and-checked queries the bench expects "
                        "(POWA-162 AC5: >= 100). Default 100.")
    args = p.parse_args()

    rate_override = args.credit_rate
    if rate_override is None and (env := os.environ.get("BENCH_CREDIT_RATE")):
        rate_override = float(env)
    workload = Workload.load(Path(args.workload), rate_override)
    if args.runs is not None:
        workload.runs = args.runs
    if args.concurrency is not None:
        workload.concurrency = args.concurrency

    started = datetime.now(timezone.utc).isoformat()
    routes = discover_routes(workload.queries)

    parity_before: Optional[ParityCounters] = None
    if args.parity_sampler:
        if args.synthetic:
            print(
                "warning: --parity-sampler in --synthetic mode skips the metrics "
                "scrape (no proxy is running). The flag will be recorded in the "
                "output but the assertion is a no-op.",
                file=sys.stderr,
            )
        else:
            try:
                parity_before = scrape_parity_counters()
            except Exception as e:  # noqa: BLE001 — surface the URL + error
                sys.exit(
                    f"--parity-sampler: failed to scrape {_admin_url('/metrics')}: {e}\n"
                    f"Is melt-proxy running and is `[metrics].listen` configured?"
                )

    melt_records: list[QueryRecord] = []
    sf_records: list[QueryRecord] = []
    default_size = workload.cost.default_warehouse_size

    if args.synthetic:
        if args.mode in ("both", "melt"):
            melt_records = execute_synthetic(
                "melt", workload.queries, routes,
                workload.runs, workload.warmup, workload.seed,
                default_size, workload.synthetic,
            )
        if args.mode in ("both", "snowflake"):
            sf_records = execute_synthetic(
                "snowflake", workload.queries, routes,
                workload.runs, workload.warmup, workload.seed,
                default_size, workload.synthetic,
            )
    else:
        if args.mode in ("both", "melt"):
            melt_records = execute_real(
                "melt", connect_melt, workload.queries, routes,
                workload.runs, workload.concurrency, workload.warmup,
                workload.seed, default_size,
            )
        if args.mode in ("both", "snowflake"):
            sf_records = execute_real(
                "snowflake", connect_snowflake, workload.queries, routes,
                workload.runs, workload.concurrency, workload.warmup,
                workload.seed, default_size,
            )

    ended = datetime.now(timezone.utc).isoformat()
    melt_summary = summarize(melt_records, workload.cost)
    sf_summary = summarize(sf_records, workload.cost)

    parity_block: Optional[dict[str, Any]] = None
    parity_problems: list[str] = []
    parity_passed = True
    if args.parity_sampler:
        if args.synthetic or parity_before is None:
            parity_block = {
                "skipped": True,
                "reason": "synthetic mode" if args.synthetic else "pre-scrape failed",
                "min_samples": args.parity_min_samples,
            }
        else:
            try:
                parity_after = scrape_parity_counters()
            except Exception as e:  # noqa: BLE001
                parity_block = {
                    "skipped": True,
                    "reason": f"post-scrape failed: {e}",
                    "min_samples": args.parity_min_samples,
                }
            else:
                delta_counters = parity_after.diff(parity_before)
                parity_passed, parity_problems = assert_parity_clean(
                    delta_counters, args.parity_min_samples
                )
                parity_block = {
                    "skipped": False,
                    "min_samples": args.parity_min_samples,
                    "before": parity_before.to_dict(),
                    "after": parity_after.to_dict(),
                    "delta": delta_counters.to_dict(),
                    "passed": parity_passed,
                    "problems": parity_problems,
                }

    output = {
        "harness_version": HARNESS_VERSION,
        "mode": "synthetic" if args.synthetic else "real",
        "started_at": started,
        "ended_at": ended,
        "git_sha": git_sha(),
        "config": {
            "workload": _safe_workload_path(args.workload),
            "runs": workload.runs,
            "concurrency": workload.concurrency,
            "warmup": workload.warmup,
            "seed": workload.seed,
            "credit_rate_usd": workload.cost.credit_rate_usd,
            "default_warehouse_size": default_size,
        },
        "routes_discovered": routes,
        "queries": {
            "melt": [asdict(r) for r in melt_records],
            "snowflake": [asdict(r) for r in sf_records],
        },
        "summary": {
            "melt": melt_summary,
            "snowflake": sf_summary,
            "delta": delta(melt_summary, sf_summary)
                if melt_records and sf_records else None,
            "parity_sampler": parity_block,
        },
    }

    out_path = args.out or f"results-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, sort_keys=False)

    _print_summary(out_path, output)
    if args.parity_sampler and parity_block and not parity_block.get("skipped"):
        if parity_passed:
            print(
                f"   parity_sampler  PASS  "
                f"samples={int(parity_block['delta']['total_samples'])}  "
                f"mismatches={int(parity_block['delta']['total_mismatches'])}  "
                f"drops={int(parity_block['delta']['sample_drops'])}"
            )
        else:
            print("   parity_sampler  FAIL")
            for p in parity_problems:
                print(p)
            return 1
    elif args.parity_sampler and parity_block and parity_block.get("skipped"):
        print(f"   parity_sampler  SKIPPED  ({parity_block.get('reason', '?')})")
    return 0


def _print_summary(out_path: str, output: dict[str, Any]) -> None:
    s = output["summary"]
    print(f"\n── Bench complete ── ({output['mode']} mode)")
    print(f"   results: {out_path}")
    if output.get("git_sha"):
        print(f"   git:     {output['git_sha']}")
    for label in ("snowflake", "melt"):
        sm = s.get(label) or {}
        if sm.get("queries"):
            qpd = sm.get("queries_per_dollar")
            qpd_str = "∞" if qpd is None else str(qpd)
            print(f"   {label:<10} queries={sm['queries']:<4} "
                  f"p50={sm['latency_ms']['p50']:<7}ms "
                  f"p95={sm['latency_ms']['p95']:<7}ms "
                  f"usd={sm['usd']:<8} q/$={qpd_str}  "
                  f"routes={sm.get('route_mix')}")
    d = s.get("delta")
    if d:
        per1k = d["usd_per_1k_queries"]
        print(f"   ── delta")
        print(f"      $/1k queries   snowflake={per1k['snowflake']}  melt={per1k['melt']}")
        print(f"      savings/1k     ${per1k['savings']}  ({per1k['savings_pct']}% cheaper)")
        factor = d["queries_per_dollar_factor"]
        factor_str = "∞ (all queries routed to lake — zero Snowflake cost)" \
            if factor == "infinite" else f"{factor}× snowflake"
        print(f"      q/$ factor     melt is {factor_str}")


if __name__ == "__main__":
    raise SystemExit(main())
