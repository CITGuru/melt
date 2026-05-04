#!/usr/bin/env python3
"""Melt agent-cost demo — `make demo`.

Runs the same 200-query agent-shaped workload twice — once forced through
Snowflake passthrough (the world without Melt), once through Melt's router —
and prints a single before/after $/query table.

Default mode is `synthetic`: no Snowflake account, no proxy required. Routes
are taken from each query template's `expected_route` (cross-checked against
`melt route` if the binary is on PATH), and latencies are sampled from the
`[latency]` block of `agent_workload.toml`. This gives a deterministic,
asciinema-friendly run that reproduces a representative number on a developer
laptop in well under two minutes.

Once POWA-92 (`melt sessions seed`) lands, swap `--mode seed` to make the
"melt" path actually open a Snowflake-driver-compatible session against the
seeded proxy and time real round-trips. Cost numbers do not change shape — the
cost model is the same.

Stdlib only (tomllib + random + statistics + subprocess). No pip install
required for the demo path.
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import statistics
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_WORKLOAD = HERE / "agent_workload.toml"
SECONDS_PER_HOUR = 3600.0

# ─── Cost model ─────────────────────────────────────────────────────────────

@dataclass
class CostModel:
    credit_rate_usd: float
    warehouse_credits_per_hour: dict
    default_warehouse_size: str
    lake_snowflake_credits_per_query: float

    def usd_for_route(self, route: str, latency_ms: float) -> float:
        if route == "lake":
            return self.lake_snowflake_credits_per_query * self.credit_rate_usd
        size = self.default_warehouse_size.upper()
        per_hour = self.warehouse_credits_per_hour[size]
        credits = (latency_ms / 1000.0 / SECONDS_PER_HOUR) * per_hour
        return credits * self.credit_rate_usd

# ─── Workload ───────────────────────────────────────────────────────────────

@dataclass
class QueryTemplate:
    name: str
    shape: str          # small_filter | small_agg | multi_join
    expected_route: str # lake | snowflake
    weight: int         # relative weight inside its shape
    sql_template: str

@dataclass
class Workload:
    total: int
    seed: int
    mix: dict
    cost: CostModel
    latency: dict
    templates: list

    @classmethod
    def load(cls, path: Path) -> "Workload":
        with open(path, "rb") as f:
            doc = tomllib.load(f)
        cost_block = doc["cost"]
        return cls(
            total=int(doc["demo"]["total_queries"]),
            seed=int(doc["demo"]["seed"]),
            mix=dict(doc["mix"]),
            cost=CostModel(
                credit_rate_usd=float(cost_block["credit_rate_usd"]),
                warehouse_credits_per_hour={
                    k: float(v) for k, v in cost_block["warehouse_credits_per_hour"].items()
                },
                default_warehouse_size=str(cost_block["default_warehouse_size"]),
                lake_snowflake_credits_per_query=float(
                    cost_block["lake_snowflake_credits_per_query"]
                ),
            ),
            latency=dict(doc["latency"]),
            templates=[
                QueryTemplate(
                    name=q["name"],
                    shape=q["shape"],
                    expected_route=q["expected_route"],
                    weight=int(q.get("weight", 1)),
                    sql_template=q["sql"].strip(),
                )
                for q in doc["query"]
            ],
        )

# ─── Query generator ────────────────────────────────────────────────────────

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"]
EVENT_TYPES = ["click", "view", "purchase", "signup", "search"]

def render(template: QueryTemplate, rng: random.Random) -> str:
    return template.sql_template.format(
        user_id=rng.randint(1, 10_000),
        session_id=f"s{rng.randint(1, 1_000_000):07d}",
        event_type=rng.choice(EVENT_TYPES),
        days=rng.randint(1, 30),
        region=rng.choice(REGIONS),
    )

def build_workload(wl: Workload) -> list:
    """Return a deterministic ordered list of (template, sql) tuples summing to
    wl.total entries with the exact mix proportions from [mix]. Within each
    shape, templates are sampled by weight so the per-template
    lake/passthrough split controls routable %."""
    rng = random.Random(wl.seed)
    by_shape = {s: [t for t in wl.templates if t.shape == s] for s in wl.mix}
    bucket = []
    for shape, count in wl.mix.items():
        choices = by_shape[shape]
        if not choices:
            sys.exit(f"workload references shape={shape!r} but no templates match")
        weights = [t.weight for t in choices]
        for _ in range(count):
            t = rng.choices(choices, weights=weights, k=1)[0]
            bucket.append((t, render(t, rng)))
    if len(bucket) != wl.total:
        sys.exit(f"mix sums to {len(bucket)}, expected {wl.total}")
    rng.shuffle(bucket)
    return bucket

# ─── Routing ────────────────────────────────────────────────────────────────

def discover_route_fn(use_melt_route: bool):
    """Return fn(sql, expected) -> 'lake'|'snowflake'.

    If `melt route` is on PATH, use it for live route prediction (catches
    router regressions in CI). Otherwise fall back to the template's
    expected_route — the demo still runs end-to-end with no Melt build.
    """
    melt_bin = shutil.which("melt") if use_melt_route else None
    if not melt_bin:
        return lambda sql, expected: expected

    def via_cli(sql: str, expected: str) -> str:
        try:
            r = subprocess.run(
                [melt_bin, "route", sql],
                capture_output=True, text=True, timeout=5,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return expected
        if r.returncode != 0:
            return expected
        # `melt route` prints "route: lake" on the first line.
        for line in r.stdout.splitlines():
            line = line.strip().lower()
            if line.startswith("route:"):
                tag = line.split(":", 1)[1].strip()
                if tag in {"lake", "snowflake", "passthrough", "dual"}:
                    return "lake" if tag == "lake" else "snowflake"
        return expected
    return via_cli

# ─── Latency sampler ────────────────────────────────────────────────────────

def sample_latency(rng: random.Random, route: str, lat: dict) -> float:
    if route == "lake":
        p50, p95 = float(lat["lake_p50_ms"]), float(lat["lake_p95_ms"])
    else:
        p50, p95 = float(lat["snowflake_p50_ms"]), float(lat["snowflake_p95_ms"])
    # Two-knot piecewise: 95% draws below p95, 5% in a heavier tail. Good
    # enough for a demo; not a benchmark.
    u = rng.random()
    if u < 0.5:
        return rng.uniform(p50 * 0.5, p50)
    if u < 0.95:
        return rng.uniform(p50, p95)
    return rng.uniform(p95, p95 * 1.8)

# ─── Run a mode ─────────────────────────────────────────────────────────────

@dataclass
class Result:
    route: str
    latency_ms: float
    usd: float
    expected_route: str

@dataclass
class ModeSummary:
    name: str
    n: int
    p50_ms: float
    p95_ms: float
    total_usd: float
    routes: dict = field(default_factory=dict)

def run_mode(name: str, wl: Workload, queries, mode: str, route_fn) -> ModeSummary:
    rng = random.Random(wl.seed + (1 if mode == "passthrough" else 2))
    results = []
    for tmpl, sql in queries:
        if mode == "passthrough":
            route = "snowflake"
        else:
            route = route_fn(sql, tmpl.expected_route)
        latency = sample_latency(rng, route, wl.latency)
        usd = wl.cost.usd_for_route(route, latency)
        results.append(Result(route=route, latency_ms=latency, usd=usd,
                              expected_route=tmpl.expected_route))

    lats = [r.latency_ms for r in results]
    routes = {}
    for r in results:
        routes[r.route] = routes.get(r.route, 0) + 1
    return ModeSummary(
        name=name,
        n=len(results),
        p50_ms=statistics.median(lats),
        p95_ms=quantile(lats, 0.95),
        total_usd=sum(r.usd for r in results),
        routes=routes,
    )

def quantile(xs, q: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[k]

# ─── Output table ───────────────────────────────────────────────────────────

# 80-column friendly. ANSI only when stdout is a TTY (asciinema-safe).
def colour(s: str, code: str) -> str:
    if not sys.stdout.isatty():
        return s
    return f"\033[{code}m{s}\033[0m"

BOLD = "1"
GREEN = "32"
DIM = "2"
RED = "31"

def render_table(passthrough: ModeSummary, melt: ModeSummary, wl: Workload) -> str:
    lake = melt.routes.get("lake", 0)
    routable = (lake / melt.n) * 100 if melt.n else 0.0
    saved = passthrough.total_usd - melt.total_usd
    pct = (saved / passthrough.total_usd) * 100 if passthrough.total_usd > 0 else 0.0
    factor = (passthrough.total_usd / melt.total_usd) if melt.total_usd > 0 else float("inf")

    width = 78
    bar = "─" * width
    out = []
    out.append(colour(bar, DIM))
    out.append(colour("  Melt agent-cost demo", BOLD)
               + colour(f"   ({wl.total} queries · seed={wl.seed} · synthetic)", DIM))
    out.append(colour(bar, DIM))
    out.append("")
    header = f"  {'mode':<14}{'queries':>10}{'p50 ms':>10}{'p95 ms':>10}{'total $':>14}{'$/1k':>14}"
    out.append(colour(header, BOLD))
    out.append("  " + "─" * (width - 2))
    for s in (passthrough, melt):
        per_k = (s.total_usd / s.n) * 1000 if s.n else 0.0
        out.append(
            f"  {s.name:<14}{s.n:>10}{s.p50_ms:>10.1f}{s.p95_ms:>10.1f}"
            f"{s.total_usd:>14.4f}{per_k:>14.4f}"
        )
    out.append("")
    out.append(colour("  routing breakdown (melt mode)", BOLD))
    for route, n in sorted(melt.routes.items(), key=lambda x: -x[1]):
        out.append(f"    {route:<10} {n:>4}  ({(n/melt.n)*100:5.1f}%)")
    out.append(f"    routable: {colour(f'{routable:.1f}%', GREEN)}")
    out.append("")
    out.append(colour("  delta", BOLD))
    out.append(f"    saved per run         {colour(f'${saved:.4f}', GREEN)}  "
               f"({pct:.1f}% cheaper)")
    out.append(f"    melt is               {colour(f'{factor:.2f}×', GREEN)} "
               f"cheaper than passthrough")
    out.append("")
    out.append(colour("  cost model", DIM))
    out.append(colour(
        f"    ${wl.cost.credit_rate_usd:.2f}/credit · "
        f"warehouse={wl.cost.default_warehouse_size} · "
        f"lake credits/query={wl.cost.lake_snowflake_credits_per_query} · "
        f"Snowflake list price",
        DIM,
    ))
    out.append(colour(bar, DIM))
    return "\n".join(out)

# ─── Main ───────────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Run the Melt agent-cost demo and print a $/query table."
    )
    p.add_argument("--workload", type=Path, default=DEFAULT_WORKLOAD,
                   help="path to agent_workload.toml")
    p.add_argument("--mode", choices=["synthetic", "seed"], default="synthetic",
                   help="synthetic: no proxy/no creds; seed: hit local seed-mode "
                        "proxy from POWA-92 (not yet wired)")
    p.add_argument("--with-melt-route", action="store_true",
                   help="cross-check each query against `melt route` (slower, "
                        "captures router improvements/regressions; off by "
                        "default for cross-machine reproducibility)")
    args = p.parse_args(argv)

    wl = Workload.load(args.workload)
    queries = build_workload(wl)
    route_fn = discover_route_fn(use_melt_route=args.with_melt_route)

    if args.mode == "seed":
        # Wired up by POWA-92 (sessions seed). Until that lands, fall through
        # to synthetic so the demo still runs end-to-end.
        print("(seed mode requires POWA-92; falling back to synthetic)",
              file=sys.stderr)

    passthrough = run_mode("passthrough", wl, queries, mode="passthrough",
                           route_fn=route_fn)
    melt = run_mode("melt", wl, queries, mode="melt", route_fn=route_fn)

    print(render_table(passthrough, melt, wl))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
