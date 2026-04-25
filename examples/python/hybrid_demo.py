"""Regression harness for the dual-execution feature.

Loads every `*.sql` file in `examples/python/variants_hybrid/` (or a
caller-supplied directory), runs each through `melt route`, and asserts
that the verdict matches the `-- @expected.*` annotations embedded in
the file's leading comments.

Designed to detect regressions in:

  - choose_strategy (Attach vs Materialize per RemoteSql node)
  - pushdown_federable_subplans collapse behavior
  - The placement walker's handling of joins, subqueries, CTEs,
    correlated EXISTS, set ops, window functions
  - Guardrails (policy-protected, oversize-fragment, write detection,
    Snowflake-only-feature short-circuit)

Forward-compatible: today (pre-Phase-1) `melt route` only emits
`route=lake | snowflake`. Variants tagged `@expected.requires = phase1`
will fail when the actual route is `snowflake` instead of `hybrid` —
that's the intended behavior. Use `--max-phase phase0` to silence
those expected-to-fail variants while Phase 1 is in flight, then drop
the flag once it ships to surface real regressions.

Usage:

    cd examples/python
    source .venv/bin/activate

    python hybrid_demo.py --variants ./variants_hybrid           # default
    python hybrid_demo.py --variants ./variants_hybrid --strict  # CI mode
    python hybrid_demo.py --variants ./variants_hybrid --only attach
    python hybrid_demo.py --variants ./variants_hybrid --max-phase phase0
    python hybrid_demo.py --variants ./variants_hybrid --execute  # also live-run

Environment (same as router_demo.py):

    MELT_CONFIG, MELT_BIN, MELT_HOST, MELT_PORT, MELT_PROTOCOL,
    SNOWFLAKE_*

This script is intentionally classifier-first; live execution via the
proxy is opt-in (`--execute`) and only used to record per-variant
latency for performance-regression dashboards.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_dotenv() -> list[Path]:
    """Best-effort .env autoload mirroring router_demo.py. Searches
    cwd, the script directory, and the repo root in that order.
    Existing process env vars always win — `override=False`. Returns
    the files that contributed values.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return []

    here = Path(__file__).resolve().parent
    candidates = [Path.cwd() / ".env", here / ".env", REPO_ROOT / ".env"]
    loaded: list[Path] = []
    seen: set[Path] = set()
    for p in candidates:
        try:
            resolved = p.resolve()
        except OSError:
            continue
        if resolved in seen or not resolved.is_file():
            continue
        seen.add(resolved)
        if load_dotenv(dotenv_path=resolved, override=False):
            loaded.append(resolved)
    return loaded


_DOTENV_LOADED = _load_dotenv()

# Phase ordering — variants that require a phase strictly above the
# operator's `--max-phase` are skipped.
_PHASES = ("phase0", "phase1", "phase2")
_PHASE_ORDER = {p: i for i, p in enumerate(_PHASES)}


# ──────────────────────────────────────────────────────────────────
# Variant + expected-metadata loading
# ──────────────────────────────────────────────────────────────────

@dataclass
class Expected:
    route: Optional[str] = None
    reason: Optional[str] = None
    strategy: Optional[str] = None
    attach: Optional[int] = None
    materialize: Optional[int] = None
    fragments: Optional[int] = None
    requires: str = "phase0"
    # True when the assertion only makes sense with a live proxy +
    # backend — e.g. PolicyProtected / AboveThreshold / Bootstrapping
    # reasons require `policy_markers`, `estimate_scan_bytes`, or
    # `DiscoveryCatalog.state_batch`, none of which the offline
    # `melt route` classifier can see. Skipped by default; flip on
    # with `--include-live`.
    requires_live: bool = False
    skip: bool = False
    raw: dict[str, str] = field(default_factory=dict)

    @classmethod
    def parse(cls, sql: str) -> "Expected":
        out = cls()
        # Annotations live in the leading comment block. We stop at
        # the first non-comment, non-blank line so `-- @expected.x`
        # buried in the body of the query (which would be unusual)
        # doesn't get picked up.
        for line in sql.splitlines():
            stripped = line.lstrip()
            if not stripped:
                continue
            if not stripped.startswith("--"):
                break
            m = re.match(r"--\s*@expected\.(\w+)\s*=\s*(.+?)\s*$", stripped)
            if not m:
                continue
            key, value = m.group(1), m.group(2).strip()
            out.raw[key] = value
            if key == "route":
                out.route = value
            elif key == "reason":
                out.reason = value
            elif key == "strategy":
                out.strategy = value if value != "—" else None
            elif key in ("attach", "materialize", "fragments"):
                try:
                    setattr(out, key, int(value))
                except ValueError:
                    pass
            elif key == "requires":
                if value not in _PHASE_ORDER:
                    raise ValueError(
                        f"unknown phase {value!r}; expected one of {_PHASES}"
                    )
                out.requires = value
            elif key == "requires_live":
                out.requires_live = value.lower() in ("true", "1", "yes", "on")
            elif key == "skip":
                out.skip = value.lower() in ("true", "1", "yes", "on")
        return out


@dataclass
class Variant:
    label: str
    path: Path
    sql: str
    expected: Expected


def load_variants(target: Path) -> list[Variant]:
    if target.is_file():
        files = [target]
    elif target.is_dir():
        files = sorted(target.glob("*.sql"))
        if not files:
            sys.exit(f"no *.sql files found in {target}")
    else:
        sys.exit(f"variants path does not exist: {target}")

    variants: list[Variant] = []
    for f in files:
        sql = f.read_text(encoding="utf-8")
        try:
            expected = Expected.parse(sql)
        except ValueError as e:
            sys.exit(f"{f}: bad @expected metadata: {e}")
        variants.append(
            Variant(label=f.stem, path=f, sql=sql, expected=expected)
        )
    return variants


# ──────────────────────────────────────────────────────────────────
# Classifier — invoke `melt route`
# ──────────────────────────────────────────────────────────────────

@dataclass
class Verdict:
    route: str                              # "lake" | "snowflake" | "hybrid"
    reason: str
    strategy: Optional[str]                 # populated only for hybrid
    attach_count: Optional[int]
    materialize_count: Optional[int]
    fragments: Optional[int]
    raw_stdout: str
    stderr_tail: str
    exit_code: int


def classify(melt_bin: Path, melt_config: Path, sql: str, timeout_s: int = 30) -> Verdict:
    try:
        proc = subprocess.run(
            [str(melt_bin), "--config", str(melt_config), "route", "--", sql],
            capture_output=True, text=True, timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return Verdict("error", "classifier timeout", None, None, None, None,
                       "", "", -1)
    return _parse_verdict(proc.stdout, proc.stderr, proc.returncode)


_RE_ROUTE = re.compile(r"^route:\s*(\S+)", re.MULTILINE)
_RE_REASON = re.compile(r"^reason:\s*(.+)$", re.MULTILINE)
_RE_STRATEGY = re.compile(r"^strategy:\s*(\S+)", re.MULTILINE)

# After Phase 1, `melt route` for hybrid emits a plan tree where each
# REMOTE node is annotated [REMOTE,attach] or [REMOTE,materialize].
# We count those tokens to derive attach/materialize counts. This
# format is documented in docs/DUAL_EXECUTION.md §10.3.
_RE_REMOTE_NODE = re.compile(r"\[REMOTE,(attach|materialize)\]")


def _parse_verdict(stdout: str, stderr: str, exit_code: int) -> Verdict:
    route_m = _RE_ROUTE.search(stdout)
    reason_m = _RE_REASON.search(stdout)
    strategy_m = _RE_STRATEGY.search(stdout)

    route = route_m.group(1).strip() if route_m else "unknown"
    reason = reason_m.group(1).strip() if reason_m else ""
    strategy = strategy_m.group(1).strip() if strategy_m else None

    attach_count: Optional[int] = None
    materialize_count: Optional[int] = None
    fragments: Optional[int] = None

    if route == "hybrid":
        node_kinds = _RE_REMOTE_NODE.findall(stdout)
        attach_count = sum(1 for k in node_kinds if k == "attach")
        materialize_count = sum(1 for k in node_kinds if k == "materialize")
        fragments = materialize_count

    return Verdict(
        route=route,
        reason=reason,
        strategy=strategy,
        attach_count=attach_count,
        materialize_count=materialize_count,
        fragments=fragments,
        raw_stdout=stdout,
        stderr_tail=stderr[-400:] if exit_code != 0 else "",
        exit_code=exit_code,
    )


# ──────────────────────────────────────────────────────────────────
# Comparison
# ──────────────────────────────────────────────────────────────────

@dataclass
class Mismatch:
    field: str
    expected: object
    actual: object

    def __str__(self) -> str:
        return f"{self.field}: expected {self.expected!r}, got {self.actual!r}"


def compare(expected: Expected, verdict: Verdict) -> list[Mismatch]:
    mismatches: list[Mismatch] = []

    def _check(field: str, want: object, got: object) -> None:
        if want is None:
            return
        if want != got:
            mismatches.append(Mismatch(field, want, got))

    _check("route", expected.route, verdict.route)

    # Reason: substring match. Reasons can have parameters like
    # "PolicyProtected { table: ..., policy_name: ... }" — we only
    # assert the variant tag.
    if expected.reason is not None:
        if expected.reason.lower() not in verdict.reason.lower():
            mismatches.append(Mismatch("reason", expected.reason, verdict.reason))

    # Hybrid-specific fields only checked when we routed hybrid.
    if expected.route == "hybrid" and verdict.route == "hybrid":
        _check("strategy", expected.strategy, verdict.strategy)
        _check("attach", expected.attach, verdict.attach_count)
        _check("materialize", expected.materialize, verdict.materialize_count)
        _check("fragments", expected.fragments, verdict.fragments)

    return mismatches


# ──────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────

# ANSI color helpers — auto-disabled when stdout isn't a TTY (CI logs
# stay clean). Override with FORCE_COLOR=1 / NO_COLOR=1.
def _color_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return sys.stdout.isatty()

_COLOR = _color_enabled()
def _c(code: str, text: str) -> str:
    if not _COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def render_variant_result(v: Variant, verdict: Verdict, mismatches: list[Mismatch],
                          skipped: bool, skip_reason: str) -> None:
    if skipped:
        print(f"  {_c('33', '⊘')} {v.label:<48}  skipped: {skip_reason}")
        return

    if not mismatches:
        line = f"  {_c('32', '✓')} {v.label:<48}  route={verdict.route}"
        if verdict.route == "hybrid":
            line += (f"  strategy={verdict.strategy or '—'}"
                     f"  attach={verdict.attach_count}"
                     f"  materialize={verdict.materialize_count}")
        print(line)
        return

    print(f"  {_c('31', '✗')} {v.label:<48}  route={verdict.route}")
    for m in mismatches:
        print(f"      · {m}")
    if verdict.exit_code != 0 and verdict.stderr_tail:
        print(f"      · classifier stderr (tail): {verdict.stderr_tail.strip()[:200]}")


def render_summary(passes: int, fails: int, skips: int) -> None:
    total = passes + fails + skips
    print()
    print(f"  total={total}  "
          f"{_c('32', f'pass={passes}')}  "
          f"{_c('31', f'fail={fails}')}  "
          f"{_c('33', f'skip={skips}')}")


# ──────────────────────────────────────────────────────────────────
# Optional: live execution to record latency
# ──────────────────────────────────────────────────────────────────

def execute_one(sql: str, host: str, port: int, protocol: str,
                account: str, user: str, password: str,
                warehouse: str = "", role: str = "",
                database: str = "", schema: str = "",
                timeout_s: int = 60) -> tuple[bool, float, str]:
    """Run `sql` once through the live Melt proxy. Returns
    (ok, elapsed_ms, error_message)."""
    try:
        import snowflake.connector
    except ImportError:
        return False, 0.0, "snowflake-connector-python not installed"

    # Only pass non-empty optional fields. snowflake-connector-python
    # treats empty strings as "set this field to empty" rather than
    # "unset" — passing "" silently breaks unqualified table refs.
    kwargs: dict[str, object] = dict(
        host=host, port=port, protocol=protocol, insecure_mode=True,
        account=account, user=user, password=password,
        login_timeout=timeout_s, network_timeout=timeout_s,
    )
    for key, value in (
        ("warehouse", warehouse),
        ("role", role),
        ("database", database),
        ("schema", schema),
    ):
        if value:
            kwargs[key] = value

    try:
        conn = snowflake.connector.connect(**kwargs)
    except Exception as e:                                       # noqa: BLE001
        return False, 0.0, f"connect: {e}"

    try:
        cur = conn.cursor()
        start = time.perf_counter()
        try:
            cur.execute(sql)
            cur.fetchmany(50)                                    # bound memory
        except Exception as e:                                   # noqa: BLE001
            return False, 0.0, f"execute: {e}"
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return True, elapsed_ms, ""
    finally:
        try:
            cur.close()
        finally:
            conn.close()


# ──────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 allow_abbrev=False)
    p.add_argument("--variants", type=Path, required=True,
                   help="path to a .sql file or directory")
    p.add_argument("--only", type=str, default=None,
                   help="run only variants whose label contains this substring")
    p.add_argument("--max-phase", choices=_PHASES, default="phase2",
                   help="skip variants requiring a phase above this (default: all)")
    p.add_argument("--strict", action="store_true",
                   help="exit non-zero on any mismatch (CI mode)")
    p.add_argument("--include-live", action="store_true",
                   help="also run variants tagged `@expected.requires_live` — "
                        "needs a live proxy connection that sees "
                        "policy_markers / sync state / real size estimates")
    p.add_argument("--execute", action="store_true",
                   help="also run each variant live for latency measurement")
    args = p.parse_args()

    melt_config = Path(os.environ.get("MELT_CONFIG", REPO_ROOT / "melt.local.toml"))
    melt_bin = Path(os.environ.get("MELT_BIN", REPO_ROOT / "target/release/melt"))
    if not melt_bin.exists():
        sys.exit(f"melt binary not found at {melt_bin}; build it first or set MELT_BIN")
    if not melt_config.exists():
        sys.exit(f"melt config not found at {melt_config}; set MELT_CONFIG")

    variants = load_variants(args.variants)
    if args.only:
        variants = [v for v in variants if args.only in v.label]
        if not variants:
            sys.exit(f"no variants matched --only={args.only!r}")

    max_phase_ord = _PHASE_ORDER[args.max_phase]
    print(f"  config = {melt_config}")
    print(f"  bin    = {melt_bin}")
    print(f"  phase  = ≤ {args.max_phase}")
    print(f"  count  = {len(variants)} variants")
    print()

    passes = fails = skips = 0
    for v in variants:
        if v.expected.skip:
            render_variant_result(v, Verdict("skip", "", None, None, None, None, "", "", 0),
                                  [], skipped=True, skip_reason="@expected.skip = true")
            skips += 1
            continue

        if _PHASE_ORDER[v.expected.requires] > max_phase_ord:
            render_variant_result(v, Verdict("skip", "", None, None, None, None, "", "", 0),
                                  [], skipped=True,
                                  skip_reason=f"requires {v.expected.requires} > {args.max_phase}")
            skips += 1
            continue

        if v.expected.requires_live and not args.include_live:
            render_variant_result(v, Verdict("skip", "", None, None, None, None, "", "", 0),
                                  [], skipped=True,
                                  skip_reason="requires_live (pass --include-live)")
            skips += 1
            continue

        verdict = classify(melt_bin, melt_config, v.sql)
        mismatches = compare(v.expected, verdict)
        render_variant_result(v, verdict, mismatches, skipped=False, skip_reason="")
        if mismatches:
            fails += 1
        else:
            passes += 1

        if args.execute and not mismatches:
            ok, ms, err = execute_one(
                v.sql,
                host=os.environ.get("MELT_HOST", "127.0.0.1"),
                port=int(os.environ.get("MELT_PORT", "8443")),
                protocol=os.environ.get("MELT_PROTOCOL", "http"),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                user=os.environ.get("SNOWFLAKE_USER", ""),
                password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
                role=os.environ.get("SNOWFLAKE_ROLE", ""),
                database=os.environ.get("SNOWFLAKE_DATABASE", ""),
                schema=os.environ.get("SNOWFLAKE_SCHEMA", ""),
            )
            if ok:
                print(f"      live: {ms:.0f}ms")
            else:
                print(f"      live: ERR  {err[:200]}")

    render_summary(passes, fails, skips)

    if args.strict and fails > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
