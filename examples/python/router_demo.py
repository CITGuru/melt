"""Routing-logic harness for SQL query families.

Loads every `*.sql` file in a variants directory, runs each through:

  1. `melt route <sql>`  — the offline classifier (fast, no execution)
  2. The live Melt proxy  — executes against the real backend
                            (DuckDB lake or Snowflake passthrough)

Prints per-variant verdict + timing + row count, then a comparison
table so you can eyeball which rewrites flip route=snowflake into
route=lake (and how much latency the flip buys you).

Usage:

    cd examples/python
    source .venv/bin/activate
    python router_demo.py --variants ./my_variants
    # or
    python router_demo.py --variants /path/to/dir   # reads /path/to/dir/*.sql
    python router_demo.py --variants /path/to/one.sql

Drop your own `.sql` files into a directory and point `--variants`
at it; each file is one variant and the filename (minus `.sql`)
becomes the label. No special marker required.

Environment:

    MELT_CONFIG           (default: ../../melt.local.toml)
    MELT_BIN              (default: ../../target/release/melt)
    MELT_HOST             (default: 127.0.0.1)
    MELT_PORT             (default: 8443)
    MELT_PROTOCOL         (default: http)
    SNOWFLAKE_ACCOUNT     required
    SNOWFLAKE_USER        required
    SNOWFLAKE_PASSWORD    required (PAT or password; falls back to
                          `pat = "..."` in melt.toml if set)
    SNOWFLAKE_DATABASE    default unset — omit unless queries use
                          relative (unqualified) table refs
    SNOWFLAKE_SCHEMA      default unset
    SNOWFLAKE_WAREHOUSE   default unset — required by Snowflake on
                          any query that scans a table
    SNOWFLAKE_ROLE        default unset — connector picks the
                          account's default role if missing
    REPEATS               default 1   (average of N runs)
    MAX_ROWS              default 50  (cap cursor.fetchmany to avoid OOM on big scans)
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import snowflake.connector

# ──────────────────────────────────────────────────────────────────
# Config loading
# ──────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_dotenv() -> list[Path]:
    """Best-effort .env autoload.

    Searches in this order, stopping at the first hit per file name:
      1. `./.env`                        (cwd, wherever you invoke from)
      2. `examples/python/.env`          (next to this script)
      3. `<repo root>/.env`              (project root)

    Existing process env vars always win — `override=False` — so
    anything the operator exported before invoking us is preserved.
    Returns the list of files actually loaded so we can print them.

    Silently no-ops when python-dotenv isn't installed; the script
    still works if credentials are already in the environment.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return []

    here = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / ".env",
        here / ".env",
        REPO_ROOT / ".env",
    ]
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


@dataclass(frozen=True)
class Settings:
    melt_bin: Path
    melt_config: Path
    host: str
    port: int
    protocol: str
    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: str
    repeats: int
    max_rows: int

    @classmethod
    def load(cls) -> "Settings":
        melt_config = Path(os.environ.get("MELT_CONFIG", REPO_ROOT / "melt.local.toml"))
        melt_bin = Path(os.environ.get("MELT_BIN", REPO_ROOT / "target/release/melt"))
        if not melt_bin.exists():
            sys.exit(
                f"melt binary not found at {melt_bin}. "
                "Build it first with `cargo build --release -p melt-cli` "
                "or set MELT_BIN."
            )
        if not melt_config.exists():
            sys.exit(f"melt config not found at {melt_config}. Set MELT_CONFIG.")

        # Credentials are only required when we actually execute. The
        # classifier path (`--skip-exec`) should work even in an empty
        # shell — that's the whole point of shipping a dry-run mode.
        password = os.environ.get("SNOWFLAKE_PASSWORD") or _read_pat_from_toml(melt_config) or ""

        return cls(
            melt_bin=melt_bin,
            melt_config=melt_config,
            host=os.environ.get("MELT_HOST", "127.0.0.1"),
            port=int(os.environ.get("MELT_PORT", "8443")),
            protocol=os.environ.get("MELT_PROTOCOL", "http"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=password,
            database=os.environ.get("SNOWFLAKE_DATABASE", ""),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", ""),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            role=os.environ.get("SNOWFLAKE_ROLE", ""),
            repeats=int(os.environ.get("REPEATS", "1")),
            max_rows=int(os.environ.get("MAX_ROWS", "50")),
        )

    def require_execution_creds(self) -> None:
        missing = [k for k, v in {
            "SNOWFLAKE_ACCOUNT": self.account,
            "SNOWFLAKE_USER": self.user,
            "SNOWFLAKE_PASSWORD (or `pat = \"…\"` in the melt config)": self.password,
        }.items() if not v]
        if missing:
            sys.exit(
                "execution mode requires credentials. Missing:\n  "
                + "\n  ".join(missing)
                + "\n(or pass --skip-exec for classifier-only mode)"
            )


_PAT_RE = re.compile(r'^\s*pat\s*=\s*"([^"]+)"', re.MULTILINE)


def _read_pat_from_toml(path: Path) -> Optional[str]:
    """Best-effort PAT extraction so local dev doesn't need an env var.
    Deliberately minimal — no full TOML parse, just the one field we need."""
    try:
        m = _PAT_RE.search(path.read_text(encoding="utf-8"))
        return m.group(1) if m else None
    except OSError:
        return None


# ──────────────────────────────────────────────────────────────────
# Variant loading
# ──────────────────────────────────────────────────────────────────


@dataclass
class Variant:
    label: str
    path: Path
    sql: str


def load_variants(target: Path) -> list[Variant]:
    """`target` can be a single .sql file, a directory of .sql files, or
    a glob-ish prefix. We resolve to a list of `Variant`."""
    if target.is_file():
        return [Variant(label=target.stem, path=target, sql=target.read_text(encoding="utf-8"))]

    if target.is_dir():
        files = sorted(target.glob("*.sql"))
        if not files:
            sys.exit(f"no *.sql files found in {target}")
        return [
            Variant(label=f.stem, path=f, sql=f.read_text(encoding="utf-8"))
            for f in files
        ]

    sys.exit(f"variants path does not exist: {target}")


# ──────────────────────────────────────────────────────────────────
# Router classifier — shells out to `melt route`
# ──────────────────────────────────────────────────────────────────


@dataclass
class RouteVerdict:
    route: str                    # "lake" | "snowflake"
    reason: str                   # parsed from `reason: ...` line
    translated_present: bool
    translated_len: int           # char count of translated SQL (0 if none)
    stderr_tail: str              # last lines of stderr if exit code != 0


def classify(s: Settings, sql: str) -> RouteVerdict:
    """Run `melt route <sql>` and parse its stdout into a verdict.

    The classifier is the cheap offline path — it can identify dialect
    translation failures, excluded tables, allowlist misses, and pure
    expression vs. table scans, but it does not know whether a table is
    actually synced into the lake. That deeper check only happens when
    a query flows through the live proxy."""
    try:
        # `--` separator — without it clap parses SQL comments (`-- …`)
        # as CLI flags and bails with a usage error.
        proc = subprocess.run(
            [str(s.melt_bin), "--config", str(s.melt_config), "route", "--", sql],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return RouteVerdict("error", "classifier timeout", False, 0, "")

    stdout = proc.stdout
    route = _extract_line(stdout, r"^route:\s*(\S+)") or "unknown"
    reason = _extract_line(stdout, r"^reason:\s*(.+)$") or ""
    translated_match = re.search(r"^translated:\s*\n(.*?)(?:\n\n|$)",
                                 stdout, re.MULTILINE | re.DOTALL)
    translated_sql = translated_match.group(1).strip() if translated_match else ""

    return RouteVerdict(
        route=route,
        reason=reason,
        translated_present=bool(translated_sql),
        translated_len=len(translated_sql),
        stderr_tail=proc.stderr[-400:] if proc.returncode != 0 else "",
    )


def _extract_line(text: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, text, re.MULTILINE)
    return m.group(1).strip() if m else None


# ──────────────────────────────────────────────────────────────────
# Live execution — through Melt via snowflake-connector-python
# ──────────────────────────────────────────────────────────────────


@dataclass
class ExecResult:
    ok: bool
    elapsed_ms_avg: float
    elapsed_ms_runs: list[float]
    rows_returned: int
    columns: list[str]
    error: str = ""


def execute(conn: Any, s: Settings, sql: str) -> ExecResult:
    """Run `sql` through the already-established Melt connection
    `s.repeats` times and return the fastest + avg timing.

    We fetch at most `s.max_rows` rows per run to keep memory bounded
    when the lake streams a large result set."""
    runs: list[float] = []
    rows_returned = 0
    columns: list[str] = []
    error = ""
    cur = conn.cursor()
    try:
        for i in range(s.repeats):
            start = time.perf_counter()
            try:
                cur.execute(sql)
                rows = cur.fetchmany(s.max_rows)
            except snowflake.connector.errors.ProgrammingError as e:
                error = f"{e.errno}: {e.msg}".strip()
                return ExecResult(False, 0.0, [], 0, [], error=error)
            elapsed = (time.perf_counter() - start) * 1000.0
            runs.append(elapsed)
            if i == 0:
                columns = [d.name for d in cur.description or []]
                rows_returned = len(rows)
    finally:
        cur.close()

    return ExecResult(
        ok=True,
        elapsed_ms_avg=statistics.mean(runs),
        elapsed_ms_runs=runs,
        rows_returned=rows_returned,
        columns=columns,
    )


# ──────────────────────────────────────────────────────────────────
# Output
# ──────────────────────────────────────────────────────────────────


def header(s: str) -> None:
    bar = "━" * (len(s) + 2)
    print(f"\n┏{bar}┓\n┃ {s} ┃\n┗{bar}┛")


def render_variant(variant: Variant, verdict: RouteVerdict, exec_: Optional[ExecResult]) -> None:
    print(f"\n── variant: {variant.label}  ({variant.path})")
    print(f"   classifier: route={verdict.route}  reason={verdict.reason or '—'}")
    if verdict.translated_present:
        print(f"   translated SQL: {verdict.translated_len} chars")
    if verdict.stderr_tail:
        print("   classifier stderr:")
        print(textwrap.indent(verdict.stderr_tail.rstrip(), "     "))

    if exec_ is None:
        print("   execution: skipped")
        return
    if not exec_.ok:
        print(f"   execution: ✗  {exec_.error}")
        return

    runs_str = ", ".join(f"{r:.0f}ms" for r in exec_.elapsed_ms_runs)
    print(f"   execution: ✓  avg={exec_.elapsed_ms_avg:.0f}ms  runs=[{runs_str}]")
    print(f"   rows_returned={exec_.rows_returned}  columns={exec_.columns[:6]}"
          + (" …" if len(exec_.columns) > 6 else ""))


def render_summary(results: list[tuple[Variant, RouteVerdict, Optional[ExecResult]]]) -> None:
    header("Summary")
    w_label = max(len(v.label) for v, _, _ in results)
    print(f"  {'variant':<{w_label}}  {'route':<9}  {'avg ms':>8}  {'rows':>5}  reason")
    print(f"  {'-' * w_label}  {'-' * 9}  {'-' * 8}  {'-' * 5}  {'-' * 40}")
    for v, ver, ex in results:
        route = ver.route
        if ex is None:
            ms = "—"
            rows = "—"
        elif not ex.ok:
            ms = "ERR"
            rows = "—"
        else:
            ms = f"{ex.elapsed_ms_avg:.0f}"
            rows = str(ex.rows_returned)
        reason = (ver.reason[:40]) if ver.reason else "—"
        print(f"  {v.label:<{w_label}}  {route:<9}  {ms:>8}  {rows:>5}  {reason}")


# ──────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], allow_abbrev=False)
    p.add_argument(
        "--variants", type=Path, required=True,
        help="path to a .sql file, or a directory containing *.sql variants",
    )
    p.add_argument(
        "--skip-exec", action="store_true",
        help="classifier-only mode — do not open a live Melt connection",
    )
    p.add_argument(
        "--only", type=str, default=None,
        help="run only variants whose label (filename stem) contains this substring",
    )
    args = p.parse_args()

    s = Settings.load()
    variants = load_variants(args.variants)
    if args.only:
        variants = [v for v in variants if args.only in v.label]
        if not variants:
            sys.exit(f"no variants matched --only={args.only!r}")

    header(
        f"Melt trend-router demo — {len(variants)} variant(s), "
        f"repeats={s.repeats}, max_rows={s.max_rows}"
    )
    if _DOTENV_LOADED:
        for p in _DOTENV_LOADED:
            print(f"  .env    = {p}")
    print(f"  proxy   = {s.protocol}://{s.host}:{s.port}")
    print(f"  config  = {s.melt_config}")
    print(f"  account = {s.account or '—'}  user = {s.user or '—'}  role = {s.role}")

    # Pre-classify everything offline before we touch the network. That
    # way an obviously-broken variant shows up in the summary even if
    # login fails on the live connection.
    classifier_results = [(v, classify(s, v.sql)) for v in variants]

    conn = None
    if not args.skip_exec:
        s.require_execution_creds()
        # Only pass non-empty optional fields — an empty string is
        # NOT the same as "unset" to snowflake-connector-python; it
        # actively sets that session context to empty and breaks any
        # unqualified table ref.
        kwargs = dict(
            host=s.host, port=s.port, protocol=s.protocol, insecure_mode=True,
            account=s.account, user=s.user, password=s.password,
        )
        for key, value in (
            ("database", s.database),
            ("schema", s.schema),
            ("warehouse", s.warehouse),
            ("role", s.role),
        ):
            if value:
                kwargs[key] = value
        print(f"  opening live connection …")
        try:
            conn = snowflake.connector.connect(**kwargs)
        except snowflake.connector.errors.Error as e:
            print(f"\n✗ login failed: {e}")
            print("   continuing in classifier-only mode.")
            conn = None

    final: list[tuple[Variant, RouteVerdict, Optional[ExecResult]]] = []
    for v, verdict in classifier_results:
        exec_ = execute(conn, s, v.sql) if conn else None
        render_variant(v, verdict, exec_)
        final.append((v, verdict, exec_))

    if conn is not None:
        conn.close()

    render_summary(final)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
