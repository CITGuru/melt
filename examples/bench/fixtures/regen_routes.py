#!/usr/bin/env python3
"""Regenerate `examples/bench/fixtures/routes.json` from `workload.toml`.

Calls `melt route "<sql>"` once per `[[query]]` entry and writes the
captured `Route`, `Reason`, `decided_by_strategy`, and `strategy_chain`
to `routes.json`. Output is deterministic — keys are sorted, queries
are emitted in workload-file order, and trailing whitespace is stripped
from every captured field.

Use the bundled `examples/bench/fixtures/melt.bench.toml` so the regen
isn't sensitive to whatever live `melt.toml` happens to be in the repo
root. The bundled config only needs to parse; `melt route`
short-circuits before any backend connection is opened.

Usage:
    python3 examples/bench/fixtures/regen_routes.py
    make routes-fixture          # equivalent, runs from `examples/bench/`

Anchored to POWA-163 / launch-checklist §A6.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import tomllib  # py311+
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]


HERE = Path(__file__).resolve().parent
BENCH_DIR = HERE.parent
REPO_ROOT = BENCH_DIR.parent.parent
WORKLOAD_PATH = BENCH_DIR / "workload.toml"
ROUTES_PATH = HERE / "routes.json"
CONFIG_PATH = HERE / "melt.bench.toml"


def find_melt_bin() -> list[str]:
    """Resolve the `melt` invocation. PATH first, then a debug/release build."""
    if env_bin := os.environ.get("MELT_BIN"):
        return [env_bin]
    if which := shutil.which("melt"):
        return [which]
    for candidate in ("release", "debug"):
        built = REPO_ROOT / "target" / candidate / "melt"
        if built.exists():
            return [str(built)]
    sys.exit(
        "could not find a `melt` binary — install it (cargo install --path "
        "crates/melt-cli) or build it (cargo build -p melt-cli) and re-run, "
        "or set MELT_BIN=<path>"
    )


def run_melt_route(melt_cmd: list[str], sql: str) -> str:
    cmd = melt_cmd + ["--config", str(CONFIG_PATH), "route", sql]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        sys.exit(
            f"`melt route` failed for SQL {sql!r}\n"
            f"stderr:\n{result.stderr}\nstdout:\n{result.stdout}"
        )
    return result.stdout


def parse_route_output(text: str) -> dict[str, Any]:
    """Extract the four fixture fields from a `melt route` text block.

    Mirrors the format emitted by `format_lazy_route` in
    `crates/melt-cli/src/runtime.rs`:

        route: <route>
        reason: <reason>
        strategy_chain: [<chain>]   chain_decided_by: <decided>   # hybrid only
        ...

    Non-hybrid routes never emit a `strategy_chain` line, so the
    fixture defaults to `null` / `[]` for those fields.
    """
    route: str | None = None
    reason: str | None = None
    decided_by_strategy: str | None = None
    strategy_chain: list[str] = []

    for raw in text.splitlines():
        line = raw.rstrip()
        if line.startswith("route: ") and route is None:
            route = line[len("route: "):].strip()
        elif line.startswith("reason: ") and reason is None:
            reason = line[len("reason: "):].strip()
        elif line.startswith("strategy_chain: ["):
            inner = line[len("strategy_chain: ["):]
            close = inner.index("]")
            chain_str = inner[:close]
            strategy_chain = (
                [s.strip() for s in chain_str.split(",") if s.strip()]
                if chain_str.strip()
                else []
            )
            tail = inner[close + 1:]
            marker = "chain_decided_by:"
            if marker in tail:
                decided_by_strategy = tail.split(marker, 1)[1].strip() or None

    if route is None or reason is None:
        sys.exit(f"could not parse `melt route` output:\n{text}")

    return {
        "route": route,
        "reason": reason,
        "decided_by_strategy": decided_by_strategy,
        "strategy_chain": strategy_chain,
    }


def load_workload() -> list[dict[str, Any]]:
    with WORKLOAD_PATH.open("rb") as f:
        data = tomllib.load(f)
    queries = data.get("query", [])
    if not queries:
        sys.exit(f"no [[query]] entries in {WORKLOAD_PATH}")
    return queries


def main() -> int:
    melt_cmd = find_melt_bin()
    queries = load_workload()

    out: list[dict[str, Any]] = []
    for q in queries:
        sql = (q.get("sql") or "").strip()
        if not sql:
            sys.exit(f"workload entry {q.get('name')!r} has no `sql`")
        text = run_melt_route(melt_cmd, sql)
        parsed = parse_route_output(text)
        out.append(
            {
                "name": q["name"],
                "sql": sql,
                **parsed,
            }
        )

    payload = {"version": 1, "queries": out}
    body = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    ROUTES_PATH.write_text(body, encoding="utf-8")
    print(f"wrote {ROUTES_PATH.relative_to(REPO_ROOT)}  ({len(out)} queries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
