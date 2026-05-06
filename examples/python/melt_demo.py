"""Demo: query a local Melt proxy using `snowflake-connector-python`.

The connector is the unmodified upstream package — only `host`, `port`,
and `protocol` differ from a normal Snowflake connection. Each query
exercises a different routing path so you can watch Melt's logs and
confirm the proxy is making the decisions you expect.

Two modes:

* `MELT_MODE=seed` — credential-free demo against a local TPC-H sf=0.01
  fixture. No Snowflake account required. Uses canned creds that
  match what `melt sessions seed` writes into `melt.demo.toml`.
* default (real mode) — forwards login to upstream Snowflake. Set
  SNOWFLAKE_USER / SNOWFLAKE_ACCOUNT / one of SNOWFLAKE_PASSWORD or
  SNOWFLAKE_PRIVATE_KEY[_FILE] to authenticate.

Run (seed mode):

    cargo run -p melt-cli -- sessions seed
    cargo run -p melt-cli -- --config melt.demo.toml all  # in another terminal
    pip install -r requirements.txt
    MELT_MODE=seed python melt_demo.py

Run (real mode):

    pip install -r requirements.txt
    export SNOWFLAKE_USER=...  SNOWFLAKE_ACCOUNT=...
    export SNOWFLAKE_PASSWORD=...                         # PAT or password
    # OR for service-account key-pair auth:
    export SNOWFLAKE_PRIVATE_KEY="$(cat ~/.melt/sf.p8)"
    python melt_demo.py

In another terminal:

    docker compose logs -f melt | grep statement_complete
"""

from __future__ import annotations

import logging
import os
import sys
import textwrap
from contextlib import closing
from dataclasses import dataclass
from typing import Optional

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger("melt_demo")


@dataclass(frozen=True)
class Settings:
    melt_host: str
    melt_port: int
    melt_protocol: str
    account: str
    user: str
    password: Optional[str]
    private_key_pem: Optional[str]
    database: str
    schema: str
    warehouse: Optional[str]
    role: Optional[str]

    @classmethod
    def from_env(cls) -> "Settings":
        # Defaults match the local docker compose setup (see
        # docker/melt.docker.toml). Override any of them with env vars.
        # In seed mode (`MELT_MODE=seed`) every Snowflake-* default
        # comes from the canned demo creds that `melt sessions seed`
        # writes — matching what `melt-core::config::SEED_*` defines.
        if os.environ.get("MELT_MODE", "").lower() == "seed":
            return cls(
                melt_host=os.environ.get("MELT_HOST", "127.0.0.1"),
                melt_port=int(os.environ.get("MELT_PORT", "8443")),
                melt_protocol=os.environ.get("MELT_PROTOCOL", "http"),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", "melt-demo"),
                user=os.environ.get("SNOWFLAKE_USER", "demo"),
                password=os.environ.get("SNOWFLAKE_PASSWORD", "demo"),
                private_key_pem=None,
                database=os.environ.get("SNOWFLAKE_DATABASE", "TPCH"),
                schema=os.environ.get("SNOWFLAKE_SCHEMA", "SF01"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "MELT_DEMO_WH"),
                role=os.environ.get("SNOWFLAKE_ROLE", "PUBLIC"),
            )
        try:
            return cls(
                melt_host=os.environ.get("MELT_HOST", "127.0.0.1"),
                melt_port=int(os.environ.get("MELT_PORT", "8443")),
                melt_protocol=os.environ.get("MELT_PROTOCOL", "http"),
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                user=os.environ["SNOWFLAKE_USER"],
                password=os.environ.get("SNOWFLAKE_PASSWORD") or None,
                private_key_pem=_read_private_key_pem(),
                database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
                schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or None,
                role=os.environ.get("SNOWFLAKE_ROLE") or None,
            )
        except KeyError as missing:
            sys.exit(
                f"missing required env var: {missing.args[0]}\n"
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and one of "
                "SNOWFLAKE_PRIVATE_KEY / SNOWFLAKE_PRIVATE_KEY_FILE / "
                "SNOWFLAKE_PASSWORD. Or use `MELT_MODE=seed` for the "
                "credential-free demo path."
            )


def _read_private_key_pem() -> Optional[str]:
    """Source the PEM string from SNOWFLAKE_PRIVATE_KEY_FILE (path) or
    SNOWFLAKE_PRIVATE_KEY (inline). Returns None if neither is set."""
    if path := os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        with open(path, "r") as f:
            return f.read()
    return os.environ.get("SNOWFLAKE_PRIVATE_KEY") or None


def _load_private_key(pem_str: Optional[str]) -> Optional[bytes]:
    """Convert a PEM-encoded RSA private key string into the DER bytes
    snowflake-connector-python's `private_key=` kwarg expects.
    Returns None if `pem_str` is missing or doesn't parse as PEM —
    callers fall back to password auth in that case."""
    if not pem_str:
        return None
    try:
        pem_bytes = pem_str.encode("utf-8") if isinstance(pem_str, str) else pem_str
        private_key = serialization.load_pem_private_key(
            pem_bytes, password=None, backend=default_backend()
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except Exception as e:
        logger.warning("Failed to load Snowflake private key: %s", e)
        return None


def _connect_kwargs(s: Settings) -> dict:
    """Assemble snowflake.connector.connect() kwargs.

    The melt-specific bits — `host`, `port`, `protocol` — point the
    connector at the local proxy instead of <account>.snowflakecomputing.com.
    Everything else mirrors a normal Snowflake connection.
    """
    base = dict(
        user=s.user,
        account=s.account,
        host=s.melt_host,
        port=s.melt_port,
        protocol=s.melt_protocol,
        database=s.database,
        schema=s.schema,
        insecure_mode=True,      # plain HTTP for local dev only
    )
    if s.warehouse:
        base["warehouse"] = s.warehouse
    if s.role:
        base["role"] = s.role

    # Prefer password / PAT auth (the simpler path). Fall back to RSA
    # key-pair when SNOWFLAKE_PASSWORD is missing and a usable PEM is
    # available.
    if s.password:
        base["password"] = s.password
    else:
        pkb = _load_private_key(s.private_key_pem)
        if pkb is not None:
            base["private_key"] = pkb
        else:
            sys.exit(
                "no usable credentials: set SNOWFLAKE_PASSWORD, or provide a "
                "PEM-encoded RSA key in SNOWFLAKE_PRIVATE_KEY / "
                "SNOWFLAKE_PRIVATE_KEY_FILE."
            )
    return base


# Each entry: (label, sql, expected_route, why)
REAL_QUERIES: list[tuple[str, str, str, str]] = [
    (
        "pure expression",
        "SELECT 1 + 1 AS answer",
        "lake",
        "no tables → router routes to lake (DuckDB computes locally)",
    ),
    (
        "translated SELECT",
        "SELECT IFF(value > 0, 'p', 'n') AS sign, "
        "       DATEADD(day, 7, ts)        AS week_later "
        "FROM analytics.public.events LIMIT 5",
        "lake (if table exists in lake) | snowflake (if not)",
        "IFF → CASE WHEN, DATEADD(day, ...) → DATEADD('day', ...)",
    ),
    (
        "write statement",
        "INSERT INTO analytics.public.events (id, value, ts) "
        "VALUES (DEFAULT, 1, CURRENT_TIMESTAMP())",
        "snowflake",
        "writes always passthrough (router classifies INSERT as a write)",
    ),
    (
        "snowflake-only feature",
        "SELECT table_schema, table_name "
        "FROM information_schema.tables LIMIT 5",
        "snowflake",
        "INFORMATION_SCHEMA references trigger UsesSnowflakeFeature passthrough",
    ),
]

# Seed-mode queries are tuned to the TPC-H sf=0.01 fixture that
# `melt sessions seed` provisions. The acceptance criterion calls
# for at least three queries that route to lake; we exceed that
# (3 SELECT shapes) and include one explicit "this requires upstream"
# query that returns SeedModeUnsupported (HTTP 422) so operators see
# the boundary.
SEED_QUERIES: list[tuple[str, str, str, str]] = [
    (
        "row count",
        "SELECT COUNT(*) AS n FROM TPCH.SF01.lineitem",
        "lake",
        "fully-qualified Lake table; router strips DB prefix to local schema",
    ),
    (
        "small projection",
        "SELECT n_nationkey, n_name FROM TPCH.SF01.nation ORDER BY n_nationkey LIMIT 5",
        "lake",
        "tiny TPC-H reference table — pulled from the local DuckDB fixture",
    ),
    (
        "aggregate",
        "SELECT o_orderstatus, COUNT(*) AS n FROM TPCH.SF01.orders GROUP BY 1",
        "lake",
        "GROUP BY against the orders fact table",
    ),
    (
        "seed boundary",
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES",
        "seed-mode-unsupported (HTTP 422)",
        "INFORMATION_SCHEMA would route to upstream — seed mode refuses cleanly",
    ),
]


def header(s: str) -> None:
    bar = "─" * (len(s) + 2)
    print(f"\n┌{bar}┐\n│ {s} │\n└{bar}┘")


def run_one(cur, label: str, sql: str, expected: str, why: str) -> None:
    print(f"\n┄┄ {label} ({expected})")
    print(textwrap.indent(textwrap.fill(why, width=78), "   ─ "))
    print(textwrap.indent(sql.strip(), "   > "))
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        print(f"   ← {len(rows)} row(s)")
        for r in rows[:5]:
            print(f"     {r}")
    except snowflake.connector.errors.ProgrammingError as e:
        # Lake or Snowflake errors come back here. With a placeholder
        # account the login itself fails before any statement runs.
        print(f"   ✗ ProgrammingError {e.errno}: {e.msg}")


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    s = Settings.from_env()

    header(
        f"Connecting to Melt at {s.melt_protocol}://{s.melt_host}:{s.melt_port} "
        f"as {s.user}@{s.account}"
    )

    kwargs = _connect_kwargs(s)
    print(kwargs)
    auth_kind = "key-pair" if "private_key" in kwargs else "password"
    print(f"  ↳ auth flavor: {auth_kind}")

    try:
        conn = snowflake.connector.connect(**kwargs)
    except snowflake.connector.errors.OperationalError as e:
        print(f"\n✗ login failed: {e}")
        print(
            "\n  This usually means Melt forwarded the login to the configured\n"
            "  Snowflake account and that account isn't reachable. Set\n"
            "  SNOWFLAKE_ACCOUNT to a real account locator, or use\n"
            "  `melt route '<sql>'` for offline routing tests."
        )
        return 1
    except snowflake.connector.errors.DatabaseError as e:
        print(f"\n✗ login failed: {e}")
        print("\n  Snowflake rejected the credential. Check the user, role, and\n"
              "  whether the public key is registered (key-pair) or the password\n"
              "  is correct.")
        return 1

    queries = (
        SEED_QUERIES
        if os.environ.get("MELT_MODE", "").lower() == "seed"
        else REAL_QUERIES
    )
    with closing(conn) as conn, closing(conn.cursor()) as cur:
        for label, sql, expected, why in queries:
            run_one(cur, label, sql, expected, why)

    header("Done")
    print("Tail Melt's logs to see the routing decisions:")
    print("  docker compose logs -f melt | grep statement_complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
