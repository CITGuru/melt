"""Seed the Melt bench dataset into Snowflake.

Creates `ANALYTICS.PUBLIC.EVENTS` (~50k rows) and `ANALYTICS.PUBLIC.USERS`
(~50k rows) shaped to satisfy the queries in `examples/bench/workload.toml`.

Idempotent: drops + recreates each table so reruns produce identical sizes.

Reads creds from `melt/.env`. Run from anywhere:

    python3 scripts/bench-seed/seed_bench.py
"""

from __future__ import annotations

import csv
import io
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import snowflake.connector

ROW_COUNT = 50_000
EVENT_DAYS = 30
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def gen_users(n: int, seed: int = 7) -> list[tuple[int, str, str]]:
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        name = f"user_{i:06d}"
        # 35% us-east-1 so the selective_join in the workload returns a
        # meaningful but not-everything slice (~17.5k rows).
        region = "us-east-1" if rng.random() < 0.35 else rng.choice(REGIONS[1:])
        rows.append((i, name, region))
    return rows


def gen_events(n: int, max_user_id: int, seed: int = 8) -> list[tuple[int, int, float, datetime]]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(1, n + 1):
        user_id = rng.randint(1, max_user_id)
        value = round(rng.uniform(0, 1000), 4)
        ts = now - timedelta(seconds=rng.randint(0, EVENT_DAYS * 86400))
        rows.append((i, user_id, value, ts))
    return rows


def to_csv(rows, header) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_env(repo_root / ".env")

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"missing env: {missing}", file=sys.stderr)
        return 2

    print(f"connecting to {os.environ['SNOWFLAKE_ACCOUNT']} as {os.environ['SNOWFLAKE_USER']}...")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    cur = conn.cursor()

    cur.execute("CREATE DATABASE IF NOT EXISTS ANALYTICS")
    cur.execute("USE DATABASE ANALYTICS")
    cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
    cur.execute("USE SCHEMA PUBLIC")

    cur.execute("DROP TABLE IF EXISTS ANALYTICS.PUBLIC.EVENTS")
    cur.execute("DROP TABLE IF EXISTS ANALYTICS.PUBLIC.USERS")
    cur.execute("""
        CREATE TABLE ANALYTICS.PUBLIC.USERS (
            ID NUMBER NOT NULL PRIMARY KEY,
            NAME VARCHAR NOT NULL,
            REGION VARCHAR NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE ANALYTICS.PUBLIC.EVENTS (
            ID NUMBER NOT NULL PRIMARY KEY,
            USER_ID NUMBER NOT NULL,
            VALUE FLOAT NOT NULL,
            TS TIMESTAMP_NTZ NOT NULL
        )
    """)

    print(f"generating {ROW_COUNT:,} users...")
    users = gen_users(ROW_COUNT)
    print(f"generating {ROW_COUNT:,} events...")
    events = gen_events(ROW_COUNT, ROW_COUNT)

    print("uploading users via PUT + COPY INTO...")
    users_csv = to_csv(users, ["ID", "NAME", "REGION"])
    upload_via_stage(cur, "USERS", users_csv, ["ID NUMBER", "NAME VARCHAR", "REGION VARCHAR"])

    print("uploading events via PUT + COPY INTO...")
    events_csv = to_csv(
        [(i, u, v, ts.strftime("%Y-%m-%d %H:%M:%S")) for (i, u, v, ts) in events],
        ["ID", "USER_ID", "VALUE", "TS"],
    )
    upload_via_stage(cur, "EVENTS", events_csv,
                     ["ID NUMBER", "USER_ID NUMBER", "VALUE FLOAT", "TS TIMESTAMP_NTZ"])

    cur.execute("SELECT COUNT(*) FROM ANALYTICS.PUBLIC.USERS")
    u_n = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ANALYTICS.PUBLIC.EVENTS")
    e_n = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ANALYTICS.PUBLIC.USERS WHERE REGION = 'us-east-1'")
    east = cur.fetchone()[0]
    print(f"users={u_n:,}  events={e_n:,}  users(us-east-1)={east:,}")

    cur.close(); conn.close()
    return 0


def upload_via_stage(cur, table: str, csv_bytes: bytes, _cols: list[str]) -> None:
    """Stage CSV via Snowflake's named-stage workflow, then COPY INTO."""
    stage = f"BENCH_SEED_{table}_STAGE"
    cur.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage} "
                "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')")
    tmpdir = Path("/tmp/melt-bench-seed")
    tmpdir.mkdir(parents=True, exist_ok=True)
    fname = tmpdir / f"{table.lower()}.csv"
    fname.write_bytes(csv_bytes)
    cur.execute(f"PUT 'file://{fname}' @{stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute(f"COPY INTO ANALYTICS.PUBLIC.{table} FROM @{stage} "
                "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"') "
                "ON_ERROR = 'ABORT_STATEMENT'")


if __name__ == "__main__":
    sys.exit(main())
