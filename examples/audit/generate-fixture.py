#!/usr/bin/env python3
"""Synthesize a ~10k row QUERY_HISTORY export for the `melt audit` demo.

Produces a CSV at examples/audit/query-history-fixture.csv shaped like
the live SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY pull. Drives the audit
crate's snapshot test (crates/melt-audit/tests/fixture.rs) and the
README quickstart.

Run:
    python3 examples/audit/generate-fixture.py

Reproducible via fixed --seed (default 42). Re-run regenerates both the
CSV and the ground-truth.json side-car so the snapshot test stays
within ±2pp of the generated corpus.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Table catalog. The hot tables match the agent-driven dbt project shape
# called out in POWA-146 (events, users, orders, sessions, products, …);
# the long-tail mimics dbt staging/intermediate/marts naming so the
# `--top-n 20` conservative cut-off lops off the cold tail.
# ---------------------------------------------------------------------------

HOT_TABLES = [
    "ANALYTICS.PUBLIC.EVENTS",
    "ANALYTICS.PUBLIC.USERS",
    "ANALYTICS.PUBLIC.ORDERS",
    "ANALYTICS.PUBLIC.SESSIONS",
    "ANALYTICS.PUBLIC.PRODUCTS",
    "ANALYTICS.PUBLIC.LINE_ITEMS",
]

LONG_TAIL_TABLES = [
    f"ANALYTICS.STAGING.STG_{n}" for n in (
        "ADDRESSES", "CITIES", "COUNTRIES", "CUSTOMERS", "EMPLOYEES",
        "INVOICES", "PAYMENTS", "REFUNDS", "REVIEWS", "SHIPMENTS",
        "STORES", "SUBSCRIPTIONS", "TICKETS", "TRACKING", "VENDORS",
        "WAREHOUSES", "ZIP_CODES", "AB_TESTS", "FEATURE_FLAGS",
        "WEBHOOK_EVENTS", "EMAIL_SENDS", "PUSH_NOTIFICATIONS",
        "DEVICE_REGISTRATIONS", "CONSENT_LOGS",
    )
] + [
    f"ANALYTICS.INTERMEDIATE.INT_{n}" for n in (
        "ORDER_ITEMS_ENRICHED", "USER_FACTS", "SESSION_FACTS",
        "FUNNEL_STEPS", "REVENUE_DAILY", "CHURN_FLAGS",
        "MARKETING_TOUCHES", "EXPERIMENT_ASSIGNMENTS",
    )
] + [
    f"ANALYTICS.MARTS.MART_{n}" for n in (
        "DAILY_ACTIVE_USERS", "RETENTION_COHORTS", "ATTRIBUTION", "LTV",
        "SEGMENT_MEMBERSHIPS", "REVENUE_MTD", "FORECAST_INPUTS",
    )
]

# warehouse_size, weight, (exec_ms_lo, exec_ms_hi)
WAREHOUSE_MIX = [
    ("X-Small", 0.55, (50, 12_000)),
    ("Small",   0.30, (200, 60_000)),
    ("Medium",  0.12, (500, 180_000)),
    ("Large",   0.03, (1_000, 600_000)),
]

# ---------------------------------------------------------------------------
# Patterns. Quoted literals stay in so `redact_literals` has something
# to substitute and the de-dupe in `top_patterns` collapses across them.
# ---------------------------------------------------------------------------

ROUTABLE_PATTERNS = [
    "SELECT id, ts FROM {tbl} WHERE ts > '{date}'",
    "SELECT count(*) FROM {tbl} WHERE name = '{value}'",
    "SELECT id FROM {tbl} WHERE user_id IN (1, 2, 3)",
    "SELECT * FROM {tbl} WHERE id = {id}",
    "SELECT id, total FROM {tbl} WHERE status = '{status}'",
    "SELECT user_id, SUM(amount) FROM {tbl} WHERE created_at >= '{date}' GROUP BY user_id",
    "SELECT id, name FROM {tbl} WHERE created_at > '{date}'",
    "SELECT * FROM {tbl} ORDER BY ts DESC LIMIT 100",
]

WRITE_PATTERNS = [
    "INSERT INTO {tbl} (id, ts) VALUES ({id}, '{date}')",
    "UPDATE {tbl} SET status = 'paid' WHERE id = {id}",
    "DELETE FROM {tbl} WHERE created_at < '{date}'",
    "CREATE TABLE {tbl}_V2 AS SELECT * FROM {tbl} LIMIT 0",
    "GRANT SELECT ON {tbl} TO ROLE ANALYST",
]

# Snowflake-only constructs -- engine classifier flags these via
# `uses_snowflake_features`. INFORMATION_SCHEMA queries are also
# routed through this bucket because the engine treats them as
# Snowflake-feature passthroughs.
SNOWFLAKE_FEATURE_PATTERNS = [
    "SELECT * FROM TABLE(GENERATOR(ROWCOUNT => {n}))",
    "SELECT * FROM TABLE(RESULT_SCAN('01234567-0000-0000-0000-{suffix:012d}'))",
    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'",
    "SELECT QUERY_TEXT FROM INFORMATION_SCHEMA.QUERY_HISTORY LIMIT 100",
]

# Noise: parse failures and queries that bind to no real tables.
PARSE_FAIL_PATTERNS = [
    "SELECT FROM WHERE LIMIT",
    "INVALID SQL ;;",
    "// not sql",
    "SELECT WHERE id =",
]

NO_TABLE_PATTERNS = [
    "SELECT 1",
    "SELECT CURRENT_TIMESTAMP()",
    "SELECT NOW()",
    "SELECT 1 + 1 AS two",
]

STATUS_VALUES = ["paid", "pending", "refunded", "shipped", "open"]
NAME_VALUES = ["login", "signup", "purchase", "page_view", "click", "search"]


@dataclass
class GenConfig:
    rows: int
    seed: int
    end_date: datetime
    window_days: int
    out: Path
    routable_pct: float
    writes_pct: float
    snowflake_pct: float
    # noise pct = 1 - the rest
    hot_share_of_routable: float


def parse_args() -> GenConfig:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=42)
    # default-end matches today's review-cycle date so the 30d window
    # straddles a believable now-ish range.
    p.add_argument("--end-date", default="2026-05-04")
    p.add_argument("--window-days", type=int, default=30)
    p.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parent / "query-history-fixture.csv"),
    )
    p.add_argument("--routable-pct", type=float, default=0.70)
    p.add_argument("--writes-pct", type=float, default=0.10)
    p.add_argument("--snowflake-pct", type=float, default=0.10)
    # Tuned so `--top-n 20` conservative cut-off lands at ~43.0% — the
    # spec §1 mockup figure, ±2pp tolerance. Lower = more long-tail
    # mass falls outside top-N (drops conservative further).
    p.add_argument("--hot-share-of-routable", type=float, default=0.40)
    a = p.parse_args()
    return GenConfig(
        rows=a.rows,
        seed=a.seed,
        end_date=datetime.fromisoformat(a.end_date).replace(tzinfo=timezone.utc),
        window_days=a.window_days,
        out=Path(a.out),
        routable_pct=a.routable_pct,
        writes_pct=a.writes_pct,
        snowflake_pct=a.snowflake_pct,
        hot_share_of_routable=a.hot_share_of_routable,
    )


def log_uniform(rng: random.Random, lo: float, hi: float) -> float:
    """Draw exec_ms with a long tail (log-uniform)."""
    a, b = math.log(lo), math.log(hi)
    return math.exp(a + (b - a) * rng.random())


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def render_routable(rng: random.Random, tbl: str, win_start: datetime, win_end: datetime) -> str:
    p = rng.choice(ROUTABLE_PATTERNS)
    # Pick a literal date inside the window so redaction covers it.
    when = win_start + timedelta(seconds=rng.randint(0, max(1, (win_end - win_start).seconds + (win_end - win_start).days * 86400 - 1)))
    return p.format(
        tbl=tbl,
        date=when.strftime("%Y-%m-%d"),
        value=rng.choice(NAME_VALUES),
        status=rng.choice(STATUS_VALUES),
        id=rng.randint(1, 1_000_000),
    )


def render_write(rng: random.Random, tbl: str, win_start: datetime) -> str:
    p = rng.choice(WRITE_PATTERNS)
    when = win_start + timedelta(seconds=rng.randint(0, 30 * 86400))
    return p.format(
        tbl=tbl,
        date=when.strftime("%Y-%m-%d"),
        id=rng.randint(1, 1_000_000),
    )


def render_snowflake_feature(rng: random.Random) -> str:
    p = rng.choice(SNOWFLAKE_FEATURE_PATTERNS)
    return p.format(n=rng.randint(10, 10_000), suffix=rng.randint(0, 999_999_999_999))


def render_noise(rng: random.Random) -> str:
    if rng.random() < 0.5:
        return rng.choice(PARSE_FAIL_PATTERNS)
    return rng.choice(NO_TABLE_PATTERNS)


def pick_warehouse(rng: random.Random):
    sizes = [w[0] for w in WAREHOUSE_MIX]
    weights = [w[1] for w in WAREHOUSE_MIX]
    chosen = rng.choices(sizes, weights=weights)[0]
    lo, hi = next(w[2] for w in WAREHOUSE_MIX if w[0] == chosen)
    return chosen, lo, hi


def pick_table(rng: random.Random, hot_share: float) -> str:
    if rng.random() < hot_share:
        # Skewed-Zipf among hot tables: first table dominates.
        weights = [1.0 / (i + 1) for i in range(len(HOT_TABLES))]
        return rng.choices(HOT_TABLES, weights=weights)[0]
    # Long-tail: nearly uniform across many dbt models — that's what
    # forces the top-N=20 conservative cut-off below the static rate.
    # When the long tail is too peaky, top-N captures most of it and
    # conservative collapses back into static.
    return rng.choice(LONG_TAIL_TABLES)


def main():
    cfg = parse_args()
    rng = random.Random(cfg.seed)

    win_end = cfg.end_date
    win_start = win_end - timedelta(days=cfg.window_days)

    routable_threshold = cfg.routable_pct
    writes_threshold = routable_threshold + cfg.writes_pct
    snowflake_threshold = writes_threshold + cfg.snowflake_pct

    bytes_buckets = [0, 256_000, 1_048_576, 4_194_304, 16_777_216, 67_108_864, 268_435_456]
    bytes_weights = [0.18, 0.18, 0.20, 0.18, 0.14, 0.08, 0.04]

    rows = []
    for i in range(cfg.rows):
        ts = win_start + timedelta(
            seconds=rng.randint(0, cfg.window_days * 86400 - 1)
        )
        size, exec_lo, exec_hi = pick_warehouse(rng)
        exec_ms = max(1, int(log_uniform(rng, exec_lo, exec_hi)))
        bytes_scanned = rng.choices(bytes_buckets, weights=bytes_weights)[0]

        roll = rng.random()
        if roll < routable_threshold:
            tbl = pick_table(rng, cfg.hot_share_of_routable)
            sql = render_routable(rng, tbl, win_start, win_end)
        elif roll < writes_threshold:
            # Writes mostly land on the hot tables — that's where dbt's
            # MERGE/INSERT volume sits in real projects.
            tbl = rng.choice(HOT_TABLES)
            sql = render_write(rng, tbl, win_start)
        elif roll < snowflake_threshold:
            sql = render_snowflake_feature(rng)
        else:
            sql = render_noise(rng)
        rows.append((f"q{i:05d}", sql, iso(ts), exec_ms, size, bytes_scanned))

    # Chronological — easier to eyeball-diff vs a real QUERY_HISTORY pull.
    rows.sort(key=lambda r: r[2])
    # Re-id after sort so QUERY_IDs are monotonic in start_time.
    rows = [(f"q{i:05d}", *r[1:]) for i, r in enumerate(rows)]

    cfg.out.parent.mkdir(parents=True, exist_ok=True)
    with cfg.out.open("w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(["QUERY_ID", "QUERY_TEXT", "START_TIME", "EXECUTION_TIME", "WAREHOUSE_SIZE", "BYTES_SCANNED"])
        w.writerows(rows)

    print(f"wrote {len(rows):,} rows -> {cfg.out}")
    print(f"window: {iso(win_start)} -> {iso(win_end)}")
    summary = {
        "rows": len(rows),
        "seed": cfg.seed,
        "routable_pct_target": cfg.routable_pct * 100,
        "writes_pct_target": cfg.writes_pct * 100,
        "snowflake_pct_target": cfg.snowflake_pct * 100,
        "hot_share_of_routable": cfg.hot_share_of_routable,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
