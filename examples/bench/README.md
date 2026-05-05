# Melt bench harness — queries-per-dollar deltas

This directory holds the artifact backing the [Melt v0.1 anchor metric](../../readme.md#why-melt-exists): a repeatable workload that runs a synthetic agent-shaped query mix against (a) **Melt-routed-to-lake** and (b) **Snowflake passthrough**, and reports the queries-per-dollar delta.

It is the math customers actually care about — total Snowflake spend per unit of agent work — measured on a workload Melt is built to compress.

## What's in here

```
examples/bench/
├── workload.toml           # query mix, concurrency, cost model
├── run_bench.py            # driver: connects, runs, writes JSON
├── requirements.txt        # snowflake-connector-python (unmodified)
├── Makefile                # `make bench` / `make bench-synthetic`
└── fixtures/               # checked-in reference runs
    ├── results-real.json       # 100×2 against a 50k-row Snowflake trial — the anchor metric
    └── results-synthetic.json  # no-credentials sanity run
```

## Reference numbers

Headline from `fixtures/results-real.json` (100 queries × 2 paths against `ANALYTICS.PUBLIC.{EVENTS,USERS}`, 50k rows each, on a Standard-edition trial account against a Large warehouse, $3/credit):

| Path                       | p50 latency | p95 latency | $ over 100 q | queries / $ |
|----------------------------|-------------|-------------|--------------|-------------|
| Snowflake passthrough (L)  | 142 ms      | 236 ms      | $0.1018      | 982         |
| Melt → lake (100% routed)  | 156 ms      | 245 ms      | $0.0000      | ∞           |

Per-query records show all four templates classified `lake` by `melt route` and all 100 Melt-path queries actually ran on the lake (`route_mix={"lake": 100}`). Lake latency tracks Snowflake's warm-warehouse latency at this dataset size — the savings story is the cost multiplier (every Snowflake query bills warehouse credits, every lake query bills $0), not raw latency.

The fixture redacts the live `warehouse` name to `null` by default; set `BENCH_RECORD_WAREHOUSE_NAME=1` for local-only runs if you need it for debugging. The cost model only needs `warehouse_size`, so this affects nothing except observability.

Re-measure as the lake side improves and as the workload (dataset size, query mix) changes. Always quote the dataset shape + warehouse size alongside the number.

## Run it (real Snowflake account)

```bash
# 1. Bring up the local Melt + Postgres + MinIO stack from the repo root.
cd ../..
docker compose up --build -d

# 2. Install the bench harness deps.
cd examples/bench
make install

# 3. Set Snowflake creds (a free 30-day trial account is fine).
export SNOWFLAKE_ACCOUNT=xy12345
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_SCHEMA=PUBLIC
# Optional:
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH      # if not set, the driver uses the user's default
export BENCH_CREDIT_RATE=3.00              # $/credit (Standard list)

# 4. Seed the bench dataset into Snowflake (idempotent — safe to rerun).
#    Creates ANALYTICS.PUBLIC.USERS + ANALYTICS.PUBLIC.EVENTS at 50k rows
#    each, shaped to satisfy the four templates in workload.toml.
python3 ../../scripts/bench-seed/seed_bench.py

# 5. Bring those tables into Melt's lake (one-shot sync), then run.
docker compose run --rm melt sync run --once
make bench
```

The output `results-<timestamp>.json` contains per-query records (route, latency, warehouse) and a summary block with `queries_per_dollar` for both paths plus the delta.

A successful run prints, e.g.:

```
── Bench complete ── (real mode)
   results: results-20260505T083500Z.json
   git:     8797ed6…
   snowflake  queries=100  p50=118.32ms p95=210.75ms usd=0.088668 q/$=1127.8  routes={'snowflake': 100}
   melt       queries=100  p50=139.56ms p95=264.99ms usd=0.0      q/$=∞       routes={'lake': 100}
   ── delta
      $/1k queries   snowflake=0.8867  melt=0.0
      savings/1k     $0.8867  (100.0% cheaper)
      q/$ factor     melt is ∞ (all queries routed to lake — zero Snowflake cost)
```

When every query in the workload is lake-eligible, `queries_per_dollar` for the Melt path is unbounded and the JSON encodes the factor as the string `"infinite"` (for `jq` parsing). Use `usd_per_1k_queries.savings_pct` as the headline cost-reduction number in that case.

## Run it (no Snowflake account)

For sanity checking the harness or regenerating the checked-in fixture:

```bash
make bench-synthetic
```

This skips the connector entirely. Routes come from `melt route "<sql>"` (offline; no Snowflake credentials needed) and latencies are sampled from a log-normal model parameterized by the `[synthetic]` block of `workload.toml`. The synthetic mode is **only** for harness validation — it does not produce a real anchor metric.

## Cost model

Every Snowflake-routed query is costed as:

```
credits = (latency_seconds / 3600) × credits_per_hour[warehouse_size]
usd     = credits × credit_rate_usd
```

The credits-per-hour table (`XS=1, S=2, M=4, L=8, XL=16`) follows Snowflake's published pricing — every step doubles. See [`docs/internal/WAREHOUSE_MANAGEMENT.md` §16](../../docs/internal/WAREHOUSE_MANAGEMENT.md#16-appendix-cost-arithmetic-worked-example) for the worked example this template was lifted from.

Lake-routed queries are attributed **zero Snowflake credits** — DuckDB runs them locally on Parquet on S3 with no warehouse touched. The Melt host's CPU/IO cost is real but small and out of scope for v1.

The default warehouse size is `L` (matching the readme's framing of a customer running everything on one shared LARGE warehouse). If your reality is different, override `default_warehouse_size` in `workload.toml`.

## Workload shape

The default mix is "agent-shaped": lots of small filters and selective joins, a thin tail of aggregations.

| Query | Weight | Expected route |
| --- | --- | --- |
| `small_filter` (single-row lookup with `LIMIT 50`) | 60% | lake |
| `selective_join` (events ⨝ users on indexed pred) | 25% | lake |
| `daily_agg` (last-7-day count by day) | 12% | lake |
| `topn_users` (top-100 users by event count) | 3% | lake |

Vary it by editing `workload.toml`. To swap in a real dbt project as the workload, replace the `[[query]]` blocks with your own SQL and re-run — the harness doesn't care about query shape.

## Assumptions and caveats

- **Flat $/credit.** Real Snowflake bills include consumption commitments, edition multipliers, and serverless adjustments the harness ignores. Override `--credit-rate` to match your contract.
- **Latency-driven cost.** Snowflake bills warehouses by wall-clock active time, not bytes scanned. The harness uses query latency as the cost proxy, which over-counts when the warehouse was already active for unrelated traffic and under-counts when a query triggers cold-start credits. Both are documented Snowflake artifacts and not Melt-specific.
- **Default warehouse size.** Without `INFORMATION_SCHEMA.QUERY_HISTORY` lookups the harness can't tell what Snowflake actually charged. v1 records the connector-pinned warehouse and costs every passthrough query at `default_warehouse_size`. Wiring `bytes_scanned` and per-query warehouse from QUERY_HISTORY is on the v1.1 list.
- **Lake routing depends on sync.** If the bench tables aren't synced to the lake yet, every query falls through to Snowflake passthrough and the delta collapses. Bring up sync (`docker compose run --rm melt sync run --once`) and verify with `melt status` before running.
- **Routing decisions are recorded once per distinct SQL.** `melt route` is deterministic per SQL string, so the harness samples it once and re-uses the answer. If you parameterize queries the routes can drift — re-run `discover_routes` on every iteration in that case.
- **Synthetic mode is a sanity check, not a benchmark.** The latency parameters in `[synthetic]` are illustrative defaults. Don't quote synthetic numbers as a Melt savings claim.

## What's deferred to v1.1+

- **`bytes_scanned` from `INFORMATION_SCHEMA.QUERY_HISTORY`** — needs a small post-run lookup loop using the captured `cur.sfqid` per query.
- **Per-statement warehouse override observation** — once warehouse-management ships (POWA-_, see [`WAREHOUSE_MANAGEMENT.md`](../../docs/internal/WAREHOUSE_MANAGEMENT.md)), the bench should record the warehouse Melt picked vs. the driver-pinned default to attribute that path's savings.
- **dbt project as workload** — swap `[[query]]` for a real dbt invocation; bound the run with a model selector. Phase 2 in the parent issue.
- **BI-tool replay** — Looker / Sigma capture → replay loop. Phase 2.
- **Cold-state isolation** — flush warehouse state between runs to avoid the warm-warehouse advantage leaking across measurements.
