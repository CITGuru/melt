# Dual execution

Dual execution (also called *hybrid*) lets a single query split across the lake and Snowflake — DuckDB runs the lake-resident parts, Snowflake runs the remote-resident parts, and the results join in DuckDB. It's the answer to the question "what about the tables I don't want to sync?": you keep them remote, declare them via `[sync].remote`, and queries that touch them route through hybrid instead of falling back to full passthrough.

Off by default. Opt in with `[router].hybrid_execution = true` and at least one `[sync].remote` glob.

For the runtime model — plan trees, placements, bridges — see [architecture.md § Plan trees and dual execution](../architecture.md#plan-trees-and-dual-execution). This guide covers the operator surface: when to enable it, how to configure it, what `melt route` shows you, which metrics matter, and how to debug.

## When to enable it

Hybrid is worth turning on when at least one of these is true:

| Situation | What hybrid does for you |
|---|---|
| One or more upstream tables are too large or too sensitive to sync to the lake | Keeps them remote (`[sync].remote = ["BIG_DATABASE.*"]`) — joins against your synced tables stay fast on the lake side, and the remote side runs in Snowflake instead of getting pulled across the wire. |
| Common dashboard joins span "the data warehouse fact table" and "this small synced dimension" | The lake dimension stays local; the fact-table predicate gets shipped to Snowflake. Saves both Snowflake compute (no full scan) and proxy bandwidth. |
| Sync is bootstrapping a newly-discovered table and you want queries to keep working in the meantime | Set `hybrid_allow_bootstrapping = true` — the router promotes pending/bootstrapping tables into the remote pool until sync catches up, instead of forcing the whole query to passthrough. |
| One table in the query is over `lake_max_scan_bytes` but the others fit | Set `hybrid_allow_oversize = true` — the router promotes just the oversize table to remote, keeps the rest local. |

Hybrid is **not** a workaround for unsynced data in general. The design assumption is that the operator is consciously declaring tables remote (via `[sync].remote`) or opting into one of the two trigger toggles. Auto-discovered tables don't go hybrid; they go pending → bootstrapping → active on the sync loop.

## Enabling it

```toml
[sync]
auto_discover = true

# Tables matching these globs are NEVER synced. Queries that touch
# them route through dual execution. Precedence: exclude > remote > include.
remote = [
    "BIG_WAREHOUSE.*",
    "FOO_DB.PUBLIC.EVENTS",
    "BAZ_*.STAGING.*",
]

[router]
hybrid_execution = true
```

`melt sync remote` (offline; reads `melt.toml` only) lists every declared-remote glob and the FQNs each one matches against the proxy's known table set. Symmetric to `melt sync list`.

```bash
melt --config melt.toml sync remote
melt --config melt.toml sync remote --json
```

For Docker: the published image bakes the ADBC Snowflake native driver (the one DuckDB's community Snowflake extension dlopens), so flipping `hybrid_execution = true` is sufficient. For non-Docker installs, see the [native driver](#native-driver) section below.

## Routes and strategies

For a hybrid-eligible query the router emits `Route::Hybrid` and picks one of two strategies per remote subtree:

| Strategy | When it fires | What runs in Snowflake | What runs in DuckDB |
|---|---|---|---|
| **Attach** | The remote subtree is a single scan (one remote table, possibly with predicates / projections that DuckDB will push down through the extension). | Whatever the extension's optimizer decides to push down (typically: the `WHERE` clause + the columns the outer query references). | Streams the result rows through the vectorized executor and joins against any local tables in the same query. |
| **Materialize** | The remote subtree covers two or more remote tables (e.g. a join between two remote tables, or a remote subquery in `IN (...)`). | Executes the entire collapsed subtree as one query — joins, filters, aggregates included. | Pulls the result rows over the wire, stages them as a temp table (`__remote_0`, `__remote_1`, …), and joins those temp tables with any local tables. |

A single hybrid query can use both. `melt route` shows you exactly which subtree picked which.

### `melt route` output

```bash
melt --config melt.local.toml route "SELECT u.region, COUNT(*) FROM ICE.ANALYTICS.EVENTS e JOIN BIG.WAREHOUSE.USERS u ON e.uid = u.id WHERE e.ts > '2026-01-01' GROUP BY u.region"
```

```text
route: hybrid
reason: remote_by_config (RemoteByConfig)
strategy: attach
remote_fragments: 0  attach_rewrites: 1  est_remote_bytes: 18432000

[REMOTE,attach] BIG.WAREHOUSE.USERS → sf_link.BIG.WAREHOUSE.USERS

local SQL:
SELECT u.region, COUNT(*)
FROM ice.analytics.events e
JOIN sf_link.BIG.WAREHOUSE.USERS u ON e.uid = u.id
WHERE e.ts > '2026-01-01'
GROUP BY u.region
```

A mixed plan looks like this — same query but with a remote subquery as well:

```text
route: hybrid
strategy: mixed
remote_fragments: 1  attach_rewrites: 1  est_remote_bytes: 22210000

[REMOTE,materialize] __remote_0 (2 tables)
SELECT buyer_id FROM sf_link.BIG.WAREHOUSE.ORDERS o
JOIN sf_link.BIG.WAREHOUSE.PRODUCTS p ON p.id = o.pid
WHERE p.category = 'electronics'

[REMOTE,attach] BIG.WAREHOUSE.USERS → sf_link.BIG.WAREHOUSE.USERS

local SQL:
SELECT u.region, COUNT(*)
FROM   sf_link.BIG.WAREHOUSE.USERS u
WHERE  u.id IN (SELECT * FROM __remote_0)
GROUP BY u.region
```

`melt route` runs the cheap classification path only — no live backend calls. `TableMissing`, `AboveThreshold`, and `PolicyProtected` decisions need a live proxy; use `melt status` for the full picture once the proxy's running.

## Configuration reference

Full field list lives in [configuration.md](../configuration.md#hybrid-dual-execution-routing--opt-in). Operator-relevant subset:

| Field | Default | What it does |
|---|---|---|
| `hybrid_execution` | `false` | Master switch. Off → declared-remote tables behave like missing tables (passthrough). |
| `hybrid_attach_enabled` | `true` | Set to `false` as a kill switch when the community Snowflake extension misbehaves. The pool also flips it off automatically when the extension fails to load. Hybrid keeps working — every node uses Materialize. |
| `hybrid_max_remote_scan_bytes` | `"5GiB"` | Sum across all Materialize fragments for one query. Above this the whole query collapses to Snowflake passthrough. Per-tenant safety net. |
| `hybrid_max_fragment_bytes` | `"2GiB"` | Per-fragment cap. Prevents a single fragment from eating the whole budget. |
| `hybrid_max_attach_scan_bytes` | `"10GiB"` | Per-Attach-scan cap. Above this, passthrough. Intentionally higher than the Materialize sum cap because Attach streams without bulk materialization. |
| `hybrid_allow_bootstrapping` | `false` | Promote pending/bootstrapping tables to remote until sync catches up. |
| `hybrid_allow_oversize` | `false` | Promote a single oversize table to remote, keep the rest of the query local. |
| `hybrid_parity_sample_rate` | `0.01` | Fraction of hybrid queries the parity sampler replays against pure Snowflake. `0.0` disables. |
| `hybrid_profile_attach_queries` | `false` | When true, `EXPLAIN ANALYZE local_sql` runs after every Attach query and the rendered plan's `snowflake_scan` lines are logged at info level. ~2× latency. Enable per-tenant during debugging. |
| `hybrid_attach_refresh_interval` | `"1h"` | Periodic `DETACH IF EXISTS sf_link; ATTACH …` cycle, lazy-triggered on connection recycle. Bounds the staleness window when upstream Snowflake schemas evolve. `0s` disables. |
| `hybrid_fragment_cache_ttl` | `"0s"` | Statement-level result cache. `> 0` enables; identical hybrid queries within the window skip Snowflake entirely. Per-table invalidation cascades from sync writes. |
| `hybrid_fragment_cache_max_entries` | `256` | Hard ceiling on cache entries; oldest evicts first once exceeded. |

## Comment hints

Operators can override routing per-query via leading SQL comments. Useful when a one-off ad-hoc query wants different behavior than the default config — or when you're isolating a parity bug.

```sql
/*+ melt_route(snowflake) */ SELECT …    -- skip lake + hybrid; passthrough
/*+ melt_route(lake) */ SELECT …          -- treat all tables as local; ignore [sync].remote
/*+ melt_route(hybrid) */ SELECT …        -- force hybrid; bypass size caps
/*+ melt_strategy(materialize) */ SELECT … -- within hybrid: every Remote node uses Materialize
/*+ melt_strategy(attach) */ SELECT …     -- within hybrid: prefer Attach (default)
```

Both `--+ ...` line comments and `/*+ ... */` block comments work. Multiple hints can co-occur (whitespace- or comma-separated). Unknown hints are silently ignored — hint syntax stays portable across SQL dialects.

Hints only apply to **leading** comments. Comments inside the query body are treated as ordinary SQL comments and not parsed for routing intent.

## Metrics

Every hybrid-related metric lives under the existing `/metrics` endpoint. The `melt_router_decisions_total{route="hybrid"}` counter is auto-covered by the regular routing-decisions counter; the rest are new.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `melt_router_hybrid_reasons_total` | counter | `reason` | Why hybrid fired: `remote_by_config`, `remote_bootstrapping`, `remote_oversize`, `mixed`. |
| `melt_hybrid_strategy_total` | counter | `strategy` | Per-query strategy distribution: `attach`, `materialize`, `mixed`. |
| `melt_hybrid_pushdown_collapsed_total` | counter | — | Number of multi-table subtrees that collapsed into one Materialize fragment. Direct proxy for the optimization's value. |
| `melt_hybrid_attach_nodes_per_query` | histogram | — | Attach nodes per hybrid query. |
| `melt_hybrid_materialize_nodes_per_query` | histogram | — | Materialize fragments per hybrid query. |
| `melt_hybrid_remote_scan_bytes` | histogram | `strategy` | Estimated bytes per fragment / per Attach scan, labeled by strategy. |
| `melt_hybrid_materialize_latency_seconds` | histogram | — | Time to stage all Materialize fragments (Snowflake → temp table). |
| `melt_hybrid_attach_unavailable_total` | counter | — | Times the router wanted Attach but the runtime forced Materialize because the extension wasn't loaded. Should be `0` when ADBC is installed. |
| `melt_hybrid_fallbacks_total` | counter | `reason` | Transport / first-batch failures that fell back to passthrough. |
| `melt_hybrid_remote_errors_total` | counter | `strategy` | Snowflake-side SQL errors. These do **not** fall back — an unexpected fragment SQL error indicates a translation bug, not a transient failure, so it surfaces to the client. |
| `melt_hybrid_parity_mismatches_total` | counter | — | Parity sampler caught a result mismatch between hybrid and pure Snowflake. Investigate immediately; this is the type-drift early warning. |
| `melt_hybrid_parity_sample_drops_total` | counter | — | Parity samples dropped because the bounded sampler channel was full. Sample rate is a ceiling, not a floor — drops are fine; rising drops mean the sampler is the bottleneck. |

## Native driver

Dual execution's Attach strategy uses [DuckDB's community Snowflake extension](https://duckdb.org/community_extensions), which is a thin wrapper around Apache Arrow's ADBC Snowflake driver (`libadbc_driver_snowflake.so` / `.dylib`). DuckDB downloads the extension itself; it does **not** download ADBC.

| Install path | What to do |
|---|---|
| Docker (`melt:dev`) | Already baked in. Skip with `--build-arg HYBRID_ADBC=false` for a slimmer image. |
| Linux / macOS dev | `python3 -m pip install adbc-driver-snowflake==1.6.0` then symlink the bundled `.so` / `.dylib` from `<site-packages>/adbc_driver_snowflake/` into `/usr/local/lib/`. |
| Conda / mamba | `conda install -c conda-forge libadbc-driver-snowflake` lands the library in `$CONDA_PREFIX/lib/`; symlink into a system path. |

When the driver isn't on the library search path:

- The proxy logs a `WARN` at startup (`hybrid Attach setup failed — sf_link won't be available; …`) and then keeps running.
- The router degrades all hybrid plans to all-Materialize automatically.
- `melt_hybrid_attach_unavailable_total` increments per query, surfacing the degradation.

Pin the ADBC version in production. ABI mismatches between the driver and the DuckDB extension show up as runtime `dlopen` failures on first ATTACH, with no other warning.

## Rollout

Hybrid is **off by default** and we recommend a phased rollout:

1. **Phase 0 — shadow.** Leave `hybrid_execution = false` and inspect `melt route` output for production-shape queries. Confirm strategy decisions look right; iterate `[sync].remote` patterns if needed.
2. **Phase 1 — staging with parity.** Flip `hybrid_execution = true` and `hybrid_parity_sample_rate = 0.1` in staging. Watch `melt_hybrid_parity_mismatches_total` for two full weeks. Pay attention to the `strategy` label — drift bugs often surface in one strategy first.
3. **Phase 2 — one production tenant.** Enable on a low-risk tenant. Watch metrics + `WARN` logs for two more weeks.
4. **Phase 3 — fleet.** Promote to the default once parity stays clean. Drop the parity sample rate to `0.01` (or lower) for steady-state.

Throughout: `hybrid_max_remote_scan_bytes`, `hybrid_max_fragment_bytes`, and `hybrid_max_attach_scan_bytes` give you blast-radius caps. Every cap above which the query passthroughs leaves a clean `melt_router_decisions_total{route="snowflake", reason="above_threshold"}` trail to alert on.

## Debugging a parity mismatch

When `melt_hybrid_parity_mismatches_total` ticks up:

1. Pull the matching `WARN hybrid_parity_mismatch` log line. It carries the query hash, the row counts each side returned, and the per-node strategy distribution.
2. Reproduce with `melt route <sql>` to inspect the plan tree — which subtrees collapsed, which strategy each node picked.
3. **Materialize node**: copy the `[REMOTE,materialize]` fragment SQL out of the `melt route` output and run it directly against Snowflake. Compare its output to what the lake side received.
4. **Attach node**: enable `hybrid_profile_attach_queries = true` for the tenant and replay. The proxy now logs `EXPLAIN ANALYZE` output after every Attach query — grep the rendered plan for `snowflake_scan` and `query_string` to see the actual SQL DuckDB sent upstream.
5. Run the original SQL against Snowflake end-to-end. Compare to step 3 + step 4 results joined as the local SQL would.

Common causes:

- **Decimal precision drift on aggregates** — the lake DECIMAL widening is conservative; some Snowflake-side `SUM` results round differently from DuckDB's. Open an issue with the offending aggregation and we'll widen the type-mapping rule.
- **Timestamp TZ drift** — confirm UTC normalization happens on whichever path produced the divergent value. Snowflake's `TIMESTAMP_LTZ` ↔ DuckDB's `TIMESTAMPTZ` round-trip is straightforward; `TIMESTAMP_NTZ` and `TIMESTAMP_TZ` are the cases to watch.
- **NULL ordering** — usually a spec difference, not a bug, between Snowflake's default and DuckDB's default. Document in your client expectations.
- **Semi-structured (`VARIANT` / `OBJECT`) projection** — the fragment projection may drop path-accessed columns if the type didn't materialize cleanly into DuckDB. Use the `melt_strategy(attach)` hint as a kill switch and re-run; if the mismatch goes away, the Materialize emitter is the culprit.
- **Attach extension pushed a predicate it shouldn't have** — rare but real; `hybrid_attach_enabled = false` is the per-tenant kill switch, and after re-running with it off, if the mismatch disappears, the extension's predicate pushdown is the culprit.

## See also

- [architecture.md § Plan trees and dual execution](../architecture.md#plan-trees-and-dual-execution) — the runtime model.
- [configuration.md § Hybrid](../configuration.md#hybrid-dual-execution-routing--opt-in) — full config reference.
- [sync.md](sync.md) — how `[sync].remote` interacts with the rest of the sync state machine.
- [policy-modes.md](policy-modes.md) — why hybrid refuses to federate policy-protected tables.
