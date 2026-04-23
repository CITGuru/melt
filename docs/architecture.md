# Architecture

This doc covers what the two Melt services actually do at runtime, how the `melt` subcommands map to them, and the state machine sync uses to manage mirrored tables. For an introduction to *why* Melt exists, read [overview.md](overview.md) first.

## Workspace layout

```
melt/
├── crates/
│   ├── melt-core/        # Shared types + StorageBackend trait + translate passes
│   ├── melt-control/     # Postgres-backed control plane (sync state, policy markers)
│   ├── melt-snowflake/   # Shared Snowflake HTTP client (proxy + sync)
│   ├── melt-router/      # SQL parse + classify + translate + route decision
│   ├── melt-proxy/       # Snowflake-compatible HTTP server
│   ├── melt-ducklake/    # DuckLake backend (read + sync)
│   ├── melt-iceberg/     # Iceberg backend (read + sync)
│   ├── melt-metrics/     # Observability: /metrics, /healthz, /readyz, /admin/reload
│   └── melt-cli/         # `melt` binary: start | sync | all | status | route | bootstrap
├── examples/             # End-to-end demos using the official Snowflake drivers, unmodified
├── docs/                 # Public documentation (you are here)
├── melt.toml             # Placeholder config (copy to melt.local.toml for dev)
└── docker-compose.yml    # Melt + Postgres + MinIO dev stack
```

Both backends are first-class and live behind the `StorageBackend` trait in `melt-core`. You pick one per deployment via `[backend.ducklake]` or `[backend.iceberg]`. The CLI can be built with both, one, or neither — see the CLI's cargo-features section.

## Long-running services

Melt has two long-running services:

- **Proxy** — terminates client connections, forwards login to Snowflake, and routes statements per request.
- **Sync** — pulls CDC from Snowflake into the lakehouse and refreshes policy markers / filtered views.

The CLI exposes them via these subcommands:


| Subcommand      | Runs                                                         | Typical deployment                      |
| --------------- | ------------------------------------------------------------ | --------------------------------------- |
| `melt all`      | Both, in one process. Easiest for dev and small prod.        | One pod / VM                            |
| `melt start`    | Proxy + admin only. Multiple instances scale horizontally.   | One or more pods behind a load balancer |
| `melt sync run` | Sync + admin only. Single tenant — only one writer per lake. | Exactly one pod                         |


### `melt all`

Boots three things concurrently inside one Tokio runtime: the proxy listener (TCP/TLS on `[proxy].listen`), the metrics admin server (HTTP on `[metrics].listen`), and the sync loops (no listener, just background tasks). If any of the three errors out, the process exits.

### `melt start`

Same as `all` minus the sync loops. Use this when you want to scale the proxy independently — e.g., run three `start` pods behind a load balancer plus one `sync run` pod. Sync is single-writer (the lakehouse only allows one writer at a time), so you can't horizontally scale that side.

### `melt sync run`

The inverse: only the sync loops + the metrics admin port. Three loops run concurrently:

1. **Bootstrap + CDC apply** — every 60 s by default. Picks up `pending` tables (registered by the router from live queries) and runs `CREATE STREAM ... SHOW_INITIAL_ROWS = TRUE` → drain → `mark_active`. Then for every `active` table: pull the stream, apply inserts / updates / deletes through the lakehouse writer.
2. **Demotion sweep** — on `[sync.lazy].demotion_interval`, drop auto-discovered tables idle for `demotion_idle_days`, including their Snowflake streams.
3. **Policy refresh** — every `[snowflake.policy].refresh_interval`, query Snowflake's `POLICY_REFERENCES` and update the local marker / filtered-view catalog.

## How Melt routes a query

Every statement that arrives at the proxy goes through `melt-router::decide`. Parsing, classification, and translation all operate on the same AST and live in the same crate — parsing once and reusing the AST for both the routing decision and the rewrite is what keeps per-statement overhead under a millisecond.

### Pipeline

```
POST /api/v2/statements
        │
        ▼
┌─────────────────┐
│ parse AST       │  sqlparser-rs (Snowflake dialect). Parse-fail → Snowflake passthrough.
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ classify        │  is_write? uses_snowflake_features? extract_tables.
│                 │  Writes and Snowflake-only features short-circuit to Snowflake.
└─────────────────┘
        │
        ▼
┌─────────────────┐   fan-out (single Postgres round trip, concurrent):
│ catalog stats   │     - tables_exist        (all tables synced + active?)
│                 │     - policy_markers      (any policy attached?)
│                 │     - estimate_scan_bytes (does it fit under the threshold?)
└─────────────────┘   All three hit TTL caches in `melt-router::stats`.
        │
        ▼
┌─────────────────┐
│ policy gate     │  passthrough / allowlist / enforce — see the policy-modes guide.
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ build plan tree │  Build → resolve placements → pushdown → insert bridges.
│ (hybrid-capable)│  For a pure-Lake query this is a no-op wrapper.
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ translate       │  Snowflake-dialect rewrites (IFF → CASE WHEN, DATEADD,
│                 │  QUALIFY, PARSE_JSON, FLATTEN, etc.). On failure, safe
│                 │  fallback to Snowflake passthrough rather than erroring.
└─────────────────┘
        │
        ▼
   Route decision
   ┌──────┬─────────────────┬──────────┐
   ▼      ▼                 ▼          ▼
  Lake   Snowflake          Hybrid    (error → passthrough)
```

### The three routes


| Route       | What runs where                                                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Lake`      | Translated DuckDB SQL runs against the backend (DuckLake or Iceberg) in the embedded DuckDB on the proxy host.                                                                                                         |
| `Snowflake` | The original statement is forwarded to upstream Snowflake unchanged.                                                                                                                                                   |
| `Hybrid`    | Opt-in. Some operators run on DuckDB, some get pushed to Snowflake; results bridge via Arrow IPC and join in DuckDB. Preserves the routing invariant: whatever Melt returns equals what Snowflake would have returned. |


`melt route "<sql>"` exercises this entire pipeline (minus upstream execution) and prints the route, the reason, and — for Hybrid plans — the annotated plan tree.

## Plan trees and dual execution

For hybrid-capable queries the router builds an internal **plan tree** from the AST — a small dialect-agnostic IR Melt owns. Nodes are operators (`Scan`, `Filter`, `Project`, `Aggregate`, `HashJoin`, `Bridge`, `RemoteSql`) and each node carries a `Placement`: `Local`, `Remote`, or `Auto` (unresolved at parse time).

### Placement resolution

1. **Scans** get their placement from the table's classification. Tables that matched `[sync].remote`, or are still bootstrapping (when `hybrid_allow_bootstrapping = true`), or are oversize (when `hybrid_allow_oversize = true`) are `Remote`. All others are `Local`.
2. **Non-scan operators** use a majority vote of their children. Remote-heavy subtrees settle on `Remote`; mixed and local-heavy subtrees settle on `Local`.

### Pushdown — the largest federable subplan

After placements are labeled, Melt walks the plan top-down and collapses the **largest subtree whose scans are all `Remote`** into a single `RemoteSql` node. The subtree is re-emitted as one Snowflake-dialect query.

This is the optimization that makes dual execution pay. Without pushdown, a query joining `snowflake.orders` with `snowflake.customers` would produce two fragments (one per table), pull both through the wire, and join inside DuckDB. With pushdown, Snowflake executes the join natively on its own warehouse and only the join result crosses the wire. Since all remote tables share one compute context (Snowflake), the precondition for pushdown fires on every purely-remote subtree.

### Bridges

A `Bridge` is a pseudo-operator inserted wherever a parent's placement differs from a child's. It marks a point where data must physically move between engines. Today only `Bridge(R→L)` exists — data moves from Snowflake into DuckDB via Arrow IPC.

```
[LOCAL] HashJoin(l.id = r.id)
├── [LOCAL] Scan(ice.analytics.orders)
└── [LOCAL] Bridge(R→L)
    └── [REMOTE] RemoteSql: SELECT id, tier FROM warehouse.customers
                            WHERE region = 'us-east-1'
```

### Bridge strategies: Attach vs Materialize

Each `RemoteSql` node that survives pushdown picks one of two execution strategies at emit time:

- `**Attach**` — chosen when the node covers **exactly one** remote table. The node's table reference in the local SQL is rewritten to `sf_link.<db>.<schema>.<table>`. DuckDB's Snowflake extension handles predicate/projection pushdown automatically and streams Arrow batches through DuckDB's vectorized executor — no temp-table materialization. Lowest-overhead path, best when there's nothing past DuckDB's own optimizer to improve on.
- `**Materialize`** — chosen when the node covers **two or more** remote tables (i.e. the pushdown rule collapsed a subtree). Melt runs the fragment against Snowflake, pulls the result as Arrow IPC, and bulk-loads it into a DuckDB temp table (`__remote_0`, `__remote_1`, …) via the Appender API. The join between those remote tables runs natively on Snowflake's warehouse; only the result rows cross the wire.

The two strategies are complementary and a single hybrid query can use both.

## Worked example: a mixed hybrid plan

```sql
SELECT u.region, COUNT(*)
FROM   sf.warehouse.users u
JOIN   ice.analytics.events e ON e.uid = u.id
WHERE  e.ts > '2026-01-01'
  AND  u.id IN (
      SELECT buyer_id FROM sf.warehouse.orders o
      JOIN   sf.warehouse.products p ON p.id = o.pid
      WHERE  p.category = 'electronics'
  )
GROUP BY u.region;
```

Assume `[sync].remote = ["WAREHOUSE.*"]` and `ice.analytics.events` is synced.

**1. Classify.** `ice.analytics.events` → `Local`. `sf.warehouse.users`, `sf.warehouse.orders`, `sf.warehouse.products` → `Remote`.

**2. Build, resolve placements, run pushdown.** The `orders JOIN products` subtree collapses — both scans are remote — into one `RemoteSql`. The single `users` scan doesn't collapse (there's nothing to collapse *with*).

```
[LOCAL] Aggregate(u.region)
├── [LOCAL] HashJoin(e.uid = u.id)
│   ├── [LOCAL] Filter(e.ts > '2026-01-01')
│   │   └── [LOCAL] Scan(ice.analytics.events)
│   └── [REMOTE] Scan(sf.warehouse.users)            ← single remote scan, NOT collapsed
└── [LOCAL] SemiJoin(u.id IN ...)
    └── [REMOTE] RemoteSql:                          ← collapsed subtree
           SELECT buyer_id FROM warehouse.orders o
           JOIN  warehouse.products p ON p.id = o.pid
           WHERE p.category = 'electronics'
```

**3. Pick strategies.** `Scan(users)` covers one table → `Attach`. `RemoteSql(orders + products)` covers two tables → `Materialize`.

**4. Insert bridges.**

```
[LOCAL] Aggregate
├── [LOCAL] HashJoin
│   ├── [LOCAL] Filter
│   │   └── [LOCAL] Scan(events)
│   └── [LOCAL] Bridge(R→L)
│       └── [REMOTE, attach] sf_link.warehouse.users
└── [LOCAL] SemiJoin
    └── [LOCAL] Bridge(R→L)
        └── [REMOTE, materialize] RemoteSql(__remote_0)
```

**5. Emit.** The router produces one fragment and one rewritten local SQL:

```
remote_fragments:
  __remote_0:
    SELECT buyer_id FROM warehouse.orders o
    JOIN   warehouse.products p ON p.id = o.pid
    WHERE  p.category = 'electronics'

attach_rewrites:
  sf.warehouse.users → sf_link.warehouse.users

local_sql:
  SELECT u.region, COUNT(*)
  FROM   sf_link.warehouse.users u                  -- Attach
  JOIN   ice.analytics.events e ON e.uid = u.id
  WHERE  e.ts > '2026-01-01'
    AND  u.id IN (SELECT buyer_id FROM __remote_0)  -- Materialize
  GROUP BY u.region
```

**6. Execute.**

1. Proxy runs `__remote_0` against Snowflake, pulls Arrow IPC, bulk-loads into `TEMP TABLE __remote_0`. Snowflake did the `orders JOIN products` natively; only the deduplicated buyer-id list crossed the wire.
2. Proxy runs the local SQL against the same DuckDB connection:
  - `ice.analytics.events` scanned from Iceberg with the timestamp filter pushed to Parquet stats.
  - `sf_link.warehouse.users` scanned via the attached extension; DuckDB's optimizer pushes the `IN (SELECT buyer_id FROM __remote_0)` predicate to Snowflake as an `IN (...)` literal list.
  - Join + semi-join + aggregate run locally.
3. Result streams to the client.

Snowflake processed two highly-filtered queries. DuckDB did the global join and aggregate. Without the strategy split this query would have either been three separate Snowflake scans pulled into DuckDB (no Materialize collapse → ~3× wire bytes) or one giant Materialize fragment (Attach optimization wasted on the small filtered users scan).

## Mirrored-table state machine

Every mirrored table is in one of four states:


| State           | Router behavior             | How it's reached                                                  |
| --------------- | --------------------------- | ----------------------------------------------------------------- |
| `pending`       | Force Snowflake passthrough | First query for a new table, or `melt sync refresh`               |
| `bootstrapping` | Force Snowflake passthrough | Sync picked up the pending row and is running `SHOW_INITIAL_ROWS` |
| `active`        | Eligible for Lake routing   | Bootstrap succeeded                                               |
| `quarantined`   | Force Snowflake passthrough | Bootstrap failed; see `bootstrap_error` via `melt sync status`    |


See the [sync guide](guides/sync.md) for the allowlist/exclude config that drives the state machine, and the operator CLI (`melt sync list`, `melt sync status`, `melt sync refresh`).

## One-shot subcommands


| Subcommand                                   | Purpose                                                                                       |
| -------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `melt status [--json]`                       | Read-only one-shot: proxy listener, catalog + Snowflake reachability, sync lag, policy state. |
| `melt route "<sql>"`                         | Read-only one-shot: print the routing decision and translated SQL for one statement.          |
| `melt sync list` / `sync status <FQN>`       | Read-only introspection of sync state.                                                        |
| `melt sync reload`                           | Hot-reload `[sync]` config against a running proxy via `POST /admin/reload`.                  |
| `melt sync refresh <FQN>`                    | Force a re-bootstrap of one table.                                                            |
| `melt bootstrap server` / `bootstrap client` | One-shot TLS bootstrap — see the [TLS guide](guides/tls.md).                                  |


All of these work against the same `melt.toml` — they're not separate binaries, just different entry points into the crate. `bootstrap` is pre-config (runs before you have a working `melt.toml`).

### `melt status`

Connects to the catalog, prints a one-screen summary: catalog reachable Y/N, number of tables tracked, number of policy markers, age of the last policy refresh, max sync lag across tables, current policy mode, lake-routing threshold, and the resolved Snowflake host. Useful as a CI check or daily Slack ping. Pass `--json` to get a structured payload for scripts/monitoring.

### `melt route "<sql>"`

The cheapest debugging tool in the binary. Doesn't need Postgres, S3, or Snowflake. Parses your SQL with the Snowflake dialect, runs it through the classifier (write detection, Snowflake-feature detection, allowlist check), runs the translation passes, and prints what the router would have done plus the translated DuckDB-dialect SQL. Use it to verify that a query you care about will route to Lake before deploying.

## Hot reload

`POST /admin/reload` (`melt sync reload`) is validate-then-apply:

1. Re-reads `melt.toml` from the path the process was launched with.
2. Parses + rebuilds the `SyncTableMatcher`. On any error, nothing is mutated and the endpoint returns 400 with the failing field named.
3. Atomically swaps the new matcher into the router via `ArcSwap::store`.

Fields that require a restart (listen addresses, TLS material, Snowflake account, backend pools) land in the response's `skipped` array — the endpoint tells you what it didn't touch.

## Graceful shutdown

`SIGTERM` triggers `axum_server::Handle::graceful_shutdown(shutdown_drain_timeout)`. The proxy stops accepting new connections, in-flight statements get the configured drain budget to finish, then the process exits. `SIGINT` (Ctrl-C) does the same.

