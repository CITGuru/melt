# Sync: what Melt mirrors to the lake

Sync has two sources of truth:

- **Config allowlist** — `[sync].include` globs are always mirrored.
- **Query-time auto-discovery** — any table a query touches that isn't excluded is registered as `pending` and bootstrapped on the sync loop's next tick. Unknown tables passthrough to Snowflake until the bootstrap completes.

For a description of what the sync service actually runs, see [architecture.md](../architecture.md#melt-sync-run).

## State machine

Every mirrored table lives in one of four states:

| State | Router behavior | How it's reached |
|---|---|---|
| `pending` | Force Snowflake passthrough | First query for a new table, or `melt sync refresh` |
| `bootstrapping` | Force Snowflake passthrough | Sync picked up the pending row and is running `SHOW_INITIAL_ROWS` |
| `active` | Eligible for Lake routing | Bootstrap succeeded |
| `quarantined` | Force Snowflake passthrough | Bootstrap failed; see `bootstrap_error` via `melt sync status` |

## Config

```toml
[sync]
auto_discover = true                      # default: on

include = [
    "ANALYTICS.PUBLIC.ORDERS",            # exact FQN
    "ANALYTICS.MARTS.*",                  # all tables in schema
    "DATA_*.STAGING.*",                   # DB prefix wildcard
]

exclude = [
    "ANALYTICS.LEGACY.*",
    # SNOWFLAKE.*, *.INFORMATION_SCHEMA.*, *.*._STAGE_* are built-in
    # (toggle with [sync.lazy].exclude_system_schemas = false).
]

# Tables matching these globs are NEVER synced — queries that touch
# them route through dual execution instead. See the dual execution
# guide.
remote = [
    # "BIG_WAREHOUSE.*",
]

[sync.lazy]
max_initial_bytes         = "50GB"
demotion_idle_days        = 30
max_concurrent_bootstraps = 2
# See the service-authentication guide for the required
# APPLY CHANGE TRACKING grant.
auto_enable_change_tracking = false
exclude_system_schemas    = true
demotion_interval         = "1h"
```

Precedence: `exclude` > `remote` > `include` > auto-discovery. Identifiers are uppercase-normalized before matching. See [dual execution](dual-execution.md) for what `remote` does and when to use it.

## Operator CLI

```bash
# Live reload against a running proxy — hits POST /admin/reload.
melt --config melt.toml sync reload

# What's tracked?
melt --config melt.toml sync list
melt --config melt.toml sync list --state pending
melt --config melt.toml sync list --state quarantined --json

# Dig into one table: state, last sync, bootstrap error, lag.
melt --config melt.toml sync status ANALYTICS.PUBLIC.ORDERS

# Force a re-bootstrap (useful when the stream went stale or schema drifted).
melt --config melt.toml sync refresh ANALYTICS.PUBLIC.ORDERS --yes
```

`sync reload` resolves its admin endpoint from `[metrics].listen` and the bearer token from `[metrics].admin_token_file` (override with `--admin` / `--token-file` / `MELT_ADMIN_TOKEN`).

## Hot-reload semantics

`POST /admin/reload` is validate-then-apply. The endpoint:

1. Re-reads `melt.toml` from the path it was launched with.
2. Parses + rebuilds the `SyncTableMatcher`. On any error, nothing is mutated and the endpoint returns 400 with the failing field named.
3. Atomically swaps the new matcher into the router via `ArcSwap::store`.

Fields that require a restart (listen addresses, TLS material, Snowflake account, backend pools) land in the response's `skipped` array — the endpoint tells you what it didn't touch.

## Admin listener auth

```toml
[metrics]
listen           = "0.0.0.0:9090"           # public → token required
admin_token_file = "/etc/melt/admin.token"  # 0600, one line
```

When `listen` is loopback (`127.0.0.1:*`) and no token is configured, unauthenticated reloads are allowed. When `listen` is non-loopback and no token is set, `melt start` / `melt all` refuses to start — we don't ship an unauthed admin surface by default.
