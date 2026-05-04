# What is Melt?

Melt is a self-hosted reverse proxy that sits in front of your Snowflake account and speaks Snowflake's REST API exactly. BI tools, drivers, and `dbt` connect to Melt as if it were Snowflake. For every incoming statement Melt makes a per-query routing decision:

- **Lake** — translate the SQL to DuckDB dialect and run it against a local lakehouse (DuckLake on Postgres + Parquet, or Iceberg on a REST catalog). Cheap, fast for small/medium analytics.
- **Snowflake** — forward the request to your real Snowflake account unchanged. Heavy joins, Snowflake-specific features, and writes always go this way.

The lake copy stays current via CDC streams pulled out of Snowflake. Most BI dashboards end up on Lake; the long tail (writes, complex transforms, Snowflake-only features) stays on Snowflake. Net effect for typical workloads: similar correctness, far lower spend.

## How Melt is split into processes

Melt has two long-running services:

- **Proxy** — terminates client connections, forwards login to Snowflake, and routes statements per request. Stateless; horizontally scalable.
- **Sync** — pulls CDC from Snowflake into the lakehouse and refreshes policy markers / filtered views. Single-writer (the lakehouse only allows one writer at a time).

You can run them in one process (`melt all`) or separately (`melt start` for proxy-only, `melt sync run` for sync-only). Split deployments are the production shape: multiple proxy pods behind a load balancer plus one sync pod.

See [architecture.md](architecture.md) for what each subcommand actually does at runtime.

## Routes

For each incoming statement Melt classifies the query (write detection, Snowflake-feature probe, allowlist / policy gates) and emits one of three routes:

| Route | What runs where | When it fires |
|---|---|---|
| **Lake** *(default for eligible reads)* | Translate the SQL to DuckDB dialect; run it locally over Iceberg/DuckLake (Parquet on S3). | Read-only, all referenced tables synced + active, no policy markers, estimated scan under `[router].lake_max_scan_bytes`. |
| **Snowflake passthrough** | Forward the request verbatim to upstream Snowflake. | Writes, Snowflake-only features, policy-protected tables, oversize estimates, and anything the lake side can't safely handle. |
| **Dual execution** *(opt-in)* | Plan-split: some operators run locally on DuckDB, some get pushed to Snowflake; results bridge via Arrow IPC and join in DuckDB. | Lifts the all-or-nothing passthrough cliff for queries that touch declared-remote tables, bootstrapping tables, or one oversize table among smaller ones. Off by default; opt in per case under `[router].hybrid_*`. |

Dual execution preserves the routing invariant: whatever Melt returns equals what Snowflake would have returned. It refuses to federate anything touching policy-protected tables, ships behind a parity sampler, and surfaces every mixed plan in `melt route` output and metrics.

`melt route "<sql>"` (no infrastructure required) prints the route, the reason, the translated DuckDB SQL, and — for dual execution — the annotated plan tree.

## Where to go next

- [Docker Compose quickstart](guides/quickstart-docker.md) — fastest path to a running Melt.
- [Local quickstart](guides/quickstart-local.md) — DuckLake or Iceberg without Compose.
- [Issuing queries](guides/issuing-queries.md) — connect drivers (curl, Python, Rust).
- [Dual execution](guides/dual-execution.md) — opt-in hybrid routing for tables you don't want to sync.
- [Configuration reference](configuration.md) — every field in `melt.toml`.
- [CLI reference](../crates/melt-cli/readme.md) — the `melt` binary's subcommands and flags.
