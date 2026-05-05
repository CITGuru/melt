# Melt

Melt is an open-source, self-hosted Rust proxy that sits in front of Snowflake and transparently routes SQL to a cheaper DuckDB-backed lakehouse (DuckLake or Iceberg) whenever it can. **Change one connection string, melt your Snowflake bill.**

Any client that speaks Snowflake's REST wire protocol — JDBC, the Python connector, Go, Looker, Sigma, Hex, dbt — connects to Melt unmodified. Every statement gets its own routing decision. The lake copy stays fresh via CDC streams pulled out of Snowflake.

## Why Melt exists

Human-driven Snowflake workloads have natural throttles — analysts run dashboards a few times a day, dbt models materialize overnight, ad-hoc queries happen when someone asks a question. AI agents don't run at that cadence. Copilots, research agents, and autonomous pipelines now generate and execute SQL at machine speed: tens of queries per prompt, thousands per day per agent, most of them small filters and joins the agent is iterating on until the answer looks right. Each one bills a warehouse.

The bet behind Melt is that most of those reads don't actually need Snowflake compute. If the underlying Parquet is already on S3 via a lakehouse table, DuckDB can run the small-to-medium query locally for cents — no warehouse spin-up, no credit burn, no minimum billing window. Writes, Snowflake-specific features, and genuinely large joins stay on Snowflake, where they belong.

The routing decision is per-query and invisible to the agent or driver issuing it. You don't rewrite your dbt project, your BI tool, or your agent's prompt. You change one connection string, and the queries that can run cheaper, do.

## How Melt routes a query

For each incoming statement Melt classifies the query (write detection, Snowflake-feature probe, allowlist / policy gates) and emits one of three routes:


| Route                                   | What runs where                                                                                                                  | When it fires                                                                                                                                                                                                        |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Lake** *(default for eligible reads)* | Translate the SQL to DuckDB dialect; run it locally over Iceberg/DuckLake (Parquet on S3).                                       | Read-only, all referenced tables synced + active, no policy markers, estimated scan under `[router].lake_max_scan_bytes`.                                                                                            |
| **Snowflake passthrough**               | Forward the request verbatim to upstream Snowflake.                                                                              | Writes, Snowflake-only features, policy-protected tables, oversize estimates, and anything the lake side can't safely handle.                                                                                        |
| **Dual execution** *(opt-in)*           | Plan-split: some operators run locally on DuckDB, some get pushed to Snowflake; results bridge via Arrow IPC and join in DuckDB. | Lifts the all-or-nothing passthrough cliff for queries that touch declared-remote tables, bootstrapping tables, or one oversize table among smaller ones. Off by default; opt in per case under `[router].hybrid_`*. |


Dual execution preserves the routing invariant: whatever Melt returns equals what Snowflake would have returned. It refuses to federate anything touching policy-protected tables, ships behind a parity sampler, and surfaces every mixed plan in `melt route` output and metrics.

`melt route "<sql>"` (no infrastructure required) prints the route, the reason, the translated DuckDB SQL, and — for dual execution — the annotated plan tree.

## Architecture at a glance

Melt has two long-running services. Run them together for dev / small prod, or split for horizontal scale:

- **Proxy** — terminates client connections, forwards login to Snowflake, runs the per-statement routing decision, executes Lake queries against DuckDB, forwards passthrough requests, and orchestrates dual-execution plans.
- **Sync** — pulls CDC out of Snowflake into the lakehouse, refreshes policy markers / filtered views, and bootstraps newly-discovered tables. Single-writer (one sync pod per lake).

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
├── docs/                 # Public documentation (overview, architecture, guides, config reference)
├── melt.toml             # Placeholder config (copy to melt.local.toml for dev)
└── docker-compose.yml    # Melt + Postgres + MinIO dev stack
```

## Quick start (Docker — recommended)

```bash
# Bring up melt + Postgres (DuckLake catalog) + MinIO (S3)
docker compose up --build

# Inspect routing for a SQL string — no Snowflake credentials required
docker compose run --rm melt route "SELECT * FROM analytics.public.events"

# Backend / sync / policy summary
docker compose run --rm melt status

# Tear down + wipe state
docker compose down --volumes
```


| URL                             | What                                |
| ------------------------------- | ----------------------------------- |
| `http://localhost:8443`         | Melt proxy (Snowflake REST shape)   |
| `http://localhost:9090/metrics` | Prometheus exposition               |
| `http://localhost:9090/healthz` | Liveness                            |
| `http://localhost:9090/readyz`  | Readiness (catalog ping)            |
| `http://localhost:9001`         | MinIO console (`melt` / `meltmelt`) |


End-to-end demos that talk to Melt with the **official** Snowflake drivers (Python and Rust), unmodified, live in `[examples/](examples/)`.

## Connect with the Snowflake Python connector

With Melt running on `127.0.0.1:8443`, use the official `snowflake-connector-python` and override the host:

```bash
pip install snowflake-connector-python
```

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user="u",
    password="p",
    account="xy12345",        # must match [snowflake].account in melt.toml
    host="127.0.0.1",         # point at Melt instead of snowflakecomputing.com
    port=8443,
    protocol="http",          # plain HTTP for local dev; TLS in prod
)

cur = conn.cursor()
cur.execute("SELECT IFF(1 > 0, 'yes', 'no') AS answer")
print(cur.fetchall())        # → [('yes',)]
```

The only difference from a direct Snowflake connection is `host` / `port` / `protocol`. The `account` field must match `[snowflake].account` in `melt.toml` or Melt rejects the login with Snowflake error `390201` — this guardrail prevents a misconfigured driver from silently routing to the wrong upstream.

For production, drop `protocol="http"` and point `host` at a hostname you own with a real TLS cert on Melt — see the [TLS guide](docs/guides/tls.md). For the JDBC, ODBC, and Rust connector equivalents, plus curl and account-name handling detail, see [Issuing queries](docs/guides/issuing-queries.md).

## Quick start (cargo only)

```bash
cargo build --workspace

cp melt.toml melt.local.toml      # melt.local.toml is gitignored
$EDITOR melt.local.toml

cargo run --bin melt -- --config melt.local.toml all          # proxy + sync
# or split:
cargo run --bin melt -- --config melt.local.toml start        # proxy only
cargo run --bin melt -- --config melt.local.toml sync run     # sync only
```

## Quick start: `melt audit` — `$/savings` projection

`melt audit` is a read-only CLI that reads `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` and produces an account-specific dollar-savings projection — no DuckDB execution, nothing leaves the box. See [the spec](#) for the full output format.

### Try it on the bundled fixture (no Snowflake required)

```bash
cargo build -p melt-audit

cargo run -p melt-audit -- \
  --fixture examples/audit/query-history-fixture.csv \
  --account ACME-DEMO \
  --window 30d \
  --out-dir /tmp/melt-audit-demo
```

This drives the full local pipeline (classify → bucket → aggregate → render) on a 10-row synthetic `QUERY_HISTORY` export and writes:

- stdout: spec §1 summary table + top routable patterns
- `melt-audit-ACME-DEMO-<date>.json` — schema-versioned JSON (`schema_version: 1`)
- `melt-audit-ACME-DEMO-<date>.talkingpoints.md` — paste-into-Slack markdown

Fixture mode never opens a network connection, so `git clone` → first audit run is a single `cargo build` away.

### Print the Snowflake grants (paste-and-go)

```bash
cargo run -p melt-audit -- --print-grants
```

Outputs the role-creation snippet from spec §2:

```sql
CREATE ROLE IF NOT EXISTS MELT_AUDIT;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE MELT_AUDIT;
GRANT USAGE ON WAREHOUSE <WAREHOUSE_NAME> TO ROLE MELT_AUDIT;
GRANT ROLE MELT_AUDIT TO USER <USER_NAME>;
```

### Live mode (Snowflake account)

```bash
cargo run -p melt-audit -- \
  --account <locator> \
  --user <service-user> \
  --token <pat-or-oauth> \
  --window 30d
```

The HTTP path against `ACCOUNT_USAGE.QUERY_HISTORY` is wired in a follow-up commit on the `feat/melt_audit_binary` branch — until then the binary surfaces a remediation hint that names the missing `MELT_AUDIT` role grants. Use the fixture path or `--print-grants` today.

## Documentation

- [Overview](docs/overview.md) — what Melt is, how it's split into services, the three routes.
- [Architecture](docs/architecture.md) — core architecture
- [Configuration reference](docs/configuration.md) — every field in `melt.toml`.
- [CLI reference](crates/melt-cli/readme.md) — the `melt` binary's subcommands and flags.

Guides:

- [Docker Compose quickstart](docs/guides/quickstart-docker.md)
- [Local quickstart (DuckLake / Iceberg)](docs/guides/quickstart-local.md)
- [Issuing queries (curl, Python, Rust)](docs/guides/issuing-queries.md)
- [Service authentication (sync creds + Snowflake grants)](docs/guides/service-authentication.md)
- [Sync (allowlist, state machine, hot reload)](docs/guides/sync.md)
- [Object storage (AWS / MinIO / R2 / B2 / Wasabi)](docs/guides/object-storage.md)
- [TLS (localhost + two production paths)](docs/guides/tls.md)
- [Policy modes (passthrough / allowlist / enforce)](docs/guides/policy-modes.md)

## License

Apache-2.0.