# Melt configuration reference

Every field Melt reads from `melt.toml`, grouped by TOML table. Every row lists the field, its type, whether it's required, its default, and what it does.

See [`melt.toml`](../melt.toml) for a worked example and [`docker/melt.docker.toml`](../docker/melt.docker.toml) for the in-compose shape. For local development, copy one of those to `melt.local.toml` (gitignored) and edit it:

```bash
cp melt.toml melt.local.toml
# ...edit to taste — real PATs, bucket names, passwords...
melt --config melt.local.toml all
```

Durations accept any [`humantime`](https://docs.rs/humantime) string (`"30s"`, `"5m"`, `"1h30m"`). Byte sizes accept [`bytesize`](https://docs.rs/bytesize) strings (`"100GB"`, `"2GiB"`, `"50000000"`).

## Table of contents

- [`[proxy]`](#proxy) — driver-facing HTTP/TLS listener
- [`[proxy.limits]`](#proxylimits) — per-session and global request caps
- [`[snowflake]`](#snowflake) — upstream account + sync service credentials
- [`[snowflake.policy]`](#snowflakepolicy) — row-access / masking policy handling
- [`[router]`](#router) — Lake vs. Snowflake routing knobs
- [`[metrics]`](#metrics) — admin listener (metrics, healthz, reload endpoint)
- [`[sync]`](#sync) — what Melt mirrors and how
- [`[sync.lazy]`](#synclazy) — discovery tunables
- [`[sync.views]`](#syncviews) — view-aware sync (decomposition vs. stream-on-view)
- [`[backend.ducklake]`](#backendducklake) — DuckLake backend
- [`[backend.ducklake.s3]`](#backendducklakes3-or-backendicebergs3) — S3 credentials for DuckLake
- [`[backend.iceberg]`](#backendiceberg) — Iceberg backend
- [`[backend.iceberg.s3]`](#backendducklakes3-or-backendicebergs3) — S3 credentials for Iceberg

Exactly one of `[backend.ducklake]` or `[backend.iceberg]` must be present. All other tables are required (except `[sync]` and `[metrics]`, which have sensible defaults).

---

## `[proxy]`

Driver-facing TCP/TLS listener.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `listen` | `SocketAddr` | yes | — | Bind address, e.g. `"0.0.0.0:8443"` |
| `tls_cert` | path | yes* | — | PEM server certificate. If the file is missing, Melt falls back to plain HTTP with a `WARN` — **local dev only** |
| `tls_key` | path | yes* | — | PEM private key paired with `tls_cert` |

\* Technically optional (plain-HTTP fallback), but production deployments must set both. See the [TLS guide](guides/tls.md) for cert strategies.

## `[proxy.limits]`

| Field | Type | Default | Description |
|---|---|---|---|
| `request_timeout` | duration | `"30s"` | Max time any single HTTP request can take before tower's timeout layer drops it |
| `max_concurrent_per_session` | u32 | `16` | Cap on in-flight `/api/v2/statements` per Snowflake session |
| `max_concurrent_global` | u32 | `256` | Cap on in-flight statements across all sessions |
| `result_store_max_bytes` | byte size | `"2GB"` | Memory ceiling for paginated result batches |
| `result_store_max_entries` | u32 | `10000` | Cap on number of result-store entries regardless of size |
| `result_store_idle_ttl` | duration | `"5m"` | Evict result-store entries that haven't been polled for this long |
| `shutdown_drain_timeout` | duration | `"30s"` | Grace period on SIGTERM/SIGINT before the listener hard-closes |

---

## `[snowflake]`

Upstream account, service credentials for sync, and retry tuning.

### Account + transport

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `account` | string | yes | — | Org-account form (`ACMECORP-PROD123`) or legacy locator (`xy12345`). Driver `ACCOUNT_NAME` is validated against this at login — mismatches return Snowflake error `390201` |
| `host` | string | no | *derived* | Explicit upstream hostname. Required for legacy-locator accounts outside `us-west-2`, PrivateLink, or custom CNAMEs. Alias: `host_override` (back-compat). Empty → `<account>.snowflakecomputing.com` |
| `request_timeout` | duration | no | `"60s"` | Per-request timeout to upstream Snowflake |
| `max_retries` | u8 | no | `3` | Retry budget on transient upstream errors |

### Service authentication (for sync)

**Pick exactly one credential field.** These drive Melt's own calls to Snowflake (CDC reader, policy refresh, stream creation). Drivers connecting through the proxy supply their own credentials at login — these never affect passthrough.

| Field | Type | Notes |
|---|---|---|
| `pat` | string | Inline Programmatic Access Token. Simplest path. |
| `pat_file` | path | File whose contents is a PAT. Preferred for k8s Secret volume mounts. |
| `private_key` | PEM | Inline RSA private key (PKCS#8 or PKCS#1). Rarely used — TOML line-wrapping is awkward. |
| `private_key_file` | path | Path to a `.p8` / `.pem` file. Production-grade. |

Related fields (some conditional):

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `user` | string | yes when using `private_key` / `private_key_file` | — | Service user's Snowflake login name. JWT signer needs to name the user explicitly. Ignored for PAT paths. |
| `role` | string | no | — | Execution role. Empty → service user's `DEFAULT_ROLE`. |
| `warehouse` | string | no | — | Compute warehouse. Empty → service user's `DEFAULT_WAREHOUSE`. |
| `database` | string | no | — | Default database for unqualified identifiers. **Required** when `[snowflake.policy].mode` is `passthrough` or `enforce`, because `INFORMATION_SCHEMA.POLICY_REFERENCES` is per-database. |
| `schema` | string | no | — | Default schema. |

See the [service authentication guide](guides/service-authentication.md) for the required Snowflake grants.

---

## `[snowflake.policy]`

How Melt honors Snowflake's row-access / masking / column-level policies.

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | enum | `"passthrough"` | `"passthrough"` \| `"allowlist"` \| `"enforce"` (see table below) |
| `refresh_interval` | duration | `"60s"` | How often sync rescans Snowflake's policy references |
| `tables` | array of FQNs | — | **Required** when `mode = "allowlist"`; the only tables eligible for Lake routing |

Mode behavior:

| Mode | What it does | Operator burden |
|---|---|---|
| `passthrough` | Sync marks every policy-protected table; router refuses to Lake-route them. Safest default. | None |
| `allowlist` | Default-deny. Only tables named in `tables` are Lake-eligible. | High — audit each table |
| `enforce` | Sync translates row-access bodies to DuckDB `WHERE` clauses + materializes filtered views; router rewrites. Untranslatable bodies fall back to passthrough markers. | Medium |

---

## `[router]`

| Field | Type | Default | Description |
|---|---|---|---|
| `lake_max_scan_bytes` | byte size | `"100GB"` | Queries whose estimated scan bytes exceed this threshold go to Snowflake passthrough |
| `table_exists_cache_ttl` | duration | `"5m"` | In-memory cache TTL for "does this table exist in the lake?" lookups |
| `estimate_bytes_cache_ttl` | duration | `"1m"` | Cache TTL for per-table byte-size estimates |

### Hybrid (dual-execution) routing — opt-in

When enabled, the router can emit `Route::Hybrid` for queries that touch declared-remote tables (matched by `[sync].remote` globs) — running the lake-resident parts locally on DuckDB and federating the Snowflake-resident parts via the community Snowflake DuckDB extension (Attach strategy) or — once Materialize lands — Arrow IPC + temp-table staging. Opt-in per the rollout plan in [docs/internal/DUAL_EXECUTION.md](internal/DUAL_EXECUTION.md).

| Field | Type | Default | Description |
|---|---|---|---|
| `hybrid_execution` | bool | `false` | Master switch. When false, Remote-classified tables passthrough as today (safe default). |
| `hybrid_max_remote_scan_bytes` | byte size | `"5GiB"` | Sum across all Materialize fragments. Above this the whole query collapses to passthrough with `AboveThreshold`. |
| `hybrid_max_fragment_bytes` | byte size | `"2GiB"` | Per-fragment cap. Prevents one fragment eating the whole budget. |
| `hybrid_attach_enabled` | bool | `true` | Set false to force every Remote node to Materialize (kill switch when the community Snowflake extension misbehaves). The pool also flips it off automatically when the extension fails to load. |
| `hybrid_max_attach_scan_bytes` | byte size | `"10GiB"` | Per-Attach-scan raw estimate cap. Above this the strategy selector downgrades the node to Materialize. |
| `hybrid_allow_bootstrapping` | bool | `false` | Allow Pending/Bootstrapping tables to be served via the remote pool while the lake catches up. |
| `hybrid_allow_oversize` | bool | `false` | Allow a single oversize lake table to be served via the remote pool while the rest of the query stays local. |
| `hybrid_parity_sample_rate` | float | `0.01` | Probability with which the parity sampler replays the original SQL against pure Snowflake to detect type-drift mismatches. |
| `hybrid_profile_attach_queries` | bool | `false` | When true, `execute_hybrid` reads DuckDB's profiler JSON after each Attach query and logs the `snowflake_scan` operator's emitted SQL. Off by default — profiler overhead is non-trivial. |

---

## `[metrics]`

Admin HTTP server: `/metrics`, `/healthz`, `/readyz`, `POST /admin/reload`.

| Field | Type | Default | Description |
|---|---|---|---|
| `listen` | `SocketAddr` | — | Bind address. Omit to disable the admin server entirely. |
| `log_format` | enum | `"pretty"` | `"json"` \| `"pretty"` |
| `log_level` | string | `"info"` | Any `tracing_subscriber::EnvFilter`-compatible value (`"info,melt_router=debug"`, etc.) |
| `admin_token` | string | — | Inline bearer token for `POST /admin/reload`. Dev only. |
| `admin_token_file` | path | — | File containing the bearer token. Preferred. |

**Auth contract:**

- Both `admin_token` + `admin_token_file` unset AND `listen` is loopback → allow unauthenticated reloads.
- Both unset AND `listen` is non-loopback → Melt refuses to start.
- Either set → `POST /admin/reload` requires `Authorization: Bearer <token>`.

Comparison uses constant-time byte equality.

---

## `[sync]`

What Melt mirrors from Snowflake to the lake, and whether the router auto-registers unknown tables.

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_discover` | bool | `true` | When true, any table a query touches that isn't in `exclude` is registered as `pending` and sync bootstraps it on the next tick |
| `include` | array of globs | `[]` | Always-synced FQN patterns. Immune to idle demotion. Globs match case-insensitively against `DB.SCHEMA.TABLE` (e.g. `"ANALYTICS.MARTS.*"`, `"DATA_*.STAGING.*"`) |
| `exclude` | array of globs | `[]` | Wins over `include` and auto-discovery. Built-in excludes (`SNOWFLAKE.*`, `*.INFORMATION_SCHEMA.*`, `*.*._STAGE_*`) are additive |
| `remote` | array of globs | `[]` | **Hybrid (dual-execution)**. Tables matching these globs are NEVER synced — queries that touch them route through `Route::Hybrid` (Attach for single-scan nodes, Materialize for collapsed multi-scan subtrees) when `[router].hybrid_execution = true`. See [docs/internal/DUAL_EXECUTION.md](internal/DUAL_EXECUTION.md). |

Precedence: `exclude` > `remote` > `include` > auto-discovery. Identifiers are uppercase-normalized before matching. `exclude` wins over `remote` so a defensive `exclude = ["SNOWFLAKE.*"]` still bites under a permissive `remote = ["*.*.*"]`.

## `[sync.lazy]`

Tunables for the discovery path. All only matter when `auto_discover = true`.

| Field | Type | Default | Description |
|---|---|---|---|
| `max_initial_bytes` | byte size | `"50GB"` | Bootstrap rejects auto-discovered tables larger than this; row lands in `quarantined`. Does **not** apply to tables in `[sync].include` |
| `demotion_idle_days` | u32 | `30` | Drop auto-discovered tables not queried in this many days. `include` tables are immortal. |
| `max_concurrent_bootstraps` | u32 | `2` | Cap on how many `pending` tables sync bootstraps in parallel |
| `auto_enable_change_tracking` | bool | `false` | When true, Melt runs `ALTER TABLE ... SET CHANGE_TRACKING = TRUE` on bootstrap. Also applied to the underlying tables of a view when `[sync.views]` is in play. Requires `GRANT APPLY CHANGE TRACKING ON SCHEMA ... TO ROLE MELT_SYNC_ROLE` in Snowflake. |
| `exclude_system_schemas` | bool | `true` | Apply the built-in exclude list (`SNOWFLAKE.*`, `*.INFORMATION_SCHEMA.*`, `*.*._STAGE_*`) |
| `demotion_interval` | duration | `"1h"` | Cadence of the idle-table sweeper. Also throttles the view-body drift rescan. |

## `[sync.views]`

View-aware sync. Controls how Snowflake views are bootstrapped into the lake.

Melt recognizes two strategies per view:

| Strategy | When it applies | What lands in the lake |
|---|---|---|
| `decomposed` (preferred) | View body is expressible in DuckDB; all base-table deps are eligible for sync | A DuckDB `CREATE OR REPLACE VIEW` at the view's FQN, resolved at query time against the synced base tables. No extra storage. |
| `stream_on_view` (fallback) | View body passes Snowflake's stream-on-view restrictions (no `GROUP BY`, `DISTINCT`, `QUALIFY`, `LIMIT`, correlated subqueries, non-deterministic functions) | The view's output materializes at its FQN as a regular Iceberg/DuckLake table, refreshed by a CDC stream (`CREATE STREAM ... ON VIEW`). |

If neither works, the view lands in `quarantined` with a specific reason (`view_body_unsupported`, `secure_view_unsupported`, etc.). Secure views and materialized views are never supported.

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_include_dependencies` | bool | `true` | When true, a view's base-table dependencies get auto-registered with `source = 'view_dependency'` and sync alongside the parent. Turn off to require every base table to appear explicitly in `[sync].include`. |
| `prefer_stream_on_view` | bool | `false` | When true, skip decomposition and go straight to stream-on-view. Pick this when you know your views use Snowflake-only functions that don't translate to DuckDB. |
| `max_dependency_depth` | u32 | `4` | Cap on how deep sync follows views-on-views when resolving dependencies. Beyond this depth the view lands in `quarantined` with `dep_graph_too_deep`. |

Backend support: the `decomposed` strategy is **DuckLake-only** in the current release. The Iceberg backend handles views via stream-on-view only; decomposition on Iceberg is a follow-up (tracked in `crates/melt-iceberg/src/sync/mod.rs`).

---

## `[backend.ducklake]`

Postgres catalog + S3 data files, accessed through DuckDB's `ducklake` extension.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `catalog_url` | string | yes | — | Postgres connection string, e.g. `"postgres://melt:secret@db/melt_catalog"`. Doubles as the control-plane catalog (sync state, policy markers). |
| `data_path` | string | yes | — | Where DuckLake writes Parquet + snapshots, e.g. `"s3://acme-melt/ducklake/"`. |
| `reader_pool_size` | usize | no | `16` | Parallel DuckDB reader connections. |
| `writer_pool_size` | usize | no | `1` | DuckLake enforces single-writer — keep this at 1. |

## `[backend.iceberg]`

Iceberg REST / Glue catalog + S3 data files, accessed through DuckDB's `iceberg` extension.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `catalog` | enum | yes | — | `"rest"` \| `"glue"` \| `"polaris"` \| `"hive"` (Hive is unsupported; use REST shim) |
| `warehouse` | string | yes | — | Iceberg warehouse root URI, e.g. `"s3://acme-melt/iceberg/"` |
| `rest_uri` | string | when `catalog = "rest"` or `"polaris"` | — | Iceberg REST catalog endpoint |
| `control_catalog_url` | Postgres URL | when sync enabled | — | Postgres control plane (sync state, policy markers). Typically points at the same Postgres as a parallel DuckLake deployment. Empty → sync runs headless and auto-discovery is disabled. |
| `reader_pool_size` | usize | no | `16` | Parallel DuckDB reader connections. |

Environment overrides (for REST catalog auth):

- `MELT_POLARIS_TOKEN` — bearer for Polaris catalogs
- `MELT_ICEBERG_TOKEN` — generic fallback

---

## `[backend.ducklake.s3]` or `[backend.iceberg.s3]`

S3-compatible storage credentials. Same shape for both backends; rendered into DuckDB's `CREATE SECRET (TYPE S3, …)`. Works with AWS, MinIO, Cloudflare R2, Backblaze B2, Wasabi, Ceph, GCS (HMAC).

| Field | Type | Default | Description |
|---|---|---|---|
| `region` | string | — | Required when the block is present. Use `"auto"` for R2. |
| `endpoint` | string | — | Omit for AWS. Set for MinIO (`"localhost:9000"`), R2 (`"<acct>.r2.cloudflarestorage.com"`), etc. |
| `url_style` | enum | `"vhost"` | `"vhost"` for AWS/R2/B2; `"path"` for MinIO, Ceph, localhost. |
| `use_ssl` | bool | `true` | Set `false` for http MinIO. |
| `access_key_id` | string | — | Inline key. **Dev only.** |
| `access_key_id_env` | env var name | — | Read the key from an env var (e.g. `"AWS_ACCESS_KEY_ID"`). |
| `secret_access_key` | string | — | Inline secret. Dev only. |
| `secret_access_key_env` | env var name | — | Read the secret from an env var. |
| `session_token` | string | — | STS session token (temp creds). |
| `session_token_env` | env var name | — | Read the session token from an env var. |
| `scope` | string | — | Restrict this secret to a bucket/prefix, e.g. `"s3://acme-melt/"`. Lets multiple S3 secrets coexist. |

**Credential resolution:**

1. Inline fields (`access_key_id` + `secret_access_key`) if both set.
2. Env-var fields (`access_key_id_env` + `secret_access_key_env`) if both named.
3. Both empty → DuckDB's built-in `PROVIDER credential_chain` (EC2 IMDS, `~/.aws/credentials`, SSO, ECS/EKS task roles). The right default on AWS.

Mixing inline + env for the same field is a validation error.

---

## Minimal working config

```toml
[proxy]
listen   = "127.0.0.1:8443"
tls_cert = "/tmp/no-cert"   # missing → plain HTTP (dev only)
tls_key  = "/tmp/no-key"

[snowflake]
account = "ACMECORP-PROD123"
pat     = "<PAT from ALTER USER ADD PROGRAMMATIC ACCESS TOKEN>"
role    = "MELT_SYNC_ROLE"
warehouse = "MELT_SYNC_WH"
database  = "ANALYTICS"

[metrics]
listen = "127.0.0.1:9090"

[backend.ducklake]
catalog_url = "postgres://melt:melt@localhost:5432/melt_catalog"
data_path   = "s3://melt/ducklake/"

[backend.ducklake.s3]
region = "us-east-1"
```

That's everything sync + the proxy need; `[proxy.limits]`, `[router]`, `[sync]`, `[snowflake.policy]`, and the other optional tables fall back to their defaults.

---

## Hot-reload semantics

`POST /admin/reload` (via `melt sync reload`) re-reads this file from the path Melt was launched with and applies the subset of fields that can be changed without rebinding sockets or reconnecting pools:

| Hot-reloadable | Restart required |
|---|---|
| `[sync].include`, `exclude`, `auto_discover` | `[proxy].listen`, `tls_cert`, `tls_key` |
| `[sync.lazy]` tunables (matcher rebuild) | `[snowflake].account`, `host` |
| `[sync.views]` knobs (next bootstrap tick picks them up) | `[backend.*]` connection pools + S3 secret |
| *(more fields to follow in later phases)* | `[metrics].listen` |

Any restart-required field that changed in the file is reported in the response's `skipped` array — the endpoint tells you what it couldn't apply.
