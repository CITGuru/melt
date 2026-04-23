# Quickstart: Local (without Docker Compose)

For when you want the same dependencies as the Compose stack but running ad-hoc — e.g., iterating on Melt itself via `cargo run`. If you just want to evaluate Melt, use the [Docker Compose quickstart](quickstart-docker.md) instead.

Two paths below: DuckLake (Postgres catalog + S3/MinIO storage) or Iceberg (REST catalog + S3 storage). Pick one — a single Melt process runs exactly one backend.

## DuckLake

### 1. Spin up Postgres + MinIO

```bash
docker run -d --name melt-pg \
  -e POSTGRES_USER=melt -e POSTGRES_PASSWORD=melt \
  -e POSTGRES_DB=melt_catalog \
  -p 5432:5432 postgres:16

docker run -d --name melt-minio \
  -e MINIO_ROOT_USER=melt -e MINIO_ROOT_PASSWORD=meltmelt \
  -p 9000:9000 -p 9001:9001 \
  minio/minio server /data --console-address ":9001"

docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID=melt -e AWS_SECRET_ACCESS_KEY=meltmelt \
  amazon/aws-cli --endpoint-url http://localhost:9000 \
  s3 mb s3://melt
```

### 2. Local config

`melt.local.toml`:

```toml
[proxy]
listen   = "127.0.0.1:8443"
tls_cert = "/tmp/no-cert"   # missing → falls back to plain HTTP for local dev
tls_key  = "/tmp/no-key"

[snowflake]
account         = "ACMECORP-PROD123"   # placeholder — see "Snowflake login" below
host            = ""                   # set explicitly for locator/PrivateLink/custom CNAME
request_timeout = "60s"
max_retries     = 3

[snowflake.policy]
mode             = "passthrough"
refresh_interval = "60s"

[router]
lake_max_scan_bytes      = "100GB"
table_exists_cache_ttl   = "5m"
estimate_bytes_cache_ttl = "1m"

[metrics]
listen     = "127.0.0.1:9090"
log_format = "pretty"
log_level  = "info"

[backend.ducklake]
catalog_url       = "postgres://melt:melt@localhost:5432/melt_catalog"
data_path         = "s3://melt/ducklake/"
s3_region         = "us-east-1"
reader_pool_size  = 4
writer_pool_size  = 1
```

### 3. S3 credentials (for the embedded DuckDB)

```bash
export AWS_ACCESS_KEY_ID=melt
export AWS_SECRET_ACCESS_KEY=meltmelt
export AWS_ENDPOINT_URL=http://localhost:9000
```

### 4. Run

```bash
melt --config melt.local.toml all       # proxy + sync + admin
# OR
melt --config melt.local.toml start     # proxy only
melt --config melt.local.toml sync run  # CDC + bootstrap + policy refresh only
```

### 5. Verify

```bash
curl -s http://127.0.0.1:9090/healthz             # → ok
curl -s http://127.0.0.1:9090/readyz              # → ready (catalog pingable)
curl -s http://127.0.0.1:9090/metrics | head -20  # Prometheus exposition
melt --config melt.local.toml status              # backend / sync / policy summary
```

## Iceberg (REST catalog)

For Iceberg you need a REST-spec catalog. [Lakekeeper](https://github.com/lakekeeper/lakekeeper) is the easiest local choice. Polaris works too.

```bash
docker run -d --name lakekeeper -p 8181:8181 \
  quay.io/lakekeeper/catalog:latest
```

Swap the `[backend.ducklake]` block in `melt.local.toml` for:

```toml
[backend.iceberg]
catalog   = "rest"
warehouse = "s3://melt/iceberg/"
region    = "us-east-1"
rest_uri  = "http://127.0.0.1:8181"
```

For Polaris specifically, also export an auth token:

```bash
export MELT_POLARIS_TOKEN="..."   # service-principal token
```

Glue is supported for **catalog discovery** (read-side stats / table listing). Cross-table writes through duckdb-iceberg for Glue depend on extension features that are still stabilizing — front Glue with Polaris/Lakekeeper for production writes.

## `melt route`: no-infrastructure smoke test

`melt route` exercises the parser, classifier, allowlist / policy gates, and translator end-to-end **without** a backend or upstream Snowflake. If you just want to see whether a specific SQL statement will route to Lake, this is the cheapest way:

```bash
melt --config melt.local.toml route "SELECT IFF(x>0,'p','n') FROM analytics.public.events"
melt --config melt.local.toml route "INSERT INTO analytics.public.events VALUES (1)"
melt --config melt.local.toml route "SELECT * FROM information_schema.tables"
melt --config melt.local.toml route "SELECT DATEADD(day, 7, ts), PARSE_JSON(payload) FROM analytics.public.events"
```

For each query the output shows:

- **`route`** — `lake` or `snowflake`
- **`reason`** — the specific variant (e.g. `WriteStatement`, `UsesSnowflakeFeature("INFORMATION_SCHEMA")`, `UnderThreshold { estimated_bytes: 0 }`)
- **`translated`** — the DuckDB-dialect SQL the backend would actually run, when `route = lake`

No Postgres, no S3, no Snowflake login required.

## Next steps

- [Issue queries against the running proxy](issuing-queries.md).
- [Set up Snowflake service credentials](service-authentication.md) so sync can pull CDC.
- [Configuration reference](../configuration.md) — every field in `melt.toml`.
