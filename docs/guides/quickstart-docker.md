# Quickstart: Docker Compose

The repo ships a complete dev stack: Melt + Postgres (DuckLake catalog) + MinIO (S3) wired together on a private network. This is the fastest path to a running Melt, and what we recommend for evaluating it.

## Bring it up

```bash
# Build the image and bring everything up
docker compose up --build

# Just the deps (run melt natively from cargo against them)
docker compose up postgres minio minio-init

# Tear down + wipe state
docker compose down --volumes
```

Endpoints once healthy:

| URL | What |
|---|---|
| `http://localhost:8443` | Melt proxy (Snowflake REST shape) |
| `http://localhost:9090/metrics` | Prometheus exposition |
| `http://localhost:9090/healthz` | Liveness |
| `http://localhost:9090/readyz` | Readiness (catalog ping) |
| `http://localhost:9001` | MinIO console (`melt` / `meltmelt`) |
| `postgres://melt:melt@localhost:5432/melt_catalog` | DuckLake catalog |

## One-shot `route` / `status`

```bash
docker compose run --rm melt route "SELECT IFF(x>0,'a','b') FROM analytics.public.events"
docker compose run --rm melt status
```

The image's runtime config defaults to `docker/melt.docker.toml`, bind-mounted at `/etc/melt/melt.toml` inside the container. Edit it in place; restart `melt` to pick up changes.

## Pointing compose at a different config

Two env vars let you swap things without touching `docker-compose.yml`:

| Var | Default | What it does |
|---|---|---|
| `MELT_CONFIG` | `./docker/melt.docker.toml` | Host path to the config file (always mounted at `/etc/melt/melt.toml` inside the container). |
| `MELT_CMD` | `all` | Subcommand to run — `start`, `sync`, `all`, `status`, or `route`. |

Examples:

```bash
# Use your local config
MELT_CONFIG=./melt.local.toml docker compose up --build

# Run the proxy half only (e.g. behind a load balancer)
MELT_CMD=start docker compose up melt

# Both at once
MELT_CONFIG=./melt.local.toml MELT_CMD=start docker compose up melt

# Or persist them in a `.env` file next to docker-compose.yml
cat >> .env <<'EOF'
MELT_CONFIG=./melt.local.toml
MELT_CMD=all
EOF
docker compose up --build
```

For one-shot runs (`docker compose run --rm melt …`), the same vars apply:

```bash
MELT_CONFIG=./melt.local.toml docker compose run --rm melt status
MELT_CONFIG=./melt.local.toml docker compose run --rm melt route "SELECT 1"
```

## Building variants

```bash
# Default: ducklake + iceberg (no kafka)
docker build -t melt:dev .

# Slim ducklake-only build
docker build -t melt-ducklake:dev --build-arg MELT_FEATURES=ducklake .

# Include Kafka CDC ingestion
docker build -t melt:dev --build-arg \
  MELT_FEATURES="ducklake,iceberg,melt-ducklake/kafka" .
```

## Running pieces independently

The compose `melt` service runs `all` by default. To model the production split deployment (proxy pods + one sync pod) override the command:

```yaml
# docker-compose.override.yml
services:
  melt:
    command: ["--config", "/etc/melt/melt.toml", "start"]
  melt-sync:
    image: melt:dev
    depends_on:
      postgres: { condition: service_healthy }
      minio-init: { condition: service_completed_successfully }
    environment:
      AWS_ACCESS_KEY_ID: melt
      AWS_SECRET_ACCESS_KEY: meltmelt
      AWS_ENDPOINT_URL: http://minio:9000
    volumes:
      - ./docker/melt.docker.toml:/etc/melt/melt.toml:ro
    command: ["--config", "/etc/melt/melt.toml", "sync"]
```

## Next steps

- [Issue queries against the running proxy](issuing-queries.md).
- [Set up Snowflake service credentials](service-authentication.md) so sync can pull CDC.
- [Configuration reference](../configuration.md) — every field in `melt.toml`.
