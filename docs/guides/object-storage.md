# S3-compatible object storage

Both backends write (sync) and read (proxy) through DuckDB's `httpfs` extension. Melt renders a `CREATE SECRET (TYPE S3, …)` per pool connection so **any** S3-compatible service works without code changes — AWS, MinIO, Cloudflare R2, Backblaze B2, Wasabi, Ceph/RadosGW, GCS (HMAC mode), etc.

Configuration lives under `[backend.ducklake.s3]` (or `[backend.iceberg.s3]`). All fields:

| Field | Purpose | Typical values |
|---|---|---|
| `region` | AWS region. Required. | `us-east-1`, `eu-west-1`, `auto` (R2) |
| `endpoint` | Hostname:port. Omit for AWS. | `localhost:9000`, `<acct>.r2.cloudflarestorage.com` |
| `url_style` | `vhost` or `path`. | `path` for MinIO / Ceph / localhost |
| `use_ssl` | HTTPS to the endpoint. | `false` for http MinIO |
| `access_key_id` / `secret_access_key` | Inline creds (dev only). | — |
| `access_key_id_env` / `secret_access_key_env` | **Read creds from env vars.** | `AWS_ACCESS_KEY_ID` etc. |
| `session_token` / `session_token_env` | STS temp creds. | — |
| `scope` | Restrict secret to a bucket prefix. | `s3://my-bucket/` |

**When all credential fields are empty, Melt emits `PROVIDER credential_chain`** — DuckDB then resolves creds the same way the AWS SDK does (env vars, `~/.aws/credentials`, EC2 IMDS, ECS/EKS task role, SSO). This is the right default on AWS.

## AWS (EC2 / EKS with IAM role)

```toml
[backend.ducklake.s3]
region = "us-east-1"
# No creds → credential_chain picks up the instance/pod role.
```

## MinIO (local dev)

```toml
[backend.ducklake.s3]
region            = "us-east-1"
endpoint          = "localhost:9000"
url_style         = "path"          # MinIO needs path-style
use_ssl           = false           # http, not https
access_key_id     = "minioadmin"
secret_access_key = "minioadmin"
```

## Cloudflare R2

```toml
[backend.ducklake.s3]
region                = "auto"
endpoint              = "<ACCOUNT_ID>.r2.cloudflarestorage.com"
# R2 API tokens are provided like AWS creds
access_key_id_env     = "R2_ACCESS_KEY_ID"
secret_access_key_env = "R2_SECRET_ACCESS_KEY"
```

## Backblaze B2 (S3-compatible endpoint)

```toml
[backend.ducklake.s3]
region                = "us-west-002"
endpoint              = "s3.us-west-002.backblazeb2.com"
access_key_id_env     = "B2_APPLICATION_KEY_ID"
secret_access_key_env = "B2_APPLICATION_KEY"
```

## Wasabi

```toml
[backend.ducklake.s3]
region                = "us-east-1"
endpoint              = "s3.us-east-1.wasabisys.com"
access_key_id_env     = "WASABI_ACCESS_KEY_ID"
secret_access_key_env = "WASABI_SECRET_ACCESS_KEY"
```
