# Melt — Rust client example

Uses [`snowflake-connector-rs`](https://crates.io/crates/snowflake-connector-rs) (estie-inc, v0.9) — the canonical Rust Snowflake client — with the connection pointed at a local Melt proxy via `SnowflakeEndpointConfig::custom_base_url`.

## Run

```bash
export MELT_HOST=127.0.0.1
export MELT_PORT=8443
export MELT_PROTOCOL=http               # plain HTTP for local dev; "https" in prod
export SNOWFLAKE_ACCOUNT=xy12345        # real account locator
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_SCHEMA=PUBLIC

cargo run --release
```

## How the redirect works

The standard `SnowflakeClient::new(...)` builder, then one extra `.with_address(...)`:

```rust
let config = SnowflakeClientConfig {
    account:   "xy12345".into(),     // still the real Snowflake account
    database:  Some("ANALYTICS".into()),
    schema:    Some("PUBLIC".into()),
    warehouse: None,
    role:      None,
    timeout:   Some(Duration::from_secs(60)),
};

let client = SnowflakeClient::new(
    "your_user",
    SnowflakeAuthMethod::Password("...".into()),
    config,
)?
// ↓ The only call that differs from a normal Snowflake connection:
.with_address("127.0.0.1", Some(8443), Some("http".into()))?;

let session = client.create_session().await?;
let rows = session.query("SELECT 1 + 1 AS answer").await?;
```

Everything else — login handshake, statement execution, result paging, error mapping — uses the connector's normal code path. Melt either translates+executes the query against the lake or forwards it to `<account>.snowflakecomputing.com`.

## Standalone Cargo project

This example is **not** a member of the parent workspace. Copy the `examples/rust` directory anywhere and `cargo run` works.

If you ever want to add it to the workspace, add the path to `members` in the root `Cargo.toml`.

## Caveats

- **Login is forwarded.** Same story as the Python example — placeholder `account` → 502 → 401. Use a real Snowflake trial account or `melt route "<sql>"` for offline tests. See `../README.md`.
- **Plain HTTP locally.** `MELT_PROTOCOL=http` works because the local Melt has no TLS cert. In production you must use `https`, and Melt's cert needs a SAN matching `<account>.snowflakecomputing.com` (typically issued from a private CA), with DNS pointing that hostname at Melt — Snowflake drivers reject connections whose SAN doesn't match.
