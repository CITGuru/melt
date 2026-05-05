# Melt — client examples

End-to-end demos that talk to a running Melt proxy using the **official Snowflake drivers**, unmodified, by overriding only the connection host. They exercise four routing paths:

| # | Query | Expected route | Why |
|---|---|---|---|
| 1 | `SELECT 1 + 1 AS answer` | `lake` | Pure expression, no tables |
| 2 | `SELECT IFF(x > 0, 'p', 'n'), DATEADD(day, 7, ts) FROM analytics.public.events` | `lake` | Translated → `CASE WHEN ... ELSE END`, `DATEADD('day', 7, ts)` |
| 3 | `INSERT INTO analytics.public.events VALUES (...)` | `snowflake` | Writes always passthrough (§4.7 `is_write`) |
| 4 | `SELECT * FROM information_schema.tables` | `snowflake` | Snowflake-only feature (§A.10) |

Both demos print the result of each query and the route the proxy logged for it.

```
melt/
├── examples/
│   ├── python/                # snowflake-connector-python
│   │   ├── melt_demo.py       # the four-query routing walkthrough
│   │   ├── router_demo.py     # operator query-variant harness (--variants <dir>)
│   │   ├── hybrid_demo.py     # dual-execution variant harness (--variants <dir>)
│   │   ├── variants/          # router_demo.py SQL templates (operator-local; gitignored)
│   │   └── variants_hybrid/   # hybrid_demo.py SQL templates + README
│   └── rust/                  # snowflake-connector-rs (estie-inc, v0.9)
```

## Prerequisites

1. **Melt running.** From the repo root:
   ```bash
   docker compose up --build
   # OR cargo run --bin melt -- --config melt.toml all
   ```
2. **A real Snowflake account.** The drivers begin every connection with `POST /session/v1/login-request`, which Melt forwards to `<account>.snowflakecomputing.com`. With the placeholder account in `melt.toml`, login returns `502 Bad Gateway` and every subsequent statement returns `401 Unauthorized`. Two options:
   - Sign up for the [Snowflake 30-day trial](https://signup.snowflake.com/) and update `account` in `melt.docker.toml` (or `melt.toml`).
   - Run `melt route "<sql>"` for offline routing tests — see the [CLI README](../crates/melt-cli/readme.md).
3. **Credentials in env vars.** Both demos read these:
   ```bash
   export MELT_HOST=127.0.0.1          # where melt is listening
   export MELT_PORT=8443
   export SNOWFLAKE_ACCOUNT=xy12345    # your real Snowflake account locator
   export SNOWFLAKE_USER=your_user
   export SNOWFLAKE_PASSWORD=your_password
   export SNOWFLAKE_DATABASE=ANALYTICS
   export SNOWFLAKE_SCHEMA=PUBLIC
   ```

## Run them

```bash
# Python
cd examples/python
pip install -r requirements.txt
python melt_demo.py

# Rust (independent Cargo project, not a workspace member)
cd examples/rust
cargo run --release
```

While each runs, in another terminal you can watch the routing decisions:

```bash
docker compose logs -f melt | grep statement_complete
# OR if running natively:
RUST_LOG=info cargo run --bin melt -- --config melt.toml all 2>&1 | grep statement_complete
```

Each statement shows up as one line:

```
INFO statement_complete route=lake       backend=ducklake outcome=ok
INFO statement_complete route=snowflake  backend=ducklake outcome=ok
```

## Expected behavior

- **Queries 1 & 2** route to Lake. The Python connector receives a Snowflake-shaped JSON envelope; the Rust connector materializes the same envelope into `SnowflakeRow`s.
- **Queries 3 & 4** route to Snowflake. The proxy streams the upstream response straight through (`Body::from_stream` — see `crates/melt-proxy/src/handlers/statement.rs`).
- The translated SQL for query 2 is what hits DuckDB:
  ```sql
  SELECT CASE WHEN x > 0 THEN 'p' ELSE 'n' END, DATEADD('day', 7, ts) FROM analytics.public.events
  ```
  You can confirm this offline with `melt route "<sql>"` before booting any infra.

## What this validates

Drivers don't know they're talking to Melt. That's the whole point of the architecture (§2 design principle 1). If these examples work, you've validated:

- TLS / hostname strategy (or plain HTTP fallback for local dev — §8)
- Login forwarding + token caching in `SessionStore`
- Per-statement routing decisions
- Lake execution + Arrow → Snowflake-JSON serialization
- Passthrough body streaming (no buffering — §4.6 item 3)
- Pagination via `GET /api/v2/statements/{handle}?partition=N`
