# Issuing queries against a running proxy

Melt speaks the real Snowflake REST API on whatever address `[proxy].listen` points at. Any client that talks to Snowflake can talk to Melt by overriding the host.

## Direct curl

```bash
# 1. Login (forwarded to upstream Snowflake; returns a session token)
curl -s -X POST http://127.0.0.1:8443/session/v1/login-request \
  -H 'Content-Type: application/json' \
  -d '{"data":{"LOGIN_NAME":"u","PASSWORD":"p","ACCOUNT_NAME":"xy12345"}}' \
  | tee /tmp/login.json

TOKEN=$(jq -r '.data.token // .data.sessionToken' /tmp/login.json)

# 2. Execute a statement (router decides Lake vs. Snowflake per request)
curl -s -X POST http://127.0.0.1:8443/api/v2/statements \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT IFF(1>0, '\''yes'\'', '\''no'\'') AS answer"}' \
  | jq .

# 3. Poll the next partition
HANDLE="<statementHandle from 2>"
curl -s "http://127.0.0.1:8443/api/v2/statements/$HANDLE?partition=1" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

## Snowflake Python connector

```bash
pip install snowflake-connector-python
```

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user="u", password="p", account="xy12345",
    host="127.0.0.1", port=8443, protocol="http",
)
cur = conn.cursor()
cur.execute("SELECT IFF(1>0, 'yes', 'no')")
print(cur.fetchall())
```

## snowflake-connector-rs (Rust)

```rust
use snowflake_connector_rs::*;

let client = SnowflakeClient::new(
    "u",
    SnowflakeAuthMethod::Password("p".into()),
    SnowflakeClientConfig { account: "xy12345".into(), ..Default::default() },
)?
.with_address("127.0.0.1", Some(8443), Some("http".into()))?;

let session = client.create_session().await?;
let rows = session.query("SELECT IFF(1>0, 'yes', 'no')").await?;
```

Full runnable demos for both clients live in [`examples/`](../../examples/).

## Account name handling

`melt.toml` is the single source of truth for which Snowflake account every request goes to. Drivers don't need to know the upstream account; they just point at Melt and provide their own credentials.

When a driver does send `ACCOUNT_NAME` in its login body, Melt enforces:

| Driver supplied | Melt's behavior |
|---|---|
| Same as `[snowflake].account` (case-insensitive) | Forward unchanged |
| Absent | Inject `[snowflake].account` so the upstream body matches the URL we POST to |
| Different from `[snowflake].account` | **Reject** with Snowflake error code `390201` ("incorrect account") + HTTP 400 |

This prevents a misconfigured driver from silently routing to the wrong upstream Snowflake. The error body is shaped like a normal Snowflake response so drivers handle it predictably:

```json
{
  "code": "390201",
  "message": "ACCOUNT_NAME 'ab67890' does not match Melt's configured upstream 'xy12345'",
  "success": false
}
```

For Python:

```python
snowflake.connector.connect(
    user="...", password="...",
    host="127.0.0.1", port=8443, protocol="http",
    # account="..." — optional. If set it must match melt.toml's [snowflake].account.
)
```

For Rust (`snowflake-connector-rs`), `account` is required by the connector but Melt accepts any value as long as it matches the proxy's configured account (or pass an empty/placeholder string and Melt will reject mismatched values clearly).

## Snowflake login dependency

The login forwarder calls `<account>.snowflakecomputing.com` (where `<account>` is `[snowflake].account` from `melt.toml`). With a placeholder `account`, the login returns `502 Bad Gateway` and no session token is minted — every subsequent call returns `401 Unauthorized`.

Three ways to handle this for local development:

1. **Real Snowflake account.** Sign up for a 30-day trial at `signup.snowflake.com`. Set `account = "<orgname>-<accountname>"` (or `<locator>` + explicit `host`). Full round-trip works.
2. **Stick to `melt route`.** No backend, no Snowflake — exercises the routing + translation paths fully. See [quickstart-local.md](quickstart-local.md#melt-route-no-infrastructure-smoke-test).

## Production TLS

Everything above assumes plain HTTP against a local Melt. In production you terminate TLS on Melt and point drivers at a hostname — see the [TLS guide](tls.md).
