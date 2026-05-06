# Melt — Python client example

Uses the **official `snowflake-connector-python`**, unmodified, with the connection pointed at a local Melt proxy via `host` / `port` / `protocol`.

## Run — credential-free seed mode (no Snowflake account required)

Resolves [KI-002](../../docs/internal/KNOWN_ISSUES.md). Provisions a local TPC-H sf=0.01 fixture (~6 MB) and runs end-to-end against it.

```bash
# 1. Provision the fixture + demo config (one-time, ~5 s).
cargo run -p melt-cli -- sessions seed

# 2. Start the proxy in another terminal (no Snowflake creds).
cargo run -p melt-cli -- --config melt.demo.toml all

# 3. Run the example.
pip install -r requirements.txt
MELT_MODE=seed python melt_demo.py
```

The demo creds (`account=melt-demo`, `user=demo`, `password=demo`, `database=TPCH`, `schema=SF01`) are baked into both `melt sessions seed` and the example — no env vars needed. See [docs/SEED_MODE.md](../../docs/SEED_MODE.md) for what seed mode supports.

## Run — real mode (forwards login to upstream Snowflake)

```bash
pip install -r requirements.txt

export MELT_HOST=127.0.0.1
export MELT_PORT=8443
export SNOWFLAKE_ACCOUNT=xy12345        # real account locator
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_SCHEMA=PUBLIC

python melt_demo.py
```

## How the redirect works

Two arguments do the work:

```python
snowflake.connector.connect(
    host="127.0.0.1",   # ← Melt's listen address (overrides <account>.snowflakecomputing.com)
    port=8443,
    protocol="http",    # ← because Melt's local config has no TLS cert; use "https" in prod
    account=ACCOUNT,    # ← still the real account; forwarded inside the login body
    user=USER,
    password=PASSWORD,
)
```

Everything else — login handshake, statement execution, partition polling, error code mapping — uses the connector's normal code path. Melt either translates+executes the query against the lake or forwards it to `<account>.snowflakecomputing.com`.

## Caveats

- **Login is forwarded.** With a placeholder `account`, the proxy returns `502` from `/session/v1/login-request` and every subsequent call returns `401`. You need a real Snowflake account for the demo to complete end-to-end. See `../README.md` for the alternatives.
- **Plain HTTP locally.** `protocol="http"` only works because Melt's example config has no TLS cert. In production you must use `"https"`, and Melt's cert needs a SAN matching `<account>.snowflakecomputing.com` (typically issued from a private CA), with DNS configured so clients reach Melt at that hostname — the connector refuses connections whose SAN doesn't match.
- **Driver TLS validation.** Production Snowflake drivers validate the server cert's SAN against `<account>.snowflakecomputing.com`. Without the SAN match they refuse to connect — that's why §8 exists.
