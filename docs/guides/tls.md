# TLS

Snowflake drivers default to connecting to `https://<account>.snowflakecomputing.com/...` and validate the server cert's SAN against that hostname. Melt sits in between — you need the driver to terminate TLS on Melt, not on Snowflake, and accept Melt's cert.

The right answer depends on where you're running.

## Localhost / dev

Leave `tls_cert` and `tls_key` pointing at missing files. Melt boots on plain HTTP with a `WARN` in the log. Point clients at `http://127.0.0.1:<port>`:

- Python: `connect(account=..., host="127.0.0.1", port=8443, protocol="http", ...)`
- JDBC:   `jdbc:snowflake://127.0.0.1:8443/?account=<ACCOUNT>&ssl=off&...`
- Rust:   `SnowflakeClientConfig::with_address("127.0.0.1:8443", ...)` + http scheme

No cert, no DNS, no CA. This is what `examples/python` and `examples/rust` do against a locally-running Melt. Never use in production.

## Production

You need a real TLS cert. Pick a path based on how much you control the clients.

### Path A — point drivers at a hostname you own *(recommended)*

All mainstream Snowflake drivers accept an explicit host parameter that overrides the derived `<account>.snowflakecomputing.com`. Point drivers at a hostname you actually own (`melt.mycompany.com`), issue a normal cert for it (Let's Encrypt, internal PKI, etc.), point `tls_cert` / `tls_key` at it, done. The driver still sends the Snowflake `account` in the login body so upstream routing still works.

| Driver | How to override host |
|---|---|
| Python (`snowflake-connector-python`) | `connect(account=..., host="melt.mycompany.com", port=443, ...)` |
| JDBC | `jdbc:snowflake://melt.mycompany.com/?account=<ACCOUNT>&...` |
| ODBC | set the `server` / `host` DSN property to `melt.mycompany.com` |
| Rust (`snowflake-connector-rs`) | `SnowflakeClientConfig::with_address("melt.mycompany.com:443", ...)` |

No cert magic, no private CA, no DNS fiddling. The only cost is telling every driver the new host (one line of config per client).

### Path B — transparent intercept of `snowflakecomputing.com` *(advanced)*

If drivers truly must require zero code change — they keep passing only `account`, compute the default `<account>.snowflakecomputing.com` URL, and still terminate on Melt — you'd do what TLS-inspecting corporate proxies (Zscaler, Palo Alto) do:

1. Stand up a private CA. Issue a cert whose SAN is `<account>.snowflakecomputing.com` from it. Point `tls_cert` / `tls_key` at that.
2. Install the private CA's root into every client's trust store (MDM, OS cert store, `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`, JVM cacerts, etc.).
3. Override DNS so `<account>.snowflakecomputing.com` resolves to Melt's IP in your network (split-horizon DNS, `/etc/hosts`, internal resolver rules).

You **cannot** get a public CA to issue this — no public CA will sign a cert for a domain you don't own, and Snowflake won't give you one either. The private-CA + trust-store + DNS triple is the only way. It's operationally expensive (you own a CA lifecycle and every client's trust chain from now on), but it's the only option if the drivers cannot be reconfigured.

`melt bootstrap` collapses steps 1–3 into two commands — one on the server, one per client — for small and mid-sized deployments. Large fleets typically replace `bootstrap server` with step-ca or Vault PKI (for cert lifecycle) and `bootstrap client` with MDM / config management (for trust distribution), but the shape is the same.

```bash
# On the Melt host — mints CA + server cert, writes melt.toml skeleton.
melt bootstrap server --snowflake-account xy12345 --output /etc/melt/

# Start Melt with the generated config.
melt --config /etc/melt/melt.toml all

# On EACH client host — fetches the CA, prints trust + hosts commands.
melt bootstrap client \
    --server https://melt.internal:8443 \
    --snowflake-account xy12345
# Review the output, verify the fingerprint matches what the server
# printed, then pipe the commands into bash if satisfied.
```

`melt bootstrap server` refuses to clobber existing cert material unless you pass `--force`, so re-running it on an already-configured host is safe. Re-running `melt bootstrap client` on a client that has already trusted the CA just re-prints the same commands — idempotent.

### `melt bootstrap server` — what it does

Mints a private CA (10-year) and a server cert whose SAN is `<account>.snowflakecomputing.com`, writes `ca.pem`, `server.pem`, `server.key` (0600 on Unix), and a ready-to-go `melt.toml` skeleton to the output dir. Then prints exactly which commands to run on each client box. One command, no openssl.

### `melt bootstrap client` — what it does

Runs on every client host. Fetches the CA over HTTP from `<url>/melt/ca.pem` (with cert verification disabled for this one bootstrap fetch — the whole point is that trust hasn't been established yet), prints its fingerprint, and emits copy-pasteable commands for adding the CA to the system trust store and routing `<account>.snowflakecomputing.com` to Melt's IP. Supports macOS, Linux, Windows; auto-detects or takes `--os <target>`.
