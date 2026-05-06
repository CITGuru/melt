# Seed Mode — credential-free demo path

Seed mode is the "git clone → working query in five minutes" entry point for Melt. It runs the full proxy — same router, same Snowflake-shaped HTTP surface — against a single-file DuckDB fixture, so prospects can see Melt routing on canned workloads without provisioning a Snowflake account.

Resolves [KI-002](internal/KNOWN_ISSUES.md). Tracked in [POWA-92](../../docs/internal/KNOWN_ISSUES.md).

## Quickstart

```bash
git clone https://github.com/<org>/melt && cd melt

# Provision the fixture + demo config (one-time, ~5 s).
cargo run -p melt-cli -- sessions seed

# Start the proxy. Listens on 127.0.0.1:8443 (HTTP).
cargo run -p melt-cli -- --config melt.demo.toml all

# In another terminal, run the example client:
cd examples/python && pip install -r requirements.txt
MELT_MODE=seed python melt_demo.py
```

Total time on a developer laptop, cold: under five minutes (cargo build is the dominant cost).

The Rust example works the same way:

```bash
cd examples/rust && MELT_MODE=seed cargo run --release
```

## What the fixture contains

`melt sessions seed` writes a single-file DuckDB at `var/melt/seed.ddb` containing the [TPC-H](https://www.tpc.org/tpch/) sf=0.01 dataset (~6 MB):

| schema | tables |
|--------|--------|
| `SF01` | `lineitem`, `orders`, `customer`, `nation`, `region`, `part`, `supplier`, `partsupp` |

The runtime ATTACHes that file as the `TPCH` database, so client queries see them as `TPCH.SF01.<table>`. The demo config sets the session default DB / schema to `TPCH` / `SF01`, so unqualified `SELECT * FROM lineitem` works too.

## Demo credentials

These are baked into both `melt sessions seed` and the example clients. Don't change them — the goal is one universally documented set of creds.

| field | value |
|-------|-------|
| account | `melt-demo` |
| user | `demo` |
| password | `demo` |
| database | `TPCH` |
| schema | `SF01` |
| warehouse | `MELT_DEMO_WH` |
| role | `PUBLIC` |

The proxy short-circuits `POST /session/v1/login-request` against these creds — it never contacts an upstream Snowflake. Logging in with anything else returns HTTP 401 with a message pointing at this document.

## What seed mode supports

Anything the router can answer **locally** against the canned fixture:

- `SELECT` queries against `TPCH.SF01.<table>`
- Pure expressions (`SELECT 1 + 1`, `SELECT CURRENT_DATE`, …)
- DuckDB-dialect functions the translator covers (date math, conditional logic, semi-structured access)

You can verify a routing decision out-of-band with:

```bash
cargo run -p melt-cli -- --config melt.demo.toml route 'SELECT COUNT(*) FROM TPCH.SF01.lineitem'
```

## What seed mode refuses

Anything that would normally pass through to upstream Snowflake. Seed mode returns `MeltError::SeedModeUnsupported` (HTTP 422) instead of dialing out:

- Write statements (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, …)
- `INFORMATION_SCHEMA` / `ACCOUNT_USAGE` references
- Tables that aren't in the fixture (the router would route them to upstream as `TableMissing`)
- Hybrid (dual-execution) plans — the demo path doesn't ATTACH the community Snowflake DuckDB extension

The error message points back to this document and tells the operator to switch to `mode = "real"`.

## Switching back to real mode

Two options:

1. Pass a different config explicitly: `cargo run -p melt-cli -- --config melt.toml all`. `melt.demo.toml` is generated alongside; nothing overwrites your real config.
2. Remove or edit `[sessions]` in your config — `mode = "real"` (or omitting the block entirely) restores the production login path.

## What seed mode does NOT do

- It does not turn Melt into a public demo server. Bind to `127.0.0.1` and treat the HTTP listener as developer-local only. The TLS path (`bootstrap server`) is unchanged for real-mode deployments.
- It does not persist state between runs. The fixture lives in `var/melt/seed.ddb` and is gitignored; regenerating with `--regenerate` is non-destructive otherwise.
- It does not exercise sync. There's nothing to sync: the fixture is the source of truth.

## File layout

| path | purpose |
|------|---------|
| `var/melt/seed.ddb` | TPC-H sf=0.01 fixture (gitignored) |
| `melt.demo.toml` | Generated config; `[sessions].mode = "seed"` |
| `crates/melt-cli/src/sessions_cmd.rs` | `melt sessions seed` implementation |
| `crates/melt-ducklake/src/local.rs` | `LocalDuckDbBackend` — single-file backend used in seed mode |
| `crates/melt-proxy/src/handlers/session.rs` | login / token short-circuit |
| `crates/melt-proxy/tests/seed_mode.rs` | end-to-end integration test |
