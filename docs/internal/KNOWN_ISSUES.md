# Known issues — internal tracking

Internal punch-list. Public-facing docs live in `docs/`.

## KI-001 — Tokio blocking-thread pinning under DuckDB timeouts

**Status:** mitigated.

A statement that times out at the proxy boundary continues running on the DuckDB connection until DuckDB itself returns. Because every DuckDB call is wrapped in `tokio::task::spawn_blocking`, the timed-out task pins its blocking thread for the duration of the runaway query. The proxy already caps the Tokio blocking-thread pool well below the default 512 (see `crates/melt-cli/src/runtime_init.rs`), and the DuckLake reader pool's `reader_checkout_timeout` makes saturation observable as a Snowflake passthrough fallback rather than queueing indefinitely.

## KI-002 — Snowflake login forwarding without credentials

**Status:** resolved by [POWA-92](/POWA/issues/POWA-92) — landed in [melt#27](https://github.com/CITGuru/melt/pull/27). Merge SHA to be appended once the PR lands.

Snowflake drivers always call `POST /session/v1/login-request`. Before this fix the proxy unconditionally forwarded that to upstream, so a fresh checkout with no Snowflake account got a 401 on every subsequent statement — including SQL that would have routed entirely to the Lake.

`melt sessions seed` now provisions a credential-free demo:

- Writes a TPC-H sf=0.01 single-file DuckDB at `var/melt/seed.ddb` (~6 MB).
- Generates `melt.demo.toml` with `[sessions].mode = "seed"`.
- The proxy short-circuits the login handler against the canned demo creds (`account=melt-demo, user=demo, password=demo`) and serves statements from a `LocalDuckDbBackend` against the fixture.
- Anything that would otherwise route to upstream returns `MeltError::SeedModeUnsupported` (HTTP 422) — no silent dial-out.

See [docs/SEED_MODE.md](../SEED_MODE.md) for the operator-facing surface and [crates/melt-proxy/tests/seed_mode.rs](../../crates/melt-proxy/tests/seed_mode.rs) for the integration test that pins the contract.
