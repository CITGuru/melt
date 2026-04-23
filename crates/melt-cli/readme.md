# melt-cli

The command-line interface for Melt (`melt`). Use it to run Melt's two long-running services (`start`, `sync`, `all`), inspect a running deployment (`status`, `sync list`, `sync status`), debug routing without infrastructure (`route`), and bootstrap TLS for new clients (`bootstrap`).

For what Melt actually *does*, see [`docs/overview.md`](../../docs/overview.md). This readme covers the binary itself.

## Install

```bash
cargo build --release --bin melt
# Resulting binary at: target/release/melt
```

Or run via cargo:

```bash
cargo run --release --bin melt -- <subcommand>
```

Pre-built docker image (recommended for evaluating Melt without setting up a Rust toolchain) — see the [Docker Compose quickstart](../../docs/guides/quickstart-docker.md).

## Subcommands

| Subcommand | What it does |
|---|---|
| `melt all` | Run both long-running services in one process (proxy + sync + admin). |
| `melt start` | Run proxy + admin only. Multiple instances scale horizontally. |
| `melt sync run` | Run sync + admin only. Single-tenant — one writer per lake. |
| `melt sync reload` | Hot-reload `[sync]` config against a running proxy via `POST /admin/reload`. |
| `melt sync list` | List tracked tables + their sync state. |
| `melt sync status <FQN>` | Detail for one mirrored table. |
| `melt sync refresh <FQN>` | Force a re-bootstrap of one table. |
| `melt status [--json]` | One-shot health summary: listener, catalog, Snowflake reachability, sync lag, policy state. |
| `melt route "<sql>"` | Print routing decision + translated SQL for one statement. No infrastructure required. |
| `melt bootstrap server` | Mint a private CA + server cert and write a `melt.toml` skeleton. See the [TLS guide](../../docs/guides/tls.md). |
| `melt bootstrap client` | Fetch the CA from a running Melt and print OS-specific trust + hosts commands. |

`start` / `sync run` / `all` / `status` / `route` all work against the same `melt.toml`. The two `bootstrap` halves are pre-config — `server` writes a config, `client` runs on machines that never see one.

### Typical deployments

- **Dev / small prod** — one `melt all` process.
- **Split prod** — multiple `melt start` pods behind a load balancer + exactly one `melt sync run` pod.

Sync is single-writer (the lakehouse only allows one writer at a time), so the sync side never scales horizontally. See [`docs/architecture.md`](../../docs/architecture.md) for what each long-running subcommand does at runtime, including the sync loops and table state machine.

## Configuration

Default config path is `melt.toml` in the cwd. Pass `--config / -c` to override.

> **Convention.** `melt.toml` ships in the repo as a **placeholder-only** example. Copy it to `melt.local.toml` (gitignored) and edit that one for local development:
>
> ```bash
> cp melt.toml melt.local.toml
> $EDITOR melt.local.toml
> melt --config melt.local.toml all
> ```

The same convention works in containers — bind-mount your real config at `/etc/melt/melt.toml`.

Every field is documented in [`docs/configuration.md`](../../docs/configuration.md). `melt.toml` itself carries inline comments explaining what each field does and which subsystem reads it.

## Cargo features

The CLI links one or both backends behind features.

```toml
[features]
default  = ["ducklake", "iceberg"]
ducklake = ["dep:melt-ducklake", "melt-ducklake/full"]
iceberg  = ["dep:melt-iceberg",  "melt-iceberg/full"]
```

Slim builds:

```bash
# DuckLake only (~30% smaller binary, no AWS SDK linked)
cargo build --release --bin melt --no-default-features --features ducklake

# Iceberg only
cargo build --release --bin melt --no-default-features --features iceberg
```

The CLI rejects a config that activates a backend feature this binary wasn't built with — you get a clear error at startup, not a confusing runtime crash.

## Cheat sheet

```bash
# First-time TLS setup — see docs/guides/tls.md for when you need this
melt bootstrap server --snowflake-account xy12345 --output ./melt-certs
melt bootstrap client --server https://melt.internal:8443 \
                      --snowflake-account xy12345

# What would the router do? (no infra required)
melt -c melt.toml route "<sql>"

# Bring everything up (one process)
melt -c melt.toml all

# Split deployment (scale proxy and sync independently)
melt -c melt.toml start    # one or more pods
melt -c melt.toml sync     # one pod (writer is single-tenant)

# Operator status check (human-readable or JSON)
melt -c melt.toml status
melt -c melt.toml status --json

# Help
melt --help
melt bootstrap --help     # lists `server` and `client` sub-subcommands
melt route --help
```

## See also

- [Overview](../../docs/overview.md) — what Melt is and why it exists.
- [Architecture](../../docs/architecture.md) — what each service does at runtime.
- [Configuration reference](../../docs/configuration.md) — every `melt.toml` field.
- Guides: [Docker](../../docs/guides/quickstart-docker.md) · [Local](../../docs/guides/quickstart-local.md) · [Issuing queries](../../docs/guides/issuing-queries.md) · [Service auth](../../docs/guides/service-authentication.md) · [Sync](../../docs/guides/sync.md) · [Object storage](../../docs/guides/object-storage.md) · [TLS](../../docs/guides/tls.md) · [Policy modes](../../docs/guides/policy-modes.md).
