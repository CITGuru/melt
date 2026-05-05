use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod audit_cmd;
mod bootstrap_client;
mod bootstrap_server;
mod config;
mod debug_cmd;
mod reload;
mod runtime;
mod shutdown;
mod sync_cmd;

use config::MeltConfig;

/// `melt` — open-source Snowflake proxy with a DuckDB-backed
/// lakehouse. See the project README for the high-level pitch.
#[derive(Parser, Debug)]
#[command(name = "melt", version, about, long_about = None)]
struct Cli {
    /// Path to `melt.toml`. When omitted, Melt searches, in order:
    /// `$MELT_CONFIG` → `./melt.local.toml` → `./melt.toml` →
    /// `$XDG_CONFIG_HOME/melt/melt.toml` (or `~/.config/melt/melt.toml`) →
    /// `~/.melt/melt.toml`.
    #[arg(short, long)]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the proxy listener only.
    Start,
    /// Run both proxy and sync in one process.
    All,
    /// Print backend health, sync lag, and routing counters.
    Status {
        /// Emit machine-readable JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },
    /// Inspect the routing decision for a SQL string without
    /// executing it.
    Route { sql: String },
    /// Stand up a Melt deployment — server-side (`server`) or client-side
    /// (`client`). Two halves of the same TLS trust bootstrap flow:
    /// `server` mints certs and writes config; `client` fetches the CA
    /// from a running Melt and prints OS-specific trust + hosts commands.
    Bootstrap {
        #[command(subcommand)]
        action: BootstrapAction,
    },
    /// Operate on the sync subsystem: hot-reload the running proxy's
    /// `[sync]` config, list / inspect tracked tables, force-refresh
    /// one.
    Sync {
        #[command(subcommand)]
        action: sync_cmd::SyncAction,
    },
    /// Inspection helpers that compare Snowflake and the lake
    /// backend side-by-side — row counts, row samples. Exits
    /// non-zero on mismatch so this can gate CI.
    Debug {
        #[command(subcommand)]
        action: debug_cmd::DebugAction,
    },
    /// Local-only `$/savings` projection from Snowflake
    /// `ACCOUNT_USAGE.QUERY_HISTORY`. No data leaves the host;
    /// upload is a separate `melt audit share` subcommand
    /// (POWA-141, not yet wired).
    Audit(audit_cmd::AuditArgs),
}

#[derive(Subcommand, Debug)]
enum BootstrapAction {
    /// Mint a private CA + server cert, write a `melt.toml` skeleton,
    /// and print client-side setup instructions. Run this once on the
    /// host that will run Melt.
    Server {
        /// Snowflake account identifier — `xy12345` or `<org>-<acct>`.
        #[arg(long)]
        snowflake_account: String,
        /// Directory to write the cert bundle + skeleton config.
        #[arg(long, default_value = "./melt-certs")]
        output: PathBuf,
        /// Overwrite existing cert material in `--output`.
        #[arg(long)]
        force: bool,
    },
    /// Fetch Melt's CA from `/melt/ca.pem` and emit OS-specific
    /// commands to trust it and route the Snowflake hostname. Run
    /// this on every client host (BI tool servers, analyst laptops,
    /// etc.) after the `server` half has minted the certs.
    Client {
        /// Melt's HTTP(S) URL, e.g. `https://melt.internal:8443`.
        #[arg(long)]
        server: String,
        /// Snowflake account the drivers on this client will connect to.
        #[arg(long)]
        snowflake_account: String,
        /// Target OS. Auto-detected if omitted.
        #[arg(long)]
        os: Option<bootstrap_client::TargetOs>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // `sync` operator sub-commands (reload/list/status/refresh) reach
    // the running proxy or Postgres directly. `sync run` is special:
    // it needs the full runtime, so we let it fall through.
    if let Command::Sync { action } = &cli.command {
        if !matches!(action, sync_cmd::SyncAction::Run) {
            let cfg_path = MeltConfig::resolve_path(cli.config.as_deref())?;
            let Command::Sync { action } = cli.command else {
                unreachable!()
            };
            return sync_cmd::run(&cfg_path, action).await;
        }
    }

    // `audit` reads `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` directly
    // with its own auth flags — no `melt.toml`, no proxy listener,
    // no metrics init. Short-circuit before any of that.
    if let Command::Audit(args) = cli.command {
        let code = audit_cmd::run(args);
        return if code == std::process::ExitCode::SUCCESS {
            Ok(())
        } else {
            // Surface a non-zero exit at the process level. clap-style
            // 2 = usage error; the inner `run` already wrote a clear
            // remediation message to stderr.
            std::process::exit(2);
        };
    }

    // `debug` subcommands build a read-only backend connection and
    // issue comparison queries — they don't need the proxy listener,
    // so short-circuit before the full runtime.
    if let Command::Debug { action } = cli.command {
        let cfg_path = MeltConfig::resolve_path(cli.config.as_deref())?;
        return debug_cmd::run(&cfg_path, action).await;
    }

    // `bootstrap` runs BEFORE a config exists (server half) or on
    // client hosts that have never seen one (client half), so we
    // short-circuit before `MeltConfig::load`.
    if let Command::Bootstrap { action } = cli.command {
        return match action {
            BootstrapAction::Server {
                snowflake_account,
                output,
                force,
            } => bootstrap_server::run(bootstrap_server::BootstrapArgs {
                snowflake_account,
                output,
                force,
            }),
            BootstrapAction::Client {
                server,
                snowflake_account,
                os,
            } => {
                bootstrap_client::run(bootstrap_client::SetupArgs {
                    server,
                    snowflake_account,
                    os,
                })
                .await
            }
        };
    }

    let cfg_path = MeltConfig::resolve_path(cli.config.as_deref())?;
    tracing::debug!(config = %cfg_path.display(), "loading melt config");
    let cfg = MeltConfig::load(&cfg_path)?;

    melt_metrics::init(&runtime::metrics_cfg(&cfg))
        .map_err(|e| anyhow::anyhow!("metrics init: {e}"))?;

    if matches!(cfg.snowflake.policy.mode, melt_core::PolicyMode::Enforce) {
        tracing::warn!(
            "snowflake.policy.mode = \"enforce\" is enabled. \
             Sync will translate row-access policies into DuckDB views \
             where possible; tables whose body uses unsupported DSL \
             (e.g. IS_DATABASE_ROLE_IN_SESSION, custom UDFs) keep the \
             passthrough marker and pass through to Snowflake."
        );
    }

    let cmd = match cli.command {
        Command::Start => runtime::Command::Start,
        Command::All => runtime::Command::All,
        Command::Status { json } => runtime::Command::Status { json },
        Command::Route { sql } => runtime::Command::Route(sql),
        Command::Bootstrap { .. } => unreachable!("handled above"),
        Command::Sync {
            action: sync_cmd::SyncAction::Run,
        } => runtime::Command::Sync,
        Command::Sync { .. } => unreachable!("non-Run handled above"),
        Command::Debug { .. } => unreachable!("handled above"),
        Command::Audit(_) => unreachable!("handled above"),
    };

    runtime::run(cfg, cfg_path, cmd).await
}
