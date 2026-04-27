//! `melt sync {reload, list, status, refresh}` — operator wrappers
//! over the running proxy's admin endpoint + direct reads of the
//! Postgres control catalog.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use melt_control::{ControlCatalog, SyncState};
use melt_core::TableRef;

use crate::config::MeltConfig;

#[derive(Subcommand, Debug)]
pub enum SyncAction {
    /// Run the sync subsystem (CDC apply + bootstrap + policy
    /// refresh) without the proxy listener. For split deployments
    /// where proxy and sync scale independently. Most operators
    /// run `melt all` instead.
    Run,

    /// Re-read `melt.toml` and apply `[sync]` changes against a
    /// running Melt via `POST /admin/reload`. Validates first; on
    /// failure nothing is mutated.
    Reload {
        /// Admin endpoint base URL. Defaults to the metrics listener
        /// from the local `melt.toml` (`http://<metrics.listen>`).
        #[arg(long)]
        admin: Option<String>,
        /// File containing the admin bearer token. Falls back to
        /// `MELT_ADMIN_TOKEN` env var, then `[metrics].admin_token_file`.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// Emit the raw admin response as JSON.
        #[arg(long)]
        json: bool,
    },

    /// Print the tracked tables and their sync state. Reads directly
    /// from the Postgres control catalog — does not hit Snowflake.
    List(ListArgs),

    /// Print detailed status for a single `DB.SCHEMA.TABLE`.
    Status { fqn: String },

    /// Force a table back to `pending` so sync re-bootstraps it.
    Refresh {
        fqn: String,
        /// Skip the "are you sure?" prompt.
        #[arg(long)]
        yes: bool,
    },

    /// Print every `[sync].remote` glob from the loaded config and the
    /// FQNs each one matches against the proxy's known table set.
    /// Symmetric to `melt sync list` — list shows synced tables; this
    /// shows declared-remote (federated) ones.
    Remote {
        /// Emit as JSON. Default is a human-readable table.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
pub struct ListArgs {
    /// Filter to rows in the given state.
    #[arg(long)]
    pub state: Option<StateFilter>,
    /// Emit as JSON. Default is a human-readable table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum StateFilter {
    Pending,
    Bootstrapping,
    Active,
    Quarantined,
}

impl From<StateFilter> for SyncState {
    fn from(s: StateFilter) -> SyncState {
        match s {
            StateFilter::Pending => SyncState::Pending,
            StateFilter::Bootstrapping => SyncState::Bootstrapping,
            StateFilter::Active => SyncState::Active,
            StateFilter::Quarantined => SyncState::Quarantined,
        }
    }
}

pub async fn run(cfg_path: &Path, action: SyncAction) -> Result<()> {
    let cfg = MeltConfig::load(cfg_path)
        .with_context(|| format!("reading config at {}", cfg_path.display()))?;
    match action {
        // `Run` defers to the regular runtime — it needs the full
        // backend setup the other sync sub-commands skip.
        SyncAction::Run => unreachable!("handled in main before MeltConfig::load is duplicated"),
        SyncAction::Reload {
            admin,
            token_file,
            json,
        } => reload(&cfg, admin, token_file, json).await,
        SyncAction::List(args) => list(&cfg, args).await,
        SyncAction::Status { fqn } => status(&cfg, &fqn).await,
        SyncAction::Refresh { fqn, yes } => refresh(&cfg, &fqn, yes).await,
        SyncAction::Remote { json } => remote(&cfg, json).await,
    }
}

/// `melt sync remote` — list `[sync].remote` patterns and the tables
/// they match. Pure config introspection (no network) so it works
/// offline.
async fn remote(cfg: &MeltConfig, json: bool) -> Result<()> {
    if cfg.sync.remote.is_empty() {
        if json {
            println!("[]");
        } else {
            println!("(no [sync].remote globs configured)");
            println!(
                "Hybrid (dual-execution) routing only fires when at least \
                 one [sync].remote pattern is set AND [router].hybrid_execution = true."
            );
        }
        return Ok(());
    }

    if json {
        let payload: Vec<serde_json::Value> = cfg
            .sync
            .remote
            .iter()
            .map(|pat| serde_json::json!({ "pattern": pat }))
            .collect();
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!("Hybrid (dual-execution) federation patterns from [sync].remote:");
    println!();
    for (i, pat) in cfg.sync.remote.iter().enumerate() {
        println!("  {:>3}. {}", i + 1, pat);
    }
    println!();
    println!(
        "Hybrid execution: {}",
        if cfg.router.hybrid_execution {
            "ON (router.hybrid_execution = true)"
        } else {
            "OFF (router.hybrid_execution = false — patterns are recognized but \
             queries fall back to passthrough)"
        }
    );
    println!(
        "Attach strategy:  {} (single-scan remote nodes use sf_link)",
        if cfg.router.hybrid_attach_enabled {
            "enabled"
        } else {
            "DISABLED — every remote node forced to Materialize"
        }
    );
    println!(
        "Trigger cases:    bootstrapping={}  oversize={}",
        cfg.router.hybrid_allow_bootstrapping, cfg.router.hybrid_allow_oversize,
    );
    Ok(())
}

// ── reload ─────────────────────────────────────────────────────

async fn reload(
    cfg: &MeltConfig,
    admin_flag: Option<String>,
    token_file_flag: Option<PathBuf>,
    json: bool,
) -> Result<()> {
    let url = resolve_admin_url(cfg, admin_flag)?;
    let token = resolve_token(cfg, token_file_flag)?;

    let client = reqwest::Client::builder()
        .build()
        .context("building http client")?;
    let mut req = client.post(format!("{url}/admin/reload"));
    if let Some(t) = &token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await.context("calling admin endpoint")?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);

    if json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        if !status.is_success() {
            std::process::exit(2);
        }
        return Ok(());
    }

    if status.is_success() {
        println!(
            "✔ Reload applied in {}ms",
            body["duration_ms"].as_u64().unwrap_or(0)
        );
        if let Some(obj) = body["changes"].as_object() {
            if obj.is_empty() {
                println!("  (no changes in [sync])");
            }
            for (field, diff) in obj {
                println!("  {field:25} {}", format_diff(diff));
            }
        }
        if let Some(skipped) = body["skipped"].as_array() {
            if !skipped.is_empty() {
                println!("\nSkipped — restart required:");
                for s in skipped {
                    println!(
                        "  {:25} {}",
                        s["field"].as_str().unwrap_or(""),
                        s["reason"].as_str().unwrap_or("")
                    );
                }
            }
        }
    } else {
        eprintln!("✗ Reload failed ({status})");
        if let Some(errs) = body["errors"].as_array() {
            for e in errs {
                eprintln!(
                    "  {:25} {}",
                    e["field"].as_str().unwrap_or(""),
                    e["error"].as_str().unwrap_or("")
                );
            }
        }
        std::process::exit(2);
    }
    Ok(())
}

fn resolve_admin_url(cfg: &MeltConfig, flag: Option<String>) -> Result<String> {
    if let Some(u) = flag {
        return Ok(u);
    }
    let addr = cfg
        .metrics
        .listen
        .ok_or_else(|| anyhow!("[metrics].listen is empty; pass --admin explicitly"))?;
    // Admin reload is plain HTTP by default — operators ship cert
    // for the listener themselves if they need TLS. Loopback default
    // keeps the local-dev path one command.
    Ok(format!("http://{addr}"))
}

fn resolve_token(cfg: &MeltConfig, flag: Option<PathBuf>) -> Result<Option<String>> {
    if let Some(path) = flag {
        let s = std::fs::read_to_string(&path)
            .with_context(|| format!("reading token file {}", path.display()))?;
        return Ok(Some(s.trim().to_string()));
    }
    if let Ok(env) = std::env::var("MELT_ADMIN_TOKEN") {
        if !env.is_empty() {
            return Ok(Some(env));
        }
    }
    if !cfg.metrics.admin_token_file.is_empty() {
        let s = std::fs::read_to_string(&cfg.metrics.admin_token_file).with_context(|| {
            format!(
                "reading [metrics].admin_token_file {}",
                cfg.metrics.admin_token_file
            )
        })?;
        return Ok(Some(s.trim().to_string()));
    }
    if !cfg.metrics.admin_token.is_empty() {
        return Ok(Some(cfg.metrics.admin_token.clone()));
    }
    Ok(None)
}

fn format_diff(v: &serde_json::Value) -> String {
    if let (Some(from), Some(to)) = (v.get("from"), v.get("to")) {
        return format!("{from} → {to}");
    }
    if let Some(added) = v.get("added").and_then(|a| a.as_array()) {
        let removed = v.get("removed").and_then(|a| a.as_array());
        let mut parts = Vec::new();
        if !added.is_empty() {
            let adds: Vec<String> = added
                .iter()
                .map(|s| s.as_str().unwrap_or("").to_string())
                .collect();
            parts.push(format!("+[{}]", adds.join(", ")));
        }
        if let Some(rem) = removed {
            if !rem.is_empty() {
                let rems: Vec<String> = rem
                    .iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .collect();
                parts.push(format!("-[{}]", rems.join(", ")));
            }
        }
        return parts.join(" ");
    }
    v.to_string()
}

// ── list / status / refresh (direct Postgres reads) ─────────────

async fn list(cfg: &MeltConfig, args: ListArgs) -> Result<()> {
    let catalog = connect_control(cfg).await?;
    let rows = match args.state {
        Some(s) => catalog.list_by_state(s.into(), None).await?,
        None => catalog.list_all_rows().await?,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }

    if rows.is_empty() {
        println!("(no tracked tables)");
        return Ok(());
    }

    println!(
        "{:10}  {:15}  {:10}  {:14}  {:10}  {:20}  table",
        "state", "source", "kind", "strategy", "queried", "error"
    );
    for r in &rows {
        let queried = r
            .last_queried_at
            .map(|t| format!("{}h ago", (chrono::Utc::now() - t).num_hours()))
            .unwrap_or_else(|| "-".into());
        let err = r
            .bootstrap_error
            .as_deref()
            .map(|s| truncate(s, 20))
            .unwrap_or_else(|| "-".into());
        let strategy = r
            .view_strategy
            .map(|s| s.as_str().to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "{:10}  {:15}  {:10}  {:14}  {:10}  {:20}  {}.{}.{}",
            r.sync_state.as_str(),
            r.source.as_str(),
            r.object_kind.as_str(),
            strategy,
            queried,
            err,
            r.table.database,
            r.table.schema,
            r.table.name,
        );
    }
    Ok(())
}

async fn status(cfg: &MeltConfig, fqn: &str) -> Result<()> {
    let catalog = connect_control(cfg).await?;
    let t = parse_fqn(fqn)?;
    match catalog.get_row(&t).await? {
        None => {
            println!("table {fqn} is not tracked");
        }
        Some(r) => {
            println!("{fqn}");
            println!("  state:            {}", r.sync_state.as_str());
            println!("  source:           {}", r.source.as_str());
            println!("  object_kind:      {}", r.object_kind.as_str());
            if let Some(s) = r.view_strategy {
                println!("  view_strategy:    {}", s.as_str());
            }
            println!("  discovered_at:    {}", r.discovered_at);
            if let Some(t) = r.last_queried_at {
                println!("  last_queried_at:  {t}");
            }
            if let Some(s) = r.last_snapshot {
                println!("  last_snapshot:    {s}");
            }
            if let Some(t) = r.last_synced_at {
                println!("  last_synced_at:   {t}");
            }
            println!("  bytes:            {}", r.bytes);
            if let Some(e) = &r.bootstrap_error {
                println!("  bootstrap_error:  {e}");
            }
        }
    }
    Ok(())
}

async fn refresh(cfg: &MeltConfig, fqn: &str, yes: bool) -> Result<()> {
    if !yes {
        println!(
            "About to flip {fqn} back to `pending`. Sync will DROP STREAM + \
             re-bootstrap on its next tick (potentially expensive for large tables).\n\
             Re-run with --yes to proceed."
        );
        return Ok(());
    }
    let catalog = connect_control(cfg).await?;
    let t = parse_fqn(fqn)?;
    catalog.refresh_table(&t).await?;
    println!("✔ {fqn} marked `pending` — sync will re-bootstrap on next tick");
    Ok(())
}

async fn connect_control(cfg: &MeltConfig) -> Result<ControlCatalog> {
    let url = resolve_control_url(cfg)?;
    ControlCatalog::connect(&url)
        .await
        .context("connecting to control catalog")
}

fn resolve_control_url(cfg: &MeltConfig) -> Result<String> {
    // DuckLake points `catalog_url` at the same Postgres; Iceberg has
    // a dedicated `control_catalog_url`. CLI doesn't know which
    // backend is active — try whichever field exists.
    #[cfg(feature = "ducklake")]
    if let Some(dl) = &cfg.backend.ducklake {
        return Ok(dl.catalog_url.clone());
    }
    #[cfg(feature = "iceberg")]
    if let Some(ib) = &cfg.backend.iceberg {
        if !ib.control_catalog_url.is_empty() {
            return Ok(ib.control_catalog_url.clone());
        }
    }
    Err(anyhow!(
        "no control catalog URL — set [backend.ducklake].catalog_url \
         or [backend.iceberg].control_catalog_url in melt.toml"
    ))
}

fn parse_fqn(fqn: &str) -> Result<TableRef> {
    let parts: Vec<&str> = fqn.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(anyhow!("FQN must be DB.SCHEMA.TABLE, got {fqn:?}"));
    }
    Ok(TableRef::new(
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut out = s.chars().take(n.saturating_sub(1)).collect::<String>();
        out.push('…');
        out
    }
}
