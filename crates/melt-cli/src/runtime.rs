use std::sync::Arc;
use std::time::Duration;

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use arc_swap::ArcSwap;
use melt_core::{DiscoveryCatalog, NoopRouterCache, RouterCache, StorageBackend, SyncTableMatcher};
use melt_proxy::SharedMatcher;
use melt_router::Cache;
use melt_snowflake::SnowflakeClient;

use crate::reload::ReloadCtx;
use crate::shutdown::{install_signal_handler, Shutdown};

use crate::config::{ActiveBackend, MeltConfig};

#[derive(Debug)]
pub enum Command {
    Start,
    Sync,
    All,
    Status { json: bool },
    Route(String),
}

pub fn metrics_cfg(cfg: &MeltConfig) -> melt_metrics::MetricsConfig {
    melt_metrics::MetricsConfig {
        listen: cfg.metrics.listen,
        log_format: cfg.metrics.log_format,
        log_level: cfg.metrics.log_level.clone(),
        admin_token: cfg.metrics.admin_token.clone(),
        admin_token_file: cfg.metrics.admin_token_file.clone(),
    }
}

pub async fn run(cfg: MeltConfig, cfg_path: PathBuf, cmd: Command) -> Result<()> {
    // Always-ready probe is the safe fallback; backend-specific
    // arms below replace it with a real catalog ping.
    let _readiness_default = melt_metrics::ReadinessProbe::always_ready();

    let snowflake = Arc::new(SnowflakeClient::new(cfg.snowflake.clone()));
    let router_cache_concrete = Arc::new(Cache::new(&cfg.router));
    let router_cache: Arc<dyn RouterCache> = router_cache_concrete.clone();
    let _ = router_cache; // satisfies unused when only `Route` arm runs

    let backend = cfg.active_backend()?;
    match backend {
        #[cfg(feature = "ducklake")]
        ActiveBackend::DuckLake(dl) => {
            run_ducklake(dl, cfg, cfg_path, snowflake, router_cache_concrete, cmd).await
        }
        #[cfg(feature = "iceberg")]
        ActiveBackend::Iceberg(ib) => {
            run_iceberg(ib, cfg, cfg_path, snowflake, router_cache_concrete, cmd).await
        }
    }
}

#[cfg(feature = "ducklake")]
async fn run_ducklake(
    dl: melt_ducklake::DuckLakeConfig,
    cfg: MeltConfig,
    cfg_path: PathBuf,
    snowflake: Arc<SnowflakeClient>,
    router_cache: Arc<Cache>,
    cmd: Command,
) -> Result<()> {
    use melt_ducklake::{CatalogClient, DuckLakeBackend, DuckLakePool};

    if let Command::Route(sql) = cmd {
        return print_lazy_route(&cfg, &sql);
    }

    let catalog = Arc::new(CatalogClient::connect(&dl.catalog_url).await?);
    let pool = Arc::new(DuckLakePool::new(dl).await?);
    let readiness = build_readiness_ducklake(catalog.clone());
    let metrics = metrics_cfg(&cfg);

    let policy_cfg = cfg.snowflake.policy.clone();
    let sync_cfg = cfg.sync.clone();
    let cache_arc: Arc<dyn RouterCache> = router_cache.clone();

    // Discovery wiring: compile the `[sync]` matcher once at startup
    // and wrap it in ArcSwap so `POST /admin/reload` can atomically
    // replace it without restarting.
    let initial_matcher = Arc::new(
        SyncTableMatcher::from_config(&cfg.sync).map_err(|e| anyhow!("[sync] matcher: {e}"))?,
    );
    let sync_matcher: SharedMatcher = Arc::new(ArcSwap::from_pointee(Some(initial_matcher)));
    let discovery: Arc<dyn DiscoveryCatalog> = catalog.clone();

    let reload_ctx = ReloadCtx::new(cfg_path.clone(), sync_matcher.clone(), cfg.sync.clone());

    match cmd {
        Command::Start => {
            let backend: Arc<dyn StorageBackend> =
                Arc::new(DuckLakeBackend::from_parts(catalog, pool));
            let hooks = build_admin_hooks(readiness.clone(), reload_ctx.clone());
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_proxy = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    melt_proxy::serve(
                        cfg.proxy.clone(),
                        backend,
                        snowflake.clone(),
                        cfg.snowflake.clone(),
                        cfg.router.clone(),
                        router_cache.clone(),
                        sync_matcher.clone(),
                        Some(discovery.clone()),
                        async move { shutdown_proxy.notified().await },
                    )
                    .await
                    .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin_with_hooks(&metrics, hooks, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::Sync => {
            let sync = Arc::new(melt_ducklake::DuckLakeSync::new(
                catalog,
                pool,
                snowflake.clone(),
                cache_arc.clone(),
                policy_cfg,
                sync_cfg.clone(),
            ));
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_cont = shutdown.notify();
            let shutdown_pol = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    sync.clone()
                        .run_continuous(Duration::from_secs(60), shutdown_cont)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_policy_refresh(shutdown_pol)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin(&metrics, readiness, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::All => {
            let backend: Arc<dyn StorageBackend> =
                Arc::new(DuckLakeBackend::from_parts(catalog.clone(), pool.clone()));
            let sync = Arc::new(melt_ducklake::DuckLakeSync::new(
                catalog,
                pool,
                snowflake.clone(),
                cache_arc.clone(),
                policy_cfg,
                sync_cfg.clone(),
            ));
            let hooks = build_admin_hooks(readiness.clone(), reload_ctx.clone());
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_proxy = shutdown.notify();
            let shutdown_cont = shutdown.notify();
            let shutdown_pol = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    melt_proxy::serve(
                        cfg.proxy.clone(),
                        backend,
                        snowflake.clone(),
                        cfg.snowflake.clone(),
                        cfg.router.clone(),
                        router_cache.clone(),
                        sync_matcher.clone(),
                        Some(discovery.clone()),
                        async move { shutdown_proxy.notified().await },
                    )
                    .await
                    .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_continuous(Duration::from_secs(60), shutdown_cont)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_policy_refresh(shutdown_pol)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin_with_hooks(&metrics, hooks, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::Status { json } => {
            print_status_ducklake(&catalog, &cfg, snowflake.as_ref(), json).await?;
        }
        Command::Route(_) => unreachable!("handled above"),
    }
    Ok(())
}

/// Wrap the readiness probe + reload closure into an `AdminHooks`
/// bundle for `melt-metrics`.
fn build_admin_hooks(
    readiness: melt_metrics::ReadinessProbe,
    ctx: ReloadCtx,
) -> melt_metrics::AdminHooks {
    melt_metrics::AdminHooks::default()
        .with_readiness(readiness)
        .with_reload(move || {
            let ctx = ctx.clone();
            async move { ctx.reload().await }
        })
}

#[cfg(feature = "iceberg")]
async fn run_iceberg(
    ib: melt_iceberg::IcebergConfig,
    cfg: MeltConfig,
    cfg_path: PathBuf,
    snowflake: Arc<SnowflakeClient>,
    router_cache: Arc<Cache>,
    cmd: Command,
) -> Result<()> {
    use melt_iceberg::{IcebergBackend, IcebergCatalogClient, IcebergPool};

    if let Command::Route(sql) = cmd {
        return print_lazy_route(&cfg, &sql);
    }

    let catalog = Arc::new(IcebergCatalogClient::connect(&ib).await?);
    catalog.assert_supported()?;
    let pool = Arc::new(IcebergPool::new(&ib).await?);
    let readiness = melt_metrics::ReadinessProbe::always_ready();
    let metrics = metrics_cfg(&cfg);

    let policy_cfg = cfg.snowflake.policy.clone();
    let sync_cfg = cfg.sync.clone();
    let cache_arc: Arc<dyn RouterCache> = router_cache.clone();

    // Optional control-plane catalog for sync state. Iceberg runs
    // headless (matcher swap starts empty) when `control_catalog_url`
    // is empty — the router then takes the legacy `tables_exist`
    // code path.
    let (sync_matcher, discovery, control_for_sync): (SharedMatcher, _, _) =
        if !ib.control_catalog_url.is_empty() {
            let control =
                Arc::new(melt_control::ControlCatalog::connect(&ib.control_catalog_url).await?);
            let matcher = Arc::new(
                SyncTableMatcher::from_config(&cfg.sync)
                    .map_err(|e| anyhow!("[sync] matcher: {e}"))?,
            );
            let dyn_disc: Arc<dyn DiscoveryCatalog> = control.clone();
            (
                Arc::new(ArcSwap::from_pointee(Some(matcher))),
                Some(dyn_disc),
                Some(control),
            )
        } else {
            (Arc::new(ArcSwap::from_pointee(None)), None, None)
        };
    let reload_ctx = ReloadCtx::new(cfg_path.clone(), sync_matcher.clone(), cfg.sync.clone());

    match cmd {
        Command::Start => {
            let backend: Arc<dyn StorageBackend> =
                Arc::new(IcebergBackend::from_parts(catalog, pool));
            let hooks = build_admin_hooks(readiness.clone(), reload_ctx.clone());
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_proxy = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    melt_proxy::serve(
                        cfg.proxy.clone(),
                        backend,
                        snowflake.clone(),
                        cfg.snowflake.clone(),
                        cfg.router.clone(),
                        router_cache.clone(),
                        sync_matcher.clone(),
                        discovery.clone(),
                        async move { shutdown_proxy.notified().await },
                    )
                    .await
                    .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin_with_hooks(&metrics, hooks, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::Sync => {
            let control = control_for_sync.clone().ok_or_else(|| {
                anyhow!("iceberg sync requires [backend.iceberg].control_catalog_url to be set")
            })?;
            let sync = Arc::new(melt_iceberg::IcebergSync::new(
                catalog,
                pool,
                snowflake.clone(),
                cache_arc.clone(),
                policy_cfg,
                sync_cfg.clone(),
                control,
            ));
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_cont = shutdown.notify();
            let shutdown_pol = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    sync.clone()
                        .run_continuous(Duration::from_secs(60), shutdown_cont)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_policy_refresh(shutdown_pol)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin(&metrics, readiness, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::All => {
            let control = control_for_sync.clone().ok_or_else(|| {
                anyhow!("iceberg `all` requires [backend.iceberg].control_catalog_url to be set")
            })?;
            let backend: Arc<dyn StorageBackend> =
                Arc::new(IcebergBackend::from_parts(catalog.clone(), pool.clone()));
            let sync = Arc::new(melt_iceberg::IcebergSync::new(
                catalog,
                pool,
                snowflake.clone(),
                cache_arc.clone(),
                policy_cfg,
                sync_cfg.clone(),
                control,
            ));
            let hooks = build_admin_hooks(readiness.clone(), reload_ctx.clone());
            let shutdown = Shutdown::new();
            install_signal_handler(shutdown.clone());
            let shutdown_proxy = shutdown.notify();
            let shutdown_cont = shutdown.notify();
            let shutdown_pol = shutdown.notify();
            let shutdown_metrics = shutdown.notify();
            tokio::try_join!(
                async {
                    melt_proxy::serve(
                        cfg.proxy.clone(),
                        backend,
                        snowflake.clone(),
                        cfg.snowflake.clone(),
                        cfg.router.clone(),
                        router_cache.clone(),
                        sync_matcher.clone(),
                        discovery.clone(),
                        async move { shutdown_proxy.notified().await },
                    )
                    .await
                    .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_continuous(Duration::from_secs(60), shutdown_cont)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    sync.clone()
                        .run_policy_refresh(shutdown_pol)
                        .await
                        .map_err(|e| anyhow!(e))
                },
                async {
                    melt_metrics::serve_admin_with_hooks(&metrics, hooks, async move {
                        shutdown_metrics.notified().await
                    })
                    .await
                    .map_err(|e| anyhow!(e))
                }
            )?;
        }
        Command::Status { json } => {
            print_status_iceberg(&catalog, &cfg, snowflake.as_ref(), json).await?;
        }
        Command::Route(_) => unreachable!("handled above"),
    }
    Ok(())
}

#[cfg(feature = "ducklake")]
fn build_readiness_ducklake(
    catalog: Arc<melt_ducklake::CatalogClient>,
) -> melt_metrics::ReadinessProbe {
    melt_metrics::ReadinessProbe::new(move || {
        let cat = catalog.clone();
        async move { cat.ping().await.is_ok() }
    })
}

#[cfg(feature = "ducklake")]
async fn print_status_ducklake(
    catalog: &melt_ducklake::CatalogClient,
    cfg: &MeltConfig,
    snowflake: &SnowflakeClient,
    json: bool,
) -> Result<()> {
    let healthy = catalog.ping().await.is_ok();
    let snap = if healthy {
        Some(catalog.status_snapshot().await?)
    } else {
        None
    };
    let sf = probe_snowflake(snowflake).await;

    let value = serde_json::json!({
        "backend":            "ducklake",
        "catalog_reachable":  healthy,
        "tables_tracked":     snap.as_ref().map(|s| s.tables_tracked),
        "policy_markers":     snap.as_ref().map(|s| s.marker_count),
        "last_policy_refresh_secs": snap.as_ref().map(|s| s.last_policy_refresh_age_secs),
        "max_sync_lag_secs":  snap.as_ref().map(|s| s.max_sync_lag_secs),
        "policy_mode":        policy_mode_label(cfg),
        "lake_threshold":     cfg.router.lake_max_scan_bytes.to_string(),
        "snowflake_host":     cfg.snowflake.host(),
        "snowflake_reachable": sf.reachable,
        "snowflake_detail":    sf.detail,
        "proxy_listen":       cfg.proxy.listen.to_string(),
        "proxy_tls":          cfg.proxy.tls_cert.exists() && cfg.proxy.tls_key.exists(),
    });

    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_status_plain(&value);
    Ok(())
}

#[cfg(feature = "iceberg")]
async fn print_status_iceberg(
    catalog: &melt_iceberg::IcebergCatalogClient,
    cfg: &MeltConfig,
    snowflake: &SnowflakeClient,
    json: bool,
) -> Result<()> {
    let tables = catalog.list_tables().await.unwrap_or_default();
    let sf = probe_snowflake(snowflake).await;

    let value = serde_json::json!({
        "backend":            "iceberg",
        "catalog_flavour":    format!("{:?}", catalog.config().catalog),
        "warehouse":          catalog.config().warehouse,
        "tables_tracked":     tables.len(),
        "policy_mode":        policy_mode_label(cfg),
        "snowflake_host":     cfg.snowflake.host(),
        "snowflake_reachable": sf.reachable,
        "snowflake_detail":    sf.detail,
        "proxy_listen":       cfg.proxy.listen.to_string(),
        "proxy_tls":          cfg.proxy.tls_cert.exists() && cfg.proxy.tls_key.exists(),
    });

    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_status_plain(&value);
    Ok(())
}

fn policy_mode_label(cfg: &MeltConfig) -> String {
    use melt_core::PolicyMode;
    match &cfg.snowflake.policy.mode {
        PolicyMode::Passthrough => "passthrough".to_string(),
        PolicyMode::AllowList { tables } => format!("allowlist ({} tables)", tables.len()),
        PolicyMode::Enforce => "enforce (NOT IMPLEMENTED)".to_string(),
    }
}

struct SnowflakeProbe {
    reachable: bool,
    detail: String,
}

/// Ask the SnowflakeClient for a service token with a 5-second cap.
/// Success means the upstream responded and the configured credential
/// is valid; failure surfaces the reason so operators can tell
/// "wrong PAT" from "DNS broken" from "Snowflake is down."
async fn probe_snowflake(client: &SnowflakeClient) -> SnowflakeProbe {
    use std::time::Duration;
    match tokio::time::timeout(Duration::from_secs(5), client.service_token()).await {
        Ok(Ok(_)) => SnowflakeProbe {
            reachable: true,
            detail: "token ok".to_string(),
        },
        Ok(Err(e)) => SnowflakeProbe {
            reachable: false,
            detail: e.to_string(),
        },
        Err(_) => SnowflakeProbe {
            reachable: false,
            detail: "timed out after 5s".to_string(),
        },
    }
}

fn print_status_plain(v: &serde_json::Value) {
    // Human-readable projection. We iterate explicitly (rather than
    // dumping via Debug) so the key order stays stable and readable.
    let fields = [
        ("backend", "backend:               "),
        ("catalog_reachable", "catalog reachable:     "),
        ("catalog_flavour", "catalog flavour:       "),
        ("warehouse", "warehouse:             "),
        ("tables_tracked", "tables tracked:        "),
        ("policy_markers", "policy markers:        "),
        ("last_policy_refresh_secs", "last policy refresh:   "),
        ("max_sync_lag_secs", "max sync lag:          "),
        ("policy_mode", "policy mode:           "),
        ("lake_threshold", "lake threshold:        "),
        ("proxy_listen", "proxy listen:          "),
        ("proxy_tls", "proxy TLS enabled:     "),
        ("snowflake_host", "snowflake host:        "),
        ("snowflake_reachable", "snowflake reachable:   "),
        ("snowflake_detail", "snowflake detail:      "),
    ];
    for (key, label) in fields {
        if let Some(val) = v.get(key) {
            if val.is_null() {
                continue;
            }
            println!("{label}{}", render_json(val));
        }
    }
}

fn render_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if n.as_i64().is_none() {
                    return format!("{f:.1}s");
                }
            }
            n.to_string()
        }
        serde_json::Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

fn print_lazy_route(cfg: &MeltConfig, sql: &str) -> Result<()> {
    use melt_router::decide::lazy_classify;

    // Lazy session for the AST resolver — `melt route` doesn't have a
    // real Snowflake session, so we synthesize a placeholder using the
    // configured account-default DB/schema if any.
    let session = melt_core::SessionInfo::new("melt-cli-route", 1);
    let outcome = lazy_classify(sql, &session, &cfg.snowflake);

    println!("input SQL: {sql}");
    println!("route: {}", outcome.route.as_str());
    match outcome.route {
        melt_core::Route::Lake { reason } => {
            println!("reason: {reason:?}");
            if let Some(t) = outcome.translated_sql {
                println!("translated:");
                println!("{t}");
            }
        }
        melt_core::Route::Snowflake { reason } => {
            println!("reason: {} ({:?})", reason.label(), reason);
        }
    }
    println!("\nNote: `melt route` runs the cheap classification path only.");
    println!("TableMissing / AboveThreshold / PolicyProtected can't be evaluated");
    println!("without a live backend — use `melt status` for the full picture.");

    let _: &dyn RouterCache = &NoopRouterCache;
    Ok(())
}
