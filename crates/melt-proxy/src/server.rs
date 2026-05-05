use std::future::Future;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;
use axum_server::Handle;
use melt_core::config::{ProxyConfig, RouterConfig};
use melt_core::{DiscoveryCatalog, MeltError, Result, StorageBackend, SyncTableMatcher};
use melt_router::Cache;
use melt_snowflake::{SnowflakeClient, SnowflakeConfig};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::trace::TraceLayer;

/// Reloadable handle to the `SyncTableMatcher`. Shared between the
/// proxy (reads per query) and the admin reload closure (swaps on
/// `POST /admin/reload`). `None` signals the legacy "no [sync] block"
/// path — router falls back to `tables_exist` lookups.
pub type SharedMatcher = Arc<ArcSwap<Option<Arc<SyncTableMatcher>>>>;

use crate::handlers;
use crate::hybrid_cache::FragmentCache;
use crate::hybrid_parity::ParityHarness;
use crate::result_store::ResultStore;
use crate::session::SessionStore;

/// Shared proxy state. Cheap to clone — every field is `Arc` or
/// trivially Copy.
#[derive(Clone)]
pub struct ProxyState {
    pub backend: Arc<dyn StorageBackend>,
    pub snowflake: Arc<SnowflakeClient>,
    pub snowflake_cfg: Arc<SnowflakeConfig>,
    pub router_cfg: Arc<RouterConfig>,
    pub router_cache: Arc<Cache>,
    /// Reloadable compiled glob matcher for `[sync].{include,exclude}`.
    /// Loaded per-query via `ArcSwap::load` so admin reload mutations
    /// take effect immediately without a restart.
    pub sync_matcher: SharedMatcher,
    /// Postgres-backed control catalog (type-erased as
    /// `DiscoveryCatalog`). Paired with `sync_matcher`.
    pub discovery: Option<Arc<dyn DiscoveryCatalog>>,
    pub sessions: Arc<SessionStore>,
    pub results: Arc<ResultStore>,
    pub request_timeout: std::time::Duration,
    /// Sibling `ca.pem` next to `tls_cert` is served at `/melt/ca.pem`
    /// when this is `Some` and the file exists. Powers `melt bootstrap client`.
    pub tls_cert: Option<std::path::PathBuf>,
    /// Bounded parity-sampler handle. Present iff
    /// `router.hybrid_parity_sample_rate > 0.0`. `execute_hybrid`
    /// opportunistically pushes a [`crate::hybrid_parity::ParitySample`]
    /// here per completed hybrid query; the background task replays
    /// the original SQL against Snowflake and compares digests.
    pub parity: Option<Arc<ParityHarness>>,
    /// Statement-level result cache for the hybrid path. Present iff
    /// `router.hybrid_fragment_cache_ttl > 0`. `execute_hybrid` checks
    /// before staging fragments and writes the eager batches on
    /// completion. `RouterCache::invalidate_table` cascades into
    /// [`FragmentCache::invalidate_table`].
    pub hybrid_cache: Option<Arc<FragmentCache>>,
}

/// Public entry point. Spins up the axum HTTPS listener.
///
/// `melt_metrics::serve_admin` is run separately by the CLI under
/// `tokio::try_join!` so the metrics admin port stays up even when
/// the main TLS listener restarts.
///
/// `shutdown` is the process-wide cooperative shutdown future owned
/// by the CLI. When it resolves, the listener triggers
/// `axum_server::Handle::graceful_shutdown(drain_timeout)`.
#[allow(clippy::too_many_arguments)]
pub async fn serve<F>(
    cfg: ProxyConfig,
    backend: Arc<dyn StorageBackend>,
    snowflake: Arc<SnowflakeClient>,
    snowflake_cfg: SnowflakeConfig,
    router_cfg: RouterConfig,
    router_cache: Arc<Cache>,
    sync_matcher: SharedMatcher,
    discovery: Option<Arc<dyn DiscoveryCatalog>>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let limits = cfg.limits.clone();
    let sessions = Arc::new(SessionStore::new(limits.clone()));
    let results = ResultStore::new(limits.clone());
    results.clone().run_idle_sweeper();

    let tls_cert_path = if cfg.tls_cert.as_os_str().is_empty() {
        None
    } else {
        Some(cfg.tls_cert.clone())
    };

    // Parity sampler — only spawned when `router.hybrid_parity_sample_rate > 0`.
    // Channel capacity is generous (64) so transient execute_hybrid
    // bursts don't drop samples; drop-on-full is the right policy for
    // a diagnostic harness.
    let parity = if router_cfg.hybrid_parity_sample_rate > 0.0 {
        Some(Arc::new(ParityHarness::spawn(
            snowflake.clone(),
            router_cfg.hybrid_parity_sample_rate,
            router_cfg.hybrid_parity_compare_mode,
            64,
        )))
    } else {
        None
    };

    // Hybrid result cache — opt-in via `router.hybrid_fragment_cache_ttl`.
    let hybrid_cache = if !router_cfg.hybrid_fragment_cache_ttl.is_zero() {
        Some(Arc::new(FragmentCache::new(
            router_cfg.hybrid_fragment_cache_ttl,
            router_cfg.hybrid_fragment_cache_max_entries,
        )))
    } else {
        None
    };

    let state = ProxyState {
        backend,
        snowflake,
        snowflake_cfg: Arc::new(snowflake_cfg),
        router_cfg: Arc::new(router_cfg),
        router_cache,
        sync_matcher,
        discovery,
        sessions,
        results,
        request_timeout: limits.request_timeout,
        tls_cert: tls_cert_path,
        parity,
        hybrid_cache,
    };

    let app = Router::new()
        .route(
            "/session/v1/login-request",
            post(handlers::session::login_request),
        )
        .route(
            "/session/v1/token-request",
            post(handlers::session::token_request),
        )
        .route("/session", post(handlers::session::close_session))
        .route("/session/heartbeat", get(handlers::session::heartbeat))
        .route("/api/v2/statements", post(handlers::statement::execute))
        .route("/api/v2/statements/:handle", get(handlers::partition::poll))
        .route(
            "/api/v2/statements/:handle/cancel",
            post(handlers::partition::cancel),
        )
        .route(
            "/queries/v1/query-request",
            post(handlers::v1::query_request),
        )
        .route(
            "/queries/v1/abort-request",
            post(handlers::v1::abort_request),
        )
        .route("/queries/:query_id/result", get(handlers::v1::result_get))
        .route(
            "/monitoring/queries/:query_id",
            get(handlers::v1::monitoring_query),
        )
        .route("/melt/ca.pem", get(handlers::ca::serve))
        // Explicit body limit (axum default is 2MB) and request
        // decompression — Python connector gzips by default and the
        // v1 Lake handler used to 400 on compressed bodies.
        .layer(DefaultBodyLimit::max(
            limits.max_body_bytes.as_u64() as usize
        ))
        .layer(RequestDecompressionLayer::new().gzip(true))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let handle = Handle::new();
    crate::shutdown::install_external(handle.clone(), shutdown, limits.shutdown_drain_timeout);

    if cfg.tls_cert.exists() && cfg.tls_key.exists() {
        let tls_cfg = crate::tls::load(&cfg.tls_cert, &cfg.tls_key).await?;
        tracing::info!(addr = %cfg.listen, "melt-proxy listening (TLS)");
        axum_server::bind_rustls(cfg.listen, tls_cfg)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| MeltError::Io(std::io::Error::other(e)))?;
    } else {
        // Dev only: plain HTTP. Production MUST set tls_cert + tls_key.
        tracing::warn!(
            addr = %cfg.listen,
            "TLS cert/key missing — falling back to plain HTTP. NOT for production."
        );
        axum_server::bind(cfg.listen)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| MeltError::Io(std::io::Error::other(e)))?;
    }

    tracing::info!("melt-proxy stopped");
    Ok(())
}
