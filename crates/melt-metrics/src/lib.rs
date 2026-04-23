//! `melt-metrics` — single source of truth for observability primitives
//! across Melt: metric registry, the `/metrics` HTTP endpoint, structured
//! logging setup, and tracing helpers.

pub mod http;
pub mod names;
pub mod registry;
pub mod spans;
pub mod tracing_init;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use names::*;
pub use spans::TimedSpan;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricsConfig {
    pub listen: Option<SocketAddr>,
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(default = "MetricsConfig::default_log_level")]
    pub log_level: String,

    /// Inline admin API bearer token. Rarely used — prefer
    /// `admin_token_file` for secret rotation.
    #[serde(default)]
    pub admin_token: String,

    /// Path to a file containing the admin API bearer token. Read
    /// once at startup. If both `admin_token` and `admin_token_file`
    /// are empty AND the listener is bound to a loopback address,
    /// the endpoint accepts unauthenticated requests. If the listener
    /// is non-loopback and no token is set, `serve_admin` refuses to
    /// start.
    #[serde(default)]
    pub admin_token_file: String,
}

impl MetricsConfig {
    fn default_log_level() -> String {
        "info".to_string()
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen: None,
            log_format: LogFormat::default(),
            log_level: Self::default_log_level(),
            admin_token: String::new(),
            admin_token_file: String::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    #[default]
    Pretty,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("metrics init failed: {0}")]
    Init(String),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, MetricsError>;

/// Initialize tracing + the global metrics registry. Must be called
/// once at process startup, before any other crate emits.
pub fn init(cfg: &MetricsConfig) -> Result<()> {
    tracing_init::init_tracing(cfg);
    registry::install_prometheus()?;
    Ok(())
}

/// `/readyz` plug-in. Backends provide a closure that returns `true`
/// when the backend is healthy.
type ReadinessFut = Pin<Box<dyn Future<Output = bool> + Send>>;
type ReadinessFn = dyn Fn() -> ReadinessFut + Send + Sync + 'static;

#[derive(Clone)]
pub struct ReadinessProbe(Arc<ReadinessFn>);

impl ReadinessProbe {
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        Self(Arc::new(move || Box::pin(f()) as ReadinessFut))
    }

    pub async fn check(&self) -> bool {
        (self.0)().await
    }

    /// Always-ready probe, useful for tests and the standalone metrics
    /// admin server when no backend is wired in yet.
    pub fn always_ready() -> Self {
        Self::new(|| async { true })
    }
}

/// Hot-reload closure. Provided by the CLI at startup; executed by
/// the admin HTTP handler. Returns the structured diff of what
/// changed.
pub type ReloadFut = Pin<Box<dyn Future<Output = melt_core::ReloadResponse> + Send>>;
pub type ReloadFn = Arc<dyn Fn() -> ReloadFut + Send + Sync + 'static>;

/// Bundle of optional admin hooks. All fields are filled in by the
/// CLI at startup; `serve_admin` threads them into the axum router.
#[derive(Clone, Default)]
pub struct AdminHooks {
    pub readiness: Option<ReadinessProbe>,
    pub reload: Option<ReloadFn>,
}

impl AdminHooks {
    pub fn with_readiness(mut self, r: ReadinessProbe) -> Self {
        self.readiness = Some(r);
        self
    }
    pub fn with_reload<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = melt_core::ReloadResponse> + Send + 'static,
    {
        self.reload = Some(Arc::new(move || Box::pin(f()) as ReloadFut));
        self
    }
}

/// Serve `/metrics`, `/healthz`, `/readyz`, `/admin/reload` on the
/// configured address. Returns when `shutdown` resolves or the
/// listener errors. Designed to be run inside a `tokio::try_join!`
/// alongside the main proxy or sync loop.
pub async fn serve_admin<F>(
    cfg: &MetricsConfig,
    readiness: ReadinessProbe,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    http::serve(
        cfg,
        AdminHooks::default().with_readiness(readiness),
        shutdown,
    )
    .await
}

/// Like [`serve_admin`] but with an attached reload closure. Used
/// by the CLI's `Start` / `All` commands to expose
/// `POST /admin/reload` against the running proxy.
pub async fn serve_admin_with_hooks<F>(
    cfg: &MetricsConfig,
    hooks: AdminHooks,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    http::serve(cfg, hooks, shutdown).await
}

/// Load the admin bearer token from `[metrics]` config. Returns
/// `None` only when both inline + file fields are empty. Caller
/// decides whether unauth is OK based on the bind address.
pub fn resolve_admin_token(cfg: &MetricsConfig) -> Result<Option<String>> {
    match (cfg.admin_token.is_empty(), cfg.admin_token_file.is_empty()) {
        (true, true) => Ok(None),
        (false, true) => Ok(Some(cfg.admin_token.clone())),
        (true, false) => {
            let s = std::fs::read_to_string(&cfg.admin_token_file).map_err(|e| {
                MetricsError::Init(format!(
                    "read admin_token_file '{}': {e}",
                    cfg.admin_token_file
                ))
            })?;
            Ok(Some(s.trim().to_string()))
        }
        (false, false) => Err(MetricsError::Init(
            "both [metrics].admin_token and admin_token_file are set; pick one".into(),
        )),
    }
}
