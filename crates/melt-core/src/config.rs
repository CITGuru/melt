use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use crate::error::{MeltError, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProxyConfig {
    pub listen: SocketAddr,
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    #[serde(default)]
    pub limits: ProxyLimits,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProxyLimits {
    #[serde(
        with = "humantime_serde",
        default = "ProxyLimits::default_request_timeout"
    )]
    pub request_timeout: Duration,

    #[serde(default = "ProxyLimits::default_max_concurrent_per_session")]
    pub max_concurrent_per_session: u32,

    #[serde(default = "ProxyLimits::default_max_concurrent_global")]
    pub max_concurrent_global: u32,

    #[serde(default = "ProxyLimits::default_result_store_max_bytes")]
    pub result_store_max_bytes: ByteSize,

    #[serde(default = "ProxyLimits::default_result_store_max_entries")]
    pub result_store_max_entries: u32,

    #[serde(with = "humantime_serde", default = "ProxyLimits::default_idle_ttl")]
    pub result_store_idle_ttl: Duration,

    #[serde(
        with = "humantime_serde",
        default = "ProxyLimits::default_drain_timeout"
    )]
    pub shutdown_drain_timeout: Duration,

    /// Maximum HTTP body size accepted on proxy routes (login,
    /// statement, partition poll). Caps what axum reads into memory
    /// before the handler runs. Larger bodies return 413. Default is
    /// conservative — operators running bulk `COPY INTO` via the REST
    /// API can raise it.
    #[serde(default = "ProxyLimits::default_max_body_bytes")]
    pub max_body_bytes: ByteSize,
}

impl ProxyLimits {
    fn default_request_timeout() -> Duration {
        Duration::from_secs(30)
    }
    fn default_max_concurrent_per_session() -> u32 {
        16
    }
    fn default_max_concurrent_global() -> u32 {
        256
    }
    fn default_result_store_max_bytes() -> ByteSize {
        ByteSize::gb(2)
    }
    fn default_result_store_max_entries() -> u32 {
        10_000
    }
    fn default_idle_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }
    fn default_drain_timeout() -> Duration {
        Duration::from_secs(30)
    }
    fn default_max_body_bytes() -> ByteSize {
        ByteSize::mib(16)
    }
}

impl Default for ProxyLimits {
    fn default() -> Self {
        Self {
            request_timeout: Self::default_request_timeout(),
            max_concurrent_per_session: Self::default_max_concurrent_per_session(),
            max_concurrent_global: Self::default_max_concurrent_global(),
            result_store_max_bytes: Self::default_result_store_max_bytes(),
            result_store_max_entries: Self::default_result_store_max_entries(),
            result_store_idle_ttl: Self::default_idle_ttl(),
            shutdown_drain_timeout: Self::default_drain_timeout(),
            max_body_bytes: Self::default_max_body_bytes(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RouterConfig {
    #[serde(default = "RouterConfig::default_lake_max")]
    pub lake_max_scan_bytes: ByteSize,

    #[serde(with = "humantime_serde", default = "RouterConfig::default_table_ttl")]
    pub table_exists_cache_ttl: Duration,

    #[serde(
        with = "humantime_serde",
        default = "RouterConfig::default_estimate_ttl"
    )]
    pub estimate_bytes_cache_ttl: Duration,

    // ── Hybrid / dual execution ──────────────────────────────────
    /// Master switch. When false (default), `Route::Hybrid` is never
    /// emitted — Remote-classified tables passthrough as today. When
    /// true, the dual-execution router emits hybrid plans subject to
    /// the per-strategy caps and trigger-case toggles below.
    #[serde(default)]
    pub hybrid_execution: bool,

    // ── Materialize-strategy caps (per RemoteSql node, 2+ scans) ──
    /// Total bytes the Materialize path may pull from Snowflake
    /// across all fragments in one query. Above this, the whole
    /// query collapses to `Route::Snowflake { AboveThreshold }`.
    #[serde(default = "RouterConfig::default_hybrid_remote_cap")]
    pub hybrid_max_remote_scan_bytes: ByteSize,
    /// Per-fragment cap. Prevents one fragment eating the whole
    /// budget.
    #[serde(default = "RouterConfig::default_hybrid_fragment_cap")]
    pub hybrid_max_fragment_bytes: ByteSize,

    // ── Attach-strategy caps (per RemoteSql node, 1 scan) ────────
    /// Set false to force every Remote node to Materialize. Useful
    /// as a kill switch when the community Snowflake extension
    /// misbehaves. The pool also flips this off automatically when
    /// the extension fails to load at startup (§8.2 / §10.6).
    #[serde(default = "RouterConfig::default_true")]
    pub hybrid_attach_enabled: bool,
    /// Per-Attach-scan raw estimate cap. Above this, the strategy
    /// selector downgrades the node to Materialize (which then
    /// applies the Materialize cap).
    #[serde(default = "RouterConfig::default_hybrid_attach_cap")]
    pub hybrid_max_attach_scan_bytes: ByteSize,

    // ── Trigger-case toggles ─────────────────────────────────────
    /// Allow Pending/Bootstrapping tables to be served via the
    /// remote pool (Case 2 in the design doc).
    #[serde(default)]
    pub hybrid_allow_bootstrapping: bool,
    /// Allow a single oversize lake table to be served via the
    /// remote pool while the rest of the query stays local
    /// (Case 3 in the design doc).
    #[serde(default)]
    pub hybrid_allow_oversize: bool,

    // ── Diagnostic ───────────────────────────────────────────────
    /// Probability with which the parity sampler replays the query
    /// against pure Snowflake to detect type-drift mismatches.
    /// 0.0 disables; 1.0 samples every query (very expensive).
    #[serde(default = "RouterConfig::default_parity_sample_rate")]
    pub hybrid_parity_sample_rate: f32,
    /// How aggressively the parity sampler compares hybrid and
    /// Snowflake results. `RowCount` (default) is the cheap first
    /// gate — only the row counts are compared, the digest path is
    /// skipped. `Hash` adds the per-row XOR-of-SHA256 digest
    /// comparison; catches cell-level drift (decimal precision,
    /// timestamp TZ, semi-structured) at the cost of buffering eager
    /// batches and running the full canonicalisation. Hash is
    /// follow-up work — keep on `RowCount` for v0.1 unless you've
    /// budgeted the extra CPU.
    #[serde(default)]
    pub hybrid_parity_compare_mode: HybridParityCompareMode,
    /// When true, `execute_hybrid` reads DuckDB's profiler JSON
    /// after each Attach query and logs the `snowflake_scan`
    /// operator's emitted SQL. Off by default — profiler overhead
    /// is non-trivial. Enable per-tenant during debugging.
    #[serde(default)]
    pub hybrid_profile_attach_queries: bool,
    /// Periodic refresh interval for the DuckDB Snowflake extension's
    /// per-table schema cache. The proxy's pool runs `DETACH IF EXISTS
    /// sf_link; ATTACH ... AS sf_link;` on each pooled connection at
    /// most once per interval (lazy: per-connection check at recycle
    /// time). Bounds the staleness window when upstream Snowflake
    /// schemas evolve. Default 1 hour. Set to 0 to disable
    /// (useful for tests).
    #[serde(
        default = "RouterConfig::default_hybrid_attach_refresh_interval",
        with = "humantime_serde"
    )]
    pub hybrid_attach_refresh_interval: Duration,
    /// Statement-level result cache TTL for the hybrid path. When
    /// set to a positive duration, identical hybrid queries within
    /// the window skip the Snowflake roundtrip entirely and replay
    /// cached batches. Default 0 (disabled).
    #[serde(
        default = "RouterConfig::default_hybrid_fragment_cache_ttl",
        with = "humantime_serde"
    )]
    pub hybrid_fragment_cache_ttl: Duration,
    /// Hard ceiling on cache entries. Oldest entries evict first
    /// once exceeded.
    #[serde(default = "RouterConfig::default_hybrid_fragment_cache_max_entries")]
    pub hybrid_fragment_cache_max_entries: usize,

    /// Strategy chain for the hybrid router's per-subtree
    /// Attach-vs-Materialize decision. Strategies are tried in
    /// order; first concrete decision wins. Each strategy can
    /// abstain (return `None`), letting the next answer.
    /// See `crates/melt-router/src/hybrid/strategy.rs` and
    /// `docs/internal/DUAL_EXECUTION.md` §13.1.
    ///
    /// Default chain `["heuristic"]` preserves today's behavior
    /// exactly. Operators opting into cost-driven decisions set
    /// `["cost", "heuristic"]` — the cost strategy answers when
    /// it has stats and a clear advantage, heuristic picks up
    /// when stats are missing or near-tie.
    #[serde(default)]
    pub hybrid_strategy: HybridStrategyConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HybridStrategyConfig {
    /// Strategy names tried in order. Recognized: `"heuristic"`,
    /// `"cost"`. Unknown names error at startup. Empty list falls
    /// back to the built-in default (Skip when Attach is on,
    /// Collapse otherwise) — same as `["heuristic"]` for current
    /// behaviour.
    #[serde(default = "HybridStrategyConfig::default_chain")]
    pub chain: Vec<String>,

    /// Cost-strategy tunables. Used when `"cost"` appears in `chain`.
    #[serde(default)]
    pub cost: CostStrategyConfig,
}

impl Default for HybridStrategyConfig {
    fn default() -> Self {
        Self {
            chain: Self::default_chain(),
            cost: CostStrategyConfig::default(),
        }
    }
}

impl HybridStrategyConfig {
    fn default_chain() -> Vec<String> {
        vec!["heuristic".to_string()]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CostStrategyConfig {
    /// Sustained Snowflake → proxy network throughput, bytes per
    /// second. Default 100 MB/s. Re-fit per deployment.
    #[serde(default = "CostStrategyConfig::default_network_bytes_per_sec")]
    pub network_bytes_per_sec: f64,

    /// DuckDB streaming-scan throughput through the Snowflake
    /// extension (Attach path). Default 5M rows/sec.
    #[serde(default = "CostStrategyConfig::default_attach_rows_per_sec")]
    pub attach_rows_per_sec: f64,

    /// DuckDB scan throughput on a materialized temp table
    /// (Materialize path's post-stage scan). Default 25M rows/sec.
    #[serde(default = "CostStrategyConfig::default_materialize_scan_rows_per_sec")]
    pub materialize_scan_rows_per_sec: f64,

    /// Per-row write cost when staging the temp table. Default
    /// 12M rows/sec.
    #[serde(default = "CostStrategyConfig::default_materialize_write_rows_per_sec")]
    pub materialize_write_rows_per_sec: f64,

    /// Fixed setup overhead per Materialize fragment. Default 5 ms.
    #[serde(default = "CostStrategyConfig::default_materialize_setup_seconds")]
    pub materialize_setup_seconds: f64,

    /// Cost difference required to flip a heuristic decision.
    /// `1.0` ⇒ always pick cheaper; `1.5` ⇒ require ≥ 50 % cheaper.
    /// Higher values reduce flapping caused by stale stats.
    /// Default 1.5.
    #[serde(default = "CostStrategyConfig::default_min_advantage_ratio")]
    pub min_advantage_ratio: f64,
}

impl Default for CostStrategyConfig {
    fn default() -> Self {
        Self {
            network_bytes_per_sec: Self::default_network_bytes_per_sec(),
            attach_rows_per_sec: Self::default_attach_rows_per_sec(),
            materialize_scan_rows_per_sec: Self::default_materialize_scan_rows_per_sec(),
            materialize_write_rows_per_sec: Self::default_materialize_write_rows_per_sec(),
            materialize_setup_seconds: Self::default_materialize_setup_seconds(),
            min_advantage_ratio: Self::default_min_advantage_ratio(),
        }
    }
}

impl CostStrategyConfig {
    fn default_network_bytes_per_sec() -> f64 {
        100.0 * 1_000_000.0
    }
    fn default_attach_rows_per_sec() -> f64 {
        5_000_000.0
    }
    fn default_materialize_scan_rows_per_sec() -> f64 {
        25_000_000.0
    }
    fn default_materialize_write_rows_per_sec() -> f64 {
        12_000_000.0
    }
    fn default_materialize_setup_seconds() -> f64 {
        0.005
    }
    fn default_min_advantage_ratio() -> f64 {
        1.5
    }
}

impl RouterConfig {
    fn default_lake_max() -> ByteSize {
        ByteSize::gb(100)
    }
    fn default_table_ttl() -> Duration {
        Duration::from_secs(5 * 60)
    }
    fn default_estimate_ttl() -> Duration {
        Duration::from_secs(60)
    }
    fn default_hybrid_remote_cap() -> ByteSize {
        ByteSize::gib(5)
    }
    fn default_hybrid_fragment_cap() -> ByteSize {
        ByteSize::gib(2)
    }
    fn default_hybrid_attach_cap() -> ByteSize {
        ByteSize::gib(10)
    }
    fn default_parity_sample_rate() -> f32 {
        0.01
    }
    fn default_hybrid_attach_refresh_interval() -> Duration {
        Duration::from_secs(60 * 60)
    }
    fn default_hybrid_fragment_cache_ttl() -> Duration {
        Duration::ZERO
    }
    fn default_hybrid_fragment_cache_max_entries() -> usize {
        256
    }
    fn default_true() -> bool {
        true
    }
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            lake_max_scan_bytes: Self::default_lake_max(),
            table_exists_cache_ttl: Self::default_table_ttl(),
            estimate_bytes_cache_ttl: Self::default_estimate_ttl(),
            hybrid_execution: false,
            hybrid_max_remote_scan_bytes: Self::default_hybrid_remote_cap(),
            hybrid_max_fragment_bytes: Self::default_hybrid_fragment_cap(),
            hybrid_attach_enabled: Self::default_true(),
            hybrid_max_attach_scan_bytes: Self::default_hybrid_attach_cap(),
            hybrid_allow_bootstrapping: false,
            hybrid_allow_oversize: false,
            hybrid_parity_sample_rate: Self::default_parity_sample_rate(),
            hybrid_parity_compare_mode: HybridParityCompareMode::default(),
            hybrid_profile_attach_queries: false,
            hybrid_attach_refresh_interval: Self::default_hybrid_attach_refresh_interval(),
            hybrid_fragment_cache_ttl: Self::default_hybrid_fragment_cache_ttl(),
            hybrid_fragment_cache_max_entries: Self::default_hybrid_fragment_cache_max_entries(),
            hybrid_strategy: HybridStrategyConfig::default(),
        }
    }
}

/// Comparison mode for the hybrid parity sampler. Trade-off between
/// correctness coverage and CPU cost — `RowCount` rejects gross drift
/// (wrong join cardinality, missing rows) cheaply; `Hash` extends the
/// check to cell-level drift via the per-row digest. Default
/// `RowCount` matches the POWA-162 plan: `Hash` is parked behind the
/// default until the bench harness shows the digest cost is workable
/// at the configured sample rate.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HybridParityCompareMode {
    /// Compare row counts only. Cheapest path; the digest comparison
    /// is skipped even when eager batches are available.
    #[default]
    RowCount,
    /// Compare row counts AND a per-row XOR-of-SHA256 digest of the
    /// canonicalised result. Catches NUMBER precision, TIMESTAMP_TZ,
    /// VARIANT and NULL ordering drift.
    Hash,
}

impl HybridParityCompareMode {
    /// Stable label for metric/log fields. Matches the `reason` label
    /// vocabulary on `melt_hybrid_parity_mismatches_total`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RowCount => "row_count",
            Self::Hash => "hash",
        }
    }
}

/// Subset of metrics config exposed in `melt-core` for shared types.
/// `melt-metrics` mirrors this with its own internal richer struct.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricsConfigShared {
    pub listen: Option<SocketAddr>,
    #[serde(default)]
    pub log_format: LogFormat,
    #[serde(default = "MetricsConfigShared::default_log_level")]
    pub log_level: String,
}

impl MetricsConfigShared {
    fn default_log_level() -> String {
        "info".to_string()
    }
}

impl Default for MetricsConfigShared {
    fn default() -> Self {
        Self {
            listen: None,
            log_format: LogFormat::default(),
            log_level: Self::default_log_level(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    #[default]
    Pretty,
}

/// Convenience top-level config envelope used by tests and tooling
/// that need the shared shape without pulling backend specifics.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MeltConfigShared {
    pub proxy: ProxyConfig,
    pub router: RouterConfig,
    pub metrics: MetricsConfigShared,
}

/// S3-compatible object store credentials + endpoint overrides.
///
/// Shared by DuckLake (data files on `s3://`) and Iceberg (data +
/// manifest files on `s3://`). Rendered into DuckDB's `CREATE SECRET
/// (TYPE S3, …)` so any S3-compatible service works: AWS, MinIO,
/// Cloudflare R2, Backblaze B2, Wasabi, Ceph/RadosGW, GCS (HMAC
/// mode), etc.
///
/// Credential resolution order:
/// 1. Inline `access_key_id` + `secret_access_key` (+ optional `session_token`)
/// 2. Env-var names: `access_key_id_env` + `secret_access_key_env`
///    (+ optional `session_token_env`) — values are read from process env
/// 3. Empty → DuckDB's `PROVIDER credential_chain` (IMDS, env,
///    config file, SSO, …). This is the right default on AWS.
///
/// Prefer env vars or credential_chain for real deployments; inline
/// keys are for local dev / MinIO only.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct S3Config {
    /// AWS region, e.g. "us-east-1". Required for AWS; for MinIO/R2
    /// put any placeholder like "us-east-1" (R2 accepts "auto").
    pub region: String,

    /// Hostname (and optional port) of the S3 endpoint. Omit for
    /// AWS so DuckDB uses the region-default URL. Examples:
    /// - MinIO:           "localhost:9000" / "minio:9000"
    /// - Cloudflare R2:   "<accountid>.r2.cloudflarestorage.com"
    /// - Backblaze B2:    "s3.us-west-002.backblazeb2.com"
    /// - Wasabi:          "s3.us-east-1.wasabisys.com"
    #[serde(default)]
    pub endpoint: String,

    /// URL style. "vhost" (default) for AWS/R2/B2/Wasabi; "path"
    /// for MinIO / Ceph / localhost:<port> setups.
    #[serde(default = "S3Config::default_url_style")]
    pub url_style: String,

    /// Whether httpfs should talk HTTPS. Default true. Set false for
    /// local MinIO on `http://`.
    #[serde(default = "S3Config::default_use_ssl")]
    pub use_ssl: bool,

    /// Inline access key. Avoid in production — use `access_key_id_env`
    /// or leave empty for credential_chain.
    #[serde(default)]
    pub access_key_id: String,

    /// Name of env var to read the access key from, e.g.
    /// `"AWS_ACCESS_KEY_ID"` or `"MINIO_ROOT_USER"`.
    #[serde(default)]
    pub access_key_id_env: String,

    /// Inline secret. Same caveat as `access_key_id`.
    #[serde(default)]
    pub secret_access_key: String,

    /// Name of env var to read the secret from, e.g.
    /// `"AWS_SECRET_ACCESS_KEY"` or `"MINIO_ROOT_PASSWORD"`.
    #[serde(default)]
    pub secret_access_key_env: String,

    /// Inline STS session token (temp credentials).
    #[serde(default)]
    pub session_token: String,

    /// Name of env var to read the session token from, e.g.
    /// `"AWS_SESSION_TOKEN"`.
    #[serde(default)]
    pub session_token_env: String,

    /// Optional SCOPE restricting this secret to a specific bucket
    /// prefix like `"s3://melt-prod/"`. Useful when multiple S3
    /// secrets coexist (DuckDB picks the longest-prefix match).
    #[serde(default)]
    pub scope: String,
}

impl S3Config {
    fn default_url_style() -> String {
        "vhost".to_string()
    }
    fn default_use_ssl() -> bool {
        true
    }

    /// Whether any credential — inline or env-var — is configured.
    /// When false, `to_duckdb_secret_sql` emits `PROVIDER credential_chain`.
    pub fn has_explicit_credentials(&self) -> bool {
        !self.access_key_id.is_empty()
            || !self.access_key_id_env.is_empty()
            || !self.secret_access_key.is_empty()
            || !self.secret_access_key_env.is_empty()
    }

    /// Resolve the configured credential tuple by reading any
    /// `*_env` fields from the process environment. Returns `None`
    /// if no credentials are configured (caller should fall back to
    /// `PROVIDER credential_chain`).
    ///
    /// Errors if a field mixes inline + env (ambiguous), or if an
    /// env var is named but unset.
    pub fn resolve_credentials(&self) -> Result<Option<ResolvedS3Credentials>> {
        if !self.has_explicit_credentials() {
            return Ok(None);
        }

        let key = resolve_field(
            "access_key_id",
            &self.access_key_id,
            &self.access_key_id_env,
        )?;
        let secret = resolve_field(
            "secret_access_key",
            &self.secret_access_key,
            &self.secret_access_key_env,
        )?;
        let token = resolve_optional_field(
            "session_token",
            &self.session_token,
            &self.session_token_env,
        )?;

        Ok(Some(ResolvedS3Credentials {
            access_key_id: key,
            secret_access_key: secret,
            session_token: token,
        }))
    }

    /// Render a DuckDB `CREATE OR REPLACE SECRET` statement that
    /// configures httpfs for this S3 endpoint. Call sites emit this
    /// once per DuckDB connection in the setup SQL, before any `ATTACH`
    /// or `COPY` that touches `s3://`.
    ///
    /// `name` must be a valid DuckDB identifier (letters, digits,
    /// underscore). Only operators set this; we hard-code `melt_s3`
    /// in the pool setup.
    pub fn to_duckdb_secret_sql(&self, name: &str) -> Result<String> {
        if self.region.is_empty() {
            return Err(MeltError::config(
                "[..s3].region is required (use e.g. 'us-east-1' or 'auto' for R2)",
            ));
        }

        let mut s = format!("CREATE OR REPLACE SECRET {name} (\n    TYPE S3");
        s.push_str(&format!(",\n    REGION    '{}'", escape_sql(&self.region)));

        if !self.endpoint.is_empty() {
            s.push_str(&format!(
                ",\n    ENDPOINT  '{}'",
                escape_sql(&self.endpoint)
            ));
        }
        s.push_str(&format!(
            ",\n    URL_STYLE '{}'",
            escape_sql(&self.url_style)
        ));
        s.push_str(&format!(",\n    USE_SSL   {}", self.use_ssl));

        match self.resolve_credentials()? {
            Some(creds) => {
                s.push_str(&format!(
                    ",\n    KEY_ID    '{}'",
                    escape_sql(&creds.access_key_id)
                ));
                s.push_str(&format!(
                    ",\n    SECRET    '{}'",
                    escape_sql(&creds.secret_access_key)
                ));
                if let Some(tok) = creds.session_token {
                    s.push_str(&format!(",\n    SESSION_TOKEN '{}'", escape_sql(&tok)));
                }
            }
            None => {
                // No explicit creds → use DuckDB's built-in provider
                // chain. Same logic as the AWS SDK (env, profile,
                // IMDS, SSO, container creds).
                s.push_str(",\n    PROVIDER credential_chain");
            }
        }

        if !self.scope.is_empty() {
            s.push_str(&format!(",\n    SCOPE     '{}'", escape_sql(&self.scope)));
        }

        s.push_str("\n);");
        Ok(s)
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: String::new(),
            endpoint: String::new(),
            url_style: Self::default_url_style(),
            use_ssl: Self::default_use_ssl(),
            access_key_id: String::new(),
            access_key_id_env: String::new(),
            secret_access_key: String::new(),
            secret_access_key_env: String::new(),
            session_token: String::new(),
            session_token_env: String::new(),
            scope: String::new(),
        }
    }
}

/// Resolved, ready-to-use S3 credentials. Produced by
/// [`S3Config::resolve_credentials`].
#[derive(Clone, Debug)]
pub struct ResolvedS3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

fn resolve_field(field: &str, inline: &str, env_name: &str) -> Result<String> {
    match (inline.is_empty(), env_name.is_empty()) {
        (true, true) => Err(MeltError::config(format!(
            "s3 credentials: `{field}` / `{field}_env` both empty; set one"
        ))),
        (false, false) => Err(MeltError::config(format!(
            "s3 credentials: both `{field}` (inline) and `{field}_env` are set; pick one"
        ))),
        (false, true) => Ok(inline.to_string()),
        (true, false) => std::env::var(env_name).map_err(|_| {
            MeltError::config(format!(
                "s3 credentials: env var `{env_name}` (named by `{field}_env`) is not set"
            ))
        }),
    }
}

fn resolve_optional_field(field: &str, inline: &str, env_name: &str) -> Result<Option<String>> {
    match (inline.is_empty(), env_name.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => Err(MeltError::config(format!(
            "s3 credentials: both `{field}` and `{field}_env` are set; pick one"
        ))),
        (false, true) => Ok(Some(inline.to_string())),
        (true, false) => match std::env::var(env_name) {
            Ok(v) if !v.is_empty() => Ok(Some(v)),
            _ => Ok(None),
        },
    }
}

/// Escape a SQL string literal for interpolation: double single-quotes.
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod s3_tests {
    use super::*;

    fn base() -> S3Config {
        S3Config {
            region: "us-east-1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn sql_uses_credential_chain_when_no_keys() {
        let sql = base().to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("TYPE S3"));
        assert!(sql.contains("REGION    'us-east-1'"));
        assert!(sql.contains("PROVIDER credential_chain"));
        assert!(!sql.contains("KEY_ID"));
    }

    #[test]
    fn sql_includes_explicit_creds() {
        let cfg = S3Config {
            access_key_id: "AKIA".into(),
            secret_access_key: "s3cr3t".into(),
            ..base()
        };
        let sql = cfg.to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("KEY_ID    'AKIA'"));
        assert!(sql.contains("SECRET    's3cr3t'"));
        assert!(!sql.contains("PROVIDER"));
    }

    #[test]
    fn sql_sets_endpoint_and_path_style_for_minio() {
        let cfg = S3Config {
            endpoint: "localhost:9000".into(),
            url_style: "path".into(),
            use_ssl: false,
            ..base()
        };
        let sql = cfg.to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("ENDPOINT  'localhost:9000'"));
        assert!(sql.contains("URL_STYLE 'path'"));
        assert!(sql.contains("USE_SSL   false"));
    }

    #[test]
    fn sql_requires_region() {
        let cfg = S3Config::default();
        let err = cfg.to_duckdb_secret_sql("x").unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("region is required")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn sql_escapes_single_quotes_in_values() {
        let cfg = S3Config {
            access_key_id: "evil'key".into(),
            secret_access_key: "s3cr'3t".into(),
            ..base()
        };
        let sql = cfg.to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("KEY_ID    'evil''key'"));
        assert!(sql.contains("SECRET    's3cr''3t'"));
    }

    #[test]
    fn sql_includes_session_token_when_set() {
        let cfg = S3Config {
            access_key_id: "A".into(),
            secret_access_key: "B".into(),
            session_token: "T".into(),
            ..base()
        };
        let sql = cfg.to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("SESSION_TOKEN 'T'"));
    }

    #[test]
    fn sql_includes_scope_when_set() {
        let cfg = S3Config {
            scope: "s3://melt-prod/".into(),
            ..base()
        };
        let sql = cfg.to_duckdb_secret_sql("melt_s3").unwrap();
        assert!(sql.contains("SCOPE     's3://melt-prod/'"));
    }

    #[test]
    fn resolve_credentials_returns_none_when_empty() {
        assert!(base().resolve_credentials().unwrap().is_none());
    }

    #[test]
    fn resolve_credentials_inline_wins() {
        let cfg = S3Config {
            access_key_id: "A".into(),
            secret_access_key: "B".into(),
            ..base()
        };
        let c = cfg.resolve_credentials().unwrap().unwrap();
        assert_eq!(c.access_key_id, "A");
        assert_eq!(c.secret_access_key, "B");
        assert!(c.session_token.is_none());
    }

    #[test]
    fn resolve_credentials_env_fallback() {
        let k = "MELT_TEST_S3_KEY";
        let s = "MELT_TEST_S3_SEC";
        std::env::set_var(k, "key-from-env");
        std::env::set_var(s, "sec-from-env");

        let cfg = S3Config {
            access_key_id_env: k.into(),
            secret_access_key_env: s.into(),
            ..base()
        };
        let c = cfg.resolve_credentials().unwrap().unwrap();
        assert_eq!(c.access_key_id, "key-from-env");
        assert_eq!(c.secret_access_key, "sec-from-env");

        std::env::remove_var(k);
        std::env::remove_var(s);
    }

    #[test]
    fn resolve_credentials_rejects_mixing_inline_and_env() {
        let cfg = S3Config {
            access_key_id: "inline".into(),
            access_key_id_env: "SOMETHING".into(),
            secret_access_key: "B".into(),
            ..base()
        };
        let err = cfg.resolve_credentials().unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("pick one")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_credentials_errors_when_env_unset() {
        let cfg = S3Config {
            access_key_id_env: "MELT_TEST_UNSET_KEY_XYZ".into(),
            secret_access_key_env: "MELT_TEST_UNSET_SEC_XYZ".into(),
            ..base()
        };
        let err = cfg.resolve_credentials().unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("is not set")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }
}
