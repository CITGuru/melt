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
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            lake_max_scan_bytes: Self::default_lake_max(),
            table_exists_cache_ttl: Self::default_table_ttl(),
            estimate_bytes_cache_ttl: Self::default_estimate_ttl(),
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
