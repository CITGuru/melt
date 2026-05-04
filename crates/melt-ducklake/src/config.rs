use std::time::Duration;

use melt_core::S3Config;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DuckLakeConfig {
    /// `postgres://melt@db/melt_catalog`
    pub catalog_url: String,

    /// `s3://bucket/ducklake/`
    pub data_path: String,

    /// S3-compatible endpoint + credentials. Rendered into DuckDB's
    /// `CREATE SECRET (TYPE S3, …)` once per pool connection so any
    /// S3-compatible service works (AWS, MinIO, R2, B2, Wasabi, …).
    #[serde(default)]
    pub s3: S3Config,

    #[serde(default = "DuckLakeConfig::default_reader_pool")]
    pub reader_pool_size: usize,
    #[serde(default = "DuckLakeConfig::default_writer_pool")]
    pub writer_pool_size: usize,

    /// How long a reader-pool checkout waits for a free connection
    /// before failing fast with a timeout. Under sustained pool
    /// saturation (KI-001), the router's Lake-failure-to-passthrough
    /// fallback absorbs the timeout pre-first-byte, so queries shed
    /// load to Snowflake instead of queueing indefinitely. See
    /// `docs/internal/KNOWN_ISSUES.md`.
    #[serde(
        with = "humantime_serde",
        default = "DuckLakeConfig::default_reader_checkout_timeout"
    )]
    pub reader_checkout_timeout: Duration,
}

impl DuckLakeConfig {
    fn default_reader_pool() -> usize {
        8
    }
    fn default_writer_pool() -> usize {
        1
    }
    fn default_reader_checkout_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_s3_block_parses() {
        let raw = r#"
            catalog_url = "postgres://melt@db/melt_catalog"
            data_path = "s3://melt/ducklake/"
            [s3]
            region    = "us-east-1"
            endpoint  = "localhost:9000"
            url_style = "path"
            use_ssl   = false
            access_key_id     = "minioadmin"
            secret_access_key = "minioadmin"
        "#;
        let cfg: DuckLakeConfig = toml::from_str(raw).unwrap();
        assert_eq!(cfg.s3.region, "us-east-1");
        assert_eq!(cfg.s3.endpoint, "localhost:9000");
        assert_eq!(cfg.s3.url_style, "path");
        assert!(!cfg.s3.use_ssl);
    }

    #[test]
    fn missing_s3_block_defaults_to_empty_s3_config() {
        let raw = r#"
            catalog_url = "postgres://..."
            data_path   = "/tmp/ducklake/"
        "#;
        let cfg: DuckLakeConfig = toml::from_str(raw).unwrap();
        assert!(cfg.s3.region.is_empty());
        assert!(!cfg.s3.has_explicit_credentials());
    }
}
