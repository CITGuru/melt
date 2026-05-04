use std::time::Duration;

use melt_core::S3Config;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IcebergCatalogKind {
    Rest,
    Glue,
    Polaris,
    Hive,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IcebergConfig {
    pub catalog: IcebergCatalogKind,
    pub warehouse: String,

    /// Required when `catalog = "rest"` or `catalog = "polaris"`.
    #[serde(default)]
    pub rest_uri: String,

    /// S3-compatible endpoint + credentials. Rendered into DuckDB's
    /// `CREATE SECRET (TYPE S3, …)` so any S3 target works (AWS,
    /// MinIO, R2, B2, Wasabi, Ceph, …).
    #[serde(default)]
    pub s3: S3Config,

    /// Postgres connection string for the control-plane catalog
    /// (sync state, policy markers, result-sync progress). Optional;
    /// when empty, sync is disabled and the router falls back to
    /// its pre-discovery routing path (all unknown tables go to
    /// Snowflake passthrough).
    ///
    /// DuckLake deployments share this with `[backend.ducklake].catalog_url`
    /// because the DuckLake catalog already lives in Postgres. Iceberg
    /// has no such requirement — operators typically point this at
    /// the same Postgres anyway for operational simplicity.
    #[serde(default)]
    pub control_catalog_url: String,

    #[serde(default = "IcebergConfig::default_pool")]
    pub reader_pool_size: usize,

    /// How long a reader-pool checkout waits for a free connection
    /// before failing fast with a timeout. Under sustained pool
    /// saturation (KI-001), the router's Lake-failure-to-passthrough
    /// fallback absorbs the timeout pre-first-byte, so queries shed
    /// load to Snowflake instead of queueing indefinitely. See
    /// `docs/internal/KNOWN_ISSUES.md`.
    #[serde(
        with = "humantime_serde",
        default = "IcebergConfig::default_reader_checkout_timeout"
    )]
    pub reader_checkout_timeout: Duration,
}

impl IcebergConfig {
    fn default_pool() -> usize {
        8
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
            catalog = "rest"
            warehouse = "s3://melt/iceberg/"
            rest_uri = "http://localhost:8181"
            [s3]
            region    = "us-east-1"
            endpoint  = "localhost:9000"
            url_style = "path"
            use_ssl   = false
        "#;
        let cfg: IcebergConfig = toml::from_str(raw).unwrap();
        assert_eq!(cfg.s3.region, "us-east-1");
        assert_eq!(cfg.s3.endpoint, "localhost:9000");
    }

    #[test]
    fn missing_s3_block_defaults_to_empty_s3_config() {
        let raw = r#"
            catalog   = "glue"
            warehouse = "s3://melt/iceberg/"
        "#;
        let cfg: IcebergConfig = toml::from_str(raw).unwrap();
        assert!(cfg.s3.region.is_empty());
        assert!(!cfg.s3.has_explicit_credentials());
    }
}
