//! AWS Glue catalog adapter for Iceberg.
//!
//! Glue stores Iceberg-managed tables as standard Glue tables whose
//! `Parameters` map carries an Iceberg-specific entry pointing at
//! `metadata_location` (the canonical `metadata.json` in S3). The
//! Iceberg spec calls this the "Glue catalog convention"; the same
//! shape is used by the official `iceberg-aws` Java module.
//!
//! Melt only consumes the entries it needs:
//! - `database` ↔ Glue database
//! - `name`     ↔ Glue table
//! - `metadata_location` table parameter
//!
//! Schema discovery is out of scope here — DuckDB's `iceberg_scan()`
//! reads the metadata.json directly and infers the schema.

use aws_sdk_glue::Client as GlueSdkClient;
use melt_core::{MeltError, Result, TableRef};

#[derive(Clone)]
pub struct GlueClient {
    inner: GlueSdkClient,
    pub default_database: String,
}

#[derive(Clone, Debug)]
pub struct GlueTable {
    pub r#ref: TableRef,
    pub metadata_location: String,
    /// Best-effort `total-files-size` parameter; Iceberg writers
    /// publish it on commit when they know the answer. Falls back to
    /// 0 when absent (router treats 0 as "unknown — let it through
    /// at the configured threshold").
    pub bytes: u64,
}

impl GlueClient {
    /// Build a client using the standard AWS credential chain
    /// (env, profile, IRSA, etc.) — `MELT_AWS_REGION` overrides if
    /// set, otherwise we follow the SDK's default region resolver.
    pub async fn new(default_database: impl Into<String>) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Ok(r) = std::env::var("MELT_AWS_REGION") {
            loader = loader.region(aws_sdk_glue::config::Region::new(r));
        }
        let cfg = loader.load().await;
        let inner = GlueSdkClient::new(&cfg);
        Ok(Self {
            inner,
            default_database: default_database.into(),
        })
    }

    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let resp = self
            .inner
            .get_databases()
            .send()
            .await
            .map_err(|e| MeltError::backend(format!("glue get_databases: {e}")))?;
        Ok(resp
            .database_list()
            .iter()
            .map(|d| d.name().to_string())
            .collect())
    }

    pub async fn list_tables_in(&self, database: &str) -> Result<Vec<GlueTable>> {
        let resp = self
            .inner
            .get_tables()
            .database_name(database)
            .send()
            .await
            .map_err(|e| MeltError::backend(format!("glue get_tables({database}): {e}")))?;

        let tables = resp.table_list();
        let mut out = Vec::with_capacity(tables.len());
        for t in tables {
            // Iceberg-managed tables advertise the location via the
            // `metadata_location` parameter. Plain Glue tables (Hive
            // or otherwise) skip silently.
            let Some(params) = t.parameters() else {
                continue;
            };
            let Some(loc) = params.get("metadata_location") else {
                continue;
            };
            let bytes = params
                .get("total-files-size")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            out.push(GlueTable {
                r#ref: TableRef::new(self.default_database.clone(), database, t.name()),
                metadata_location: loc.clone(),
                bytes,
            });
        }
        Ok(out)
    }

    /// Convenience used by the catalog adapter to walk every table
    /// in every database in one call.
    pub async fn list_all(&self) -> Result<Vec<GlueTable>> {
        let mut out = Vec::new();
        for db in self.list_databases().await? {
            match self.list_tables_in(&db).await {
                Ok(mut t) => out.append(&mut t),
                Err(e) => tracing::warn!(error = %e, db = %db, "glue list_tables_in failed"),
            }
        }
        Ok(out)
    }
}
