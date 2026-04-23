use std::sync::Arc;

use deadpool::managed::{self, Manager, Metrics, RecycleResult};
use duckdb::Connection;
use melt_core::{MeltError, Result};
use parking_lot::Mutex as SyncMutex;

use crate::config::DuckLakeConfig;

/// `DuckDBManager` opens an in-memory DuckDB connection and runs the
/// `INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:postgres:...'; USE lake;`
/// boilerplate so any handle the pool hands out — read or write —
/// already sees the lake.
pub struct DuckDBManager {
    setup_sql: String,
}

impl DuckDBManager {
    pub fn new(cfg: &DuckLakeConfig) -> Self {
        let setup_sql = build_setup_sql(cfg);
        Self { setup_sql }
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory().map_err(|e| MeltError::backend(e.to_string()))?;
        // Setup may fail in environments without S3/Postgres reachability;
        // we tolerate it on dev/test boxes by logging and proceeding so
        // `cargo check` and offline tests don't require infrastructure.
        if let Err(e) = conn.execute_batch(&self.setup_sql) {
            tracing::warn!(error = %e, "DuckLake ATTACH failed — backend will return errors until the lake is reachable");
        }
        Ok(conn)
    }
}

impl Manager for DuckDBManager {
    type Type = SyncMutex<Connection>;
    type Error = MeltError;

    fn create(
        &self,
    ) -> impl std::future::Future<Output = std::result::Result<SyncMutex<Connection>, MeltError>> + Send
    {
        let setup_sql = self.setup_sql.clone();
        async move {
            let manager = DuckDBManager { setup_sql };
            let conn = tokio::task::spawn_blocking(move || manager.open())
                .await
                .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
            Ok(SyncMutex::new(conn))
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn recycle(
        &self,
        _conn: &mut SyncMutex<Connection>,
        _: &Metrics,
    ) -> impl std::future::Future<Output = RecycleResult<MeltError>> + Send {
        async { Ok(()) }
    }
}

pub type ReaderPool = managed::Pool<DuckDBManager>;

/// Many-readers-one-writer pool. The single-writer constraint is
/// enforced structurally: `write()` returns a `MutexGuard` so two
/// writers cannot exist concurrently — the type system says no.
pub struct DuckLakePool {
    readers: ReaderPool,
    writer: Arc<tokio::sync::Mutex<Connection>>,
}

impl DuckLakePool {
    pub async fn new(cfg: DuckLakeConfig) -> Result<Self> {
        let manager = DuckDBManager::new(&cfg);
        let readers = ReaderPool::builder(manager)
            .max_size(cfg.reader_pool_size.max(1))
            .build()
            .map_err(|e| MeltError::backend(format!("reader pool: {e}")))?;

        let writer_manager = DuckDBManager::new(&cfg);
        let writer = tokio::task::block_in_place(|| writer_manager.open())?;
        Ok(Self {
            readers,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
        })
    }

    pub async fn read(&self) -> Result<deadpool::managed::Object<DuckDBManager>> {
        self.readers
            .get()
            .await
            .map_err(|e| MeltError::backend(format!("reader checkout: {e}")))
    }

    pub async fn write(&self) -> tokio::sync::MutexGuard<'_, Connection> {
        self.writer.lock().await
    }
}

/// Build the setup SQL DuckDB runs on every pool connection.
///
/// The S3 secret comes first so the subsequent `ATTACH` can resolve
/// `s3://` paths. The secret is named `melt_s3`; DuckDB matches it
/// by longest-prefix `SCOPE` when multiple secrets exist.
///
/// Rendering failures (bad S3 config, missing env vars) log and
/// emit a fallback that omits the secret — `ATTACH` will then fail
/// against real S3 endpoints but DuckDB boots cleanly, which is
/// what we want for `cargo check` and the `melt route` offline path.
fn build_setup_sql(cfg: &DuckLakeConfig) -> String {
    let mut s = String::from(
        "INSTALL ducklake;\n\
         LOAD ducklake;\n\
         INSTALL httpfs;\n\
         LOAD httpfs;\n",
    );

    if !cfg.s3.region.is_empty() {
        match cfg.s3.to_duckdb_secret_sql("melt_s3") {
            Ok(secret_sql) => {
                s.push_str(&secret_sql);
                s.push('\n');
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "DuckLake S3 secret setup failed — s3:// paths will error \
                     until config is fixed"
                );
            }
        }
    }

    s.push_str(&format!(
        "ATTACH 'ducklake:postgres:{catalog}' AS lake (DATA_PATH '{data}');\n\
         USE lake;",
        catalog = cfg.catalog_url.replace('\'', "''"),
        data = cfg.data_path.replace('\'', "''"),
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_sql_emits_secret_and_attach() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "s3://melt/ducklake/".into(),
            s3: melt_core::S3Config {
                region: "us-east-1".into(),
                endpoint: "localhost:9000".into(),
                url_style: "path".into(),
                use_ssl: false,
                access_key_id: "minioadmin".into(),
                secret_access_key: "minioadmin".into(),
                ..Default::default()
            },
            reader_pool_size: 4,
            writer_pool_size: 1,
        };
        let sql = build_setup_sql(&cfg);
        assert!(sql.contains("INSTALL ducklake"));
        assert!(sql.contains("INSTALL httpfs"));
        assert!(sql.contains("CREATE OR REPLACE SECRET melt_s3"));
        assert!(sql.contains("ENDPOINT  'localhost:9000'"));
        assert!(sql.contains("URL_STYLE 'path'"));
        assert!(sql.contains("KEY_ID    'minioadmin'"));
        assert!(sql.contains("ATTACH 'ducklake:postgres:postgres://melt@db/c'"));
        assert!(sql.contains("DATA_PATH 's3://melt/ducklake/'"));
    }

    #[test]
    fn region_only_config_uses_credential_chain() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "s3://melt/ducklake/".into(),
            s3: melt_core::S3Config {
                region: "eu-west-1".into(),
                ..Default::default()
            },
            reader_pool_size: 4,
            writer_pool_size: 1,
        };
        let sql = build_setup_sql(&cfg);
        assert!(sql.contains("REGION    'eu-west-1'"));
        assert!(sql.contains("PROVIDER credential_chain"));
    }

    #[test]
    fn empty_region_skips_secret_but_still_attaches() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "/tmp/ducklake/".into(),
            s3: melt_core::S3Config::default(),
            reader_pool_size: 4,
            writer_pool_size: 1,
        };
        let sql = build_setup_sql(&cfg);
        assert!(!sql.contains("CREATE OR REPLACE SECRET"));
        assert!(sql.contains("ATTACH"));
    }
}
