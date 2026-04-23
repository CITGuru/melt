use std::sync::Arc;

use deadpool::managed::{self, Manager, Metrics, RecycleResult};
use duckdb::Connection;
use melt_core::{MeltError, Result};
use parking_lot::Mutex as SyncMutex;

use crate::config::{IcebergCatalogKind, IcebergConfig};

/// Builds DuckDB connections pre-configured with the `iceberg` +
/// `httpfs` extensions and the catalog `ATTACH`ed under the alias
/// `ice`. Shared by both the reader pool and the single writer so
/// every handle sees the same lake state.
pub struct IcebergDuckDBManager {
    setup_sql: String,
}

impl IcebergDuckDBManager {
    pub fn new(cfg: &IcebergConfig) -> Self {
        Self {
            setup_sql: build_setup_sql(cfg),
        }
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory().map_err(|e| MeltError::backend(e.to_string()))?;
        if let Err(e) = conn.execute_batch(&self.setup_sql) {
            tracing::warn!(error = %e, "Iceberg setup failed — backend errors until catalog is reachable");
        }
        Ok(conn)
    }
}

impl Manager for IcebergDuckDBManager {
    type Type = SyncMutex<Connection>;
    type Error = MeltError;

    fn create(
        &self,
    ) -> impl std::future::Future<Output = std::result::Result<SyncMutex<Connection>, MeltError>> + Send
    {
        let setup_sql = self.setup_sql.clone();
        async move {
            let manager = IcebergDuckDBManager { setup_sql };
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

pub type ReaderPool = managed::Pool<IcebergDuckDBManager>;

/// Many-readers-one-writer pool. Matches `melt-ducklake::pool` so both
/// backends enforce the single-writer constraint structurally: `write()`
/// returns a Tokio `MutexGuard`, so two writers cannot exist at the
/// same time — the type system says no. Reads proceed concurrently
/// via deadpool.
pub struct IcebergPool {
    readers: ReaderPool,
    writer: Arc<tokio::sync::Mutex<Connection>>,
}

impl IcebergPool {
    pub async fn new(cfg: &IcebergConfig) -> Result<Self> {
        let manager = IcebergDuckDBManager::new(cfg);
        let readers = ReaderPool::builder(manager)
            .max_size(cfg.reader_pool_size.max(1))
            .build()
            .map_err(|e| MeltError::backend(format!("reader pool: {e}")))?;

        let writer_manager = IcebergDuckDBManager::new(cfg);
        let writer = tokio::task::spawn_blocking(move || writer_manager.open())
            .await
            .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
        Ok(Self {
            readers,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
        })
    }

    pub async fn read(&self) -> Result<deadpool::managed::Object<IcebergDuckDBManager>> {
        self.readers
            .get()
            .await
            .map_err(|e| MeltError::backend(format!("reader checkout: {e}")))
    }

    pub async fn write(&self) -> tokio::sync::MutexGuard<'_, Connection> {
        self.writer.lock().await
    }
}

/// Install iceberg+httpfs, configure region, ATTACH REST catalogs as
/// `ice`. Glue is not attached here — discovery still works via
/// `IcebergCatalogClient`; sync writer errors clearly for Glue
/// because the duckdb-iceberg extension is still maturing.
fn build_setup_sql(cfg: &IcebergConfig) -> String {
    let mut s = String::from(
        "INSTALL iceberg;\n\
         LOAD iceberg;\n\
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
                    "Iceberg S3 secret setup failed — s3:// paths will error \
                     until config is fixed"
                );
            }
        }
    }

    if matches!(
        cfg.catalog,
        IcebergCatalogKind::Rest | IcebergCatalogKind::Polaris
    ) && !cfg.rest_uri.is_empty()
    {
        if let Ok(token) =
            std::env::var("MELT_POLARIS_TOKEN").or_else(|_| std::env::var("MELT_ICEBERG_TOKEN"))
        {
            s.push_str(&format!(
                "CREATE OR REPLACE SECRET melt_iceberg (\
                    TYPE ICEBERG, TOKEN '{}');\n",
                token.replace('\'', "''")
            ));
        }
        s.push_str(&format!(
            "ATTACH '{warehouse}' AS ice (\
                TYPE ICEBERG, ENDPOINT '{endpoint}');\n",
            warehouse = cfg.warehouse.replace('\'', "''"),
            endpoint = cfg.rest_uri.replace('\'', "''"),
        ));
    }

    s
}
