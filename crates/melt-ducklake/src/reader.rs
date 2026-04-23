use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use melt_core::{
    BackendKind, MeltError, QueryContext, RecordBatchStream, Result, StorageBackend, TableRef,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::catalog::CatalogClient;
use crate::config::DuckLakeConfig;
use crate::pool::DuckLakePool;

/// Read-side `StorageBackend` impl for DuckLake. Holds shared `Arc`s
/// to the same catalog and pool the sync subsystem uses, so routing
/// stats and CDC writes always agree on lake state.
pub struct DuckLakeBackend {
    catalog: Arc<CatalogClient>,
    pool: Arc<DuckLakePool>,
}

impl DuckLakeBackend {
    pub async fn new(cfg: DuckLakeConfig) -> Result<Self> {
        let catalog = Arc::new(CatalogClient::connect(&cfg.catalog_url).await?);
        let pool = Arc::new(DuckLakePool::new(cfg).await?);
        Ok(Self { catalog, pool })
    }

    pub fn from_parts(catalog: Arc<CatalogClient>, pool: Arc<DuckLakePool>) -> Self {
        Self { catalog, pool }
    }

    pub fn catalog(&self) -> Arc<CatalogClient> {
        self.catalog.clone()
    }

    pub fn pool(&self) -> Arc<DuckLakePool> {
        self.pool.clone()
    }
}

/// How many record batches we buffer between the blocking DuckDB
/// reader and the async consumer. Two is enough to keep the producer
/// busy (one in flight + one queued) without unbounded memory growth.
/// Pathological single-row-batch streams cannot blow up a single
/// handle's memory because the channel back-pressures the reader.
const BATCH_CHANNEL_DEPTH: usize = 2;

#[async_trait]
impl StorageBackend for DuckLakeBackend {
    async fn execute(&self, sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        let pool = self.pool.clone();
        let sql = sql.to_owned();

        // Backpressured mpsc; `duckdb::ArrowStream` is !Send so it
        // stays in `spawn_blocking` and only `RecordBatch` crosses.
        let (tx, rx) =
            mpsc::channel::<Result<arrow::record_batch::RecordBatch>>(BATCH_CHANNEL_DEPTH);

        tokio::task::spawn_blocking(move || {
            let mutex = match futures::executor::block_on(pool.read()) {
                Ok(m) => m,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };
            let guard = mutex.lock();
            let mut stmt = match guard.prepare(&sql) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.blocking_send(Err(MeltError::backend(e.to_string())));
                    return;
                }
            };
            let arrow_iter = match stmt.query_arrow([]) {
                Ok(it) => it,
                Err(e) => {
                    let _ = tx.blocking_send(Err(MeltError::backend(e.to_string())));
                    return;
                }
            };
            for batch in arrow_iter {
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(|r| r);
        Ok(Box::pin(stream))
    }

    async fn estimate_scan_bytes(&self, tables: &[TableRef]) -> Result<u64> {
        self.catalog.estimate_scan_bytes(tables).await
    }

    async fn tables_exist(&self, tables: &[TableRef]) -> Result<Vec<bool>> {
        self.catalog.tables_exist_batch(tables).await
    }

    async fn policy_markers(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        self.catalog.policy_markers_batch(tables).await
    }

    async fn policy_views(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        self.catalog.policy_views_batch(tables).await
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        self.catalog.list_tables().await
    }

    fn kind(&self) -> BackendKind {
        BackendKind::DuckLake
    }
}
