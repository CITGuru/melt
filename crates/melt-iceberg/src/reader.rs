use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use melt_core::{
    BackendKind, MeltError, QueryContext, RecordBatchStream, Result, StorageBackend, TableRef,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::catalog::IcebergCatalogClient;
use crate::config::IcebergConfig;
use crate::pool::IcebergPool;

pub struct IcebergBackend {
    catalog: Arc<IcebergCatalogClient>,
    pool: Arc<IcebergPool>,
}

impl IcebergBackend {
    pub async fn new(cfg: IcebergConfig) -> Result<Self> {
        let catalog = Arc::new(IcebergCatalogClient::connect(&cfg).await?);
        catalog.assert_supported()?;
        let pool = Arc::new(IcebergPool::new(&cfg).await?);
        Ok(Self { catalog, pool })
    }

    pub fn from_parts(catalog: Arc<IcebergCatalogClient>, pool: Arc<IcebergPool>) -> Self {
        Self { catalog, pool }
    }

    pub fn catalog(&self) -> Arc<IcebergCatalogClient> {
        self.catalog.clone()
    }

    pub fn pool(&self) -> Arc<IcebergPool> {
        self.pool.clone()
    }
}

/// One batch in flight + one queued. Bounded so large results don't
/// materialize in memory before the proxy's pagination layer pulls.
/// Matches `melt-ducklake::reader::BATCH_CHANNEL_DEPTH`.
const BATCH_CHANNEL_DEPTH: usize = 2;

#[async_trait]
impl StorageBackend for IcebergBackend {
    async fn execute(&self, sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        let pool = self.pool.clone();
        let sql = sql.to_owned();

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

    async fn estimate_scan_bytes(&self, tables: &[TableRef]) -> Result<Vec<u64>> {
        self.catalog.estimate_scan_bytes(tables).await
    }

    async fn tables_exist(&self, tables: &[TableRef]) -> Result<Vec<bool>> {
        self.catalog.tables_exist(tables).await
    }

    async fn policy_markers(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        self.catalog.policy_markers(tables).await
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        self.catalog.list_tables().await
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Iceberg
    }
}
