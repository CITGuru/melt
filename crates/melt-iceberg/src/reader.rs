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

    fn hybrid_attach_available(&self) -> bool {
        self.pool.sf_link_available()
    }

    /// Runs `EXPLAIN ANALYZE <sql>` on a checked-out reader. Pulls
    /// every row of the rendered plan into a single string. Used by
    /// the dual-execution router's profiler tap; doubles execution
    /// cost so the proxy gates this on `hybrid_profile_attach_queries`.
    async fn analyze_query(&self, sql: &str, _ctx: &QueryContext) -> Result<String> {
        let pool = self.pool.clone();
        // DuckDB's EXPLAIN ANALYZE returns a 2-column result
        // (`explain_key`, `explain_value`). The rendered plan lives in
        // `explain_value`; we concatenate per-row to a single text
        // blob the proxy can grep for `snowflake_scan` operators.
        let analyze = format!("EXPLAIN ANALYZE {sql}");
        tokio::task::spawn_blocking(move || -> Result<String> {
            let mutex = futures::executor::block_on(pool.read())?;
            let guard = mutex.lock();
            let mut stmt = guard
                .prepare(&analyze)
                .map_err(|e| MeltError::backend(e.to_string()))?;
            let rows = stmt
                .query_map([], |row| {
                    let key: String = row.get(0).unwrap_or_default();
                    let value: String = row.get(1).unwrap_or_default();
                    Ok(format!("{key}\n{value}"))
                })
                .map_err(|e| MeltError::backend(e.to_string()))?;
            let mut out = String::new();
            for r in rows {
                let line = r.map_err(|e| MeltError::backend(e.to_string()))?;
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&line);
            }
            Ok(out)
        })
        .await
        .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))?
    }
}
