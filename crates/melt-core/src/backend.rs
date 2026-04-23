use async_trait::async_trait;

use crate::error::Result;
use crate::stream::RecordBatchStream;
use crate::table::{QueryContext, TableRef};

/// The pluggable lakehouse seam. All current implementations are
/// DuckDB-powered, so `execute` takes SQL **already in DuckDB
/// dialect** — translation happens upstream in `melt-router` before
/// the backend is called.
///
/// If a future backend speaks a different dialect, it must perform
/// any required rewriting internally; the trait contract is
/// "DuckDB-dialect SQL in, Arrow record batches out."
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Execute a query and return a streaming result. The stream is
    /// consumed lazily so large results don't materialize in memory
    /// before pagination.
    async fn execute(&self, sql: &str, ctx: &QueryContext) -> Result<RecordBatchStream>;

    /// Estimate total bytes scanned for the listed tables. Used by
    /// the router's size threshold; should be fast (target <10ms via
    /// cached catalog stats).
    async fn estimate_scan_bytes(&self, tables: &[TableRef]) -> Result<u64>;

    /// Batch existence check — avoids N serial round trips when a
    /// query references many tables. Returns one bool per input in
    /// the same order.
    async fn tables_exist(&self, tables: &[TableRef]) -> Result<Vec<bool>>;

    /// Batch policy-marker check. For each input returns
    /// `Some(policy_name)` if sync has marked it as policy-protected,
    /// or `None` if unmarked.
    ///
    /// Always returns `vec![None; tables.len()]` in `PolicyMode::Enforce`
    /// (filtered views are exposed instead) and is not consulted in
    /// `PolicyMode::AllowList`.
    async fn policy_markers(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>>;

    /// Batch lookup of enforce-mode filtered view names. Returns
    /// `Some(view_name)` per input that has a translated policy view,
    /// or `None` if no view is registered. Default impl returns all
    /// `None` so backends without enforce support compile cleanly.
    async fn policy_views(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        Ok(vec![None; tables.len()])
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>>;

    fn kind(&self) -> BackendKind;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    DuckLake,
    Iceberg,
}

impl BackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendKind::DuckLake => "ducklake",
            BackendKind::Iceberg => "iceberg",
        }
    }
}

/// Trait the router exposes for cache invalidation. Sync subsystems
/// hold an `Arc<dyn RouterCache>` and call it after writes — without
/// depending on the `melt-router` crate directly. The CLI provides
/// the concrete impl.
#[async_trait]
pub trait RouterCache: Send + Sync {
    async fn invalidate_table(&self, table: &TableRef);
    async fn invalidate_all(&self);
}

/// No-op router cache, useful for tests and standalone tools.
pub struct NoopRouterCache;

#[async_trait]
impl RouterCache for NoopRouterCache {
    async fn invalidate_table(&self, _table: &TableRef) {}
    async fn invalidate_all(&self) {}
}
