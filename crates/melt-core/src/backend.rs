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

    /// Estimate bytes scanned for each of the listed tables, in the
    /// same order as the input. Used by the router's size threshold
    /// (legacy single-cap path) AND by the dual-execution router's
    /// per-table oversize gate and per-fragment Materialize cap. Should
    /// be fast (target <10 ms via cached catalog stats).
    ///
    /// Returns one `u64` per input table. If a backend genuinely cannot
    /// distinguish per-table bytes (some catalog implementations only
    /// expose a SUM), it MAY return a vec where the first element is
    /// the sum and the rest are zeros — but per-table fidelity is
    /// strongly preferred since the hybrid router's oversize trigger
    /// case relies on it (it promotes the over-cap table to remote
    /// while keeping the small ones local).
    async fn estimate_scan_bytes(&self, tables: &[TableRef]) -> Result<Vec<u64>>;

    /// Sum-of-per-table convenience for the existing `lake_max_scan_bytes`
    /// guardrail. Default impl sums [`Self::estimate_scan_bytes`] so
    /// backends only have to override the per-table version.
    async fn estimate_scan_bytes_total(&self, tables: &[TableRef]) -> Result<u64> {
        Ok(self.estimate_scan_bytes(tables).await?.iter().sum())
    }

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

    /// Run `EXPLAIN ANALYZE <sql>` and return the rendered plan as
    /// text. Used by the dual-execution router's
    /// `router.hybrid_profile_attach_queries` knob to surface the
    /// outgoing Snowflake query strings emitted by DuckDB's
    /// `snowflake_scan` operator — the Attach path's only window
    /// into what Snowflake actually saw at the operator level.
    ///
    /// Default impl returns `Ok(String::new())` so backends without
    /// EXPLAIN support degrade silently. Backends that DO implement
    /// it must NOT return rows for the underlying `sql`; only the
    /// rendered plan. EXPLAIN ANALYZE re-executes the query against
    /// the backend, so callers should gate on
    /// `cfg.hybrid_profile_attach_queries` to avoid the doubled cost.
    async fn analyze_query(&self, _sql: &str, _ctx: &QueryContext) -> Result<String> {
        Ok(String::new())
    }

    /// Whether the dual-execution router's Attach strategy can
    /// actually use `sf_link.<...>` references on this backend right
    /// now. False when:
    ///   - the operator opted out of hybrid Attach via
    ///     `router.hybrid_attach_enabled = false`, OR
    ///   - the pool's startup probe failed to `INSTALL snowflake FROM
    ///     community` / `LOAD snowflake` / `ATTACH ... AS sf_link`
    ///     (most commonly: the ADBC native Snowflake driver isn't on
    ///     the library path — DuckDB's community Snowflake extension
    ///     dlopens it at first ATTACH).
    ///
    /// The router consults this at decide-time. When false, hybrid
    /// plans that would have produced Attach nodes are degraded to
    /// Materialize-everywhere so they still work; the
    /// `melt_hybrid_attach_unavailable_total` counter increments to
    /// surface the degradation. Default impl returns `true` so
    /// backends without hybrid support compile cleanly.
    fn hybrid_attach_available(&self) -> bool {
        true
    }
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
