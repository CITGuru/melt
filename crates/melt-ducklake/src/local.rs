//! Single-file DuckDB backend for `melt sessions seed`.
//!
//! Production Melt always pairs DuckDB with a Postgres-backed DuckLake
//! catalog (see [`crate::reader::DuckLakeBackend`]). The credential-free
//! demo path can't assume Postgres is on the host — the whole point of
//! seed mode is "git clone → working query in five minutes" with zero
//! infrastructure. So we open a single read-only DuckDB file generated
//! by `melt sessions seed --init` and attach it as the configured demo
//! database (default `TPCH`), exposing the canned TPC-H sf=0.01
//! fixture under `TPCH.SF01.<table>`.
//!
//! The backend is intentionally minimal:
//! * Estimates always return zero, so the router stays under
//!   `lake_max_scan_bytes` and never tries to fall back to upstream.
//! * No policy markers, no enforce-mode views — the demo data is
//!   public.
//! * A single shared connection guarded by a `Mutex` is enough for
//!   demo workloads; the global concurrency cap on `SessionStore`
//!   bounds the worst case.
//!
//! Real-mode workloads keep using `DuckLakeBackend`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use duckdb::Connection;
use futures::StreamExt;
use melt_core::{
    BackendKind, MeltError, QueryContext, RecordBatchStream, Result, StorageBackend, TableRef,
};
use parking_lot::Mutex as SyncMutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Two batches of buffered headroom is enough to keep the blocking
/// reader busy while back-pressuring on the async consumer. Mirror
/// of the constant in [`DuckLakeBackend`].
const BATCH_CHANNEL_DEPTH: usize = 2;

/// Single-file DuckDB backend used by seed mode. Holds one
/// `duckdb::Connection` opened in-memory with the seed fixture
/// attached read-only as `TPCH`.
pub struct LocalDuckDbBackend {
    conn: Arc<SyncMutex<Connection>>,
    fixture_path: PathBuf,
    database: String,
    schema: String,
}

impl LocalDuckDbBackend {
    /// Open `fixture_path` and attach it as `database` (read-only).
    /// Tables in the fixture are exposed as `<database>.<schema>.<t>`
    /// — the seed fixture writes its tables into `<schema>` directly,
    /// so no view-rewriting is needed at query time.
    pub fn open(fixture_path: PathBuf, database: &str, schema: &str) -> Result<Self> {
        if !fixture_path.is_file() {
            return Err(MeltError::config(format!(
                "seed mode fixture not found at {} — run `melt sessions seed --init` first",
                fixture_path.display()
            )));
        }
        let conn = Connection::open_in_memory().map_err(|e| MeltError::backend(e.to_string()))?;
        // ATTACH the fixture and switch the connection's current
        // catalog to it. The current catalog matters because the
        // router strips the database prefix from references that match
        // `session.database` (so `TPCH.SF01.lineitem` becomes
        // `SF01.lineitem` in the translated SQL). Without `USE`, the
        // connection would still resolve those against the in-memory
        // catalog and 404. See `melt-router::translate::strip_database`.
        let setup_sql = format!(
            "ATTACH '{path}' AS {db} (READ_ONLY);
             USE {db};",
            path = sql_escape(&fixture_path.to_string_lossy()),
            db = quote_ident(database),
        );
        conn.execute_batch(&setup_sql)
            .map_err(|e| MeltError::backend(format!("seed: setup failed: {e}")))?;
        Ok(Self {
            conn: Arc::new(SyncMutex::new(conn)),
            fixture_path,
            database: database.to_string(),
            schema: schema.to_string(),
        })
    }

    /// Path to the on-disk fixture. Surface for `melt status` and the
    /// integration test's "fixture exists" assertion.
    pub fn fixture_path(&self) -> &PathBuf {
        &self.fixture_path
    }

    fn matches_demo_namespace(&self, t: &TableRef) -> bool {
        t.database.eq_ignore_ascii_case(&self.database)
            && t.schema.eq_ignore_ascii_case(&self.schema)
    }
}

#[async_trait]
impl StorageBackend for LocalDuckDbBackend {
    async fn execute(&self, sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        let conn = self.conn.clone();
        let sql = sql.to_owned();

        let (tx, rx) =
            mpsc::channel::<Result<arrow::record_batch::RecordBatch>>(BATCH_CHANNEL_DEPTH);

        tokio::task::spawn_blocking(move || {
            let guard = conn.lock();
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
        // Demo fixture stays under any sane `lake_max_scan_bytes`. The
        // router uses these estimates to decide Lake vs upstream; since
        // seed mode has no upstream, returning zero keeps every demo
        // table on the Lake path.
        Ok(vec![0; tables.len()])
    }

    async fn estimate_table_rows(&self, tables: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![0; tables.len()])
    }

    async fn tables_exist(&self, tables: &[TableRef]) -> Result<Vec<bool>> {
        let conn = self.conn.clone();
        let database = self.database.clone();
        let schema = self.schema.clone();
        let queries: Vec<TableRef> = tables.to_vec();
        let demo_match: Vec<bool> = queries
            .iter()
            .map(|t| self.matches_demo_namespace(t))
            .collect();
        let table_names: Vec<String> = queries.iter().map(|t| t.name.to_lowercase()).collect();
        tokio::task::spawn_blocking(move || -> Result<Vec<bool>> {
            let guard = conn.lock();
            // Pull every table name in the demo schema once, then
            // check membership locally. Cheaper than N round-trips
            // and avoids parameter-binding edge cases. `duckdb_tables`
            // exposes every attached database; `information_schema`
            // would only see the current catalog.
            let mut stmt = guard
                .prepare(
                    "SELECT lower(table_name) FROM duckdb_tables() \
                     WHERE lower(database_name) = lower(?) \
                       AND lower(schema_name)   = lower(?)",
                )
                .map_err(|e| MeltError::backend(e.to_string()))?;
            let names: std::collections::HashSet<String> = stmt
                .query_map([database.as_str(), schema.as_str()], |row| {
                    let name: String = row.get(0)?;
                    Ok(name)
                })
                .map_err(|e| MeltError::backend(e.to_string()))?
                .filter_map(std::result::Result::ok)
                .collect();
            Ok(table_names
                .iter()
                .zip(demo_match.iter())
                .map(|(name, in_demo)| *in_demo && names.contains(name))
                .collect())
        })
        .await
        .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))?
    }

    async fn policy_markers(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        Ok(vec![None; tables.len()])
    }

    async fn policy_views(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        Ok(vec![None; tables.len()])
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let conn = self.conn.clone();
        let database = self.database.clone();
        let schema = self.schema.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<TableRef>> {
            let guard = conn.lock();
            let mut stmt = guard
                .prepare(
                    "SELECT table_name FROM duckdb_tables() \
                     WHERE lower(database_name) = lower(?) \
                       AND lower(schema_name)   = lower(?)",
                )
                .map_err(|e| MeltError::backend(e.to_string()))?;
            let rows = stmt
                .query_map([database.as_str(), schema.as_str()], |row| {
                    let name: String = row.get(0)?;
                    Ok(name)
                })
                .map_err(|e| MeltError::backend(e.to_string()))?;
            let mut out = Vec::new();
            for row in rows {
                let name = row.map_err(|e| MeltError::backend(e.to_string()))?;
                out.push(TableRef::new(database.clone(), schema.clone(), name));
            }
            Ok(out)
        })
        .await
        .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))?
    }

    fn kind(&self) -> BackendKind {
        // Reuse the DuckLake label so dashboards keyed on
        // `BackendKind` don't need a third variant just for the demo
        // path. We're still routing through DuckDB; the only thing
        // missing is the Postgres catalog.
        BackendKind::DuckLake
    }

    async fn analyze_query(&self, _sql: &str, _ctx: &QueryContext) -> Result<String> {
        // Demo path doesn't surface EXPLAIN ANALYZE — the
        // dual-execution Attach diagnostic that needs it isn't
        // wired here. Falls back to the trait default.
        Ok(String::new())
    }

    fn hybrid_attach_available(&self) -> bool {
        // Hybrid Attach requires the community Snowflake DuckDB
        // extension and a real upstream — neither is available in
        // seed mode. Returning false keeps the router from emitting
        // Attach plans that would fail at execute time.
        false
    }
}

/// Quote a DuckDB identifier (database / schema / table). Backslash
/// quote-doubling is the only required escape; we keep it minimal so
/// the fixture's known-safe names render cleanly.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a single SQL string literal value. Same rule as the helper
/// in `melt_core::config` but kept private here to avoid pulling
/// `escape_sql` (which is module-private) across the crate boundary.
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}
