use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::{
    Config as PgConfig, ManagerConfig, Pool as PgPool, RecyclingMethod, Runtime,
};
use melt_core::{
    CatalogError, DiscoveryCatalog, MeltError, ObjectKind, ProtectedTable, Result, SyncSource,
    SyncState, TableRef, ViewStrategy,
};
use melt_snowflake::SnapshotId;
use tokio_postgres::NoTls;

use crate::schema::CATALOG_DDL;
use crate::state::{DepKind, SyncStateRow, ViewBodyRow};

/// Postgres-backed control-plane client. Owns the `melt_*` tables
/// (sync state, sync progress, policy markers, policy views). Shared
/// by both backend crates through the `Arc<ControlCatalog>` handle —
/// all sync-state reads / writes go through this one type regardless
/// of whether the data-plane is DuckLake or Iceberg.
#[derive(Clone)]
pub struct ControlCatalog {
    pool: PgPool,
}

impl ControlCatalog {
    /// Connect to the control-plane Postgres and run the idempotent
    /// DDL. `url` is any `postgres://…` connection string.
    pub async fn connect(url: &str) -> Result<Self> {
        let mut pg = PgConfig::new();
        pg.url = Some(url.to_string());
        pg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = pg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| MeltError::config(format!("invalid control_catalog_url: {e}")))?;
        let this = Self { pool };
        this.ensure_schema().await?;
        Ok(this)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let client = self.client().await?;
        client.batch_execute(CATALOG_DDL).await.map_err(|e| {
            // tokio_postgres' Display only emits "db error";
            // unpack the DbError for a useful migration trace.
            let detail = if let Some(db_err) = e.as_db_error() {
                format!(
                    "{} (sqlstate {}): {}",
                    db_err.severity(),
                    db_err.code().code(),
                    db_err.message()
                )
            } else {
                e.to_string()
            };
            MeltError::Catalog(CatalogError::Other(format!("ensure_schema: {detail}")))
        })?;
        Ok(())
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool
            .get()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Unavailable(format!("pg pool: {e}"))))
    }

    pub async fn ping(&self) -> Result<()> {
        let client = self.client().await?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Unavailable(format!("ping: {e}"))))?;
        Ok(())
    }

    /// Per-table scan-byte estimate, in input order. Each element
    /// covers direct bytes (for base tables) plus dependency bytes
    /// (for decomposed views, summed across `melt_view_dependencies`).
    /// Tables not tracked at all return `0`.
    ///
    /// The dual-execution router relies on per-table fidelity here for
    /// the oversize trigger case (§10.3 in the design doc) and the
    /// per-fragment Materialize cap.
    pub async fn estimate_scan_bytes(&self, tables: &[TableRef]) -> Result<Vec<u64>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        // Inputs carry a 1-based `ord` so we can map the aggregated
        // result back into the caller's order even when some tables
        // are unknown (no rows from either CTE).
        let inputs_clause = tables
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "({}, {}, {}, {})",
                    i + 1,
                    pg_lit(&t.database),
                    pg_lit(&t.schema),
                    pg_lit(&t.name)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        // Decomposed views: 0 bytes locally; SUM dependency bytes via
        // melt_view_dependencies. Each row tagged with `ord` so the
        // outer query can GROUP BY input position.
        let sql = format!(
            "WITH inputs(ord, database, schema, name) AS (VALUES {inputs_clause}), \
                  direct AS ( \
                      SELECT i.ord, s.bytes \
                      FROM inputs i \
                      JOIN melt_table_stats s ON s.database=i.database \
                                              AND s.schema=i.schema \
                                              AND s.name=i.name \
                      WHERE NOT ( \
                          s.object_kind = 'view' AND s.view_strategy = 'decomposed' \
                      ) \
                  ), \
                  deps AS ( \
                      SELECT i.ord, ds.bytes \
                      FROM inputs i \
                      JOIN melt_table_stats s ON s.database=i.database \
                                              AND s.schema=i.schema \
                                              AND s.name=i.name \
                      JOIN melt_view_dependencies vd \
                        ON vd.parent_db     = s.database \
                       AND vd.parent_schema = s.schema \
                       AND vd.parent_name   = s.name \
                      JOIN melt_table_stats ds \
                        ON ds.database = vd.dep_db \
                       AND ds.schema   = vd.dep_schema \
                       AND ds.name     = vd.dep_name \
                      WHERE s.object_kind = 'view' AND s.view_strategy = 'decomposed' \
                  ), \
                  combined AS ( \
                      SELECT ord, bytes FROM direct \
                      UNION ALL \
                      SELECT ord, bytes FROM deps \
                  ) \
             SELECT i.ord, COALESCE(SUM(c.bytes), 0)::BIGINT AS bytes \
             FROM inputs i \
             LEFT JOIN combined c ON c.ord = i.ord \
             GROUP BY i.ord \
             ORDER BY i.ord"
        );
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("estimate: {e}"))))?;
        // Rows come back in `ord` order; sanity-check the count matches
        // the input slice so a mismatched row set surfaces as an error
        // rather than silently producing wrong per-table estimates.
        if rows.len() != tables.len() {
            return Err(MeltError::Catalog(CatalogError::Other(format!(
                "estimate_scan_bytes: expected {} rows, got {}",
                tables.len(),
                rows.len()
            ))));
        }
        Ok(rows
            .iter()
            .map(|r| {
                let b: i64 = r.get("bytes");
                b.max(0) as u64
            })
            .collect())
    }

    /// Per-table row-count estimates. Mirrors [`Self::estimate_scan_bytes`]
    /// — same per-input-position output ordering, same view-decomposition
    /// rules (a decomposed view's row count is the SUM of its dependencies).
    /// Used by the cost strategy's Attach-vs-Materialize crossover decision.
    pub async fn estimate_table_rows(&self, tables: &[TableRef]) -> Result<Vec<u64>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        let inputs_clause = tables
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "({}, {}, {}, {})",
                    i + 1,
                    pg_lit(&t.database),
                    pg_lit(&t.schema),
                    pg_lit(&t.name)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH inputs(ord, database, schema, name) AS (VALUES {inputs_clause}), \
                  direct AS ( \
                      SELECT i.ord, s.rows_count \
                      FROM inputs i \
                      JOIN melt_table_stats s ON s.database=i.database \
                                              AND s.schema=i.schema \
                                              AND s.name=i.name \
                      WHERE NOT ( \
                          s.object_kind = 'view' AND s.view_strategy = 'decomposed' \
                      ) \
                  ), \
                  deps AS ( \
                      SELECT i.ord, ds.rows_count \
                      FROM inputs i \
                      JOIN melt_table_stats s ON s.database=i.database \
                                              AND s.schema=i.schema \
                                              AND s.name=i.name \
                      JOIN melt_view_dependencies vd \
                        ON vd.parent_db     = s.database \
                       AND vd.parent_schema = s.schema \
                       AND vd.parent_name   = s.name \
                      JOIN melt_table_stats ds \
                        ON ds.database = vd.dep_db \
                       AND ds.schema   = vd.dep_schema \
                       AND ds.name     = vd.dep_name \
                      WHERE s.object_kind = 'view' AND s.view_strategy = 'decomposed' \
                  ), \
                  combined AS ( \
                      SELECT ord, rows_count FROM direct \
                      UNION ALL \
                      SELECT ord, rows_count FROM deps \
                  ) \
             SELECT i.ord, COALESCE(SUM(c.rows_count), 0)::BIGINT AS rows_count \
             FROM inputs i \
             LEFT JOIN combined c ON c.ord = i.ord \
             GROUP BY i.ord \
             ORDER BY i.ord"
        );
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("estimate_rows: {e}"))))?;
        if rows.len() != tables.len() {
            return Err(MeltError::Catalog(CatalogError::Other(format!(
                "estimate_table_rows: expected {} rows, got {}",
                tables.len(),
                rows.len()
            ))));
        }
        Ok(rows
            .iter()
            .map(|r| {
                let n: i64 = r.get("rows_count");
                n.max(0) as u64
            })
            .collect())
    }

    /// Return whether each input table exists AND is in `active` state.
    /// Tables in `pending`/`bootstrapping`/`quarantined` come back as
    /// `false` so the router forces Snowflake passthrough. Use
    /// [`ControlCatalog::state_batch`] when you need the actual states.
    pub async fn tables_exist_batch(&self, tables: &[TableRef]) -> Result<Vec<bool>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let states = self.state_batch(tables).await?;
        Ok(states
            .into_iter()
            .map(|s| matches!(s, Some(SyncState::Active)))
            .collect())
    }

    /// Fetch the `sync_state` of each input table in one round-trip.
    /// `None` means the table isn't tracked at all.
    pub async fn state_batch(&self, tables: &[TableRef]) -> Result<Vec<Option<SyncState>>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        let in_clause = build_in_clause(tables);
        let sql = format!(
            "SELECT database, schema, name, sync_state \
             FROM melt_table_stats \
             WHERE (database, schema, name) IN ({in_clause})"
        );
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("state_batch: {e}"))))?;
        let mut by_table: std::collections::HashMap<TableRef, SyncState> =
            std::collections::HashMap::with_capacity(rows.len());
        for r in rows {
            let t = TableRef::new(
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
            );
            by_table.insert(t, SyncState::from_db(r.get::<_, &str>(3)));
        }
        Ok(tables.iter().map(|t| by_table.get(t).copied()).collect())
    }

    pub async fn list_tables(&self) -> Result<Vec<TableRef>> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT database, schema, name FROM melt_table_stats \
                 WHERE sync_state = 'active'",
                &[],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("list: {e}"))))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                TableRef::new(
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect())
    }

    /// All tables regardless of state (used by CLI `melt sync list`).
    pub async fn list_all_rows(&self) -> Result<Vec<SyncStateRow>> {
        let client = self.client().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT {STATE_ROW_COLUMNS} \
                     FROM melt_table_stats s \
                     LEFT JOIN melt_sync_progress p \
                       ON (s.database = p.database AND s.schema = p.schema AND s.name = p.name) \
                     ORDER BY s.database, s.schema, s.name"
                ),
                &[],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("list_all: {e}"))))?;
        Ok(rows.into_iter().map(row_to_state_row).collect())
    }

    /// Rows in a specific state (optionally limited). Sync's bootstrap
    /// picker uses `(Pending, Some(N))` to pull up to N bootstrap
    /// candidates per tick.
    pub async fn list_by_state(
        &self,
        state: SyncState,
        limit: Option<i64>,
    ) -> Result<Vec<SyncStateRow>> {
        let client = self.client().await?;
        let sql = match limit {
            Some(n) => format!(
                "SELECT {STATE_ROW_COLUMNS} \
                 FROM melt_table_stats s \
                 LEFT JOIN melt_sync_progress p USING (database, schema, name) \
                 WHERE sync_state = $1 \
                 ORDER BY discovered_at ASC \
                 LIMIT {n}"
            ),
            None => format!(
                "SELECT {STATE_ROW_COLUMNS} \
                 FROM melt_table_stats s \
                 LEFT JOIN melt_sync_progress p USING (database, schema, name) \
                 WHERE sync_state = $1 \
                 ORDER BY discovered_at ASC"
            ),
        };
        let rows = client
            .query(&sql, &[&state.as_str()])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("list_by_state: {e}"))))?;
        Ok(rows.into_iter().map(row_to_state_row).collect())
    }

    /// Fetch a single row by FQN. `None` if untracked.
    pub async fn get_row(&self, t: &TableRef) -> Result<Option<SyncStateRow>> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {STATE_ROW_COLUMNS} \
                     FROM melt_table_stats s \
                     LEFT JOIN melt_sync_progress p USING (database, schema, name) \
                     WHERE s.database=$1 AND s.schema=$2 AND s.name=$3"
                ),
                &[&t.database, &t.schema, &t.name],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("get_row: {e}"))))?;
        Ok(row.map(row_to_state_row))
    }

    /// Router-facing: upsert rows for each input table with
    /// `last_queried_at = now()`. New rows land as `pending` with the
    /// given source. Existing rows have their source bumped to
    /// `include` when we get the stronger signal (include > discovered
    /// for demotion-immunity), and their `last_queried_at` always
    /// bumped. Returns the resulting state of each row so the router
    /// can decide routing in one round-trip.
    pub async fn ensure_discovered(
        &self,
        tables: &[TableRef],
        source: SyncSource,
    ) -> Result<Vec<SyncState>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        for t in tables {
            // Upsert: bump last_queried_at; promote source→include on
            // signal; sync_state untouched. `remote` is operator-
            // declared (`[sync].remote`) and must never be overwritten
            // by automatic discovery — same intent as `include`.
            tx.execute(
                "INSERT INTO melt_table_stats \
                   (database, schema, name, sync_state, source, \
                    discovered_at, last_queried_at) \
                 VALUES ($1, $2, $3, 'pending', $4, now(), now()) \
                 ON CONFLICT (database, schema, name) DO UPDATE \
                   SET last_queried_at = now(), \
                       source = CASE \
                           WHEN melt_table_stats.source = 'remote' THEN 'remote' \
                           WHEN EXCLUDED.source = 'include' THEN 'include' \
                           ELSE melt_table_stats.source \
                       END",
                &[&t.database, &t.schema, &t.name, &source.as_str()],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("ensure_discovered: {e}")))
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;

        let states = self.state_batch(tables).await?;
        Ok(states
            .into_iter()
            .map(|o| o.unwrap_or(SyncState::Pending))
            .collect())
    }

    pub async fn mark_bootstrapping(&self, t: &TableRef) -> Result<()> {
        self.set_state(t, SyncState::Bootstrapping, None).await
    }

    pub async fn mark_active(&self, t: &TableRef) -> Result<()> {
        self.set_state(t, SyncState::Active, None).await
    }

    pub async fn mark_quarantined(&self, t: &TableRef, reason: &str) -> Result<()> {
        self.set_state(t, SyncState::Quarantined, Some(reason))
            .await
    }

    /// Flip a row back to `pending` without clearing sync_progress
    /// (unlike [`Self::refresh_table`]). Used by the view-bootstrap
    /// path when a view is waiting on its base-table dependencies —
    /// the caller stashes a hint in `bootstrap_error` so operators
    /// can tell "waiting for deps" apart from a true failure.
    pub async fn mark_pending(&self, t: &TableRef, hint: Option<&str>) -> Result<()> {
        self.set_state(t, SyncState::Pending, hint).await
    }

    /// Stamp `last_queried_at` on a batch of tables. Called by the
    /// router in non-discovery mode (e.g. `auto_discover = false` with
    /// an include hit) so the CLI can still show query recency.
    pub async fn mark_queried(&self, tables: &[TableRef]) -> Result<()> {
        if tables.is_empty() {
            return Ok(());
        }
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        for t in tables {
            tx.execute(
                "UPDATE melt_table_stats \
                 SET last_queried_at = now() \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("mark_queried: {e}"))))?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;
        Ok(())
    }

    /// CLI: force a table back to `pending` so sync re-bootstraps it.
    /// Clears any prior bootstrap error and drops the sync-progress
    /// row so the fresh snapshot starts at 0.
    pub async fn refresh_table(&self, t: &TableRef) -> Result<()> {
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        tx.execute(
            "UPDATE melt_table_stats \
             SET sync_state = 'pending', bootstrap_error = NULL \
             WHERE database=$1 AND schema=$2 AND name=$3",
            &[&t.database, &t.schema, &t.name],
        )
        .await
        .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("refresh_table: {e}"))))?;
        tx.execute(
            "DELETE FROM melt_sync_progress \
             WHERE database=$1 AND schema=$2 AND name=$3",
            &[&t.database, &t.schema, &t.name],
        )
        .await
        .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("refresh_progress: {e}"))))?;
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;
        Ok(())
    }

    /// Demotion: remove an auto-discovered table that hasn't been
    /// queried in a while. Cleans up sync progress and policy bindings
    /// too. Also deletes any view-body / view-dependency records
    /// keyed on this table (both as parent and as dep) so we don't
    /// leave dangling rows when a view or base table is dropped.
    pub async fn drop_table(&self, t: &TableRef) -> Result<()> {
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        for stmt in [
            "DELETE FROM melt_sync_progress     WHERE database=$1 AND schema=$2 AND name=$3",
            "DELETE FROM melt_policy_markers    WHERE database=$1 AND schema=$2 AND name=$3",
            "DELETE FROM melt_policy_views      WHERE database=$1 AND schema=$2 AND name=$3",
            "DELETE FROM melt_view_bodies       WHERE database=$1 AND schema=$2 AND name=$3",
            "DELETE FROM melt_view_dependencies \
             WHERE parent_db=$1 AND parent_schema=$2 AND parent_name=$3",
            "DELETE FROM melt_view_dependencies \
             WHERE dep_db=$1 AND dep_schema=$2 AND dep_name=$3",
            "DELETE FROM melt_table_stats       WHERE database=$1 AND schema=$2 AND name=$3",
        ] {
            tx.execute(stmt, &[&t.database, &t.schema, &t.name])
                .await
                .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("drop_table: {e}"))))?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;
        Ok(())
    }

    /// Auto-discovered tables not queried within `idle_days` days.
    /// Returns rows with `source = 'discovered'` unconditionally; rows
    /// with `source = 'view_dependency'` are only returned when no
    /// surviving `active` parent view still references them (ref-count
    /// via `melt_view_dependencies`). `include` tables are immortal.
    pub async fn idle_discovered(&self, idle_days: u32) -> Result<Vec<TableRef>> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT database, schema, name FROM melt_table_stats s \
                 WHERE (last_queried_at IS NULL \
                        OR last_queried_at < now() - ($1 || ' days')::interval) \
                   AND ( \
                     source = 'discovered' \
                     OR ( \
                       source = 'view_dependency' \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM melt_view_dependencies d \
                         JOIN melt_table_stats ps \
                           ON ps.database = d.parent_db \
                          AND ps.schema   = d.parent_schema \
                          AND ps.name     = d.parent_name \
                         WHERE d.dep_db     = s.database \
                           AND d.dep_schema = s.schema \
                           AND d.dep_name   = s.name \
                           AND ps.sync_state = 'active' \
                       ) \
                     ) \
                   )",
                &[&(idle_days as i32).to_string()],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("idle_discovered: {e}")))
            })?;
        Ok(rows
            .into_iter()
            .map(|r| {
                TableRef::new(
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect())
    }

    /// Stamp the detected Snowflake object kind on a row. Called at
    /// bootstrap time once `describe_object_kind` resolves.
    pub async fn set_object_kind(&self, t: &TableRef, kind: ObjectKind) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "UPDATE melt_table_stats SET object_kind = $4 \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name, &kind.as_str()],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("set_object_kind: {e}")))
            })?;
        Ok(())
    }

    /// Record the strategy a view was bootstrapped with. `None`
    /// clears the column back to NULL (used when a view falls back to
    /// `base_table` object kind, though today we just re-bootstrap in
    /// that case).
    pub async fn set_view_strategy(
        &self,
        t: &TableRef,
        strategy: Option<ViewStrategy>,
    ) -> Result<()> {
        let client = self.client().await?;
        let s = strategy.map(|v| v.as_str());
        client
            .execute(
                "UPDATE melt_table_stats SET view_strategy = $4 \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name, &s],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("set_view_strategy: {e}")))
            })?;
        Ok(())
    }

    /// Upsert the body snapshot for a view. `duckdb_body` is populated
    /// for `decomposed` views; `None` for `stream_on_view`.
    pub async fn write_view_body(
        &self,
        t: &TableRef,
        snowflake_body: &str,
        duckdb_body: Option<&str>,
        checksum: &str,
    ) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "INSERT INTO melt_view_bodies \
                   (database, schema, name, snowflake_body, duckdb_body, body_checksum, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, now()) \
                 ON CONFLICT (database, schema, name) DO UPDATE \
                   SET snowflake_body = EXCLUDED.snowflake_body, \
                       duckdb_body    = EXCLUDED.duckdb_body, \
                       body_checksum  = EXCLUDED.body_checksum, \
                       updated_at     = now()",
                &[
                    &t.database,
                    &t.schema,
                    &t.name,
                    &snowflake_body,
                    &duckdb_body,
                    &checksum,
                ],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("write_view_body: {e}"))))?;
        Ok(())
    }

    /// Fetch the stored view body record, if any.
    pub async fn read_view_body(&self, t: &TableRef) -> Result<Option<ViewBodyRow>> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT database, schema, name, snowflake_body, duckdb_body, \
                        body_checksum, updated_at \
                 FROM melt_view_bodies \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("read_view_body: {e}"))))?;
        Ok(row.map(|r| ViewBodyRow {
            table: TableRef::new(
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
            ),
            snowflake_body: r.get::<_, String>(3),
            duckdb_body: r.get::<_, Option<String>>(4),
            body_checksum: r.get::<_, String>(5),
            updated_at: r.get::<_, chrono::DateTime<chrono::Utc>>(6),
        }))
    }

    /// Transactionally rewrite the dependency set for a parent view:
    /// delete the old set, then insert the new one. Keeps the table
    /// idempotent w.r.t. body drift.
    pub async fn write_view_dependencies(
        &self,
        parent: &TableRef,
        deps: &[(TableRef, DepKind)],
    ) -> Result<()> {
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        tx.execute(
            "DELETE FROM melt_view_dependencies \
             WHERE parent_db=$1 AND parent_schema=$2 AND parent_name=$3",
            &[&parent.database, &parent.schema, &parent.name],
        )
        .await
        .map_err(|e| {
            MeltError::Catalog(CatalogError::Other(format!("write_view_deps delete: {e}")))
        })?;
        for (dep, kind) in deps {
            tx.execute(
                "INSERT INTO melt_view_dependencies \
                   (parent_db, parent_schema, parent_name, \
                    dep_db, dep_schema, dep_name, dep_kind, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, now())",
                &[
                    &parent.database,
                    &parent.schema,
                    &parent.name,
                    &dep.database,
                    &dep.schema,
                    &dep.name,
                    &kind.as_str(),
                ],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("write_view_deps insert: {e}")))
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;
        Ok(())
    }

    /// Every parent view that declared a dependency on `dep`. Used to
    /// cascade re-bootstrap when a base table is invalidated.
    pub async fn dependent_views(&self, dep: &TableRef) -> Result<Vec<TableRef>> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT parent_db, parent_schema, parent_name \
                 FROM melt_view_dependencies \
                 WHERE dep_db=$1 AND dep_schema=$2 AND dep_name=$3",
                &[&dep.database, &dep.schema, &dep.name],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("dependent_views: {e}")))
            })?;
        Ok(rows
            .into_iter()
            .map(|r| {
                TableRef::new(
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect())
    }

    /// Variant of [`Self::ensure_discovered`] tailored to views'
    /// transitive dependencies. Rows land as `sync_state = 'pending'`
    /// with `source = 'view_dependency'` so demotion knows they
    /// were pulled in by a parent. Existing rows keep their stronger
    /// source (`include` > `discovered`/`view_dependency`) and bump
    /// `last_queried_at`. Returns the resolved state of each row.
    pub async fn ensure_discovered_view_dependency(
        &self,
        deps: &[TableRef],
    ) -> Result<Vec<SyncState>> {
        if deps.is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        for t in deps {
            tx.execute(
                "INSERT INTO melt_table_stats \
                   (database, schema, name, sync_state, source, \
                    discovered_at, last_queried_at) \
                 VALUES ($1, $2, $3, 'pending', 'view_dependency', now(), now()) \
                 ON CONFLICT (database, schema, name) DO UPDATE \
                   SET last_queried_at = now(), \
                       source = CASE \
                           WHEN melt_table_stats.source = 'remote' THEN 'remote' \
                           WHEN melt_table_stats.source = 'include' THEN 'include' \
                           WHEN melt_table_stats.source = 'discovered' THEN 'discovered' \
                           ELSE 'view_dependency' \
                       END",
                &[&t.database, &t.schema, &t.name],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!(
                    "ensure_discovered_view_dependency: {e}"
                )))
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;

        let states = self.state_batch(deps).await?;
        Ok(states
            .into_iter()
            .map(|o| o.unwrap_or(SyncState::Pending))
            .collect())
    }

    /// Rows currently tracked with a given object_kind. Used by the
    /// drift-rescan loop to iterate over views only.
    pub async fn list_by_object_kind(&self, kind: ObjectKind) -> Result<Vec<TableRef>> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT database, schema, name FROM melt_table_stats \
                 WHERE object_kind = $1",
                &[&kind.as_str()],
            )
            .await
            .map_err(|e| {
                MeltError::Catalog(CatalogError::Other(format!("list_by_object_kind: {e}")))
            })?;
        Ok(rows
            .into_iter()
            .map(|r| {
                TableRef::new(
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect())
    }

    async fn set_state(&self, t: &TableRef, state: SyncState, error: Option<&str>) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "UPDATE melt_table_stats \
                 SET sync_state = $4, \
                     bootstrap_error = $5 \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name, &state.as_str(), &error],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("set_state: {e}"))))?;
        Ok(())
    }

    pub async fn last_synced_snapshot(&self, t: &TableRef) -> Result<Option<SnapshotId>> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT last_snapshot FROM melt_sync_progress \
                 WHERE database=$1 AND schema=$2 AND name=$3",
                &[&t.database, &t.schema, &t.name],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("snapshot: {e}"))))?;
        Ok(row.map(|r| SnapshotId(r.get::<_, i64>(0))))
    }

    pub async fn record_sync_progress(&self, t: &TableRef, s: SnapshotId) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "INSERT INTO melt_sync_progress (database, schema, name, last_snapshot, last_synced_at) \
                 VALUES ($1, $2, $3, $4, now()) \
                 ON CONFLICT (database, schema, name) DO UPDATE \
                   SET last_snapshot = EXCLUDED.last_snapshot, last_synced_at = now()",
                &[&t.database, &t.schema, &t.name, &s.0],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("record: {e}"))))?;
        Ok(())
    }

    pub async fn policy_markers_batch(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        let in_clause = build_in_clause(tables);
        let sql = format!(
            "SELECT database, schema, name, policy_name \
             FROM melt_policy_markers \
             WHERE (database, schema, name) IN ({in_clause})"
        );
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("markers: {e}"))))?;
        let mut by_table: std::collections::HashMap<TableRef, String> = Default::default();
        for r in rows {
            let t = TableRef::new(
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
            );
            by_table.insert(t, r.get::<_, String>(3));
        }
        Ok(tables.iter().map(|t| by_table.get(t).cloned()).collect())
    }

    /// Persist markers from the policy refresh loop, tagged
    /// `source='discovered'`. Manual markers (inserted out-of-band
    /// with `source='manual'`) keep their source on conflict.
    pub async fn write_policy_markers(&self, p: &[ProtectedTable]) -> Result<()> {
        if p.is_empty() {
            return Ok(());
        }
        let mut client = self.client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx begin: {e}"))))?;
        for marker in p {
            tx.execute(
                "INSERT INTO melt_policy_markers \
                   (database, schema, name, policy_name, policy_kind, source, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, 'discovered', now()) \
                 ON CONFLICT (database, schema, name, policy_name) DO UPDATE \
                   SET policy_kind = EXCLUDED.policy_kind, \
                       updated_at  = now()",
                &[
                    &marker.table.database,
                    &marker.table.schema,
                    &marker.table.name,
                    &marker.policy_name,
                    &marker.policy_kind.as_str(),
                ],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("write marker: {e}"))))?;
        }
        tx.commit()
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("tx commit: {e}"))))?;
        Ok(())
    }

    pub async fn policy_views_batch(&self, tables: &[TableRef]) -> Result<Vec<Option<String>>> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        let in_clause = build_in_clause(tables);
        let sql = format!(
            "SELECT database, schema, name, view_name \
             FROM melt_policy_views \
             WHERE (database, schema, name) IN ({in_clause})"
        );
        let rows = client
            .query(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("views: {e}"))))?;
        let mut by_table: std::collections::HashMap<TableRef, String> = Default::default();
        for r in rows {
            let t = TableRef::new(
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, String>(2),
            );
            by_table.insert(t, r.get::<_, String>(3));
        }
        Ok(tables.iter().map(|t| by_table.get(t).cloned()).collect())
    }

    pub async fn write_policy_view(
        &self,
        table: &TableRef,
        view_name: &str,
        duckdb_where: &str,
        source_body: &str,
    ) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "INSERT INTO melt_policy_views \
                   (database, schema, name, view_name, duckdb_where, source_body, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, now()) \
                 ON CONFLICT (database, schema, name) DO UPDATE \
                   SET view_name    = EXCLUDED.view_name, \
                       duckdb_where = EXCLUDED.duckdb_where, \
                       source_body  = EXCLUDED.source_body, \
                       updated_at   = now()",
                &[
                    &table.database,
                    &table.schema,
                    &table.name,
                    &view_name,
                    &duckdb_where,
                    &source_body,
                ],
            )
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("write_view: {e}"))))?;
        Ok(())
    }

    pub async fn retain_policy_views(&self, keep: &[TableRef]) -> Result<()> {
        let client = self.client().await?;
        if keep.is_empty() {
            client
                .execute("DELETE FROM melt_policy_views", &[])
                .await
                .map_err(|e| {
                    MeltError::Catalog(CatalogError::Other(format!("retain_views: {e}")))
                })?;
            return Ok(());
        }
        let in_clause = build_in_clause(keep);
        let sql = format!(
            "DELETE FROM melt_policy_views \
             WHERE (database, schema, name) NOT IN ({in_clause})"
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("retain_views: {e}"))))?;
        Ok(())
    }

    pub async fn status_snapshot(&self) -> Result<StatusSnapshot> {
        let client = self.client().await?;
        // Age columns are NULL on empty source tables; we avoid the
        // `-infinity` sentinel because `now() - '-infinity'` errors in Postgres.
        let row = client
            .query_one(
                "SELECT
                    (SELECT COUNT(*) FROM melt_table_stats)::BIGINT,
                    (SELECT COUNT(*) FROM melt_policy_markers)::BIGINT,
                    EXTRACT(EPOCH FROM (now() - (SELECT MAX(updated_at) FROM melt_policy_markers)))::DOUBLE PRECISION,
                    EXTRACT(EPOCH FROM (now() - (SELECT MIN(last_synced_at) FROM melt_sync_progress)))::DOUBLE PRECISION",
                &[],
            )
            .await
            .map_err(|e| {
                // tokio-postgres Display strips SQLSTATE; pull DbError manually.
                let detail = e
                    .as_db_error()
                    .map(|db| format!("{} ({})", db.message(), db.code().code()))
                    .unwrap_or_else(|| e.to_string());
                MeltError::Catalog(CatalogError::Other(format!("status: {detail}")))
            })?;
        Ok(StatusSnapshot {
            tables_tracked: row.get::<_, i64>(0).max(0) as u64,
            marker_count: row.get::<_, i64>(1).max(0) as u64,
            last_policy_refresh_age_secs: row.get::<_, Option<f64>>(2),
            max_sync_lag_secs: row.get::<_, Option<f64>>(3).map(|v| v.max(0.0)),
        })
    }

    /// Drop discovered policy markers that are no longer in `keep`.
    /// Operates only on `source='discovered'` rows; manual markers
    /// always survive.
    pub async fn retain_policy_markers(&self, keep: &[TableRef]) -> Result<()> {
        let client = self.client().await?;
        if keep.is_empty() {
            client
                .execute(
                    "DELETE FROM melt_policy_markers WHERE source = 'discovered'",
                    &[],
                )
                .await
                .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("retain: {e}"))))?;
            return Ok(());
        }
        let in_clause = build_in_clause(keep);
        let sql = format!(
            "DELETE FROM melt_policy_markers \
             WHERE source = 'discovered' \
               AND (database, schema, name) NOT IN ({in_clause})"
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| MeltError::Catalog(CatalogError::Other(format!("retain: {e}"))))?;
        Ok(())
    }
}

/// Column list reused by every `SyncStateRow`-returning query.
/// Must stay in sync with `row_to_state_row`'s indices (0 → ...).
const STATE_ROW_COLUMNS: &str = "s.database, s.schema, s.name, s.sync_state, s.source, \
     s.discovered_at, s.last_queried_at, s.bootstrap_error, \
     s.bytes, s.rows_count, \
     p.last_snapshot, p.last_synced_at, \
     s.object_kind, s.view_strategy";

fn build_in_clause(tables: &[TableRef]) -> String {
    tables
        .iter()
        .map(|t| {
            format!(
                "({}, {}, {})",
                pg_lit(&t.database),
                pg_lit(&t.schema),
                pg_lit(&t.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn pg_lit(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

fn row_to_state_row(r: tokio_postgres::Row) -> SyncStateRow {
    let object_kind = ObjectKind::from_db(r.get::<_, &str>(12));
    let view_strategy = r.get::<_, Option<&str>>(13).and_then(ViewStrategy::from_db);
    SyncStateRow {
        table: TableRef::new(
            r.get::<_, String>(0),
            r.get::<_, String>(1),
            r.get::<_, String>(2),
        ),
        sync_state: SyncState::from_db(r.get::<_, &str>(3)),
        source: SyncSource::from_db(r.get::<_, &str>(4)),
        object_kind,
        view_strategy,
        discovered_at: r.get::<_, chrono::DateTime<chrono::Utc>>(5),
        last_queried_at: r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(6),
        bootstrap_error: r.get::<_, Option<String>>(7),
        bytes: (r.get::<_, i64>(8)).max(0) as u64,
        rows_count: (r.get::<_, i64>(9)).max(0) as u64,
        last_snapshot: r.get::<_, Option<i64>>(10),
        last_synced_at: r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(11),
    }
}

#[derive(Clone, Debug)]
pub struct MarkerRow {
    pub table: TableRef,
    pub policy_name: String,
    pub policy_kind: String,
}

#[derive(Clone, Debug)]
pub struct StatusSnapshot {
    pub tables_tracked: u64,
    pub marker_count: u64,
    /// `None` when no policy refresh has ever recorded a marker —
    /// either because the operator runs in `AllowList` mode (loop
    /// disabled) or because the first refresh hasn't happened yet.
    pub last_policy_refresh_age_secs: Option<f64>,
    /// `None` when no table has ever been synced (catalog is empty).
    pub max_sync_lag_secs: Option<f64>,
}

/// Type-erase a `ControlCatalog` so backend constructors can hand the
/// same `Arc` to both reader and sync without forcing callers to know
/// about deadpool.
pub type SharedControl = Arc<ControlCatalog>;

/// Blanket `DiscoveryCatalog` impl so `melt-router` and `melt-proxy`
/// can depend on the trait alone. Router takes
/// `Arc<dyn DiscoveryCatalog>` and never has to know this is
/// Postgres.
#[async_trait]
impl DiscoveryCatalog for ControlCatalog {
    async fn ensure_discovered(
        &self,
        tables: &[TableRef],
        source: SyncSource,
    ) -> Result<Vec<SyncState>> {
        ControlCatalog::ensure_discovered(self, tables, source).await
    }

    async fn state_batch(&self, tables: &[TableRef]) -> Result<Vec<Option<SyncState>>> {
        ControlCatalog::state_batch(self, tables).await
    }

    async fn mark_queried(&self, tables: &[TableRef]) -> Result<()> {
        ControlCatalog::mark_queried(self, tables).await
    }
}
