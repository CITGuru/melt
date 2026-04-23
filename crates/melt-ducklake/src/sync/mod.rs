//! DuckLake sync — CDC apply + policy refresh.

pub mod apply;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod snowflake;

use std::sync::Arc;
use std::time::{Duration, Instant};

use melt_control::{DepKind, SyncState};
use melt_core::{
    MatchOutcome, MeltError, ObjectKind, PolicyConfig, PolicyMode, Result, RouterCache, SyncConfig,
    SyncTableMatcher, TableRef, ViewStrategy,
};
use melt_snowflake::{
    classify_view_body, translate_view_body, SnowflakeClient, ViewBodyClassification, ViewDef,
};
use metrics::counter;
use std::collections::{HashSet, VecDeque};
use tokio::sync::Notify;

use crate::catalog::CatalogClient;
use crate::pool::DuckLakePool;
use crate::schema::SyncReport;

/// Outcome of a single view-bootstrap attempt. Returned by
/// [`DuckLakeSync::bootstrap_view`] so the caller can decide whether
/// to `mark_active` (both terminal variants) or leave the row in
/// `pending` for the next tick to retry (the `WaitingForDeps` case).
enum ViewBootstrapOutcome {
    Decomposed,
    StreamOnView,
    /// Deps aren't all `Active` yet; parent row was flipped back to
    /// `pending` with a hint in `bootstrap_error`.
    WaitingForDeps,
}

pub struct DuckLakeSync {
    pub catalog: Arc<CatalogClient>,
    pub pool: Arc<DuckLakePool>,
    pub snowflake: Arc<SnowflakeClient>,
    pub router_cache: Arc<dyn RouterCache>,
    pub policy_cfg: PolicyConfig,
    pub sync_cfg: SyncConfig,
    /// Timestamp of the last demotion sweep. Stale at startup so the
    /// first tick runs one immediately — cheap against an empty
    /// catalog, valuable against a warm one.
    last_demotion_at: parking_lot::Mutex<Option<Instant>>,
    /// Most-recent `pool.write().await` wait, in milliseconds.
    /// Read by `run_continuous` to decide backoff. Relaxed ordering
    /// — observability only, never read for correctness.
    last_writer_lock_wait: std::sync::atomic::AtomicU64,
}

impl DuckLakeSync {
    pub fn new(
        catalog: Arc<CatalogClient>,
        pool: Arc<DuckLakePool>,
        snowflake: Arc<SnowflakeClient>,
        router_cache: Arc<dyn RouterCache>,
        policy_cfg: PolicyConfig,
        sync_cfg: SyncConfig,
    ) -> Self {
        Self {
            catalog,
            pool,
            snowflake,
            router_cache,
            policy_cfg,
            sync_cfg,
            last_demotion_at: parking_lot::Mutex::new(None),
            last_writer_lock_wait: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Sync a single table. Caller is responsible for picking the
    /// table set; `run_continuous` does this in a loop.
    ///
    /// Staleness guard: before pulling changes, we probe the Snowflake
    /// stream. If Snowflake reports it stale (change-tracking
    /// retention elapsed without consumption), we drop + flip the
    /// table back to `pending` so the next iteration re-bootstraps
    /// cleanly. Missing unconsumed changes is unavoidable in that
    /// case; the only alternative is silent data loss, which isn't.
    pub async fn sync_table(&self, t: &TableRef) -> Result<SyncReport> {
        let started = Instant::now();
        let token = self.snowflake.service_token().await?;

        // Stream stale (past STALE_AFTER) or missing → rebootstrap.
        // Detected off the `read_stream_since` failure body via
        // `is_stream_unrecoverable` rather than an upfront probe.
        let last = self.catalog.last_synced_snapshot(t).await?;
        let changes = match self.snowflake.read_stream_since(&token, t, last).await {
            Ok(c) => c,
            Err(e) if melt_snowflake::is_stream_unrecoverable(&e) => {
                tracing::warn!(
                    table = %t,
                    error = %e,
                    "sync: stream stale or missing — rebuilding from scratch"
                );
                counter!(melt_metrics::SYNC_STREAM_STALE).increment(1);
                let _ = self.snowflake.drop_stream(&token, t).await;
                self.catalog.refresh_table(t).await?;
                self.router_cache.invalidate_table(t).await;

                // Cascade re-bootstrap to dependent decomposed views.
                if let Ok(deps) = self.catalog.dependent_views(t).await {
                    for parent in &deps {
                        tracing::info!(
                            parent = %parent,
                            dep = %t,
                            "sync.view: cascading re-bootstrap"
                        );
                        let _ = self
                            .catalog
                            .mark_pending(parent, Some("view.dep_invalidated"))
                            .await;
                        self.router_cache.invalidate_table(parent).await;
                    }
                }
                return Ok(empty_report(t.clone(), melt_snowflake::SnapshotId(0)));
            }
            Err(e) => return Err(e),
        };

        // Single writer slot. We time the actual lock acquisition
        // (not total iteration latency) so `run_continuous`'s
        // backoff heuristic doesn't misattribute slow Snowflake
        // round-trips to lake contention.
        let lock_started = std::time::Instant::now();
        let mut writer = self.pool.write().await;
        let lock_wait = lock_started.elapsed();
        if lock_wait > Duration::from_secs(2) {
            tracing::debug!(
                table = %t,
                lock_wait_ms = lock_wait.as_millis() as u64,
                "sync: writer lock acquisition was slow"
            );
        }
        self.last_writer_lock_wait.store(
            lock_wait.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let report = apply::write_changes(&mut writer, t, changes, started)?;
        drop(writer);

        // CDC commit point: drop the consume table only AFTER both
        // the lake apply and the catalog snapshot record succeed,
        // so a crash between the two leaves the consume table for
        // the next tick to replay (apply is idempotent on
        // `__row_id`).
        self.catalog
            .record_sync_progress(t, report.snapshot)
            .await?;
        let consume_name = melt_snowflake::SnowflakeClient::consume_table_name(t);
        if let Err(e) = self
            .snowflake
            .drop_consume_table(&token, &consume_name)
            .await
        {
            tracing::warn!(
                error = %e,
                table = %t,
                consume = %consume_name,
                "sync: post-apply drop of consume table failed; \
                 next tick will replay (apply is idempotent)"
            );
        }
        self.router_cache.invalidate_table(t).await;
        counter!(melt_metrics::SYNC_ROWS_APPLIED)
            .increment(report.rows_inserted + report.rows_updated + report.rows_deleted);
        Ok(report)
    }

    /// Bootstrap the next batch of `pending` tables. Up to
    /// `[sync.lazy].max_concurrent_bootstraps` are attempted per tick.
    ///
    /// For each table:
    /// 1. Flip to `bootstrapping` (router keeps forcing passthrough).
    /// 2. Optionally `ALTER TABLE … SET CHANGE_TRACKING = TRUE` when
    ///    `auto_enable_change_tracking = true`.
    /// 3. `CREATE STREAM IF NOT EXISTS … SHOW_INITIAL_ROWS = TRUE`.
    /// 4. Drain the initial snapshot via the existing CDC pipeline —
    ///    `SHOW_INITIAL_ROWS` returns every current row as an INSERT
    ///    event on first consumption.
    /// 5. Record starting snapshot + flip to `active`.
    ///
    /// Any error at steps 2–4 lands the row in `quarantined` with a
    /// human-readable reason. Operators unquarantine with `melt sync
    /// refresh`.
    pub async fn bootstrap_pending(&self) -> Result<()> {
        let limit = self.sync_cfg.lazy.max_concurrent_bootstraps.max(1) as i64;
        // Pick up `Bootstrapping` rows alongside `Pending` ones —
        // the former are crash-recovery cases left mid-bootstrap by
        // a prior process kill. `bootstrap_base_table` is idempotent
        // so re-entering is safe.
        let pending = self
            .catalog
            .list_by_state(SyncState::Pending, Some(limit))
            .await?;
        let bootstrapping_retry = self
            .catalog
            .list_by_state(SyncState::Bootstrapping, Some(limit))
            .await
            .unwrap_or_default();
        if !bootstrapping_retry.is_empty() {
            tracing::info!(
                recovered = bootstrapping_retry.len(),
                "sync: retrying previously-stuck bootstrapping rows"
            );
        }
        let all_rows: Vec<_> = pending.iter().chain(bootstrapping_retry.iter()).collect();
        for row in &all_rows {
            let table = row.table.clone();
            // Flip state before we start so a crash leaves the row
            // in `bootstrapping` (and gets picked up on restart via
            // the same path).
            if let Err(e) = self.catalog.mark_bootstrapping(&table).await {
                tracing::warn!(error = %e, table = %table, "bootstrap: mark failed");
                continue;
            }
            let started = Instant::now();
            match self.bootstrap_table(&table).await {
                Ok(()) => {
                    let secs = started.elapsed().as_secs_f64();
                    tracing::info!(
                        table = %table,
                        elapsed_secs = secs,
                        "sync: bootstrap complete"
                    );
                    metrics::histogram!(melt_metrics::SYNC_BOOTSTRAP_DURATION_SECONDS).record(secs);
                }
                Err(e) => {
                    tracing::warn!(error = %e, table = %table, "sync: bootstrap failed");
                    counter!(
                        melt_metrics::SYNC_BOOTSTRAP_FAILURES,
                        melt_metrics::LABEL_REASON => "bootstrap_err",
                    )
                    .increment(1);
                    let _ = self.catalog.mark_quarantined(&table, &e.to_string()).await;
                }
            }
            self.router_cache.invalidate_table(&table).await;
        }
        Ok(())
    }

    /// Kind-aware dispatcher. Probes Snowflake for the object type,
    /// stamps it on `melt_table_stats`, then delegates to the
    /// appropriate per-kind bootstrap.
    async fn bootstrap_table(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;
        let kind = self
            .snowflake
            .describe_object_kind(&token, table)
            .await
            .unwrap_or(ObjectKind::Unknown);
        let _ = self.catalog.set_object_kind(table, kind).await;

        match kind {
            ObjectKind::BaseTable | ObjectKind::ExternalTable => {
                self.bootstrap_base_table(&token, table).await
            }
            ObjectKind::View => {
                match self.bootstrap_view(&token, table).await? {
                    ViewBootstrapOutcome::Decomposed => {
                        counter!(
                            melt_metrics::SYNC_VIEWS,
                            melt_metrics::LABEL_BACKEND => "ducklake",
                            "strategy" => "decomposed",
                        )
                        .increment(1);
                        self.catalog
                            .set_view_strategy(table, Some(ViewStrategy::Decomposed))
                            .await?;
                        self.catalog.mark_active(table).await?;
                        Ok(())
                    }
                    ViewBootstrapOutcome::StreamOnView => {
                        counter!(
                            melt_metrics::SYNC_VIEWS,
                            melt_metrics::LABEL_BACKEND => "ducklake",
                            "strategy" => "stream_on_view",
                        )
                        .increment(1);
                        self.catalog
                            .set_view_strategy(table, Some(ViewStrategy::StreamOnView))
                            .await?;
                        self.catalog.mark_active(table).await?;
                        Ok(())
                    }
                    ViewBootstrapOutcome::WaitingForDeps => {
                        // Parent was already flipped back to pending
                        // by `bootstrap_view`. Don't mark_active; let
                        // the next tick re-enter.
                        Ok(())
                    }
                }
            }
            ObjectKind::SecureView => Err(MeltError::backend(
                "secure_view_unsupported: Snowflake masks secure-view bodies and \
                 disallows stream-on-view on them",
            )),
            ObjectKind::MaterializedView => Err(MeltError::backend(
                "materialized_view_unsupported: sync cannot stream materialized views",
            )),
            ObjectKind::Unknown => Err(MeltError::backend(
                "object_not_found: INFORMATION_SCHEMA.TABLES has no row for this FQN",
            )),
        }
    }

    async fn bootstrap_base_table(
        &self,
        token: &melt_snowflake::ServiceToken,
        table: &TableRef,
    ) -> Result<()> {
        let token_str = token.as_str();

        // Tear down stale stream/consume/lake artifacts before re-drain
        // (Pending/Bootstrapping only). Otherwise new INSERTs would
        // layer on top of stale rows because apply upserts only by
        // `__row_id` for rows in the current batch.
        let _ = self.snowflake.drop_stream(token_str, table).await;
        let consume_name = melt_snowflake::SnowflakeClient::consume_table_name(table);
        let _ = self
            .snowflake
            .drop_consume_table(token_str, &consume_name)
            .await;
        let qualified_lake_table = format!("\"{}\".\"{}\"", table.schema, table.name);
        {
            let writer = self.pool.write().await;
            if let Err(e) =
                writer.execute_batch(&format!("DROP TABLE IF EXISTS {qualified_lake_table}"))
            {
                tracing::warn!(
                    error = %e,
                    table = %table,
                    "bootstrap: failed to drop stale lake table; continuing anyway"
                );
            }
        }

        if self.sync_cfg.lazy.auto_enable_change_tracking {
            // Best-effort: a table that already has change tracking
            // returns OK; a missing grant or ownership error bubbles
            // up and lands the row in quarantined.
            self.snowflake
                .enable_change_tracking(token_str, table)
                .await?;
        }

        self.snowflake
            .create_stream_if_not_exists(token_str, table)
            .await?;

        // Pre-drain COUNT(*) to distinguish empty source from wedged
        // stream — only reliable before initial-rows is consumed.
        let source_count = self
            .snowflake
            .count_table(token_str, table)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    error = %e,
                    table = %table,
                    "bootstrap: pre-drain COUNT(*) failed; skipping stream-rows sanity check"
                );
                // Sentinel = couldn't verify; trust the stream rather than quarantine on a flake.
                u64::MAX
            });

        let started = Instant::now();
        let changes = self
            .snowflake
            .read_stream_since(token_str, table, None)
            .await?;
        let mut writer = self.pool.write().await;
        let report = apply::write_changes(&mut writer, table, changes, started)?;
        drop(writer);

        let drained = report.rows_inserted + report.rows_updated + report.rows_deleted;
        // Enforce: 0 rows w/ source>0 → wedged; drained<source → partial; > → OK.
        if source_count != u64::MAX && source_count > 0 && drained == 0 {
            return Err(MeltError::backend(format!(
                "bootstrap_stream_empty: Snowflake source has {source_count} row(s) but the \
                 CDC stream returned 0. Change tracking was probably not active when the \
                 stream was created, so the initial-rows snapshot window is empty. Fix: \
                 \n  1. ALTER TABLE {db}.{schema}.{name} SET CHANGE_TRACKING = TRUE; \
                 \n  2. DROP STREAM IF EXISTS {db}.{schema}.\"{name}__melt_stream\"; \
                 \n  3. `melt sync refresh {db}.{schema}.{name} --yes`",
                db = table.database,
                schema = table.schema,
                name = table.name,
            )));
        }
        if source_count != u64::MAX && drained < source_count {
            return Err(MeltError::backend(format!(
                "bootstrap_partial_drain: Snowflake source has {source_count} row(s) but only \
                 {drained} were drained from the CDC stream. This usually indicates a \
                 multi-partition response was truncated upstream. The table will be retried \
                 on the next sync tick."
            )));
        }

        self.catalog
            .record_sync_progress(table, report.snapshot)
            .await?;
        self.catalog.mark_active(table).await?;

        // Same commit point as `sync_table`: drop the consume table
        // only after apply + record_sync_progress + mark_active all
        // succeeded. Failed drop is non-fatal — next tick will
        // replay (idempotent on `__row_id`).
        let consume_name = melt_snowflake::SnowflakeClient::consume_table_name(table);
        if let Err(e) = self
            .snowflake
            .drop_consume_table(token_str, &consume_name)
            .await
        {
            tracing::warn!(
                error = %e,
                table = %table,
                consume = %consume_name,
                "bootstrap: post-drain drop of consume table failed; \
                 next tick will replay (apply is idempotent)"
            );
        }

        tracing::info!(
            table = %table,
            source_count,
            drained,
            rows_inserted = report.rows_inserted,
            bytes_written = report.bytes_written,
            "bootstrap_base_table: drain complete",
        );
        Ok(())
    }

    /// Bootstrap a Snowflake view. Tries decomposition first then
    /// falls back to `CREATE STREAM ... ON VIEW`. Caller writes
    /// strategy + mark_active based on the returned
    /// [`ViewBootstrapOutcome`].
    async fn bootstrap_view(
        &self,
        token: &melt_snowflake::ServiceToken,
        table: &TableRef,
    ) -> Result<ViewBootstrapOutcome> {
        let token_str = token.as_str();
        let def = self
            .snowflake
            .fetch_view_definition(token_str, table)
            .await?
            .ok_or_else(|| MeltError::backend("view_body_unavailable: GET_DDL returned no rows"))?;

        if def.is_secure {
            return Err(MeltError::backend(
                "secure_view_unsupported: view body is masked by Snowflake",
            ));
        }

        let (classification, body_text) = classify_view_body(&def.body);
        let body_for_store = body_text.clone().unwrap_or_else(|| def.body.clone());
        let checksum = body_checksum(&body_for_store);
        let _ = self
            .catalog
            .write_view_body(table, &def.body, None, &checksum)
            .await;

        let matcher = SyncTableMatcher::from_config(&self.sync_cfg)
            .map_err(|e| MeltError::backend(format!("matcher rebuild failed: {e}")))?;

        // Resolve the full dependency closure up-front. Dead-end
        // early when an excluded or unsupported dep appears so the
        // stream-on-view fallback gets to run.
        let closure = match self
            .resolve_view_deps(token, &def, self.sync_cfg.views.max_dependency_depth)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::info!(
                    error = %e,
                    table = %table,
                    "sync.view: dep resolution failed — will try stream-on-view"
                );
                return self
                    .try_stream_on_view(token, table, &classification, &def, &body_for_store)
                    .await;
            }
        };

        // Check matcher opt-out BEFORE doing catalog writes: if any
        // base-table dep is explicitly excluded, we can't decompose.
        let any_excluded = closure
            .base_tables
            .iter()
            .chain(closure.nested_views.iter())
            .any(|t| matches!(matcher.classify(t), MatchOutcome::Excluded));
        let prefer_stream = self.sync_cfg.views.prefer_stream_on_view;

        if !prefer_stream
            && !any_excluded
            && matches!(
                classification,
                ViewBodyClassification::StreamCompatible
                    | ViewBodyClassification::DecomposableOnly(_)
            )
        {
            let duck_body = match translate_view_body(&body_for_store) {
                Ok(s) => s,
                Err(e) => {
                    tracing::info!(
                        error = %e,
                        table = %table,
                        "sync.view: body translation failed — will try stream-on-view"
                    );
                    return self
                        .try_stream_on_view(token, table, &classification, &def, &body_for_store)
                        .await;
                }
            };

            // Transitively register every dep as a ViewDependency row
            // (only effective when `auto_include_dependencies` is
            // true). The matcher's Excluded check above ensured no
            // opt-out conflicts land here.
            let mut all_deps: Vec<TableRef> = Vec::new();
            all_deps.extend(closure.base_tables.iter().cloned());
            all_deps.extend(closure.nested_views.iter().cloned());
            if self.sync_cfg.views.auto_include_dependencies && !all_deps.is_empty() {
                let _ = self
                    .catalog
                    .ensure_discovered_view_dependency(&all_deps)
                    .await;
            }

            // Persist the translated body + the dep graph so
            // drift-rescan and dependent-views cascade can find them.
            let _ = self
                .catalog
                .write_view_body(table, &def.body, Some(&duck_body), &checksum)
                .await;
            let mut dep_rows: Vec<(TableRef, DepKind)> = Vec::new();
            for t in &closure.base_tables {
                dep_rows.push((t.clone(), DepKind::BaseTable));
            }
            for t in &closure.nested_views {
                dep_rows.push((t.clone(), DepKind::View));
            }
            let _ = self.catalog.write_view_dependencies(table, &dep_rows).await;

            // Wait for deps to settle. Base tables bootstrap via the
            // normal ON TABLE path; intermediate views recurse into
            // this code on their own tick.
            let states = self.catalog.state_batch(&all_deps).await?;
            let ready = states.iter().all(|s| matches!(s, Some(SyncState::Active)));
            if !ready {
                let unready: Vec<String> = states
                    .iter()
                    .zip(all_deps.iter())
                    .filter(|(s, _)| !matches!(s, Some(SyncState::Active)))
                    .map(|(_, t)| format!("{t}"))
                    .collect();
                let hint = format!(
                    "view.waiting_for_deps: {} dep(s) not yet active — [{}]",
                    unready.len(),
                    unready.join(", ")
                );
                tracing::info!(table = %table, %hint, "sync.view: deferring");
                let _ = self.catalog.mark_pending(table, Some(&hint)).await;
                return Ok(ViewBootstrapOutcome::WaitingForDeps);
            }

            self.materialize_view_body(table, &duck_body).await?;
            tracing::info!(
                table = %table,
                deps = all_deps.len(),
                "sync.view.decomposed"
            );
            return Ok(ViewBootstrapOutcome::Decomposed);
        }

        self.try_stream_on_view(token, table, &classification, &def, &body_for_store)
            .await
    }

    /// Stream-on-view fallback. Only viable when the body is
    /// `StreamCompatible` per [`classify_view_body`]. Returns
    /// `ViewBootstrapOutcome::StreamOnView` on success; errors bubble
    /// up and land in quarantine.
    async fn try_stream_on_view(
        &self,
        token: &melt_snowflake::ServiceToken,
        table: &TableRef,
        classification: &ViewBodyClassification,
        def: &ViewDef,
        body_text: &str,
    ) -> Result<ViewBootstrapOutcome> {
        match classification {
            ViewBodyClassification::StreamCompatible => {}
            ViewBodyClassification::DecomposableOnly(r) => {
                return Err(MeltError::backend(format!(
                    "view_body_incompatible_with_stream: {r}; decomposition also \
                     unavailable (dep excluded or translation failed)"
                )));
            }
            ViewBodyClassification::Unsupported(r) => {
                return Err(MeltError::backend(format!("view_body_unsupported: {r}")));
            }
        }
        // `body_text` is stored for drift detection only here; the
        // stream-on-view path doesn't need a translated DuckDB body.
        let _ = body_text;

        let token_str = token.as_str();
        if self.sync_cfg.lazy.auto_enable_change_tracking {
            self.snowflake
                .enable_change_tracking_on_view(token_str, table)
                .await
                .map_err(|e| MeltError::backend(format!("ALTER VIEW SET CHANGE_TRACKING: {e}")))?;
            // Every underlying base table needs change tracking too.
            // Ownership failures here are a clear operator-actionable
            // error — bubble up with the offending FQN list.
            for (dep, kind) in &def.base_tables {
                if matches!(kind, ObjectKind::BaseTable) {
                    self.snowflake
                        .enable_change_tracking(token_str, dep)
                        .await
                        .map_err(|e| {
                            MeltError::backend(format!(
                                "ALTER TABLE SET CHANGE_TRACKING on dep {dep}: {e}"
                            ))
                        })?;
                }
            }
        }

        self.snowflake
            .create_stream_on_view_if_not_exists(token_str, table)
            .await?;

        let started = Instant::now();
        let changes = self
            .snowflake
            .read_stream_since(token_str, table, None)
            .await?;
        let mut writer = self.pool.write().await;
        let report = apply::write_changes(&mut writer, table, changes, started)?;
        drop(writer);

        // Record deps as `dep_kind='base_table'` so the dependent-views
        // cascade + demotion ref-count still work for stream-on-view
        // (operators sometimes also query the underlying tables).
        let dep_rows: Vec<(TableRef, DepKind)> = def
            .base_tables
            .iter()
            .filter(|(_, k)| matches!(k, ObjectKind::BaseTable | ObjectKind::View))
            .map(|(t, k)| {
                (
                    t.clone(),
                    if matches!(k, ObjectKind::View) {
                        DepKind::View
                    } else {
                        DepKind::BaseTable
                    },
                )
            })
            .collect();
        let _ = self.catalog.write_view_dependencies(table, &dep_rows).await;

        self.catalog
            .record_sync_progress(table, report.snapshot)
            .await?;
        tracing::info!(
            table = %table,
            deps = def.base_tables.len(),
            "sync.view.stream"
        );
        Ok(ViewBootstrapOutcome::StreamOnView)
    }

    /// Materialize the translated body as a DuckDB view in DuckLake.
    /// Generalization of the old `materialize_view(table, where)`
    /// helper — here we drop the view first (so the shape can change)
    /// and recreate against the translated body verbatim.
    async fn materialize_view_body(&self, table: &TableRef, duck_body: &str) -> Result<()> {
        let qualified = format!("\"{}\".\"{}\"", table.schema, table.name);
        let drop_sql = format!("DROP VIEW IF EXISTS {qualified}");
        let create_sql = format!("CREATE OR REPLACE VIEW {qualified} AS {duck_body}");
        let writer = self.pool.write().await;
        writer
            .execute_batch(&drop_sql)
            .map_err(|e| MeltError::backend(format!("DROP VIEW: {e}")))?;
        writer
            .execute_batch(&create_sql)
            .map_err(|e| MeltError::backend(format!("CREATE VIEW: {e}")))?;
        Ok(())
    }

    /// BFS through the view-dependency graph rooted at `def`. Each
    /// hop reads the current view's `fetch_view_definition`; base
    /// tables terminate the walk. Caps at `max_depth` to avoid
    /// pathological graphs.
    async fn resolve_view_deps(
        &self,
        token: &melt_snowflake::ServiceToken,
        def: &ViewDef,
        max_depth: u32,
    ) -> Result<ViewDepClosure> {
        let token_str = token.as_str();
        let mut closure = ViewDepClosure::default();
        let mut seen: HashSet<TableRef> = HashSet::new();
        let mut to_visit: VecDeque<(TableRef, ObjectKind, u32)> = VecDeque::new();
        for (dep, kind) in &def.base_tables {
            to_visit.push_back((dep.clone(), *kind, 1));
        }
        while let Some((t, kind, depth)) = to_visit.pop_front() {
            if depth > max_depth {
                return Err(MeltError::backend(format!(
                    "dep_graph_too_deep: {t} beyond max_dependency_depth={max_depth}"
                )));
            }
            if !seen.insert(t.clone()) {
                continue;
            }
            match kind {
                ObjectKind::BaseTable | ObjectKind::ExternalTable => {
                    closure.base_tables.push(t);
                }
                ObjectKind::View => {
                    closure.nested_views.push(t.clone());
                    if let Some(nested) =
                        self.snowflake.fetch_view_definition(token_str, &t).await?
                    {
                        if nested.is_secure {
                            return Err(MeltError::backend(format!(
                                "secure_view_in_dependency_graph: {t}"
                            )));
                        }
                        for (dep, k) in nested.base_tables {
                            to_visit.push_back((dep, k, depth + 1));
                        }
                    }
                }
                ObjectKind::SecureView => {
                    return Err(MeltError::backend(format!(
                        "secure_view_in_dependency_graph: {t}"
                    )));
                }
                ObjectKind::MaterializedView => {
                    return Err(MeltError::backend(format!(
                        "materialized_view_in_dependency_graph: {t}"
                    )));
                }
                ObjectKind::Unknown => {
                    return Err(MeltError::backend(format!("unknown_dep_kind: {t}")));
                }
            }
        }
        Ok(closure)
    }

    /// Drop auto-discovered tables that haven't been queried in
    /// `[sync.lazy].demotion_idle_days` days. Includes the Snowflake
    /// stream cleanup so we don't leak metadata objects in the
    /// customer's account. Throttled to once per
    /// `[sync.lazy].demotion_interval`.
    async fn maybe_demote(&self) -> Result<()> {
        let now = Instant::now();
        let should_run = {
            let mut guard = self.last_demotion_at.lock();
            let due = guard
                .map(|t| now.duration_since(t) >= self.sync_cfg.lazy.demotion_interval)
                .unwrap_or(true);
            if due {
                *guard = Some(now);
            }
            due
        };
        if !should_run {
            return Ok(());
        }

        let idle = self
            .catalog
            .idle_discovered(self.sync_cfg.lazy.demotion_idle_days)
            .await?;
        if idle.is_empty() {
            return Ok(());
        }
        let token = self.snowflake.service_token().await?;
        for t in &idle {
            tracing::info!(table = %t, "sync: demoting idle auto-discovered table");
            // Best-effort stream drop; Snowflake returns OK for
            // IF EXISTS even when the stream was already gone.
            let _ = self.snowflake.drop_stream(&token, t).await;
            if let Err(e) = self.catalog.drop_table(t).await {
                tracing::warn!(error = %e, table = %t, "sync: catalog drop failed");
            }
            self.router_cache.invalidate_table(t).await;
        }
        Ok(())
    }

    /// Walk every tracked view, re-fetch the body from Snowflake, and
    /// if the checksum changed flip the row back to `pending` so the
    /// next bootstrap tick rebuilds. Runs on the same cadence as the
    /// demotion sweep — frequent enough to catch `ALTER VIEW` drift
    /// within an hour, cheap enough to not cost a round-trip per
    /// CDC loop.
    async fn maybe_rescan_view_bodies(&self) -> Result<()> {
        // Reuse the demotion throttle so both housekeeping passes
        // fire at the same cadence without sharing state.
        let views = self
            .catalog
            .list_by_object_kind(ObjectKind::View)
            .await
            .unwrap_or_default();
        if views.is_empty() {
            return Ok(());
        }
        let token = self.snowflake.service_token().await?;
        for t in &views {
            let Ok(Some(def)) = self.snowflake.fetch_view_definition(&token, t).await else {
                continue;
            };
            let checksum = body_checksum(&def.body);
            let existing = self.catalog.read_view_body(t).await.ok().flatten();
            let stale = existing
                .as_ref()
                .map(|r| r.body_checksum != checksum)
                .unwrap_or(true);
            if stale {
                tracing::info!(
                    table = %t,
                    "sync.view: body drift detected — flipping to pending for re-bootstrap"
                );
                let _ = self.catalog.mark_pending(t, Some("view.body_drift")).await;
                let _ = self
                    .catalog
                    .write_view_body(t, &def.body, None, &checksum)
                    .await;
                self.router_cache.invalidate_table(t).await;
            }
        }
        Ok(())
    }

    /// Continuous CDC loop. Sleeps `interval` between full passes.
    ///
    /// Per-iteration, we apply **exponential backoff** when the
    /// writer is saturated: the writer is a single `tokio::Mutex`,
    /// so contention manifests as a long wait on `pool.write()`. We
    /// time the acquire and double the sleep (capped at
    /// `interval * 8`) when the wait exceeds 5s, giving readers
    /// breathing room before pulling the next CDC batch.
    ///
    /// `shutdown` is the process-wide cooperative shutdown signal;
    /// when it fires, the loop exits at the next safe point (between
    /// iterations or while sleeping).
    pub async fn run_continuous(
        self: Arc<Self>,
        interval: Duration,
        shutdown: Arc<Notify>,
    ) -> Result<()> {
        let mut backoff = interval;
        loop {
            // Pick up any tables the router discovered since last
            // tick BEFORE touching the active set — bootstrap
            // latency should not hide behind CDC scan time.
            if let Err(e) = self.bootstrap_pending().await {
                tracing::warn!(error = %e, "sync: bootstrap_pending iteration failed");
            }
            // Demote idle auto-discovered tables on a separate cadence
            // (throttled internally by `[sync.lazy].demotion_interval`).
            if let Err(e) = self.maybe_demote().await {
                tracing::warn!(error = %e, "sync: demotion sweep failed");
            }

            // Drift-rescan: every tick, re-checksum the stored view
            // bodies against Snowflake's current view DDL. Cheap when
            // the view count is small; the rescan itself is throttled
            // by `demotion_interval` so huge deployments don't pay
            // the round-trip cost every tick.
            if let Err(e) = self.maybe_rescan_view_bodies().await {
                tracing::warn!(error = %e, "sync: view drift rescan failed");
            }

            let tables = match self.catalog.list_tables().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "sync: list_tables failed, sleeping");
                    if sleep_or_shutdown(interval, &shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            // Backoff fires only when the lake writer lock itself
            // was contended >5s (not when Snowflake calls were slow).
            let mut max_lock_wait_ms: u64 = 0;
            for t in &tables {
                if let Err(e) = self.sync_table(t).await {
                    tracing::warn!(error = %e, table = %t, "sync_table failed");
                }
                let lw = self
                    .last_writer_lock_wait
                    .load(std::sync::atomic::Ordering::Relaxed);
                if lw > max_lock_wait_ms {
                    max_lock_wait_ms = lw;
                }
            }
            if max_lock_wait_ms > 5_000 {
                backoff = (backoff * 2).min(interval * 8);
                tracing::warn!(
                    backoff_secs = backoff.as_secs(),
                    max_lock_wait_ms,
                    "sync: lake writer contended — backing off",
                );
            } else {
                backoff = interval;
            }
            if sleep_or_shutdown(backoff, &shutdown).await {
                tracing::info!("sync: shutdown received — stopping continuous loop");
                return Ok(());
            }
        }
    }

    /// Periodically rescan Snowflake's policy references and update
    /// the catalog's policy markers. No-op in `AllowList` mode.
    /// Write-before-retain avoids clear-before-write races.
    pub async fn run_policy_refresh(self: Arc<Self>, shutdown: Arc<Notify>) -> Result<()> {
        if matches!(self.policy_cfg.mode, PolicyMode::AllowList { .. }) {
            return Ok(());
        }
        loop {
            let token = match self.snowflake.service_token().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "policy refresh: token fetch failed");
                    if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let protected = match self.snowflake.list_policy_protected_tables(&token).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "policy refresh: snowflake call failed");
                    counter!(melt_metrics::SYNC_POLICY_REFRESHES, melt_metrics::LABEL_OUTCOME => "err").increment(1);
                    if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            // Enforce: build a filtered view per table; untranslatable bodies fall back to marker.
            // Passthrough: every protected table gets a marker.
            let enforcing = matches!(self.policy_cfg.mode, PolicyMode::Enforce);
            let mut markers_to_write = Vec::with_capacity(protected.len());
            let keep: Vec<TableRef> = protected.iter().map(|p| p.table.clone()).collect();

            for marker in &protected {
                let translated = if enforcing {
                    marker.policy_body.as_deref().and_then(|body| {
                        match melt_snowflake::policy_dsl::translate(body) {
                            Ok(duck) => Some((duck, body.to_string())),
                            Err(e) => {
                                tracing::warn!(
                                    table = %marker.table,
                                    policy = %marker.policy_name,
                                    error = %e,
                                    "policy enforce: translation failed; keeping passthrough"
                                );
                                None
                            }
                        }
                    })
                } else {
                    None
                };

                match translated {
                    Some((duck_where, source_body)) => {
                        if let Err(e) = self.materialize_view(&marker.table, &duck_where).await {
                            tracing::warn!(
                                error = %e,
                                table = %marker.table,
                                "enforce: view CREATE failed; falling back to marker"
                            );
                            markers_to_write.push(marker.clone());
                        } else if let Err(e) = self
                            .catalog
                            .write_policy_view(
                                &marker.table,
                                &Self::view_name_for(&marker.table),
                                &duck_where,
                                &source_body,
                            )
                            .await
                        {
                            tracing::warn!(
                                error = %e,
                                table = %marker.table,
                                "enforce: catalog view record failed"
                            );
                        }
                    }
                    None => markers_to_write.push(marker.clone()),
                }
            }

            if let Err(e) = self.catalog.write_policy_markers(&markers_to_write).await {
                tracing::warn!(error = %e, "policy refresh: write failed");
            }
            if let Err(e) = self.catalog.retain_policy_markers(&keep).await {
                tracing::warn!(error = %e, "policy refresh: retain failed");
            }
            if enforcing {
                if let Err(e) = self.catalog.retain_policy_views(&keep).await {
                    tracing::warn!(error = %e, "policy refresh: retain views failed");
                }
            }
            for t in &keep {
                self.router_cache.invalidate_table(t).await;
            }
            counter!(melt_metrics::SYNC_POLICY_REFRESHES, melt_metrics::LABEL_OUTCOME => "ok")
                .increment(1);
            if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                tracing::info!("sync: shutdown received — stopping policy refresh");
                return Ok(());
            }
        }
    }

    /// Run `CREATE OR REPLACE VIEW <view> AS SELECT * FROM <table>
    /// WHERE <duck_where>` against the lake's writer connection. The
    /// view is the only thing the router rewrites onto in
    /// `PolicyMode::Enforce`.
    async fn materialize_view(&self, table: &TableRef, duck_where: &str) -> Result<()> {
        let view = Self::view_name_for(table);
        let qualified = format!("\"{}\".\"{}\"", table.schema, table.name);
        let sql = format!(
            "CREATE OR REPLACE VIEW {view} AS SELECT * FROM {qualified} WHERE {duck_where}"
        );
        let writer = self.pool.write().await;
        writer
            .execute_batch(&sql)
            .map_err(|e| MeltError::backend(format!("CREATE VIEW: {e}")))?;
        Ok(())
    }

    fn view_name_for(table: &TableRef) -> String {
        format!("\"{}\".\"{}__melt_filtered\"", table.schema, table.name)
    }
}

/// Sleep for `dur` or until `shutdown` fires, whichever comes first.
/// Returns `true` if shutdown fired so callers can break the loop.
async fn sleep_or_shutdown(dur: Duration, shutdown: &Notify) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(dur) => false,
        _ = shutdown.notified() => true,
    }
}

/// Convenience: surface a "no work to do" report for tables that are
/// already current.
pub fn empty_report(table: TableRef, snapshot: melt_snowflake::SnapshotId) -> SyncReport {
    SyncReport {
        table,
        snapshot,
        rows_inserted: 0,
        rows_updated: 0,
        rows_deleted: 0,
        bytes_written: 0,
        elapsed: Duration::ZERO,
    }
}

/// Re-exported helper for CLI error parity. Enforce IS implemented —
/// `run_policy_refresh` + `materialize_view` cover it; this helper
/// is retained for CLI shapes that pre-date that work.
pub fn enforce_unsupported() -> MeltError {
    MeltError::config(
        "snowflake.policy.mode = \"enforce\" is not available in this CLI \
         path; use \"passthrough\" or \"allowlist\".",
    )
}

/// Flattened view-dependency closure. Populated by
/// [`DuckLakeSync::resolve_view_deps`].
#[derive(Default, Debug, Clone)]
pub struct ViewDepClosure {
    /// Leaves of the graph — the actual native tables that need to
    /// be synced for the parent view to materialize.
    pub base_tables: Vec<TableRef>,
    /// Intermediate views encountered on the way down. Registered as
    /// `view_dependency` rows so they take their own bootstrap path
    /// on the next tick.
    pub nested_views: Vec<TableRef>,
}

/// Stable checksum of a view body. Used only for drift detection —
/// an identical body must produce an identical checksum; collisions
/// aren't a security concern. `DefaultHasher` is cheap, stable across
/// runs of the same binary, and is the path of least dependency-
/// weight. If the algorithm changes, every view will re-bootstrap
/// once (the old checksums won't match the new hash), then stabilize.
fn body_checksum(body: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut h);
    format!("dh64:{:016x}", h.finish())
}

#[cfg(test)]
mod checksum_tests {
    use super::body_checksum;

    #[test]
    fn same_body_yields_same_checksum() {
        let body = "SELECT id FROM analytics.public.orders WHERE region = 'US'";
        assert_eq!(body_checksum(body), body_checksum(body));
    }

    #[test]
    fn different_body_yields_different_checksum() {
        let a = "SELECT id FROM orders WHERE region = 'US'";
        let b = "SELECT id FROM orders WHERE region = 'EU'";
        assert_ne!(body_checksum(a), body_checksum(b));
    }

    #[test]
    fn prefix_tag_is_stable() {
        let body = "SELECT 1";
        assert!(body_checksum(body).starts_with("dh64:"));
    }
}
