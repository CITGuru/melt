//! Iceberg sync — same shape as DuckLake's, different write path.
//! The MVP focuses on the policy refresher; CDC apply lands as the
//! Parquet writer + manifest commit logic graduates from the §11
//! long-tail into a real implementation.

pub mod snowflake;
pub mod writer;

use std::sync::Arc;
use std::time::{Duration, Instant};

use melt_control::{ControlCatalog, DepKind, SyncState};
use melt_core::{
    MeltError, ObjectKind, PolicyConfig, PolicyMode, Result, RouterCache, SyncConfig, TableRef,
    ViewStrategy,
};
use melt_snowflake::{classify_view_body, SnowflakeClient, ViewBodyClassification};
use metrics::counter;
use tokio::sync::Notify;

use crate::catalog::IcebergCatalogClient;
use crate::pool::IcebergPool;

pub struct IcebergSync {
    pub catalog: Arc<IcebergCatalogClient>,
    pub pool: Arc<IcebergPool>,
    pub snowflake: Arc<SnowflakeClient>,
    pub router_cache: Arc<dyn RouterCache>,
    pub policy_cfg: PolicyConfig,
    pub sync_cfg: SyncConfig,
    /// Shared Postgres control catalog. Iceberg's own catalog
    /// (REST/Glue) is the data-plane; state-machine writes land here.
    pub control: Arc<ControlCatalog>,
    last_demotion_at: parking_lot::Mutex<Option<Instant>>,
}

impl IcebergSync {
    pub fn new(
        catalog: Arc<IcebergCatalogClient>,
        pool: Arc<IcebergPool>,
        snowflake: Arc<SnowflakeClient>,
        router_cache: Arc<dyn RouterCache>,
        policy_cfg: PolicyConfig,
        sync_cfg: SyncConfig,
        control: Arc<ControlCatalog>,
    ) -> Self {
        Self {
            catalog,
            pool,
            snowflake,
            router_cache,
            policy_cfg,
            sync_cfg,
            control,
            last_demotion_at: parking_lot::Mutex::new(None),
        }
    }

    pub async fn run_continuous(
        self: Arc<Self>,
        interval: Duration,
        shutdown: Arc<Notify>,
    ) -> Result<()> {
        loop {
            if let Err(e) = self.bootstrap_pending().await {
                tracing::warn!(error = %e, "iceberg sync: bootstrap_pending failed");
            }
            if let Err(e) = self.maybe_demote().await {
                tracing::warn!(error = %e, "iceberg sync: demotion sweep failed");
            }

            let tables = self.catalog.list_tables().await.unwrap_or_default();
            for t in &tables {
                if let Err(e) = self.sync_one(t).await {
                    tracing::warn!(error = %e, table = %t, "iceberg sync_one failed");
                }
            }
            if sleep_or_shutdown(interval, &shutdown).await {
                tracing::info!("iceberg sync: shutdown received — stopping continuous loop");
                return Ok(());
            }
        }
    }

    /// Mirror of `DuckLakeSync::bootstrap_pending`. Iceberg's initial
    /// snapshot path still goes through the same Snowflake stream
    /// drain; the write side is the iceberg writer module.
    async fn bootstrap_pending(&self) -> Result<()> {
        let limit = self.sync_cfg.lazy.max_concurrent_bootstraps.max(1) as i64;
        let pending = self
            .control
            .list_by_state(SyncState::Pending, Some(limit))
            .await?;
        for row in &pending {
            let table = row.table.clone();
            if let Err(e) = self.control.mark_bootstrapping(&table).await {
                tracing::warn!(error = %e, table = %table, "iceberg bootstrap: mark failed");
                continue;
            }
            let started = Instant::now();
            match self.bootstrap_table(&table).await {
                Ok(()) => {
                    metrics::histogram!(melt_metrics::SYNC_BOOTSTRAP_DURATION_SECONDS)
                        .record(started.elapsed().as_secs_f64());
                }
                Err(e) => {
                    tracing::warn!(error = %e, table = %table, "iceberg bootstrap failed");
                    counter!(
                        melt_metrics::SYNC_BOOTSTRAP_FAILURES,
                        melt_metrics::LABEL_REASON => "bootstrap_err",
                    )
                    .increment(1);
                    let _ = self.control.mark_quarantined(&table, &e.to_string()).await;
                }
            }
            self.router_cache.invalidate_table(&table).await;
        }
        Ok(())
    }

    /// Kind-aware dispatcher. Iceberg supports:
    /// - `BaseTable` / `ExternalTable` → the existing `ON TABLE`
    ///   stream path.
    /// - `View` → stream-on-view only. Decomposition is DuckLake-only
    ///   in this PR because the Iceberg read path doesn't have a
    ///   durable lake-side `CREATE VIEW` analogue today (tracked as a
    ///   follow-up).
    /// - Everything else → quarantine with a specific reason.
    async fn bootstrap_table(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;
        let kind = self
            .snowflake
            .describe_object_kind(&token, table)
            .await
            .unwrap_or(ObjectKind::Unknown);
        let _ = self.control.set_object_kind(table, kind).await;

        match kind {
            ObjectKind::BaseTable | ObjectKind::ExternalTable => {
                self.bootstrap_base_table(table).await
            }
            ObjectKind::View => {
                tracing::info!(
                    table = %table,
                    "sync.view: iceberg backend uses stream-on-view; \
                     decomposition is a follow-up (see melt-iceberg/src/sync/mod.rs)"
                );
                self.bootstrap_view_stream_only(table).await
            }
            ObjectKind::SecureView => Err(MeltError::backend(
                "secure_view_unsupported: Snowflake masks secure-view bodies",
            )),
            ObjectKind::MaterializedView => {
                Err(MeltError::backend("materialized_view_unsupported"))
            }
            ObjectKind::Unknown => Err(MeltError::backend(
                "object_not_found: INFORMATION_SCHEMA.TABLES has no row",
            )),
        }
    }

    async fn bootstrap_base_table(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;
        if self.sync_cfg.lazy.auto_enable_change_tracking {
            self.snowflake.enable_change_tracking(&token, table).await?;
        }
        self.snowflake
            .create_stream_if_not_exists(&token, table)
            .await?;
        self.drain_stream_into_iceberg(table).await
    }

    /// Stream-on-view path on the Iceberg backend. Mirrors the
    /// DuckLake version but skips decomposition (no lake-side
    /// `CREATE VIEW`). The view's rowset materializes as a regular
    /// Iceberg table at the view's FQN.
    async fn bootstrap_view_stream_only(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;
        let def = self
            .snowflake
            .fetch_view_definition(&token, table)
            .await?
            .ok_or_else(|| MeltError::backend("view_body_unavailable"))?;
        if def.is_secure {
            return Err(MeltError::backend(
                "secure_view_unsupported: view body is masked by Snowflake",
            ));
        }
        let (classification, _) = classify_view_body(&def.body);
        match classification {
            ViewBodyClassification::StreamCompatible => {}
            ViewBodyClassification::DecomposableOnly(r) => {
                return Err(MeltError::backend(format!(
                    "view_body_incompatible_with_stream: {r} (iceberg backend \
                     only supports stream-on-view)"
                )));
            }
            ViewBodyClassification::Unsupported(r) => {
                return Err(MeltError::backend(format!("view_body_unsupported: {r}")));
            }
        }

        if self.sync_cfg.lazy.auto_enable_change_tracking {
            self.snowflake
                .enable_change_tracking_on_view(&token, table)
                .await?;
            for (dep, kind) in &def.base_tables {
                if matches!(kind, ObjectKind::BaseTable) {
                    self.snowflake.enable_change_tracking(&token, dep).await?;
                }
            }
        }
        self.snowflake
            .create_stream_on_view_if_not_exists(&token, table)
            .await?;

        self.drain_stream_into_iceberg(table).await?;

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
        let _ = self.control.write_view_dependencies(table, &dep_rows).await;
        let _ = self
            .control
            .set_view_strategy(table, Some(ViewStrategy::StreamOnView))
            .await;
        counter!(
            melt_metrics::SYNC_VIEWS,
            melt_metrics::LABEL_BACKEND => "iceberg",
            "strategy" => "stream_on_view",
        )
        .increment(1);
        tracing::info!(table = %table, "sync.view.stream (iceberg)");
        Ok(())
    }

    /// Shared stream-drain pipeline: read from Snowflake, hand the
    /// batch stream to the Iceberg writer, mark the row active. Used
    /// by both the base-table and stream-on-view paths.
    async fn drain_stream_into_iceberg(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;
        let stream = self
            .snowflake
            .read_stream_since(&token, table, None)
            .await?;
        let pool = self.pool.clone();
        let catalog = self.catalog.clone();
        let table_owned = table.clone();
        let started = Instant::now();
        let rows = tokio::task::spawn_blocking(move || -> Result<u64> {
            let mut guard = futures::executor::block_on(pool.write());
            crate::sync::writer::write_changes(
                catalog.config(),
                &mut guard,
                &table_owned,
                stream,
                started,
            )
        })
        .await
        .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
        let _ = rows;
        self.control
            .record_sync_progress(table, melt_snowflake::SnapshotId(0))
            .await?;
        self.control.mark_active(table).await?;
        Ok(())
    }

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
            .control
            .idle_discovered(self.sync_cfg.lazy.demotion_idle_days)
            .await?;
        if idle.is_empty() {
            return Ok(());
        }
        let token = self.snowflake.service_token().await?;
        for t in &idle {
            let _ = self.snowflake.drop_stream(&token, t).await;
            if let Err(e) = self.control.drop_table(t).await {
                tracing::warn!(error = %e, table = %t, "iceberg sync: control drop failed");
            }
            self.router_cache.invalidate_table(t).await;
        }
        Ok(())
    }

    async fn sync_one(&self, table: &TableRef) -> Result<()> {
        let token = self.snowflake.service_token().await?;

        // Stream stale or missing → rebootstrap. Detected off
        // `read_stream_since` errors via the shared helper.
        let stream = match self.snowflake.read_stream_since(&token, table, None).await {
            Ok(s) => s,
            Err(e) if melt_snowflake::is_stream_unrecoverable(&e) => {
                tracing::warn!(
                    table = %table,
                    error = %e,
                    "iceberg sync: stream stale or missing — rebuilding from scratch"
                );
                counter!(melt_metrics::SYNC_STREAM_STALE).increment(1);
                let _ = self.snowflake.drop_stream(&token, table).await;
                self.control.refresh_table(table).await?;
                self.router_cache.invalidate_table(table).await;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let pool = self.pool.clone();
        let catalog = self.catalog.clone();
        let table_owned = table.clone();
        let started = std::time::Instant::now();
        let rows = tokio::task::spawn_blocking(move || -> Result<u64> {
            let mut guard = futures::executor::block_on(pool.write());
            crate::sync::writer::write_changes(
                catalog.config(),
                &mut guard,
                &table_owned,
                stream,
                started,
            )
        })
        .await
        .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
        if rows > 0 {
            self.router_cache.invalidate_table(table).await;
        }
        Ok(())
    }

    pub async fn run_policy_refresh(self: Arc<Self>, shutdown: Arc<Notify>) -> Result<()> {
        if matches!(self.policy_cfg.mode, PolicyMode::AllowList { .. }) {
            return Ok(());
        }
        loop {
            let token = match self.snowflake.service_token().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "iceberg policy refresh: token fetch failed");
                    if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let protected = match self.snowflake.list_policy_protected_tables(&token).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "iceberg policy refresh: snowflake call failed");
                    counter!(melt_metrics::SYNC_POLICY_REFRESHES, melt_metrics::LABEL_OUTCOME => "err").increment(1);
                    if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            if let Err(e) = self.catalog.write_policy_markers(&protected) {
                tracing::warn!(error = %e, "iceberg policy refresh: write failed");
            }
            let keep: Vec<TableRef> = protected.iter().map(|p| p.table.clone()).collect();
            if let Err(e) = self.catalog.retain_policy_markers(&keep) {
                tracing::warn!(error = %e, "iceberg policy refresh: retain failed");
            }
            for t in &keep {
                self.router_cache.invalidate_table(t).await;
            }
            counter!(melt_metrics::SYNC_POLICY_REFRESHES, melt_metrics::LABEL_OUTCOME => "ok")
                .increment(1);
            if sleep_or_shutdown(self.policy_cfg.refresh_interval, &shutdown).await {
                tracing::info!("iceberg sync: shutdown received — stopping policy refresh");
                return Ok(());
            }
        }
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

pub fn enforce_unsupported() -> MeltError {
    MeltError::config(
        "snowflake.policy.mode = \"enforce\" is not yet implemented; \
         use \"passthrough\" or \"allowlist\".",
    )
}
