use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool::managed::{self, Manager, Metrics, RecycleResult};
use duckdb::Connection;
use melt_core::{MeltError, Result};
use parking_lot::Mutex as SyncMutex;

use crate::config::{IcebergCatalogKind, IcebergConfig};

/// Builds DuckDB connections pre-configured with the `iceberg` +
/// `httpfs` extensions and the catalog `ATTACH`ed under the alias
/// `ice`. Shared by both the reader pool and the single writer so
/// every handle sees the same lake state.
///
/// When `extra_setup_sql` is set (typically by the proxy when
/// `router.hybrid_execution = true`), it's appended to the per-
/// connection setup. Used by the dual-execution router to install +
/// load the community Snowflake extension and `ATTACH` it as
/// `sf_link` for the Attach strategy.
pub struct IcebergDuckDBManager {
    setup_sql: String,
    extra_setup_sql: Option<String>,
    /// Shared flag the pool exposes via [`IcebergPool::sf_link_available`].
    /// Starts `true` (no extra setup configured ⇒ N/A; we report
    /// `false` from the pool helper instead). Flips to `false` on the
    /// first connection whose `extra_setup_sql` fails — once Attach
    /// can't be loaded on one connection, every other connection in
    /// this pool will fail the same way (same image, same env), so a
    /// single observation is sufficient to mark the pool degraded.
    /// The router reads this at decide-time and counts via
    /// `melt_hybrid_attach_unavailable_total`.
    extra_setup_ok: Arc<AtomicBool>,
    /// `(refresh_sql, interval_secs)` for the periodic schema-cache
    /// refresh. `None` when refresh is disabled or no `extra_setup_sql`
    /// was configured. Both `Some(_)` is required for refresh to fire.
    /// Bounds the staleness window of the DuckDB Snowflake extension's
    /// per-table schema cache when upstream Snowflake schemas evolve.
    refresh_attach: Option<(String, Duration)>,
    /// Last refresh time (Unix nanos). Shared so `recycle()` reads
    /// across all connections in the pool see one another's writes —
    /// only one connection per interval pays the refresh cost; the
    /// rest skip it via the elapsed check.
    last_refresh_ns: Arc<AtomicU64>,
}

impl IcebergDuckDBManager {
    pub fn new(cfg: &IcebergConfig) -> Self {
        Self {
            setup_sql: build_setup_sql(cfg),
            extra_setup_sql: None,
            extra_setup_ok: Arc::new(AtomicBool::new(true)),
            refresh_attach: None,
            last_refresh_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Constructor variant that appends additional setup SQL run on
    /// every new connection — used to install the community
    /// Snowflake extension for hybrid Attach. See module docs.
    pub fn new_with_extra_sql(cfg: &IcebergConfig, extra_setup_sql: Option<String>) -> Self {
        Self::new_with_extra_sql_and_flag(cfg, extra_setup_sql, Arc::new(AtomicBool::new(true)))
    }

    /// Constructor that takes a pre-existing `extra_setup_ok` flag.
    /// Used internally by [`IcebergPool`] so the reader pool's
    /// manager and the singleton writer manager share one atomic ⇒
    /// a setup failure on either path is observable from
    /// [`IcebergPool::sf_link_available`].
    pub fn new_with_extra_sql_and_flag(
        cfg: &IcebergConfig,
        extra_setup_sql: Option<String>,
        extra_setup_ok: Arc<AtomicBool>,
    ) -> Self {
        Self {
            setup_sql: build_setup_sql(cfg),
            extra_setup_sql,
            extra_setup_ok,
            refresh_attach: None,
            last_refresh_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Configure periodic schema-cache refresh. `refresh_sql` is the
    /// `DETACH IF EXISTS sf_link; ATTACH ...` string from
    /// `melt_snowflake::sf_link_refresh_sql`. `interval == 0`
    /// disables refresh entirely (useful for tests). Pass the same
    /// `last_refresh_ns` Arc as the writer manager so per-connection
    /// recycle observations agree.
    pub fn with_refresh(
        mut self,
        refresh_sql: String,
        interval: Duration,
        shared: Arc<AtomicU64>,
    ) -> Self {
        self.last_refresh_ns = shared;
        if !interval.is_zero() {
            self.refresh_attach = Some((refresh_sql, interval));
        }
        self
    }

    /// Shared flag the pool surfaces upward; see field docs above.
    pub fn extra_setup_flag(&self) -> Arc<AtomicBool> {
        self.extra_setup_ok.clone()
    }

    /// Try to refresh `sf_link` on the given connection if the
    /// configured interval has elapsed since the last refresh. Cheap
    /// no-op when refresh isn't configured or hasn't elapsed; runs
    /// `DETACH IF EXISTS sf_link; ATTACH ...` only on whichever
    /// connection happens to be checked out at the boundary.
    fn maybe_refresh(&self, conn: &Connection) {
        let Some((sql, interval)) = self.refresh_attach.as_ref() else {
            return;
        };
        let now = Instant::now();
        let now_ns = unix_now_ns();
        let last = self.last_refresh_ns.load(Ordering::Relaxed);
        // First-ever check: stamp `now` and skip — connections come
        // in already-bootstrapped, no need to refresh on first use.
        if last == 0 {
            self.last_refresh_ns.store(now_ns, Ordering::Relaxed);
            return;
        }
        let elapsed_ns = now_ns.saturating_sub(last);
        if elapsed_ns < interval.as_nanos() as u64 {
            return;
        }
        // Race: two connections check elapsed at the same time.
        // CAS-claim the refresh slot — loser skips, winner runs.
        if self
            .last_refresh_ns
            .compare_exchange(last, now_ns, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if let Err(e) = conn.execute_batch(sql) {
            tracing::warn!(
                error = %e,
                "hybrid: periodic sf_link refresh failed; schema cache may be stale until next attempt"
            );
        } else {
            tracing::debug!(
                elapsed_ms = (now.elapsed().as_millis()) as u64,
                "hybrid: sf_link refreshed"
            );
        }
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory().map_err(|e| MeltError::backend(e.to_string()))?;
        if let Err(e) = conn.execute_batch(&self.setup_sql) {
            tracing::warn!(error = %e, "Iceberg setup failed — backend errors until catalog is reachable");
        }
        // Hybrid attach is best-effort: if the community Snowflake
        // extension isn't installed or the credentials don't work,
        // log the warning but keep the connection alive. Hybrid
        // queries that need sf_link will fail at execute time and
        // fall back to passthrough via the first-batch-error path.
        // The strategy selector forces Materialize when
        // `cfg.hybrid_attach_enabled = false`; this is the runtime
        // companion check.
        if let Some(extra) = &self.extra_setup_sql {
            if let Err(e) = conn.execute_batch(extra) {
                tracing::warn!(
                    error = %e,
                    "hybrid Attach setup failed — sf_link won't be available; \
                     hybrid queries will fall back to passthrough or Materialize"
                );
                // Surface to the router so its decide-time path can
                // emit the unavailable counter and degrade Attach
                // nodes to Materialize. Single failed observation is
                // enough — see field docs.
                self.extra_setup_ok.store(false, Ordering::Relaxed);
            }
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
        let extra_setup_sql = self.extra_setup_sql.clone();
        let extra_setup_ok = self.extra_setup_ok.clone();
        let refresh_attach = self.refresh_attach.clone();
        let last_refresh_ns = self.last_refresh_ns.clone();
        async move {
            let manager = IcebergDuckDBManager {
                setup_sql,
                extra_setup_sql,
                extra_setup_ok,
                refresh_attach,
                last_refresh_ns,
            };
            let conn = tokio::task::spawn_blocking(move || manager.open())
                .await
                .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
            Ok(SyncMutex::new(conn))
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn recycle(
        &self,
        conn: &mut SyncMutex<Connection>,
        _: &Metrics,
    ) -> impl std::future::Future<Output = RecycleResult<MeltError>> + Send {
        // `recycle` runs on every checkout AFTER the first. We use
        // it as the trigger for periodic `sf_link` refresh — cheap
        // when nothing's due, runs DETACH/ATTACH on exactly one
        // connection per interval thanks to the CAS guard.
        let conn_locked = conn.lock();
        self.maybe_refresh(&conn_locked);
        drop(conn_locked);
        async { Ok(()) }
    }
}

fn unix_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
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
    /// Mirror of the manager's `extra_setup_ok` flag. `None` when no
    /// extra setup was configured (hybrid Attach not requested);
    /// `Some(false)` once any connection's extra setup failed.
    extra_setup_ok: Option<Arc<AtomicBool>>,
}

impl IcebergPool {
    pub async fn new(cfg: &IcebergConfig) -> Result<Self> {
        Self::new_with_extra_sql(cfg, None).await
    }

    /// Constructor variant that runs `extra_setup_sql` on every new
    /// connection (after the standard iceberg/httpfs setup). Used by
    /// the proxy to install the community Snowflake extension and
    /// `ATTACH` it as `sf_link` for the dual-execution router's
    /// Attach strategy.
    pub async fn new_with_extra_sql(
        cfg: &IcebergConfig,
        extra_setup_sql: Option<String>,
    ) -> Result<Self> {
        Self::new_with_extra_sql_and_refresh(cfg, extra_setup_sql, None).await
    }

    /// Like [`Self::new_with_extra_sql`] but additionally configures
    /// periodic `sf_link` refresh per
    /// `router.hybrid_attach_refresh_interval`. `refresh` is
    /// `Some((refresh_sql, interval))` — both must be present for
    /// refresh to fire. `interval == Duration::ZERO` disables.
    pub async fn new_with_extra_sql_and_refresh(
        cfg: &IcebergConfig,
        extra_setup_sql: Option<String>,
        refresh: Option<(String, Duration)>,
    ) -> Result<Self> {
        let shared_flag = Arc::new(AtomicBool::new(true));
        let extra_setup_ok = extra_setup_sql.is_some().then(|| shared_flag.clone());
        let shared_last_refresh = Arc::new(AtomicU64::new(0));

        let mut manager = IcebergDuckDBManager::new_with_extra_sql_and_flag(
            cfg,
            extra_setup_sql.clone(),
            shared_flag.clone(),
        );
        if let Some((sql, interval)) = refresh.clone() {
            manager = manager.with_refresh(sql, interval, shared_last_refresh.clone());
        }
        let readers = ReaderPool::builder(manager)
            .max_size(cfg.reader_pool_size.max(1))
            .build()
            .map_err(|e| MeltError::backend(format!("reader pool: {e}")))?;

        let mut writer_manager =
            IcebergDuckDBManager::new_with_extra_sql_and_flag(cfg, extra_setup_sql, shared_flag);
        if let Some((sql, interval)) = refresh {
            writer_manager = writer_manager.with_refresh(sql, interval, shared_last_refresh);
        }
        let writer = tokio::task::spawn_blocking(move || writer_manager.open())
            .await
            .map_err(|e| MeltError::backend(format!("spawn_blocking: {e}")))??;
        Ok(Self {
            readers,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            extra_setup_ok,
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

    /// Whether hybrid Attach (`sf_link.<...>` references) is usable
    /// against this pool. Reads the manager's `extra_setup_ok` flag.
    /// Returns `false` when no extra setup was configured (the proxy
    /// passes `Some(extra_setup_sql)` only when
    /// `router.hybrid_execution = true`); returns `true` only when
    /// extra setup was attempted and hasn't been observed to fail.
    pub fn sf_link_available(&self) -> bool {
        self.extra_setup_ok
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
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
