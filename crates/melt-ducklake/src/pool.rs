use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool::managed::{self, Manager, Metrics, RecycleResult, Timeouts};
use deadpool::Runtime;
use duckdb::Connection;
use melt_core::{MeltError, Result};
use parking_lot::Mutex as SyncMutex;

use crate::config::DuckLakeConfig;

/// `DuckDBManager` opens an in-memory DuckDB connection and runs the
/// `INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:postgres:...'; USE lake;`
/// boilerplate so any handle the pool hands out — read or write —
/// already sees the lake.
///
/// `extra_setup_sql` is appended to the per-connection setup. The
/// proxy uses it to install + load the community Snowflake extension
/// and `ATTACH` it as `sf_link` for the dual-execution router's
/// Attach strategy.
pub struct DuckDBManager {
    setup_sql: String,
    extra_setup_sql: Option<String>,
    /// Shared "is the hybrid Attach `extra_setup_sql` working?" flag.
    /// See the parallel comment on `IcebergDuckDBManager::extra_setup_ok`.
    extra_setup_ok: Arc<AtomicBool>,
    /// `(refresh_sql, interval)` for periodic schema-cache refresh.
    /// Mirror of the same field in `IcebergDuckDBManager`.
    refresh_attach: Option<(String, Duration)>,
    last_refresh_ns: Arc<AtomicU64>,
}

impl DuckDBManager {
    pub fn new(cfg: &DuckLakeConfig) -> Self {
        Self::new_with_extra_sql(cfg, None)
    }

    pub fn new_with_extra_sql(cfg: &DuckLakeConfig, extra_setup_sql: Option<String>) -> Self {
        Self::new_with_extra_sql_and_flag(cfg, extra_setup_sql, Arc::new(AtomicBool::new(true)))
    }

    /// Constructor that takes a pre-existing `extra_setup_ok` flag.
    /// Used by [`DuckLakePool`] so reader and writer share one
    /// observation surface.
    pub fn new_with_extra_sql_and_flag(
        cfg: &DuckLakeConfig,
        extra_setup_sql: Option<String>,
        extra_setup_ok: Arc<AtomicBool>,
    ) -> Self {
        let setup_sql = build_setup_sql(cfg);
        Self {
            setup_sql,
            extra_setup_sql,
            extra_setup_ok,
            refresh_attach: None,
            last_refresh_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    /// See `IcebergDuckDBManager::with_refresh`.
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

    pub fn extra_setup_flag(&self) -> Arc<AtomicBool> {
        self.extra_setup_ok.clone()
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory().map_err(|e| MeltError::backend(e.to_string()))?;
        if let Err(e) = conn.execute_batch(&self.setup_sql) {
            tracing::warn!(error = %e, "DuckLake ATTACH failed — backend will return errors until the lake is reachable");
        }
        if let Some(extra) = &self.extra_setup_sql {
            if let Err(e) = conn.execute_batch(extra) {
                tracing::warn!(
                    error = %e,
                    "hybrid Attach setup failed — sf_link won't be available; \
                     hybrid queries will fall back to passthrough or Materialize"
                );
                self.extra_setup_ok.store(false, Ordering::Relaxed);
            }
        }
        Ok(conn)
    }

    /// See `IcebergDuckDBManager::maybe_refresh`.
    fn maybe_refresh(&self, conn: &Connection) {
        let Some((sql, interval)) = self.refresh_attach.as_ref() else {
            return;
        };
        let now = Instant::now();
        let now_ns = unix_now_ns();
        let last = self.last_refresh_ns.load(Ordering::Relaxed);
        if last == 0 {
            self.last_refresh_ns.store(now_ns, Ordering::Relaxed);
            return;
        }
        let elapsed_ns = now_ns.saturating_sub(last);
        if elapsed_ns < interval.as_nanos() as u64 {
            return;
        }
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
}

fn unix_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl Manager for DuckDBManager {
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
            let manager = DuckDBManager {
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
        let conn_locked = conn.lock();
        self.maybe_refresh(&conn_locked);
        drop(conn_locked);
        async { Ok(()) }
    }
}

pub type ReaderPool = managed::Pool<DuckDBManager>;

/// Many-readers-one-writer pool. The single-writer constraint is
/// enforced structurally: `write()` returns a `MutexGuard` so two
/// writers cannot exist concurrently — the type system says no.
pub struct DuckLakePool {
    readers: ReaderPool,
    writer: Arc<tokio::sync::Mutex<Connection>>,
    /// Mirror of the manager's `extra_setup_ok` flag. `None` when
    /// no extra setup was configured (hybrid Attach not requested);
    /// `Some(false)` once any connection's extra setup failed.
    extra_setup_ok: Option<Arc<AtomicBool>>,
    /// Wait-for-checkout timeout applied to every `read()`. Set from
    /// `[backend.ducklake].reader_checkout_timeout`. The router's
    /// Lake-failure-to-passthrough fallback (in `melt-proxy`) absorbs
    /// the resulting `PoolError::Timeout` pre-first-byte, so a
    /// saturated reader pool sheds load to Snowflake instead of
    /// queueing indefinitely. KI-001 mitigation #2.
    reader_checkout_timeout: Duration,
}

impl DuckLakePool {
    pub async fn new(cfg: DuckLakeConfig) -> Result<Self> {
        Self::new_with_extra_sql(cfg, None).await
    }

    /// Constructor variant that runs `extra_setup_sql` on every new
    /// connection (after the standard ducklake setup). Used by the
    /// proxy to install the community Snowflake extension for hybrid
    /// Attach.
    pub async fn new_with_extra_sql(
        cfg: DuckLakeConfig,
        extra_setup_sql: Option<String>,
    ) -> Result<Self> {
        Self::new_with_extra_sql_and_refresh(cfg, extra_setup_sql, None).await
    }

    /// See [`super::pool::IcebergPool::new_with_extra_sql_and_refresh`].
    pub async fn new_with_extra_sql_and_refresh(
        cfg: DuckLakeConfig,
        extra_setup_sql: Option<String>,
        refresh: Option<(String, Duration)>,
    ) -> Result<Self> {
        let shared_flag = Arc::new(AtomicBool::new(true));
        let extra_setup_ok = extra_setup_sql.is_some().then(|| shared_flag.clone());
        let shared_last_refresh = Arc::new(AtomicU64::new(0));
        let reader_checkout_timeout = cfg.reader_checkout_timeout;

        let mut manager = DuckDBManager::new_with_extra_sql_and_flag(
            &cfg,
            extra_setup_sql.clone(),
            shared_flag.clone(),
        );
        if let Some((sql, interval)) = refresh.clone() {
            manager = manager.with_refresh(sql, interval, shared_last_refresh.clone());
        }
        // `runtime(Tokio1)` is required because `timeout_get` uses
        // `tokio::time::timeout` internally; deadpool refuses to apply
        // a wait timeout otherwise.
        let readers = ReaderPool::builder(manager)
            .max_size(cfg.reader_pool_size.max(1))
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| MeltError::backend(format!("reader pool: {e}")))?;

        let mut writer_manager =
            DuckDBManager::new_with_extra_sql_and_flag(&cfg, extra_setup_sql, shared_flag);
        if let Some((sql, interval)) = refresh {
            writer_manager = writer_manager.with_refresh(sql, interval, shared_last_refresh);
        }
        let writer = tokio::task::block_in_place(|| writer_manager.open())?;
        Ok(Self {
            readers,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            extra_setup_ok,
            reader_checkout_timeout,
        })
    }

    /// Check out a reader connection with a bounded wait. Backed by
    /// `deadpool::Pool::timeout_get` so callers fail fast under pool
    /// saturation instead of queueing indefinitely on
    /// `Pool::get().await`. The router's Lake-failure-to-passthrough
    /// fallback in `melt-proxy::execution::run` absorbs the timeout
    /// pre-first-byte. KI-001 mitigation #2.
    pub async fn read(&self) -> Result<deadpool::managed::Object<DuckDBManager>> {
        let timeouts = Timeouts {
            wait: Some(self.reader_checkout_timeout),
            create: None,
            recycle: None,
        };
        self.readers
            .timeout_get(&timeouts)
            .await
            .map_err(|e| MeltError::backend(format!("reader checkout: {e}")))
    }

    pub async fn write(&self) -> tokio::sync::MutexGuard<'_, Connection> {
        self.writer.lock().await
    }

    /// See [`super::pool::IcebergPool::sf_link_available`] — same
    /// semantics, mirrored here so both backend pools expose a
    /// uniform readiness signal.
    pub fn sf_link_available(&self) -> bool {
        self.extra_setup_ok
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
    }
}

/// Build the setup SQL DuckDB runs on every pool connection.
///
/// The S3 secret comes first so the subsequent `ATTACH` can resolve
/// `s3://` paths. The secret is named `melt_s3`; DuckDB matches it
/// by longest-prefix `SCOPE` when multiple secrets exist.
///
/// Rendering failures (bad S3 config, missing env vars) log and
/// emit a fallback that omits the secret — `ATTACH` will then fail
/// against real S3 endpoints but DuckDB boots cleanly, which is
/// what we want for `cargo check` and the `melt route` offline path.
fn build_setup_sql(cfg: &DuckLakeConfig) -> String {
    let mut s = String::from(
        "INSTALL ducklake;\n\
         LOAD ducklake;\n\
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
                    "DuckLake S3 secret setup failed — s3:// paths will error \
                     until config is fixed"
                );
            }
        }
    }

    s.push_str(&format!(
        "ATTACH 'ducklake:postgres:{catalog}' AS lake (DATA_PATH '{data}');\n\
         USE lake;",
        catalog = cfg.catalog_url.replace('\'', "''"),
        data = cfg.data_path.replace('\'', "''"),
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_sql_emits_secret_and_attach() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "s3://melt/ducklake/".into(),
            s3: melt_core::S3Config {
                region: "us-east-1".into(),
                endpoint: "localhost:9000".into(),
                url_style: "path".into(),
                use_ssl: false,
                access_key_id: "minioadmin".into(),
                secret_access_key: "minioadmin".into(),
                ..Default::default()
            },
            reader_pool_size: 4,
            writer_pool_size: 1,
            reader_checkout_timeout: Duration::from_secs(5),
        };
        let sql = build_setup_sql(&cfg);
        assert!(sql.contains("INSTALL ducklake"));
        assert!(sql.contains("INSTALL httpfs"));
        assert!(sql.contains("CREATE OR REPLACE SECRET melt_s3"));
        assert!(sql.contains("ENDPOINT  'localhost:9000'"));
        assert!(sql.contains("URL_STYLE 'path'"));
        assert!(sql.contains("KEY_ID    'minioadmin'"));
        assert!(sql.contains("ATTACH 'ducklake:postgres:postgres://melt@db/c'"));
        assert!(sql.contains("DATA_PATH 's3://melt/ducklake/'"));
    }

    #[test]
    fn region_only_config_uses_credential_chain() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "s3://melt/ducklake/".into(),
            s3: melt_core::S3Config {
                region: "eu-west-1".into(),
                ..Default::default()
            },
            reader_pool_size: 4,
            writer_pool_size: 1,
            reader_checkout_timeout: Duration::from_secs(5),
        };
        let sql = build_setup_sql(&cfg);
        assert!(sql.contains("REGION    'eu-west-1'"));
        assert!(sql.contains("PROVIDER credential_chain"));
    }

    #[test]
    fn empty_region_skips_secret_but_still_attaches() {
        let cfg = DuckLakeConfig {
            catalog_url: "postgres://melt@db/c".into(),
            data_path: "/tmp/ducklake/".into(),
            s3: melt_core::S3Config::default(),
            reader_pool_size: 4,
            writer_pool_size: 1,
            reader_checkout_timeout: Duration::from_secs(5),
        };
        let sql = build_setup_sql(&cfg);
        assert!(!sql.contains("CREATE OR REPLACE SECRET"));
        assert!(sql.contains("ATTACH"));
    }
}
