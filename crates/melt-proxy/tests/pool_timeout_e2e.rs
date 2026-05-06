//! Routing-correctness test for KI-001 #2 (`reader_checkout_timeout`).
//!
//! When the reader pool is saturated and `pool.timeout_get` fails fast,
//! the resulting `MeltError::backend("reader checkout: …")` surfaces
//! through `StorageBackend::execute` *before the first byte goes out*.
//! The router's existing Lake-failure-to-passthrough fallback in
//! `crates/melt-proxy/src/execution.rs::run` MUST absorb that error and
//! emit a passthrough — otherwise the timeout is observable to the
//! driver as a query error, breaking the routing-correctness invariant
//! ("the user sees what Snowflake would have returned, since
//! passthrough is verbatim").
//!
//! This test simulates the pool-exhaustion scenario by wiring a mock
//! `StorageBackend` whose `execute()` returns the same error string
//! `melt-ducklake::pool::DuckLakePool::read` and
//! `melt-iceberg::pool::IcebergPool::read` produce on
//! `deadpool::managed::PoolError::Timeout(Wait)`. We assert:
//!
//! 1. The backend's `execute()` ran (proves the router went Lake first).
//! 2. The HTTP response is non-2xx because the proxy attempted the
//!    passthrough leg and there is no real Snowflake upstream in this
//!    test harness — the same convention as
//!    `unknown_table_falls_through_to_passthrough_path` in
//!    `statement_e2e.rs`.
//!
//! Direct surfaced lake errors (no fallback) would never reach the
//! passthrough leg and would fail with a different shape, so this
//! pair of assertions is sufficient evidence.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::to_bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use melt_core::config::{ProxyLimits, RouterConfig};
use melt_core::{
    BackendKind, MeltError, PolicyConfig, PolicyMode, QueryContext, RecordBatchStream, Result,
    SessionId, SessionInfo, StorageBackend, TableRef,
};
use melt_proxy::handlers::statement::execute;
use melt_proxy::result_store::ResultStore;
use melt_proxy::session::SessionStore;
use melt_proxy::ProxyState;
use melt_router::Cache;
use melt_snowflake::{SnowflakeClient, SnowflakeConfig};
use tokio::sync::Semaphore;

/// Backend whose `execute()` mimics a saturated reader pool: returns
/// the exact error string `DuckLakePool::read` / `IcebergPool::read`
/// surface when `deadpool::Pool::timeout_get` fires `PoolError::Timeout(Wait)`.
/// `tables_exist` reports the seeded table as present so the router
/// emits `Route::Lake` and we can prove the fallback fired.
struct PoolExhaustedBackend {
    tables: Vec<TableRef>,
    execute_calls: Arc<AtomicUsize>,
}

impl PoolExhaustedBackend {
    fn new(tables: Vec<TableRef>) -> Self {
        Self {
            tables,
            execute_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn execute_count(&self) -> Arc<AtomicUsize> {
        self.execute_calls.clone()
    }
}

#[async_trait]
impl StorageBackend for PoolExhaustedBackend {
    async fn execute(&self, _sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        self.execute_calls.fetch_add(1, Ordering::Relaxed);
        // Mirror the wrapping in `DuckLakePool::read` — the prefix
        // `reader checkout:` is what the proxy logs see, and the
        // `Timeout(Wait)` payload is what `deadpool::PoolError`'s
        // Display emits.
        Err(MeltError::backend(
            "reader checkout: Timeout occurred while waiting for a slot to become available",
        ))
    }

    async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![1024; t.len()])
    }

    async fn tables_exist(&self, t: &[TableRef]) -> Result<Vec<bool>> {
        Ok(t.iter().map(|x| self.tables.contains(x)).collect())
    }

    async fn policy_markers(&self, t: &[TableRef]) -> Result<Vec<Option<String>>> {
        Ok(vec![None; t.len()])
    }

    async fn list_tables(&self) -> Result<Vec<TableRef>> {
        Ok(self.tables.clone())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::DuckLake
    }
}

fn build_state(backend: Arc<dyn StorageBackend>) -> ProxyState {
    let limits = ProxyLimits::default();
    let sessions = Arc::new(SessionStore::new(limits.clone()));
    let results = ResultStore::new(limits.clone());

    let router_cfg = RouterConfig::default();
    let router_cache = Arc::new(Cache::new(&router_cfg));

    let snowflake_cfg = SnowflakeConfig {
        policy: PolicyConfig {
            mode: PolicyMode::Passthrough,
            refresh_interval: Duration::from_secs(60),
        },
        ..SnowflakeConfig::default()
    };
    let snowflake = Arc::new(SnowflakeClient::new(snowflake_cfg.clone()));

    ProxyState {
        backend,
        snowflake,
        snowflake_cfg: Arc::new(snowflake_cfg),
        router_cfg: Arc::new(router_cfg),
        router_cache,
        sync_matcher: Arc::new(arc_swap::ArcSwap::from_pointee(None)),
        discovery: None,
        sessions,
        results,
        request_timeout: limits.request_timeout,
        tls_cert: None,
        parity: None,
        hybrid_cache: None,
        session_mode: melt_core::config::SessionMode::Real,
    }
}

fn seed_session(state: &ProxyState, token: &str) {
    let info = SessionInfo {
        id: SessionId::new(),
        token: token.to_string(),
        role: None,
        warehouse: None,
        database: Some("analytics".into()),
        schema: Some("public".into()),
        expires_at: Instant::now() + Duration::from_secs(3600),
        concurrency: Arc::new(Semaphore::new(16)),
    };
    let _ = state.sessions.register(token.to_string());
    state.sessions.update(token, |existing| {
        *existing = info.clone();
    });
}

fn bearer(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        "authorization",
        HeaderValue::from_str(&format!("Snowflake Token=\"{token}\"")).unwrap(),
    );
    h
}

/// Pool-exhaustion timeout from a Lake-routed query falls through to
/// passthrough rather than surfacing as a query error. Without the
/// router's Lake-failure-to-passthrough fallback this would assert
/// success on a non-existent route.
#[tokio::test]
async fn pool_exhaustion_timeout_falls_back_to_passthrough() {
    let orders = TableRef::new("analytics", "public", "orders");
    let backend = PoolExhaustedBackend::new(vec![orders.clone()]);
    let exec_calls = backend.execute_count();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state = build_state(backend);
    let token = "test-bearer-token";
    seed_session(&state, token);

    let body = Bytes::from(r#"{"statement": "SELECT id FROM analytics.public.orders"}"#);
    let resp = execute(State(state), RawQuery(None), bearer(token), body).await;
    let (parts, body) = resp.into_parts();

    assert_eq!(
        exec_calls.load(Ordering::Relaxed),
        1,
        "router should attempt Lake before falling back; backend.execute() must run exactly once",
    );
    // Passthrough leg has no real upstream in the test harness, so a
    // non-2xx confirms the proxy did try the fallback path. The
    // alternative — surfacing the pool-timeout as a query error —
    // would short-circuit before passthrough and never produce this
    // shape. Same convention as `statement_e2e.rs`.
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    let body_str = std::str::from_utf8(&bytes).unwrap_or("<non-utf8>");
    assert!(
        !parts.status.is_success(),
        "expected non-2xx because passthrough has no upstream; got {} body={}",
        parts.status,
        body_str,
    );
}
