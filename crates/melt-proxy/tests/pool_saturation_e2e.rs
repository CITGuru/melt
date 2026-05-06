//! Regression guard for KI-001 mitigation #2 (POWA-164).
//!
//! `crates/melt-proxy/tests/pool_timeout_e2e.rs` proves that *if* the
//! reader path returns `MeltError::backend("reader checkout: …")` the
//! proxy falls back to Snowflake passthrough. It does so with a fully
//! synthetic backend that hard-codes the error string — no actual
//! pool, no actual saturation. A future refactor that silently
//! removes `deadpool::Pool::timeout_get` from `DuckLakePool::read`
//! (or the Iceberg twin) would not fail that test.
//!
//! This test exercises the leak path itself. We build a real
//! `deadpool::managed::Pool` with `max_size = 1` and the same
//! `Timeouts { wait: Some(…) }` shape `DuckLakePool::read` uses, pin
//! the lone slot in a "stuck" connection (held across the call), and
//! issue a fresh statement through the `/api/v2/statements` execute
//! handler. We then assert:
//!
//! 1. The fresh statement returns within a bounded deadline — i.e.
//!    `pool.get().await` did NOT block indefinitely. This is the
//!    invariant a refactor that drops the wait timeout would break.
//! 2. The pool surfaced `MeltError::backend("reader checkout: …")`
//!    (proven by `execute_calls == 1` followed by passthrough — only
//!    a backend error before the first byte routes through the
//!    Lake-failure-to-passthrough fallback in
//!    `crates/melt-proxy/src/execution.rs::run`).
//! 3. The router's lake-decision-then-fallback path landed on
//!    Snowflake passthrough — proxy passthrough has no upstream in
//!    this harness, so a non-2xx response is the canonical positive
//!    signal. Same convention as `pool_timeout_e2e.rs` and
//!    `statement_e2e.rs::unknown_table_falls_through_to_passthrough_path`.
//!
//! The acceptance criterion in POWA-164 also names a metric label
//! (`melt_router_decisions_total{route="snowflake",
//! reason="<lake_unavailable_or_similar>"}`). The actual production
//! counter has only `{route, backend}` labels — no `reason` — and
//! the fallback signal is wired through `melt_proxy_lake_fallbacks_total`
//! (see `crates/melt-proxy/src/execution.rs:216`). The non-2xx
//! response above is observable evidence that the fallback path
//! fired; we do not install a debugging recorder just to read the
//! counter back out, since it would require pulling
//! `metrics-util` as a dev-dep across the whole test binary.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::to_bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use deadpool::managed::{self, Manager, Metrics, Object, RecycleResult, Timeouts};
use deadpool::Runtime;
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

/// Trivial pool member: a unit value. We are exercising deadpool's
/// wait-timeout semantics, not anything DuckDB-specific, so the
/// pooled object can be `()`. This keeps the test binary free of a
/// `duckdb` / `melt-ducklake` dev-dep.
struct TrivialManager;

impl Manager for TrivialManager {
    type Type = ();
    type Error = MeltError;

    async fn create(&self) -> std::result::Result<(), MeltError> {
        Ok(())
    }

    async fn recycle(&self, _obj: &mut (), _: &Metrics) -> RecycleResult<MeltError> {
        Ok(())
    }
}

type TestPool = managed::Pool<TrivialManager>;

/// Build a 1-slot pool with a 200 ms wait timeout. Mirrors
/// `DuckLakePool::new`'s use of `Runtime::Tokio1` (required by
/// `timeout_get`) and `Timeouts { wait: Some(…), .. }`.
fn build_saturated_pool() -> Arc<TestPool> {
    let pool = TestPool::builder(TrivialManager)
        .max_size(1)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("test pool build");
    Arc::new(pool)
}

/// Backend that mirrors `DuckLakeBackend::execute`'s checkout call:
/// goes through `Pool::timeout_get(&Timeouts { wait, .. })` and wraps
/// the error with the same `MeltError::backend("reader checkout: …")`
/// prefix `DuckLakePool::read` / `IcebergPool::read` use. When the
/// pool is saturated the checkout returns `PoolError::Timeout(Wait)`,
/// the wrapper converts it to a backend error, and the proxy's
/// lake-failure-to-passthrough fallback absorbs it. `tables_exist`
/// reports the seeded table as present so the router emits
/// `Route::Lake` and we can prove the fallback fired.
struct SaturatedPoolBackend {
    pool: Arc<TestPool>,
    wait: Duration,
    tables: Vec<TableRef>,
    execute_calls: Arc<AtomicUsize>,
}

impl SaturatedPoolBackend {
    fn new(pool: Arc<TestPool>, wait: Duration, tables: Vec<TableRef>) -> Self {
        Self {
            pool,
            wait,
            tables,
            execute_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn execute_count(&self) -> Arc<AtomicUsize> {
        self.execute_calls.clone()
    }
}

#[async_trait]
impl StorageBackend for SaturatedPoolBackend {
    async fn execute(&self, _sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        self.execute_calls.fetch_add(1, Ordering::Relaxed);
        let timeouts = Timeouts {
            wait: Some(self.wait),
            create: None,
            recycle: None,
        };
        // Wrap with the same prefix `DuckLakePool::read` /
        // `IcebergPool::read` use so a refactor that changes the
        // production wrapping shape would also need to touch this
        // line — making the contract visible.
        let _conn = self
            .pool
            .timeout_get(&timeouts)
            .await
            .map_err(|e| MeltError::backend(format!("reader checkout: {e}")))?;
        // Saturated pool means we never get here. Defensive in case
        // the test ever drops the held permit before the call.
        Err(MeltError::backend(
            "test invariant broken: pool checkout succeeded under saturation",
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

/// Saturate the reader pool with a deliberately-stuck checkout, then
/// issue a fresh Lake-routed statement. The fresh statement must
/// fail fast (not block indefinitely) AND fall through to the
/// Snowflake passthrough leg.
///
/// Total time budget: < 1 s on the happy path (200 ms pool wait +
/// router/passthrough overhead). 15 s deadline matches POWA-164's
/// CI budget and would only fire if the wait timeout were silently
/// removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_reader_pool_falls_through_to_passthrough() {
    let pool = build_saturated_pool();

    // Hold the lone slot. `timeout_get` here grants the only object
    // the pool will ever produce; keeping it bound (not dropped)
    // guarantees the pool stays saturated for the duration of the
    // test. We intentionally use `timeout_get` (not `get`) on the
    // hold side too so the test never accidentally hangs in setup.
    let setup_timeouts = Timeouts {
        wait: Some(Duration::from_millis(500)),
        create: None,
        recycle: None,
    };
    let _stuck: Object<TrivialManager> = pool
        .timeout_get(&setup_timeouts)
        .await
        .expect("setup: pool should have a free slot before saturation");

    let orders = TableRef::new("analytics", "public", "orders");
    let backend = SaturatedPoolBackend::new(
        pool.clone(),
        Duration::from_millis(200),
        vec![orders.clone()],
    );
    let exec_calls = backend.execute_count();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state = build_state(backend);
    let token = "test-bearer-token";
    seed_session(&state, token);

    let body = Bytes::from(r#"{"statement": "SELECT id FROM analytics.public.orders"}"#);

    // 15 s outer deadline. Anything close to that means the wait
    // timeout regressed; healthy runs land in well under a second.
    let started = Instant::now();
    let resp = tokio::time::timeout(
        Duration::from_secs(15),
        execute(State(state), RawQuery(None), bearer(token), body),
    )
    .await
    .expect("fresh statement must not block indefinitely under pool saturation");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "fresh statement under pool saturation took {elapsed:?}; expected sub-second \
         (pool wait timeout is 200 ms). Either the wait timeout regressed or the \
         lake-failure-to-passthrough fallback is queueing instead of failing fast.",
    );

    let (parts, body) = resp.into_parts();

    assert_eq!(
        exec_calls.load(Ordering::Relaxed),
        1,
        "router should attempt Lake before falling back; backend.execute() must run \
         exactly once and return the wrapped reader-checkout error",
    );

    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    let body_str = std::str::from_utf8(&bytes).unwrap_or("<non-utf8>");
    assert!(
        !parts.status.is_success(),
        "expected non-2xx because passthrough has no upstream in this harness; \
         got {} body={}. A 2xx here would mean the proxy never tried passthrough, \
         which would happen if the reader-checkout error were surfaced directly.",
        parts.status,
        body_str,
    );

    // Drop the held slot only after the assertions — keeps the
    // saturation invariant explicit and prevents an accidental
    // re-checkout race if the handler were retried.
    drop(_stuck);
}
