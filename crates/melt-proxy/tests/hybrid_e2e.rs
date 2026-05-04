//! End-to-end tests for the dual-execution router (hybrid path).
//!
//! Exercises the full proxy pipeline — `POST /api/v2/statements` →
//! router → `execute_hybrid` → response builder — with a mock
//! `StorageBackend` and a real `SyncTableMatcher` configured with
//! `[sync].remote` patterns. Asserts:
//!
//! - Hybrid plans actually reach `execute_hybrid` (no silent
//!   fallthrough).
//! - The mock backend sees `CREATE TEMP TABLE __remote_N AS ...`
//!   statements for Materialize fragments and the rewritten
//!   `local_sql` afterwards.
//! - The runtime "Attach unavailable" gate degrades cleanly to
//!   Materialize and bumps the metric.
//! - Comment hints (`/*+ melt_route(snowflake) */`) bypass the
//!   router decision tree.
//! - The hybrid result cache (when enabled) returns cached batches
//!   on identical re-issued queries without touching the backend.
//!
//! NOTE: We do NOT exercise the actual community Snowflake extension
//! here (that requires the runtime ADBC driver). The Materialize
//! fragments execute against the mock backend, which returns canned
//! Arrow batches as if they came from Snowflake. Production parity
//! comes from the Python regression variants
//! (`examples/python/variants_hybrid/`) running against a live proxy.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::body::to_bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use melt_core::config::{ProxyLimits, RouterConfig};
use melt_core::{
    BackendKind, MeltError, PolicyConfig, PolicyMode, QueryContext, RecordBatchStream, Result,
    SessionId, SessionInfo, StorageBackend, SyncConfig, SyncTableMatcher, TableRef,
};
use melt_proxy::handlers::statement::execute;
use melt_proxy::handlers::v1::query_request;
use melt_proxy::result_store::ResultStore;
use melt_proxy::session::SessionStore;
use melt_proxy::{FragmentCache, ProxyState};
use melt_router::Cache;
use melt_snowflake::{SnowflakeClient, SnowflakeConfig};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

/// Mock backend that records every SQL execute() it sees and returns
/// canned Arrow batches for `CREATE TEMP TABLE` and `SELECT` patterns.
struct MockHybridBackend {
    /// Local lake tables — what `tables_exist` sees as locally available.
    tables: Vec<TableRef>,
    /// Per-table size estimate (uniform; real backends vary).
    bytes_per_table: u64,
    /// Captured SQL strings the backend was asked to execute, in order.
    /// Lets tests assert the staging step ran and the final local_sql
    /// referenced the staged temp table.
    seen: Arc<Mutex<Vec<String>>>,
    /// Whether `hybrid_attach_available` returns true. Backends with a
    /// successful pool startup return true; failed ATTACH at boot
    /// returns false. Atomic so tests can flip mid-run.
    attach_available: Arc<AtomicBool>,
    /// Counts execute() calls for assertions about cache hit vs miss.
    execute_calls: Arc<AtomicUsize>,
}

impl MockHybridBackend {
    fn new(tables: Vec<TableRef>) -> Self {
        Self {
            tables,
            bytes_per_table: 1024,
            seen: Arc::new(Mutex::new(Vec::new())),
            attach_available: Arc::new(AtomicBool::new(true)),
            execute_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_attach_unavailable(mut self) -> Self {
        self.attach_available = Arc::new(AtomicBool::new(false));
        self
    }

    fn with_oversize_estimate(mut self, bytes: u64) -> Self {
        self.bytes_per_table = bytes;
        self
    }

    fn seen_clone(&self) -> Arc<Mutex<Vec<String>>> {
        self.seen.clone()
    }

    fn execute_count_handle(&self) -> Arc<AtomicUsize> {
        self.execute_calls.clone()
    }
}

#[async_trait]
impl StorageBackend for MockHybridBackend {
    async fn execute(&self, sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        self.seen.lock().push(sql.to_string());
        self.execute_calls.fetch_add(1, Ordering::Relaxed);

        // Empty schema for DDL-shaped statements (CREATE TEMP TABLE AS,
        // EXPLAIN ANALYZE) — the proxy drains them but doesn't render
        // the rows.
        if sql.starts_with("CREATE TEMP TABLE") {
            let schema = Arc::new(Schema::new(vec![Field::new("ok", DataType::Int32, false)]));
            let arr = Int32Array::from(vec![1]);
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])
                .map_err(|e| MeltError::backend(format!("test fixture build: {e}")))?;
            return Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])));
        }

        // For everything else (the local_sql), return a 2-column,
        // 3-row canned response — same shape as `statement_e2e.rs`'s
        // mock — so the response builder has something to render.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(vec![10, 20, 30]);
        let names = StringArray::from(vec!["a", "b", "c"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)])
            .map_err(|e| MeltError::backend(format!("test fixture build: {e}")))?;
        Ok(Box::pin(futures::stream::iter(vec![Ok(batch)])))
    }

    async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![self.bytes_per_table; t.len()])
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

    fn hybrid_attach_available(&self) -> bool {
        self.attach_available.load(Ordering::Relaxed)
    }
}

fn matcher_with_remote(remote: &[&str]) -> Arc<SyncTableMatcher> {
    Arc::new(
        SyncTableMatcher::from_config(&SyncConfig {
            auto_discover: false,
            include: Vec::new(),
            exclude: Vec::new(),
            remote: remote.iter().map(|s| s.to_string()).collect(),
            ..SyncConfig::default()
        })
        .expect("valid patterns"),
    )
}

fn build_state_with_hybrid(
    backend: Arc<dyn StorageBackend>,
    matcher: Arc<SyncTableMatcher>,
    cfg_tweak: impl FnOnce(&mut RouterConfig),
) -> ProxyState {
    let limits = ProxyLimits::default();
    let sessions = Arc::new(SessionStore::new(limits.clone()));
    let results = ResultStore::new(limits.clone());

    let mut router_cfg = RouterConfig::default();
    router_cfg.hybrid_execution = true;
    cfg_tweak(&mut router_cfg);
    let router_cache = Arc::new(Cache::new(&router_cfg));

    let snowflake_cfg = SnowflakeConfig {
        policy: PolicyConfig {
            mode: PolicyMode::Passthrough,
            refresh_interval: Duration::from_secs(60),
        },
        ..SnowflakeConfig::default()
    };
    let snowflake = Arc::new(SnowflakeClient::new(snowflake_cfg.clone()));

    // Mirror the prod boot path: build the cache iff TTL > 0.
    let hybrid_cache = (!router_cfg.hybrid_fragment_cache_ttl.is_zero()).then(|| {
        Arc::new(FragmentCache::new(
            router_cfg.hybrid_fragment_cache_ttl,
            router_cfg.hybrid_fragment_cache_max_entries,
        ))
    });

    ProxyState {
        backend,
        snowflake,
        snowflake_cfg: Arc::new(snowflake_cfg),
        router_cfg: Arc::new(router_cfg),
        router_cache,
        sync_matcher: Arc::new(arc_swap::ArcSwap::from_pointee(Some(matcher))),
        discovery: None,
        sessions,
        results,
        request_timeout: limits.request_timeout,
        tls_cert: None,
        parity: None,
        hybrid_cache,
    }
}

fn seed_session(state: &ProxyState, token: &str) {
    let info = SessionInfo {
        id: SessionId::new(),
        token: token.to_string(),
        role: None,
        warehouse: None,
        database: Some("LOCAL".into()),
        schema: Some("PUB".into()),
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

async fn response_json(resp: axum::response::Response) -> serde_json::Value {
    let (parts, body) = resp.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    let body_str = std::str::from_utf8(&bytes)
        .unwrap_or("<non-utf8>")
        .to_string();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "non-JSON response body (status={}): {e}; body={body_str}",
            parts.status,
        )
    });
    if !parts.status.is_success() {
        panic!("non-2xx status {}: {body_str}", parts.status);
    }
    json
}

// ── tests ──────────────────────────────────────────────────────────

/// Single Remote table query → routes hybrid → execute_hybrid runs
/// the local_sql against the mock backend and returns its canned
/// rows. v1 builder emits an Attach rewrite (no Materialize
/// fragments) for single-scan queries, so the mock should see one
/// execute call (the local_sql) and zero `CREATE TEMP TABLE`.
#[tokio::test]
async fn hybrid_single_remote_table_runs_local_sql_only() {
    let backend = MockHybridBackend::new(vec![]);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(r#"{"statement": "SELECT id, name FROM REMOTE.PUB.USERS"}"#);
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let json = response_json(resp).await;

    assert_eq!(json["code"], "090001");
    let data = &json["data"];
    assert_eq!(
        data.as_array().unwrap().len(),
        3,
        "should return mock backend's canned 3-row response"
    );

    let executed = seen.lock();
    assert_eq!(
        executed.len(),
        1,
        "single Attach scan should execute local_sql once: {executed:?}"
    );
    assert!(
        executed[0].contains("sf_link"),
        "local_sql should reference sf_link: {}",
        executed[0]
    );
}

/// Multi-remote-table query → Materialize collapse → execute_hybrid
/// stages a temp table THEN runs the local_sql. Two execute calls:
/// the CREATE TEMP TABLE and the local_sql referencing __remote_0.
#[tokio::test]
async fn hybrid_multi_remote_query_stages_temp_table_then_runs_local_sql() {
    let backend = MockHybridBackend::new(vec![]);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state = build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.*"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(
        r#"{"statement": "SELECT u.id, o.amount FROM REMOTE.PUB.USERS u JOIN REMOTE.PUB.ORDERS o ON o.user_id = u.id"}"#,
    );
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let json = response_json(resp).await;
    assert_eq!(json["code"], "090001");

    let executed = seen.lock();
    assert!(
        executed.len() >= 2,
        "multi-remote query should stage at least one temp table + run local_sql; got: {executed:?}"
    );
    assert!(
        executed[0].starts_with("CREATE TEMP TABLE __remote_0 AS"),
        "first execute should be the staging DDL; got: {}",
        executed[0]
    );
    let local_sql = executed.last().unwrap();
    assert!(
        local_sql.contains("__remote_0"),
        "local_sql should reference the staged placeholder; got: {local_sql}"
    );
}

/// `/*+ melt_route(snowflake) */` hint → router skips classification
/// entirely and emits passthrough. Because the test setup has no real
/// Snowflake to forward to, the proxy errors out — but the error
/// path proves the hint took effect (no execute() calls reached the
/// backend).
#[tokio::test]
async fn hybrid_hint_route_snowflake_skips_backend() {
    let backend = MockHybridBackend::new(vec![]);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(
        r#"{"statement": "/*+ melt_route(snowflake) */ SELECT id FROM REMOTE.PUB.USERS"}"#,
    );
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let (parts, _body) = resp.into_parts();
    // Passthrough fails because there's no upstream Snowflake; any
    // non-2xx confirms we went down that path. Same convention as
    // statement_e2e.rs's unknown-table test.
    assert!(
        !parts.status.is_success(),
        "expected non-2xx because passthrough has no upstream in tests; got {}",
        parts.status,
    );
    assert!(
        seen.lock().is_empty(),
        "no backend execute() should have run when hint forced passthrough; got: {:?}",
        seen.lock()
    );
}

/// `/*+ melt_route(lake) */` hint → router clears the remote-table
/// promotion → reroutes to lake. With our mock backend not
/// containing the table, the lake path emits a `TableMissing`
/// passthrough — but the SHAPE of the seen execute() calls is
/// distinctly different from hybrid (zero temp-table staging).
#[tokio::test]
async fn hybrid_hint_route_lake_overrides_remote_match() {
    let backend = MockHybridBackend::new(vec![]);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body =
        Bytes::from(r#"{"statement": "/*+ melt_route(lake) */ SELECT id FROM REMOTE.PUB.USERS"}"#);
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let (_parts, _body) = resp.into_parts();
    let executed = seen.lock();
    assert!(
        executed
            .iter()
            .all(|s| !s.starts_with("CREATE TEMP TABLE __remote_")),
        "lake-override should not stage hybrid temp tables; got: {executed:?}"
    );
}

/// `hybrid_attach_available() == false` ⇒ decide-time gate degrades
/// the plan to all-Materialize. The mock backend records the staging
/// DDL — proving the rewrite pass turned what would have been an
/// Attach rewrite into a Materialize fragment. Also bumps
/// `melt_hybrid_attach_unavailable_total` (we don't assert on the
/// metric here — that's a unit test on `decide.rs`).
#[tokio::test]
async fn hybrid_attach_unavailable_forces_materialize_path() {
    let backend = MockHybridBackend::new(vec![]).with_attach_unavailable();
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(r#"{"statement": "SELECT id FROM REMOTE.PUB.USERS"}"#);
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let json = response_json(resp).await;
    assert_eq!(json["code"], "090001");

    let executed = seen.lock();
    assert!(
        executed
            .iter()
            .any(|s| s.starts_with("CREATE TEMP TABLE __remote_")),
        "attach-unavailable degraded plan should produce a Materialize fragment; got: {executed:?}"
    );
}

/// Cache enabled → second identical query skips the backend
/// entirely. Uses the v1 endpoint because the cache only writes on
/// fully-drained results (`drain_full = true`); the v2 statements
/// API streams in chunks and would skip cache writes by design.
#[tokio::test]
async fn hybrid_cache_serves_repeat_queries_without_backend() {
    let backend = MockHybridBackend::new(vec![]);
    let counts = backend.execute_count_handle();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state = build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |c| {
        c.hybrid_fragment_cache_ttl = Duration::from_secs(60);
        c.hybrid_fragment_cache_max_entries = 8;
    });
    seed_session(&state, "tk");

    // v1's query_request takes a JSON body shaped like
    // `{"sqlText": "...", "warehouse": "...", ...}`. Minimum: sqlText.
    let body = Bytes::from(r#"{"sqlText": "SELECT id, name FROM REMOTE.PUB.USERS"}"#);
    let resp1 = query_request(
        State(state.clone()),
        RawQuery(None),
        bearer("tk"),
        body.clone(),
    )
    .await;
    // v1 may return its own envelope shape; we don't strictly
    // need to parse — the assertion is on backend-call count.
    let _ = resp1;
    let after_first = counts.load(Ordering::Relaxed);
    assert!(
        after_first >= 1,
        "first query should hit the backend at least once; saw {after_first}"
    );

    let resp2 = query_request(State(state), RawQuery(None), bearer("tk"), body).await;
    let _ = resp2;
    let after_second = counts.load(Ordering::Relaxed);
    assert_eq!(
        after_first, after_second,
        "cached identical query should not invoke the backend a second time \
         (before={after_first}, after={after_second})"
    );
}

/// Cache disabled (TTL=0, default) → repeat queries always execute.
/// Sibling-test to the one above so the assertion isn't tautological.
#[tokio::test]
async fn hybrid_no_cache_replays_each_query() {
    let backend = MockHybridBackend::new(vec![]);
    let counts = backend.execute_count_handle();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state = build_state_with_hybrid(
        backend,
        matcher_with_remote(&["REMOTE.PUB.USERS"]),
        |_| {}, // default TTL=0 ⇒ no cache
    );
    seed_session(&state, "tk");

    let sql = r#"{"statement": "SELECT id FROM REMOTE.PUB.USERS"}"#;
    let _ = execute(
        State(state.clone()),
        RawQuery(None),
        bearer("tk"),
        Bytes::from(sql),
    )
    .await;
    let after_first = counts.load(Ordering::Relaxed);
    let _ = execute(State(state), RawQuery(None), bearer("tk"), Bytes::from(sql)).await;
    let after_second = counts.load(Ordering::Relaxed);
    assert!(
        after_second > after_first,
        "cache-disabled re-issued query should hit backend again \
         (before={after_first}, after={after_second})"
    );
}

/// Oversize per-table estimate above `hybrid_max_attach_scan_bytes`
/// → router emits passthrough. Verifies the new Attach-cap path
/// in decide.rs.
#[tokio::test]
async fn hybrid_oversize_attach_above_cap_passthroughs() {
    // 20 GiB per scan; default attach cap is 10 GiB.
    let backend = MockHybridBackend::new(vec![]).with_oversize_estimate(20 * 1024 * 1024 * 1024);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(r#"{"statement": "SELECT id FROM REMOTE.PUB.USERS"}"#);
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let (parts, _) = resp.into_parts();
    assert!(
        !parts.status.is_success(),
        "expected non-2xx because passthrough has no upstream; got {}",
        parts.status,
    );
    assert!(
        seen.lock().is_empty(),
        "passthrough due to attach cap should not invoke the backend; got: {:?}",
        seen.lock(),
    );
}

/// `/*+ melt_route(hybrid) */` bypasses size caps → execute_hybrid
/// runs even with oversize scan estimates.
#[tokio::test]
async fn hybrid_hint_route_hybrid_bypasses_caps() {
    let backend = MockHybridBackend::new(vec![]).with_oversize_estimate(50 * 1024 * 1024 * 1024);
    let seen = backend.seen_clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let state =
        build_state_with_hybrid(backend, matcher_with_remote(&["REMOTE.PUB.USERS"]), |_| {});
    seed_session(&state, "tk");

    let body = Bytes::from(
        r#"{"statement": "/*+ melt_route(hybrid) */ SELECT id FROM REMOTE.PUB.USERS"}"#,
    );
    let resp = execute(State(state), RawQuery(None), bearer("tk"), body).await;
    let json = response_json(resp).await;
    assert_eq!(json["code"], "090001");
    assert!(
        !seen.lock().is_empty(),
        "hint should override the size cap and reach the backend",
    );
}
