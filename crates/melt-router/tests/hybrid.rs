//! Integration tests for the dual-execution router (Phase 1).
//!
//! Mirrors `tests/decide.rs`'s pattern (mock `StorageBackend` + the
//! real `Cache` + `route` entry point) but configures a
//! `SyncTableMatcher` with `[sync].remote` patterns so we can drive
//! the hybrid path end-to-end.
//!
//! Each test asserts on the `Route::Hybrid` shape the router emits —
//! strategy mix, fragment count, attach-rewrite count, reason —
//! mirroring the metadata the Python regression variants in
//! `examples/python/variants_hybrid/` declare.
//!
//! These tests do NOT exercise `execute_hybrid` (which needs a real
//! DuckDB pool and the community Snowflake extension); they cover
//! the routing decision only. End-to-end execution coverage lives in
//! the Python variants.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use melt_core::config::RouterConfig;
use melt_core::{
    BackendKind, BridgeStrategy, MeltError, NodeKind, PolicyConfig, PolicyMode, QueryContext,
    RecordBatchStream, Result, Route, SessionId, SessionInfo, StorageBackend, SyncConfig,
    SyncTableMatcher, TableRef,
};
use melt_router::hybrid::choose_strategy;
use melt_router::{route, Cache};
use melt_snowflake::SnowflakeConfig;
use tokio::sync::Semaphore;

struct MockBackend {
    tables: Vec<TableRef>,
    bytes_per_table: u64,
}

#[async_trait]
impl StorageBackend for MockBackend {
    async fn execute(&self, _sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        Err(MeltError::backend("mock"))
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
}

fn session() -> SessionInfo {
    SessionInfo {
        id: SessionId::new(),
        token: "t".into(),
        role: None,
        warehouse: None,
        database: Some("LOCAL_DB".into()),
        schema: Some("PUB".into()),
        expires_at: Instant::now() + std::time::Duration::from_secs(3600),
        concurrency: Arc::new(Semaphore::new(1)),
    }
}

/// Router config with hybrid_execution turned ON. Phase-0 flag-off
/// behaviour is covered by `tests/decide.rs::*`.
fn hybrid_cfg() -> RouterConfig {
    RouterConfig {
        hybrid_execution: true,
        ..RouterConfig::default()
    }
}

fn sf_cfg() -> SnowflakeConfig {
    SnowflakeConfig {
        policy: PolicyConfig {
            mode: PolicyMode::Passthrough,
            refresh_interval: std::time::Duration::from_secs(60),
        },
        ..SnowflakeConfig::default()
    }
}

/// Build a SyncTableMatcher with the given `[sync].remote` globs
/// (and no include/exclude). Mirrors what Python variants assume.
fn matcher_with_remote(remote: &[&str]) -> SyncTableMatcher {
    SyncTableMatcher::from_config(&SyncConfig {
        auto_discover: false,
        include: Vec::new(),
        exclude: Vec::new(),
        remote: remote.iter().map(|s| s.to_string()).collect(),
        ..SyncConfig::default()
    })
    .expect("valid patterns")
}

// ── Strategy mechanics (Phase 1 happy path) ──────────────────────

/// Variant 50 / 70 equivalent: a single Remote table query. Routes
/// hybrid; v1 builder emits the all-remote-whole-statement form
/// (1 Materialize fragment, 0 attach rewrites). Once richer per-node
/// strategy selection lands the same query may switch to
/// `strategy=attach`, but the ROUTE stays hybrid either way.
#[tokio::test]
async fn single_remote_table_routes_hybrid() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.USERS"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT id FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // Single-scan remote → Attach. Materialize-via-collapse needs
    // 2+ scans; one scan falls through to the attach rewrite so the
    // DuckDB extension can push down predicates natively.
    assert_eq!(plan.remote_fragments.len(), 0);
    assert_eq!(plan.attach_rewrites.len(), 1);
    assert_eq!(plan.strategy_label(), "attach");
    assert!(
        outcome
            .translated_sql
            .as_deref()
            .unwrap_or("")
            .contains("sf_link"),
        "local_sql should reference the sf_link alias"
    );
}

/// Variants 51, 56, 61, 62, 63: mixed local + single-remote
/// references. The v1 builder emits Attach rewrites for the lone
/// Remote scan and leaves the local table untouched.
#[tokio::test]
async fn mixed_local_and_remote_uses_attach() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.USERS"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT * FROM LOCAL_DB.PUB.ORDERS o JOIN REMOTE.PUB.USERS u ON u.id = o.uid",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    assert_eq!(plan.attach_rewrites.len(), 1);
    assert_eq!(plan.remote_fragments.len(), 0);
    let local_sql = outcome.translated_sql.as_deref().unwrap_or("");
    assert!(
        local_sql.contains("sf_link"),
        "Attach rewrite should produce sf_link.<...>; got: {local_sql}"
    );
}

/// Variants 52, 53, 55, 57: every referenced table is Remote.
/// Whole-statement collapse → 1 Materialize fragment, 0 attach.
#[tokio::test]
async fn all_remote_collapses_to_one_fragment() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT a.x, b.y FROM REMOTE.PUB.A a JOIN REMOTE.PUB.B b ON a.id = b.id",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    assert_eq!(plan.remote_fragments.len(), 1);
    assert_eq!(plan.attach_rewrites.len(), 0);
    let frag = &plan.remote_fragments[0];
    assert_eq!(
        frag.scanned_tables.len(),
        2,
        "one fragment should cover both REMOTE tables"
    );
    assert_eq!(plan.strategy_label(), "materialize");
}

/// Variants 60, 54: remote subquery inside a local query. The v1
/// builder collapses the all-remote subquery into a Materialize
/// fragment.
#[tokio::test]
async fn all_remote_in_subquery_collapses() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT * FROM LOCAL_DB.PUB.ORDERS o \
         WHERE o.uid IN (SELECT id FROM REMOTE.PUB.A JOIN REMOTE.PUB.B USING (id))",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // The subquery (REMOTE.A + REMOTE.B) collapses into one fragment.
    assert_eq!(plan.remote_fragments.len(), 1, "{plan:?}");
    assert_eq!(plan.attach_rewrites.len(), 0);
}

// ── Bail-out paths (correct degradation) ─────────────────────────

/// Variant 64: window function over a Remote table → bail to
/// passthrough. v1's safe degradation; future work can lift.
#[tokio::test]
async fn window_over_remote_bails_to_snowflake() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT u.* FROM REMOTE.PUB.USERS u \
         QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    match &outcome.route {
        Route::Snowflake { reason } => {
            assert!(
                matches!(reason, melt_core::PassthroughReason::TranslationFailed { detail }
                         if detail.contains("hybrid_bail") && detail.contains("window")),
                "expected hybrid_bail: window_over_remote, got {reason:?}"
            );
        }
        other => panic!("expected Snowflake, got {other:?}"),
    }
}

/// Set ops anywhere in the query → bail to passthrough.
#[tokio::test]
async fn set_op_bails_to_snowflake() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT id FROM REMOTE.PUB.A UNION SELECT id FROM REMOTE.PUB.B",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    match &outcome.route {
        Route::Snowflake { reason } => {
            assert!(
                matches!(reason, melt_core::PassthroughReason::TranslationFailed { detail }
                         if detail.contains("set_op")),
                "expected hybrid_bail: set_op, got {reason:?}"
            );
        }
        other => panic!("expected Snowflake, got {other:?}"),
    }
}

// ── Trigger-case feature gate ────────────────────────────────────

/// `hybrid_execution = false` is the safe default — Remote-classified
/// tables passthrough as today (covers variant 70 in pre-hybrid mode).
#[tokio::test]
async fn hybrid_execution_off_passthroughs_remote() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&RouterConfig::default()));
    let outcome = route(
        "SELECT * FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &RouterConfig::default(), // hybrid_execution defaults to false
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    match &outcome.route {
        Route::Snowflake { reason } => {
            assert!(
                matches!(reason, melt_core::PassthroughReason::TableMissing(_)),
                "expected TableMissing on first remote (today's safe default), got {reason:?}"
            );
        }
        other => panic!("expected Snowflake passthrough when hybrid is off, got {other:?}"),
    }
}

// ── Guardrail: policy-protected table never goes hybrid ──────────

/// Critical security invariant from §10.1 in the design doc: a
/// policy-protected table forces full passthrough even when the table
/// is Remote-classified and hybrid is enabled. Hybrid uses service-
/// role credentials and would silently bypass row-access policies.
#[tokio::test]
async fn policy_protected_remote_table_passthroughs() {
    let mut markers = std::collections::HashMap::new();
    let table = TableRef::new("REMOTE", "PUB", "USERS");
    markers.insert(table.clone(), "row_filter_users".into());

    struct ProtectedBackend {
        markers: std::collections::HashMap<TableRef, String>,
    }
    #[async_trait]
    impl StorageBackend for ProtectedBackend {
        async fn execute(&self, _: &str, _: &QueryContext) -> Result<RecordBatchStream> {
            Err(MeltError::backend("mock"))
        }
        async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
            Ok(vec![1024; t.len()])
        }
        async fn tables_exist(&self, t: &[TableRef]) -> Result<Vec<bool>> {
            Ok(vec![true; t.len()])
        }
        async fn policy_markers(&self, t: &[TableRef]) -> Result<Vec<Option<String>>> {
            Ok(t.iter().map(|x| self.markers.get(x).cloned()).collect())
        }
        async fn list_tables(&self) -> Result<Vec<TableRef>> {
            Ok(Vec::new())
        }
        fn kind(&self) -> BackendKind {
            BackendKind::DuckLake
        }
    }

    let backend = ProtectedBackend { markers };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT * FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    match &outcome.route {
        Route::Snowflake { reason } => {
            assert!(
                matches!(reason, melt_core::PassthroughReason::PolicyProtected { .. }),
                "policy-protected hybrid query MUST passthrough; got {reason:?}",
            );
        }
        other => panic!("hybrid bypassed a policy-protected table — {other:?}"),
    }
}

// ── choose_strategy unit checks (also covered in emit.rs tests) ──

#[tokio::test]
async fn choose_strategy_one_table_attach_two_materialize() {
    let cfg = RouterConfig::default();
    let one = melt_core::PlanNode::new(
        0,
        NodeKind::RemoteSql {
            sql: String::new(),
            tables: vec![TableRef::new("D", "S", "T")],
        },
        melt_core::Placement::Remote,
    );
    let two = melt_core::PlanNode::new(
        0,
        NodeKind::RemoteSql {
            sql: String::new(),
            tables: vec![TableRef::new("D", "S", "T1"), TableRef::new("D", "S", "T2")],
        },
        melt_core::Placement::Remote,
    );
    assert_eq!(choose_strategy(&one, &cfg), BridgeStrategy::Attach);
    assert_eq!(choose_strategy(&two, &cfg), BridgeStrategy::Materialize);
}

// ── Strategy chain crossover (cost vs heuristic) ────────────────────

/// Backend variant that exposes both bytes AND row counts, so the
/// cost strategy can do real cost math. Stats are positional —
/// matched to whatever `tables_in_order` the router computes.
struct StatsBackend {
    tables: Vec<TableRef>,
    bytes_per_table: u64,
    rows_per_table: u64,
}

#[async_trait]
impl StorageBackend for StatsBackend {
    async fn execute(&self, _sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        Err(MeltError::backend("mock"))
    }
    async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![self.bytes_per_table; t.len()])
    }
    async fn estimate_table_rows(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![self.rows_per_table; t.len()])
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

fn cost_chain_cfg(min_advantage_ratio: f64, attach_rows_per_sec: f64) -> RouterConfig {
    use melt_core::config::{CostStrategyConfig, HybridStrategyConfig};
    let mut c = hybrid_cfg();
    c.hybrid_strategy = HybridStrategyConfig {
        chain: vec!["cost".into(), "heuristic".into()],
        cost: CostStrategyConfig {
            min_advantage_ratio,
            attach_rows_per_sec,
            ..CostStrategyConfig::default()
        },
    };
    c
}

/// With default chain `["heuristic"]`, single-table queries always
/// route as Attach. Behaviour-preserving baseline.
#[tokio::test]
async fn default_chain_preserves_heuristic_attach() {
    let backend = StatsBackend {
        tables: vec![],
        bytes_per_table: 1_000_000,
        rows_per_table: 10_000,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.USERS"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let outcome = route(
        "SELECT id FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // Heuristic single-table → Attach (no Materialize fragments).
    assert_eq!(plan.attach_rewrites.len(), 1);
    assert_eq!(plan.remote_fragments.len(), 0);
}

/// With `["cost", "heuristic"]` chain and constants tuned so
/// Materialize wins by a clear margin, the cost strategy flips
/// the decision: a single-table query produces a Materialize
/// fragment instead of an Attach rewrite.
#[tokio::test]
async fn cost_chain_can_flip_single_table_to_materialize() {
    // Crippled attach throughput (100K rows/sec) makes Materialize
    // the cost-cheaper choice even for single-table scans.
    let cfg = cost_chain_cfg(1.5, 100_000.0);
    let backend = StatsBackend {
        tables: vec![],
        bytes_per_table: 1_000_000_000,
        rows_per_table: 10_000_000,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.USERS"]);
    let cache = Arc::new(Cache::new(&cfg));
    let outcome = route(
        "SELECT id FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &cfg,
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // Cost strategy flipped Skip → Collapse: single-table query
    // becomes a Materialize fragment, not an Attach rewrite.
    assert_eq!(
        plan.remote_fragments.len(),
        1,
        "expected cost strategy to materialize single-table query when attach is crippled; \
         attach_rewrites={} fragments={}",
        plan.attach_rewrites.len(),
        plan.remote_fragments.len(),
    );
    assert_eq!(plan.attach_rewrites.len(), 0);
}

/// When stats are missing (zero rows estimate), the cost strategy
/// abstains and the heuristic answers — preserving today's
/// behaviour. Important regression guard: cost in the chain must
/// not break the no-stats path.
#[tokio::test]
async fn cost_chain_defers_when_row_stats_missing() {
    let cfg = cost_chain_cfg(1.5, 100_000.0);
    // bytes are present, rows are zero ⇒ cost strategy abstains
    let backend = StatsBackend {
        tables: vec![],
        bytes_per_table: 1_000_000_000,
        rows_per_table: 0,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.USERS"]);
    let cache = Arc::new(Cache::new(&cfg));
    let outcome = route(
        "SELECT id FROM REMOTE.PUB.USERS",
        &session(),
        &backend,
        &cfg,
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // Heuristic answers with single-table → Attach.
    assert_eq!(plan.attach_rewrites.len(), 1);
    assert_eq!(plan.remote_fragments.len(), 0);
}

/// Multi-table queries always Materialize regardless of strategy
/// chain — cost abstains for multi-table, heuristic picks Collapse.
/// Regression guard so the cost path doesn't accidentally flip
/// multi-table queries away from Materialize.
#[tokio::test]
async fn cost_chain_multi_table_still_materializes() {
    let cfg = cost_chain_cfg(1.5, 100_000.0);
    let backend = StatsBackend {
        tables: vec![],
        bytes_per_table: 1_000_000,
        rows_per_table: 10_000,
    };
    let matcher = matcher_with_remote(&["REMOTE.PUB.*"]);
    let cache = Arc::new(Cache::new(&cfg));
    let outcome = route(
        "SELECT u.id FROM REMOTE.PUB.USERS u JOIN REMOTE.PUB.ORDERS o ON o.uid = u.id",
        &session(),
        &backend,
        &cfg,
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    assert!(
        !plan.remote_fragments.is_empty(),
        "expected Materialize for multi-table; fragments={} attach={}",
        plan.remote_fragments.len(),
        plan.attach_rewrites.len(),
    );
}
