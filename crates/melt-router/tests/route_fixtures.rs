//! POWA-11 — routing-correctness regression suite.
//!
//! Locks in `melt route`-style output for the routing decisions Melt
//! ships today so we don't regress them as the router evolves (dual
//! execution, hint syntax, warehouse selection). Coverage:
//!
//! 1. The four `examples/README.md` queries.
//! 2. The dual-execution worked example from `docs/architecture.md`
//!    §"Worked example: a mixed hybrid plan".
//! 3. Comment-hint coverage (`/*+ melt_route(snowflake|lake|hybrid) */`).
//! 4. Policy-protected table refuses hybrid (PolicyProtected wins).
//!
//! The fixture format mirrors what the `print_lazy_route` helper in
//! `melt-cli/src/runtime.rs` emits, minus the trailing CLI notes — so
//! reading the diff from a regression is the same as reading what an
//! operator would see at the terminal.
//!
//! The account-mismatch (390201) integration test is acceptance
//! criterion #5 and lives in `crates/melt-proxy/tests/` (separate
//! checkout because it needs proxy/login fixtures).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use melt_core::config::RouterConfig;
use melt_core::{
    BackendKind, MeltError, PolicyConfig, PolicyMode, QueryContext, RecordBatchStream, Result,
    Route, SessionId, SessionInfo, StorageBackend, SyncConfig, SyncTableMatcher, TableRef,
};
use melt_router::decide::lazy_classify_with_matcher;
use melt_router::{route, Cache, RouteOutcome};
use melt_snowflake::SnowflakeConfig;
use tokio::sync::Semaphore;

// ── Test infrastructure ────────────────────────────────────────────

/// Mock backend used for live `route()` paths that need a backend
/// (per-table byte estimates, policy markers, table existence).
/// Reports the same byte budget for every table; honours an optional
/// policy-marker map so the policy-protected fixtures fire.
struct MockBackend {
    tables: Vec<TableRef>,
    bytes_per_table: u64,
    markers: std::collections::HashMap<TableRef, String>,
}

#[async_trait]
impl StorageBackend for MockBackend {
    async fn execute(&self, _: &str, _: &QueryContext) -> Result<RecordBatchStream> {
        Err(MeltError::backend("mock"))
    }
    async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        Ok(vec![self.bytes_per_table; t.len()])
    }
    async fn tables_exist(&self, t: &[TableRef]) -> Result<Vec<bool>> {
        Ok(t.iter().map(|x| self.tables.contains(x)).collect())
    }
    async fn policy_markers(&self, t: &[TableRef]) -> Result<Vec<Option<String>>> {
        Ok(t.iter().map(|x| self.markers.get(x).cloned()).collect())
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
        database: Some("analytics".into()),
        schema: Some("public".into()),
        expires_at: Instant::now() + std::time::Duration::from_secs(3600),
        concurrency: Arc::new(Semaphore::new(1)),
    }
}

fn cfg() -> RouterConfig {
    RouterConfig::default()
}

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

/// Pretty-print a `RouteOutcome` in the same shape `melt route` emits
/// (sans trailing CLI notes). Keeps fixtures human-readable so the
/// diff on regression makes the route + reason obvious.
fn format_outcome(input: &str, outcome: &RouteOutcome) -> String {
    let mut s = String::new();
    writeln!(s, "input SQL: {input}").unwrap();
    writeln!(s, "route: {}", outcome.route.as_str()).unwrap();
    match &outcome.route {
        Route::Lake { reason } => {
            writeln!(s, "reason: {reason:?}").unwrap();
            if let Some(t) = &outcome.translated_sql {
                writeln!(s, "translated:").unwrap();
                writeln!(s, "{t}").unwrap();
            }
        }
        Route::Snowflake { reason } => {
            writeln!(s, "reason: {} ({:?})", reason.label(), reason).unwrap();
        }
        Route::Hybrid {
            plan,
            reason,
            estimated_remote_bytes,
        } => {
            writeln!(s, "reason: {} ({})", reason.label(), reason).unwrap();
            writeln!(s, "strategy: {}", plan.strategy_label()).unwrap();
            writeln!(
                s,
                "remote_fragments: {}  attach_rewrites: {}  est_remote_bytes: {}",
                plan.remote_fragments.len(),
                plan.attach_rewrites.len(),
                estimated_remote_bytes
            )
            .unwrap();
            for frag in &plan.remote_fragments {
                writeln!(
                    s,
                    "[REMOTE,materialize] {} ({} table{})",
                    frag.placeholder,
                    frag.scanned_tables.len(),
                    if frag.scanned_tables.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                )
                .unwrap();
                writeln!(s, "{}", frag.snowflake_sql).unwrap();
            }
            for rw in &plan.attach_rewrites {
                writeln!(
                    s,
                    "[REMOTE,attach] {} -> {}",
                    rw.original.fqn(),
                    rw.alias_reference
                )
                .unwrap();
            }
            if !plan.local_sql.is_empty() {
                writeln!(s, "local SQL:").unwrap();
                writeln!(s, "{}", plan.local_sql).unwrap();
            }
        }
    }
    s
}

fn assert_outcome(input: &str, outcome: &RouteOutcome, expected: &str) {
    let actual = format_outcome(input, outcome);
    let lhs = actual.trim_end();
    let rhs = expected.trim_end();
    if lhs != rhs {
        panic!(
            "fixture mismatch for {input:?}\n\n--- ACTUAL ---\n{lhs}\n\n--- EXPECTED ---\n{rhs}\n"
        );
    }
}

// ── 1. examples/README.md ─────────────────────────────────────────

/// Query 1: pure expression, no tables → lake (DuckDB computes locally).
#[test]
fn examples_query_1_pure_expression() {
    let outcome = lazy_classify_with_matcher(
        "SELECT 1 + 1 AS answer",
        &session(),
        &sf_cfg(),
        None,
        &cfg(),
    );
    assert_outcome(
        "SELECT 1 + 1 AS answer",
        &outcome,
        r#"input SQL: SELECT 1 + 1 AS answer
route: lake
reason: UnderThreshold { estimated_bytes: 0 }
translated:
SELECT 1 + 1 AS answer
"#,
    );
}

/// Query 2: IFF + DATEADD over a synced table → lake, with the
/// translator rewriting both into DuckDB-native forms.
#[test]
fn examples_query_2_iff_dateadd_translates() {
    let sql = "SELECT IFF(x > 0, 'p', 'n'), DATEADD(day, 7, ts) FROM analytics.public.events";
    let outcome = lazy_classify_with_matcher(sql, &session(), &sf_cfg(), None, &cfg());
    assert_outcome(
        sql,
        &outcome,
        // Translator strips the redundant default-database prefix
        // (`analytics.` ← session.database) so DuckDB sees the
        // 2-part name. The router records the original FQN for
        // sync/policy resolution; only the unparsed SQL is shortened.
        r#"input SQL: SELECT IFF(x > 0, 'p', 'n'), DATEADD(day, 7, ts) FROM analytics.public.events
route: lake
reason: UnderThreshold { estimated_bytes: 0 }
translated:
SELECT CASE WHEN x > 0 THEN 'p' ELSE 'n' END, DATEADD('day', 7, ts) FROM public.events
"#,
    );
}

/// Query 3: INSERT → write_statement passthrough.
#[test]
fn examples_query_3_insert_passthrough() {
    let sql = "INSERT INTO analytics.public.events VALUES (1, 'x', CURRENT_TIMESTAMP())";
    let outcome = lazy_classify_with_matcher(sql, &session(), &sf_cfg(), None, &cfg());
    assert_outcome(
        sql,
        &outcome,
        r#"input SQL: INSERT INTO analytics.public.events VALUES (1, 'x', CURRENT_TIMESTAMP())
route: snowflake
reason: write_statement (WriteStatement)
"#,
    );
}

/// Query 4: information_schema → uses_snowflake_feature passthrough.
#[test]
fn examples_query_4_information_schema_passthrough() {
    let sql = "SELECT * FROM information_schema.tables";
    let outcome = lazy_classify_with_matcher(sql, &session(), &sf_cfg(), None, &cfg());
    assert_outcome(
        sql,
        &outcome,
        r#"input SQL: SELECT * FROM information_schema.tables
route: snowflake
reason: uses_snowflake_feature (UsesSnowflakeFeature("INFORMATION_SCHEMA"))
"#,
    );
}

// ── 2. dual-execution worked example ─────────────────────────────

/// `docs/architecture.md` §"Worked example: a mixed hybrid plan".
///
/// The doc writes `[sync].remote = ["WAREHOUSE.*"]` for brevity; the
/// real glob is matched against `DB.SCHEMA.TABLE` so the precise
/// equivalent is `*.WAREHOUSE.*` (any database, schema = WAREHOUSE).
/// Confirms the planner emits the mixed `attach + materialize` shape
/// the doc walks through (single-scan `users` → Attach; collapsed
/// `orders JOIN products` subquery → Materialize).
#[test]
fn dual_execution_worked_example() {
    let sql = "SELECT u.region, COUNT(*) \
               FROM   sf.warehouse.users u \
               JOIN   ice.analytics.events e ON e.uid = u.id \
               WHERE  e.ts > '2026-01-01' \
                 AND  u.id IN ( \
                     SELECT buyer_id FROM sf.warehouse.orders o \
                     JOIN   sf.warehouse.products p ON p.id = o.pid \
                     WHERE  p.category = 'electronics' \
                 ) \
               GROUP BY u.region";
    let matcher = matcher_with_remote(&["*.WAREHOUSE.*"]);
    let outcome =
        lazy_classify_with_matcher(sql, &session(), &sf_cfg(), Some(&matcher), &hybrid_cfg());
    let plan = match &outcome.route {
        Route::Hybrid { plan, .. } => plan.clone(),
        other => panic!("expected Hybrid, got {other:?}"),
    };
    // Shape assertions (doc §Worked example, step 3-5):
    //   • exactly one Materialize fragment (the collapsed
    //     orders+products subquery);
    //   • exactly one Attach rewrite (the single users scan);
    //   • the Materialize fragment scans both warehouse tables.
    assert_eq!(
        plan.remote_fragments.len(),
        1,
        "expected one Materialize fragment for the orders+products subquery"
    );
    assert_eq!(
        plan.attach_rewrites.len(),
        1,
        "expected one Attach rewrite for the users scan"
    );
    assert_eq!(
        plan.strategy_label(),
        "mixed",
        "single-scan users + collapsed orders+products = mixed strategy"
    );
    let frag = &plan.remote_fragments[0];
    assert_eq!(
        frag.scanned_tables.len(),
        2,
        "the collapsed fragment should cover both WAREHOUSE.ORDERS and WAREHOUSE.PRODUCTS"
    );
    let attach = &plan.attach_rewrites[0];
    assert_eq!(attach.original.name.to_uppercase(), "USERS");
    assert!(
        attach.alias_reference.contains("sf_link"),
        "attach rewrite must reference the sf_link extension catalog, got {}",
        attach.alias_reference
    );
    // local_sql must reference both bridge surfaces — the
    // Materialize placeholder and the sf_link alias — so the local
    // DuckDB query joins across both bridges.
    let local = plan.local_sql.as_str();
    assert!(
        local.contains(&frag.placeholder),
        "local_sql must reference the Materialize placeholder {}, got {local}",
        frag.placeholder
    );
    assert!(
        local.contains("sf_link"),
        "local_sql must reference sf_link.<...>, got {local}"
    );
}

// ── 3. comment-hint coverage ─────────────────────────────────────

/// `/*+ melt_route(snowflake) */` is honoured by `route()` (the live
/// path); it's evaluated by `parse_hints` before the table classifier
/// runs, so any SQL — even a pure expression that would normally route
/// lake — passes through.
#[tokio::test]
async fn comment_hint_snowflake_overrides() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 0,
        markers: Default::default(),
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let sql = "/*+ melt_route(snowflake) */ SELECT 1 + 1 AS answer";
    let outcome = route(
        sql,
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(),
        &cache,
        None,
        None,
    )
    .await;
    assert_outcome(
        sql,
        &outcome,
        r#"input SQL: /*+ melt_route(snowflake) */ SELECT 1 + 1 AS answer
route: snowflake
reason: operator_hint (OperatorHint)
"#,
    );
}

/// `/*+ melt_route(lake) */` keeps a normally-lake-eligible query on
/// the lake path; it isn't an early exit, just a Remote-table override.
/// For a no-table query the route is lake either way — we lock in that
/// the hint doesn't accidentally bounce it elsewhere.
#[tokio::test]
async fn comment_hint_lake_routes_lake() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 0,
        markers: Default::default(),
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let sql = "/*+ melt_route(lake) */ SELECT 1 + 1 AS answer";
    let outcome = route(
        sql,
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(),
        &cache,
        None,
        None,
    )
    .await;
    assert_outcome(
        sql,
        &outcome,
        r#"input SQL: /*+ melt_route(lake) */ SELECT 1 + 1 AS answer
route: lake
reason: UnderThreshold { estimated_bytes: 0 }
translated:
SELECT 1 + 1 AS answer
"#,
    );
}

/// `/*+ melt_route(hybrid) */` only takes effect when there is at
/// least one Remote-classified table — the hint bypasses size caps on
/// the hybrid plan path. Confirms a Remote scan with the hint emits
/// `Route::Hybrid` (single-scan → Attach strategy).
#[tokio::test]
async fn comment_hint_hybrid_emits_hybrid_plan() {
    let backend = MockBackend {
        tables: vec![],
        bytes_per_table: 1024,
        markers: Default::default(),
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let sql = "/*+ melt_route(hybrid) */ SELECT id FROM REMOTE.PUB.USERS";
    let outcome = route(
        sql,
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
    assert_eq!(plan.strategy_label(), "attach");
    assert!(
        outcome
            .translated_sql
            .as_deref()
            .unwrap_or("")
            .contains("sf_link"),
        "Attach rewrite should reference sf_link.<...>; got: {:?}",
        outcome.translated_sql
    );
}

// ── 4. policy-protected table refuses hybrid ─────────────────────

/// Critical security invariant from §10.1: a policy-protected table
/// MUST passthrough even when (a) it's Remote-classified, (b) hybrid
/// is enabled, and (c) the operator passed `melt_route(hybrid)` to
/// bypass size caps. Hybrid uses service-role credentials and would
/// silently bypass row-access / masking policies, so PolicyProtected
/// always wins over the hybrid path.
#[tokio::test]
async fn policy_protected_remote_table_refuses_hybrid() {
    let table = TableRef::new("REMOTE", "PUB", "USERS");
    let mut markers = std::collections::HashMap::new();
    markers.insert(table.clone(), "row_filter_users".into());
    let backend = MockBackend {
        tables: vec![table.clone()],
        bytes_per_table: 1024,
        markers,
    };
    let matcher = matcher_with_remote(&["REMOTE.*.*"]);
    let cache = Arc::new(Cache::new(&hybrid_cfg()));
    let sql = "/*+ melt_route(hybrid) */ SELECT id FROM REMOTE.PUB.USERS";
    let outcome = route(
        sql,
        &session(),
        &backend,
        &hybrid_cfg(),
        &sf_cfg(),
        &cache,
        Some(&matcher),
        None,
    )
    .await;
    assert_outcome(
        sql,
        &outcome,
        r#"input SQL: /*+ melt_route(hybrid) */ SELECT id FROM REMOTE.PUB.USERS
route: snowflake
reason: policy_protected (PolicyProtected { table: TableRef { database: "REMOTE", schema: "PUB", name: "USERS" }, policy_name: "row_filter_users" })
"#,
    );
}
