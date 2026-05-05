//! POWA-163 — bench-workload routing golden fixture.
//!
//! Re-runs the offline classifier (the same code path `melt route`
//! invokes) over every `[[query]]` entry in
//! `examples/bench/workload.toml` and asserts the result matches
//! `examples/bench/fixtures/routes.json` exactly.
//!
//! The fixture is the auditable record of what Melt routes the
//! agent-shaped workload to — the headline cost-savings number on the
//! Melt v0.1 README only stays honest as long as those four queries
//! keep classifying as `lake`. A silent change to the router that
//! flips one of them to `snowflake` (or to `hybrid`) would invalidate
//! the queries-per-dollar delta without anyone noticing, which is why
//! this test is wired into `cargo test` rather than only the bench
//! script.
//!
//! Regenerate via `make routes-fixture` (or
//! `python3 examples/bench/fixtures/regen_routes.py`) after an
//! intentional routing change. Both `melt route` and this test
//! produce JSON in the same shape; if they diverge the regen will
//! fail this assert and the diff will show what moved.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use melt_core::config::RouterConfig;
use melt_core::{
    PolicyConfig, PolicyMode, Route, SessionId, SessionInfo, SyncConfig, SyncTableMatcher,
};
use melt_router::decide::lazy_classify_with_matcher;
use melt_router::RouteOutcome;
use melt_snowflake::SnowflakeConfig;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

const FIXTURE_VERSION: u64 = 1;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

#[derive(serde::Deserialize)]
struct Workload {
    #[serde(rename = "query")]
    queries: Vec<WorkloadQuery>,
}

#[derive(serde::Deserialize)]
struct WorkloadQuery {
    name: String,
    sql: String,
}

fn session() -> SessionInfo {
    SessionInfo {
        id: SessionId::new(),
        token: "bench-fixture".into(),
        role: None,
        warehouse: None,
        database: Some("analytics".into()),
        schema: Some("public".into()),
        expires_at: Instant::now() + std::time::Duration::from_secs(3600),
        concurrency: Arc::new(Semaphore::new(1)),
    }
}

fn sf_cfg() -> SnowflakeConfig {
    SnowflakeConfig {
        policy: PolicyConfig {
            mode: PolicyMode::Passthrough,
            refresh_interval: std::time::Duration::from_secs(60),
        },
        database: "analytics".into(),
        schema: "public".into(),
        ..SnowflakeConfig::default()
    }
}

fn router_cfg() -> RouterConfig {
    RouterConfig::default()
}

fn matcher() -> Option<SyncTableMatcher> {
    SyncTableMatcher::from_config(&SyncConfig {
        auto_discover: true,
        include: Vec::new(),
        exclude: Vec::new(),
        remote: Vec::new(),
        ..SyncConfig::default()
    })
    .ok()
}

/// Render a `RouteOutcome` into the JSON shape `routes.json` stores.
/// Keep in lockstep with `parse_route_output` in
/// `examples/bench/fixtures/regen_routes.py` — both must agree on the
/// exact text that lands in the fixture.
fn outcome_to_record(name: &str, sql: &str, outcome: &RouteOutcome) -> Value {
    let route_str = outcome.route.as_str().to_string();
    let (reason, decided_by_strategy, strategy_chain) = match &outcome.route {
        Route::Lake { reason } => (format!("{reason:?}"), Value::Null, Vec::<String>::new()),
        Route::Snowflake { reason } => (
            format!("{} ({:?})", reason.label(), reason),
            Value::Null,
            Vec::new(),
        ),
        Route::Hybrid { plan, reason, .. } => (
            format!("{} ({})", reason.label(), reason),
            Value::String(plan.chain_decided_by.clone()),
            plan.strategy_chain.clone(),
        ),
    };

    json!({
        "name": name,
        "sql": sql,
        "route": route_str,
        "reason": reason,
        "decided_by_strategy": decided_by_strategy,
        "strategy_chain": strategy_chain,
    })
}

#[test]
fn bench_workload_routes_match_fixture() {
    let root = repo_root();
    let workload_path = root.join("examples/bench/workload.toml");
    let routes_path = root.join("examples/bench/fixtures/routes.json");

    let workload_raw = std::fs::read_to_string(&workload_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", workload_path.display()));
    let workload: Workload = toml::from_str(&workload_raw)
        .unwrap_or_else(|e| panic!("parse {}: {e}", workload_path.display()));

    let routes_raw = std::fs::read_to_string(&routes_path).unwrap_or_else(|e| {
        panic!(
            "read {} (regenerate with `make routes-fixture`): {e}",
            routes_path.display()
        )
    });
    let routes: Value = serde_json::from_str(&routes_raw)
        .unwrap_or_else(|e| panic!("parse {}: {e}", routes_path.display()));

    assert_eq!(
        routes.get("version").and_then(Value::as_u64),
        Some(FIXTURE_VERSION),
        "fixture {} has unexpected `version`; bump FIXTURE_VERSION here \
         or rerun `make routes-fixture`",
        routes_path.display(),
    );

    let fixture_queries = routes
        .get("queries")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("`queries` array missing from {}", routes_path.display()));

    assert_eq!(
        fixture_queries.len(),
        workload.queries.len(),
        "workload has {} queries but fixture has {} — rerun \
         `make routes-fixture` after extending workload.toml",
        workload.queries.len(),
        fixture_queries.len(),
    );

    let session = session();
    let sf_cfg = sf_cfg();
    let cfg = router_cfg();
    let matcher = matcher();

    let mut diffs: Vec<String> = Vec::new();
    for (i, q) in workload.queries.iter().enumerate() {
        let sql = q.sql.trim();
        let outcome = lazy_classify_with_matcher(sql, &session, &sf_cfg, matcher.as_ref(), &cfg);
        let actual = outcome_to_record(&q.name, sql, &outcome);
        let expected = &fixture_queries[i];
        if &actual != expected {
            diffs.push(format!(
                "[{}] {}\n  actual:   {}\n  expected: {}",
                i,
                q.name,
                serde_json::to_string(&actual).unwrap_or_default(),
                serde_json::to_string(expected).unwrap_or_default(),
            ));
        }
    }

    assert!(
        diffs.is_empty(),
        "bench routing fixture drift detected — rerun \
         `make routes-fixture` if the change is intentional, \
         else investigate the router change.\n\n{}",
        diffs.join("\n\n"),
    );
}
