//! Snapshot acceptance test for `melt audit`'s local-processing
//! pipeline. Loads `examples/audit/query-history-fixture.csv` (the
//! ~10k synthetic agent-driven dbt mix from POWA-146), runs
//! `aggregate`, and asserts the resulting JSON is within ±2
//! percentage points of `examples/audit/ground-truth.json` per spec
//! §8 and POWA-140's "smallest test that proves it".
//!
//! Regenerate the fixture + ground-truth pair with
//! `python3 examples/audit/generate-fixture.py` (seed pinned in the
//! script). After regenerating, sync the count fields in
//! `ground-truth.json` to whatever the new corpus produces.
//!
//! No Snowflake hit. No DuckDB. Pure local pipeline.

use std::path::PathBuf;

use melt_audit::classify::{audit_session, classify_query, Bucket};
use melt_audit::fixture::load_query_history_csv;
use melt_audit::output::{aggregate, render_json, render_talkingpoints, AggregateConfig};

fn fixtures_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // crates/melt-audit/Cargo.toml → workspace root → examples/audit
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("examples")
        .join("audit")
}

fn load_fixture() -> Vec<melt_audit::QueryHistoryRow> {
    let path = fixtures_dir().join("query-history-fixture.csv");
    load_query_history_csv(&path).expect("fixture loads")
}

fn ground_truth() -> serde_json::Value {
    let path = fixtures_dir().join("ground-truth.json");
    let body = std::fs::read_to_string(&path).expect("ground-truth.json readable");
    serde_json::from_str(&body).expect("ground-truth.json parses")
}

const PCT_TOLERANCE: f64 = 2.0;

fn approx_pct(actual: f64, expected: f64) {
    let diff = (actual - expected).abs();
    assert!(
        diff <= PCT_TOLERANCE,
        "pct diverged: actual={actual:.2}, expected={expected:.2}, tolerance ±{PCT_TOLERANCE} pp",
    );
}

#[test]
fn fixture_static_and_conservative_within_tolerance() {
    let rows = load_fixture();
    let cfg = AggregateConfig {
        account: "ACME-DEMO".to_string(),
        credit_price_usd: 3.0,
        top_n: 20,
        window_days: 30,
        explicit_window_bounds: None,
    };
    let out = aggregate(&rows, &cfg);

    let truth = ground_truth();
    let expected_static = truth["routable_static"]["pct"].as_f64().unwrap();
    let expected_conservative = truth["routable_conservative"]["pct"].as_f64().unwrap();

    approx_pct(out.routable_static.pct, expected_static);
    approx_pct(out.routable_conservative.pct, expected_conservative);

    let expected_total = truth["total_queries"].as_u64().unwrap();
    assert_eq!(out.total_queries, expected_total);

    let expected_static_count = truth["routable_static"]["count"].as_u64().unwrap();
    assert_eq!(out.routable_static.count, expected_static_count);

    let breakdown = &truth["passthrough_reasons_breakdown"];
    assert_eq!(
        out.passthrough_reasons_breakdown.writes,
        breakdown["writes"].as_u64().unwrap()
    );
    assert_eq!(
        out.passthrough_reasons_breakdown.snowflake_features,
        breakdown["snowflake_features"].as_u64().unwrap()
    );
    assert_eq!(
        out.passthrough_reasons_breakdown.parse_failed,
        breakdown["parse_failed"].as_u64().unwrap()
    );

    // Top patterns: at least one entry, and the first table matches
    // the ground-truth pin.
    assert!(
        !out.top_patterns.is_empty(),
        "top_patterns should be populated"
    );
    let expected_top_table = truth["top_patterns_first_table"].as_str().unwrap();
    assert_eq!(out.top_patterns[0].table_fqn, expected_top_table);
}

#[test]
fn fixture_top_n_one_collapses_to_single_table() {
    let rows = load_fixture();
    let cfg = AggregateConfig {
        account: "ACME-DEMO".to_string(),
        credit_price_usd: 3.0,
        top_n: 1,
        window_days: 30,
        explicit_window_bounds: None,
    };
    let out = aggregate(&rows, &cfg);

    // With top-N=1, the conservative count must be ≤ static count
    // and pct must be ≤ static pct.
    assert!(
        out.routable_conservative.count <= out.routable_static.count,
        "conservative count {} > static count {}",
        out.routable_conservative.count,
        out.routable_static.count,
    );
    assert!(
        out.routable_conservative.pct <= out.routable_static.pct,
        "conservative pct {} > static pct {}",
        out.routable_conservative.pct,
        out.routable_static.pct,
    );
}

#[test]
fn fixture_redacts_literals_in_top_patterns() {
    let rows = load_fixture();
    let cfg = AggregateConfig {
        account: "ACME-DEMO".to_string(),
        credit_price_usd: 3.0,
        top_n: 20,
        window_days: 30,
        explicit_window_bounds: None,
    };
    let out = aggregate(&rows, &cfg);

    for p in &out.top_patterns {
        assert!(
            !p.pattern_redacted.contains('\''),
            "pattern still contains a single-quoted literal: {}",
            p.pattern_redacted,
        );
        assert!(
            !p.pattern_redacted.contains("'2026-04-01'"),
            "literal date leaked into top_patterns: {}",
            p.pattern_redacted,
        );
    }

    let json = render_json(&out);
    assert!(!json.contains("'2026-04-01'"), "literal leaked into JSON");

    let talking = render_talkingpoints(&out);
    assert!(
        talking.contains("TODO(POWA-139)"),
        "talking-points must include POWA-139 framing TODO until GTM responds"
    );
    assert!(!talking.contains("'2026-04-01'"));
}

#[test]
fn classify_uses_router_helpers_directly() {
    // Defensive: regression guard against the audit re-implementing
    // engine logic instead of re-using `melt_router::classify::*`.
    let session = audit_session();
    assert_eq!(
        classify_query("INSERT INTO t VALUES (1)", &session).bucket,
        Bucket::PassthroughForced,
    );
    assert_eq!(
        classify_query("SELECT 1 FROM A.B.C", &session).bucket,
        Bucket::RoutableCandidate,
    );
}
