//! End-to-end smoke test for credential-free seed mode (KI-002 /
//! POWA-92).
//!
//! Wires the same `ProxyState` the real proxy runs with — but driven
//! by `LocalDuckDbBackend` against a freshly generated TPC-H sf=0.01
//! fixture and `SessionMode::Seed`. We exercise the four contracts
//! that make seed mode useful as a demo path:
//!
//! 1. `POST /session/v1/login-request` with the canned demo creds
//!    returns the seeded session token without contacting upstream
//!    Snowflake.
//! 2. The session token resolves through `SessionStore::lookup`, so
//!    the statement handler accepts it.
//! 3. A SELECT against the canned TPC-H tables is routed to Lake and
//!    returns rows. We run three different queries to satisfy the
//!    acceptance criterion ("at least 3 queries route to Lake").
//! 4. A query that would otherwise route to upstream Snowflake (write
//!    statement, INFORMATION_SCHEMA reference) returns
//!    `SeedModeUnsupported` (HTTP 422), not a silent passthrough.
//!
//! The fixture is generated inside the test's `TempDir`. We do NOT
//! depend on `var/melt/seed.ddb` already existing — the test is
//! self-contained so CI doesn't need a pre-seeded checkout.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use duckdb::Connection;
use melt_core::config::{
    ProxyLimits, RouterConfig, SessionMode, SEED_ACCOUNT, SEED_DATABASE, SEED_PASSWORD,
    SEED_SCHEMA, SEED_TOKEN, SEED_USER,
};
use melt_core::{PolicyConfig, PolicyMode, SeedClaims};
use melt_ducklake::LocalDuckDbBackend;
use melt_proxy::handlers::session::{login_request, token_request};
use melt_proxy::handlers::statement::execute;
use melt_proxy::result_store::ResultStore;
use melt_proxy::session::SessionStore;
use melt_proxy::ProxyState;
use melt_router::Cache;
use melt_snowflake::{SnowflakeClient, SnowflakeConfig};
use serde_json::{json, Value};
use tempfile::TempDir;

/// TPC-H tables emitted by DuckDB's `dbgen`. Mirrors the constant in
/// `melt-cli`'s `sessions_cmd` module — we don't import that here
/// because `melt-cli` would pull the whole CLI dep tree into a test.
const TPCH_TABLES: &[&str] = &[
    "lineitem", "orders", "customer", "nation", "region", "part", "supplier", "partsupp",
];

fn generate_fixture(dest: &PathBuf) -> anyhow::Result<()> {
    let conn = Connection::open(dest)?;
    conn.execute_batch("INSTALL tpch; LOAD tpch;")?;
    conn.execute_batch("CALL dbgen(sf = 0.01);")?;
    let move_sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {schema};
         {moves}
         ",
        schema = SEED_SCHEMA,
        moves = TPCH_TABLES
            .iter()
            .map(|t| format!(
                "CREATE TABLE {schema}.{table} AS SELECT * FROM main.{table};\n\
                 DROP TABLE main.{table};",
                schema = SEED_SCHEMA,
                table = t
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
    conn.execute_batch(&move_sql)?;
    Ok(())
}

fn build_seed_state(fixture: PathBuf) -> ProxyState {
    let limits = ProxyLimits::default();
    let sessions = Arc::new(SessionStore::new(limits.clone()));
    sessions.seed(SEED_TOKEN, SeedClaims::demo_default());
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

    let backend: Arc<dyn melt_core::StorageBackend> =
        Arc::new(LocalDuckDbBackend::open(fixture, SEED_DATABASE, SEED_SCHEMA).unwrap());

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
        session_mode: SessionMode::Seed,
    }
}

fn snowflake_bearer(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        "authorization",
        HeaderValue::from_str(&format!("Snowflake Token=\"{token}\"")).unwrap(),
    );
    h
}

async fn response_to_json(resp: axum::response::Response) -> (u16, Value) {
    let (parts, body) = resp.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
    (parts.status.as_u16(), json)
}

fn login_body() -> Bytes {
    Bytes::from(
        json!({
            "data": {
                "ACCOUNT_NAME": SEED_ACCOUNT,
                "LOGIN_NAME":   SEED_USER,
                "PASSWORD":     SEED_PASSWORD,
                "CLIENT_APP_ID":   "MeltSeedTest",
                "CLIENT_APP_VERSION": "0.0.0",
            }
        })
        .to_string(),
    )
}

#[tokio::test]
#[serial_test::serial(seed_duckdb)]
async fn login_short_circuits_to_seed_token_without_upstream_call() {
    let tmp = TempDir::new().unwrap();
    let fixture = tmp.path().join("seed.ddb");
    generate_fixture(&fixture).unwrap();
    let state = build_seed_state(fixture);

    let resp = login_request(
        State(state.clone()),
        RawQuery(None),
        HeaderMap::new(),
        login_body(),
    )
    .await;
    let (status, json) = response_to_json(resp).await;
    assert_eq!(status, 200, "login should succeed in seed mode: {json}");
    assert_eq!(
        json["data"]["token"].as_str(),
        Some(SEED_TOKEN),
        "expected SEED_TOKEN in login response: {json}"
    );
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["sessionInfo"]["databaseName"], SEED_DATABASE);
    assert_eq!(json["data"]["sessionInfo"]["schemaName"], SEED_SCHEMA);

    // SessionStore must already have the token registered (server::serve
    // calls `sessions.seed(...)` at startup; we mirror that in
    // `build_seed_state`). Drivers expect to immediately use the
    // returned token without round-tripping anything.
    let info = state
        .sessions
        .lookup(SEED_TOKEN)
        .expect("SessionStore should have the seeded token");
    assert_eq!(info.token, SEED_TOKEN);
    assert_eq!(info.database.as_deref(), Some(SEED_DATABASE));
}

#[tokio::test]
#[serial_test::serial(seed_duckdb)]
async fn login_with_wrong_creds_is_rejected_cleanly() {
    let tmp = TempDir::new().unwrap();
    let fixture = tmp.path().join("seed.ddb");
    generate_fixture(&fixture).unwrap();
    let state = build_seed_state(fixture);

    let body = Bytes::from(
        json!({
            "data": {
                "ACCOUNT_NAME": "some-other-account",
                "LOGIN_NAME":   "wrong-user",
                "PASSWORD":     "wrong",
            }
        })
        .to_string(),
    );
    let resp = login_request(State(state), RawQuery(None), HeaderMap::new(), body).await;
    let (status, json) = response_to_json(resp).await;
    assert_eq!(status, 401, "wrong creds should produce 401, got {json}");
    assert_eq!(json["success"], false);
    let msg = json["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("seed mode"),
        "rejection message should mention seed mode: {msg}"
    );
}

#[tokio::test]
#[serial_test::serial(seed_duckdb)]
async fn token_refresh_returns_seed_envelope_in_seed_mode() {
    let tmp = TempDir::new().unwrap();
    let fixture = tmp.path().join("seed.ddb");
    generate_fixture(&fixture).unwrap();
    let state = build_seed_state(fixture);

    let resp = token_request(State(state), RawQuery(None), HeaderMap::new(), login_body()).await;
    let (status, json) = response_to_json(resp).await;
    assert_eq!(
        status, 200,
        "token refresh in seed mode should succeed: {json}"
    );
    assert_eq!(json["data"]["token"].as_str(), Some(SEED_TOKEN));
}

#[tokio::test]
#[serial_test::serial(seed_duckdb)]
async fn three_lake_queries_route_locally_against_tpch_fixture() {
    let tmp = TempDir::new().unwrap();
    let fixture = tmp.path().join("seed.ddb");
    generate_fixture(&fixture).unwrap();
    let state = build_seed_state(fixture);

    // Lake-routable queries against the canned TPC-H fixture. Each
    // exercises a different shape (full scan, projection, aggregate)
    // so the test is broader than just "the same query three times."
    let queries = [
        "SELECT COUNT(*) AS n FROM TPCH.SF01.lineitem",
        "SELECT n_name FROM TPCH.SF01.nation ORDER BY n_nationkey LIMIT 5",
        "SELECT o_orderstatus, COUNT(*) FROM TPCH.SF01.orders GROUP BY o_orderstatus",
    ];

    // Pre-flight: assert the router actually decides Lake for each
    // SQL. Drives the same code path the handler uses, isolated from
    // the rest of the request envelope. If this trips, the integration
    // test below will report a less-informative passthrough error.
    let session = state
        .sessions
        .lookup(SEED_TOKEN)
        .expect("seeded session present");
    for sql in &queries {
        let outcome = melt_router::route(
            sql,
            &session,
            state.backend.as_ref(),
            &state.router_cfg,
            &state.snowflake_cfg,
            &state.router_cache,
            None,
            None,
        )
        .await;
        let route_str = format!("{:?}", outcome.route);
        assert!(
            route_str.contains("Lake"),
            "{sql} should route to Lake, got {route_str}"
        );
    }

    for sql in queries {
        let body = Bytes::from(json!({ "statement": sql }).to_string());
        let resp = execute(
            State(state.clone()),
            RawQuery(None),
            snowflake_bearer(SEED_TOKEN),
            body,
        )
        .await;
        let (status, json) = response_to_json(resp).await;
        assert_eq!(status, 200, "{sql} → {status} {json}");
        // Snowflake-shaped success envelope.
        assert_eq!(json["code"], "090001", "{sql} → {json}");
        assert!(
            json["data"].is_array() && !json["data"].as_array().unwrap().is_empty(),
            "{sql} returned no data partition: {json}"
        );
    }
}

#[tokio::test]
#[serial_test::serial(seed_duckdb)]
async fn passthrough_routed_query_returns_seed_unsupported() {
    let tmp = TempDir::new().unwrap();
    let fixture = tmp.path().join("seed.ddb");
    generate_fixture(&fixture).unwrap();
    let state = build_seed_state(fixture);

    // INFORMATION_SCHEMA is a known UsesSnowflakeFeature passthrough
    // trigger — the router will route it to upstream, and seed mode
    // must refuse cleanly with 422 instead of dialing out.
    let body = Bytes::from(
        json!({ "statement": "SELECT table_name FROM INFORMATION_SCHEMA.TABLES" }).to_string(),
    );
    let resp = execute(
        State(state),
        RawQuery(None),
        snowflake_bearer(SEED_TOKEN),
        body,
    )
    .await;
    let (status, json) = response_to_json(resp).await;
    assert_eq!(status, 422, "expected 422, got {status} {json}");
    let msg = json["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("seed mode"),
        "message should reference seed mode: {msg}"
    );
}
