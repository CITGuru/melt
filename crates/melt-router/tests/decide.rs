//! Smoke tests for the router decision tree. The MVP backend is a
//! mock implementing `StorageBackend`; real backend integration is
//! exercised by `melt-ducklake` / `melt-iceberg` integration tests.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use melt_core::config::RouterConfig;
use melt_core::{
    BackendKind, MeltError, PolicyConfig, PolicyMode, QueryContext, RecordBatchStream, Result,
    Route, SessionId, SessionInfo, StorageBackend, TableRef,
};
use melt_router::{route, Cache};
use melt_snowflake::SnowflakeConfig;
use tokio::sync::Semaphore;

struct MockBackend {
    tables: Vec<TableRef>,
    bytes: u64,
    markers: std::collections::HashMap<TableRef, String>,
}

#[async_trait]
impl StorageBackend for MockBackend {
    async fn execute(&self, _sql: &str, _ctx: &QueryContext) -> Result<RecordBatchStream> {
        Err(MeltError::backend("mock"))
    }
    async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<Vec<u64>> {
        // Mock backend reports the same per-table byte count for every
        // input — sufficient for the existing decide tests, which only
        // care about the SUM crossing `lake_max_scan_bytes`.
        let per = self.bytes / t.len().max(1) as u64;
        Ok(vec![per; t.len()])
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

fn sf_cfg(mode: PolicyMode) -> SnowflakeConfig {
    SnowflakeConfig {
        policy: PolicyConfig {
            mode,
            refresh_interval: std::time::Duration::from_secs(60),
        },
        ..SnowflakeConfig::default()
    }
}

#[tokio::test]
async fn writes_passthrough() {
    let backend = MockBackend {
        tables: vec![],
        bytes: 0,
        markers: Default::default(),
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let outcome = route(
        "INSERT INTO analytics.public.x VALUES (1)",
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(PolicyMode::Passthrough),
        &cache,
        None,
        None,
    )
    .await;
    assert!(matches!(outcome.route, Route::Snowflake { .. }));
}

#[tokio::test]
async fn lake_when_table_present_and_under_threshold() {
    let table = TableRef::new("analytics", "public", "orders");
    let backend = MockBackend {
        tables: vec![table.clone()],
        bytes: 1024,
        markers: Default::default(),
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let outcome = route(
        "SELECT * FROM analytics.public.orders",
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(PolicyMode::Passthrough),
        &cache,
        None,
        None,
    )
    .await;
    assert!(
        matches!(outcome.route, Route::Lake { .. }),
        "outcome: {:?}",
        outcome.route
    );
}

#[tokio::test]
async fn passthrough_on_policy_marker() {
    let table = TableRef::new("analytics", "public", "orders");
    let mut markers = std::collections::HashMap::new();
    markers.insert(table.clone(), "row_filter_orders".into());
    let backend = MockBackend {
        tables: vec![table.clone()],
        bytes: 1024,
        markers,
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let outcome = route(
        "SELECT * FROM analytics.public.orders",
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(PolicyMode::Passthrough),
        &cache,
        None,
        None,
    )
    .await;
    match outcome.route {
        Route::Snowflake { reason } => {
            let label = reason.label();
            assert_eq!(label, "policy_protected", "got {label:?}");
        }
        other => panic!("expected Snowflake passthrough, got {other:?}"),
    }
}

#[tokio::test]
async fn allowlist_denies_unlisted() {
    let listed = TableRef::new("analytics", "public", "events");
    let backend = MockBackend {
        tables: vec![TableRef::new("analytics", "public", "orders")],
        bytes: 0,
        markers: Default::default(),
    };
    let cache = Arc::new(Cache::new(&cfg()));
    let outcome = route(
        "SELECT * FROM analytics.public.orders",
        &session(),
        &backend,
        &cfg(),
        &sf_cfg(PolicyMode::AllowList {
            tables: vec![listed],
        }),
        &cache,
        None,
        None,
    )
    .await;
    match outcome.route {
        Route::Snowflake { reason } => assert_eq!(reason.label(), "not_in_allowlist"),
        other => panic!("expected NotInAllowList, got {other:?}"),
    }
}
