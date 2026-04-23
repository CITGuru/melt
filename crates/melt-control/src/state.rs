//! Sync state machine types shared across backends.
//!
//! The enums themselves (`SyncState`, `SyncSource`) live in
//! `melt-core` so `melt-router` can reference them without pulling
//! in this crate's Postgres client. We re-export for convenience.

pub use melt_core::{ObjectKind, SyncSource, SyncState, ViewStrategy};

use melt_core::TableRef;

/// A row of `melt_table_stats` hydrated for the CLI and admin
/// tooling.
#[derive(Clone, Debug, serde::Serialize)]
pub struct SyncStateRow {
    pub table: TableRef,
    pub sync_state: SyncState,
    pub source: SyncSource,
    pub object_kind: ObjectKind,
    pub view_strategy: Option<ViewStrategy>,
    pub discovered_at: chrono::DateTime<chrono::Utc>,
    pub last_queried_at: Option<chrono::DateTime<chrono::Utc>>,
    pub bootstrap_error: Option<String>,
    pub bytes: u64,
    pub rows_count: u64,
    pub last_snapshot: Option<i64>,
    pub last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Persistent view body record mirroring `melt_view_bodies`.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ViewBodyRow {
    pub table: TableRef,
    pub snowflake_body: String,
    pub duckdb_body: Option<String>,
    pub body_checksum: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Dependency kind for `melt_view_dependencies`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepKind {
    BaseTable,
    View,
}

impl DepKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DepKind::BaseTable => "base_table",
            DepKind::View => "view",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "view" => DepKind::View,
            _ => DepKind::BaseTable,
        }
    }
}
