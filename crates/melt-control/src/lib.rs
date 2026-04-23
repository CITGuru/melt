//! `melt-control` — Postgres-backed control plane shared by both
//! backends.
//!
//! Stores sync state, policy markers, and filtered-view bindings.
//! Independent of which data-plane catalog (DuckLake-on-Postgres,
//! Iceberg-on-REST/Glue) the active backend uses for table metadata.
//!
//! DuckLake today points its `catalog_url` at a Postgres that does
//! double duty: data-plane for DuckLake's own tables AND control-plane
//! for `melt_*` tables. That's still fine — this crate just formalizes
//! the schema so the Iceberg backend can share it via a dedicated
//! `[backend.iceberg].control_catalog_url`.

pub mod catalog;
pub mod schema;
pub mod state;

pub use catalog::{ControlCatalog, MarkerRow, SharedControl, StatusSnapshot};
pub use schema::{SyncReport, CATALOG_DDL};
pub use state::{DepKind, SyncStateRow, ViewBodyRow};

pub use melt_core::{DiscoveryCatalog, ObjectKind, SyncSource, SyncState, ViewStrategy};
