//! `melt-ducklake` — DuckLake backend (read + sync).
//!
//! Cargo features:
//! - `read` (default) — `DuckLakeBackend` only. Tiny dependency footprint.
//! - `write` — `sync` module: CDC apply + policy refresh.
//! - `full` — both.

pub mod catalog;
pub mod config;
pub mod pool;
pub mod reader;
pub mod schema;

#[cfg(feature = "write")]
pub mod sync;

pub use catalog::{CatalogClient, MarkerRow, StatusSnapshot};
pub use config::DuckLakeConfig;
pub use pool::{DuckDBManager, DuckLakePool};
pub use reader::DuckLakeBackend;
pub use schema::SyncReport;

#[cfg(feature = "write")]
pub use sync::DuckLakeSync;
