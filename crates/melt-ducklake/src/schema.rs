//! DDL + per-table `SyncReport` live in `melt-control`. Re-export
//! here so existing `melt_ducklake::schema::…` call sites keep
//! compiling without a crate-name change.

pub use melt_control::{SyncReport, CATALOG_DDL};
