//! `melt-iceberg` — Iceberg backend.
//!
//! DuckDB is the execution engine for both reads and writes: the
//! `iceberg` extension is loaded at pool startup and the catalog is
//! `ATTACH`ed under the alias `ice`, so tables resolve natively as
//! `ice.<schema>.<table>` and sync applies changes via plain
//! `INSERT INTO` / `DELETE FROM`. No hand-rolled Parquet or manifest
//! JSON lives in Rust anymore — duckdb-iceberg owns the Parquet
//! write and the snapshot commit.
//!
//! Catalogs supported:
//! - **REST / Polaris**: first-class via `ATTACH ... (TYPE ICEBERG, ENDPOINT ...)`.
//! - **Glue**: discovery only. Cross-table writes through duckdb-iceberg
//!   for Glue depend on extension features that are still stabilizing;
//!   operators should front Glue with a REST shim for now.
//! - **Hive**: unsupported (see `IcebergCatalogClient::assert_supported`).

pub mod catalog;
pub mod config;
pub mod glue;
pub mod pool;
pub mod reader;
pub mod rest;

#[cfg(feature = "write")]
pub mod sync;

pub use catalog::IcebergCatalogClient;
pub use config::{IcebergCatalogKind, IcebergConfig};
pub use pool::IcebergPool;
pub use reader::IcebergBackend;

#[cfg(feature = "write")]
pub use sync::IcebergSync;
