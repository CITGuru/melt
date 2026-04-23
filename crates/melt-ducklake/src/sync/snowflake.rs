//! Thin re-export module so callers stay symmetric with the Iceberg
//! crate's `sync::snowflake`. The actual CDC reader lives in
//! `melt-snowflake` and is shared.

pub use melt_snowflake::{ChangeAction, ChangeBatch, ChangeStream, SnapshotId};
