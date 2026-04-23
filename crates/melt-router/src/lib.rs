//! `melt-router` — decides where a query should run AND rewrites it
//! into DuckDB dialect when the answer is Lake. Pure logic.

pub mod classify;
pub mod decide;
pub mod enforce;
pub mod parse;
pub mod stats;
pub mod translate;

pub use decide::{route, RouteOutcome};
pub use stats::Cache;
