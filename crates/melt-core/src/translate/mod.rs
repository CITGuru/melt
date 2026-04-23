//! Snowflake → DuckDB dialect rewriting.
//!
//! Each pass takes the AST and rewrites parts in place. The full set
//! is intentionally short for the MVP; new rules are one-line patches
//! plus a fixture pair under `fixtures/` (in the `melt-router` crate).
//!
//! This module lives in `melt-core` so both `melt-router` (query
//! translation at route time) and `melt-snowflake` (view-body
//! translation at bootstrap time) can share the passes without
//! introducing a crate cycle. `melt-router::translate` is now a thin
//! re-export of this module.

use crate::error::{MeltError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

pub mod bind;
pub mod date;
pub mod functions;
pub mod hide_internal;
pub mod lateral_flatten;
pub mod qualify;
pub mod semi_structured;
pub mod strip_database;

/// Apply every translation pass to the AST in place. Falls through
/// to `Err(MeltError::Translate)` only on actively broken input —
/// otherwise unsupported constructs are left as-is and the executing
/// backend will surface a clearer error than we could.
pub fn translate_ast(ast: &mut [Statement]) -> Result<()> {
    for s in ast.iter_mut() {
        functions::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        date::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        qualify::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        semi_structured::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        lateral_flatten::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        hide_internal::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
        // strip_database MUST run last so earlier passes still see 3-part names.
        strip_database::rewrite(s).map_err(|e| MeltError::translate(e.to_string()))?;
    }
    Ok(())
}

/// Parse a Snowflake-dialect view body, run every translate pass over
/// it, and render it back as DuckDB-dialect SQL. Used by sync to
/// materialize decomposed views (`CREATE VIEW ... AS <translated>`
/// on the lake side) without surfacing the router's AST machinery to
/// callers.
///
/// Returns the translated body; the caller wraps it in a
/// `CREATE OR REPLACE VIEW` statement.
pub fn translate_body(snowflake_body: &str) -> Result<String> {
    let mut ast = Parser::parse_sql(&SnowflakeDialect {}, snowflake_body)
        .map_err(|e| MeltError::parse(e.to_string()))?;
    translate_ast(&mut ast)?;
    Ok(ast
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";\n"))
}

pub type TranslateResult<T> = std::result::Result<T, TranslateError>;

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
    #[error("unsupported construct: {0}")]
    Unsupported(String),
}
