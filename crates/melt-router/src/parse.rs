use melt_core::{MeltError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

/// Parse Snowflake SQL into a vector of statements. Multiple
/// statements per request are rare from BI tools but legal.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    Parser::parse_sql(&SnowflakeDialect {}, sql).map_err(|e| MeltError::parse(e.to_string()))
}

/// Render an AST back to a SQL string. Used after translation.
///
/// **Caveat:** sqlparser's AST → string round-trip is not fully
/// lossless (comments dropped, quoted identifier casing may
/// normalize). Acceptable for executing the translated query.
pub fn unparse(ast: &[Statement]) -> String {
    ast.iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(";\n")
}
