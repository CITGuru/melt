//! Anonymous `?` placeholder handling. Snowflake expects positional
//! `?`; DuckDB accepts the same syntax. Token-level rewriting only
//! becomes necessary when we start translating positions across
//! AST-reordering passes — until then the function is the identity.

pub fn rewrite_placeholders(sql: &str) -> String {
    sql.to_string()
}
