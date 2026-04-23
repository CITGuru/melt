//! Strip the leading database component from 3-part table references
//! so DuckDB's attached catalog (`lake`, via `USE lake`) can resolve
//! the remaining `SCHEMA.TABLE` pair.
//!
//! Snowflake FQNs are `DATABASE.SCHEMA.TABLE`. In Melt's DuckLake
//! model, sync writes the mirror as `<schema>.<table>` inside the
//! attached `lake` catalog and drops the source database prefix.
//! Queries routed to Lake keep their Snowflake-style names unless a
//! rewrite pass shortens them, which is this pass's job:
//!
//! ```text
//! SELECT … FROM DB.SCHEMA.TABLE    →    SELECT … FROM SCHEMA.TABLE
//! FROM DB.SCHEMA.TABLE AS t            →    FROM SCHEMA.TABLE AS t
//! JOIN DB.SCHEMA.OTHER ON …            →    JOIN SCHEMA.OTHER ON …
//! ```
//!
//! 2-part references pass through unchanged. 1-part too. More than 3
//! parts (uncommon — some dialects allow catalog.db.schema.table) are
//! left alone; the assumption is the extra head component matters to
//! the backend.
//!
//! We use `visit_relations_mut` so we touch only table references in
//! FROM / JOIN / INSERT / UPDATE / DELETE, not column references
//! (which may use a-part-per-dot syntax that doesn't correspond to
//! catalog lookup).

use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations_mut, ObjectName, Statement};

use super::TranslateResult;

pub fn rewrite(stmt: &mut Statement) -> TranslateResult<()> {
    let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
        if name.0.len() == 3 {
            name.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::rewrite;
    use sqlparser::dialect::SnowflakeDialect;
    use sqlparser::parser::Parser;

    fn roundtrip(sql: &str) -> String {
        let mut ast = Parser::parse_sql(&SnowflakeDialect {}, sql).expect("parse");
        for stmt in ast.iter_mut() {
            rewrite(stmt).expect("rewrite");
        }
        ast.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(";\n")
    }

    #[test]
    fn three_part_is_stripped() {
        let out = roundtrip("SELECT * FROM DB.SCHEMA.T");
        assert!(out.contains("FROM SCHEMA.T"), "got: {out}");
        assert!(!out.contains("DB.SCHEMA"), "got: {out}");
    }

    #[test]
    fn two_part_is_unchanged() {
        let out = roundtrip("SELECT * FROM SCHEMA.T");
        assert!(out.contains("FROM SCHEMA.T"), "got: {out}");
    }

    #[test]
    fn one_part_is_unchanged() {
        let out = roundtrip("SELECT * FROM T");
        assert!(out.contains("FROM T"), "got: {out}");
    }

    #[test]
    fn joins_and_aliases() {
        let out = roundtrip("SELECT t.id FROM DB.S.T AS t JOIN DB.S.U ON t.id = U.id");
        assert!(!out.contains("DB."), "got: {out}");
        assert!(out.contains("FROM S.T"), "got: {out}");
        assert!(out.contains("JOIN S.U"), "got: {out}");
    }

    #[test]
    fn quoted_identifiers_survive() {
        let out = roundtrip(r#"SELECT * FROM "DB"."SCHEMA"."T""#);
        assert!(!out.contains(r#""DB"."SCHEMA""#), "got: {out}");
        assert!(out.contains(r#""SCHEMA"."T""#), "got: {out}");
    }
}
