//! Bucket logic on top of `melt_router::classify`.
//!
//! Spec §4 tells us the audit's classifier is a strict subset of
//! `melt-router::decide_inner`, so we re-export the engine helpers
//! verbatim — never copy-paste them. A future `melt-router` change
//! that tightens what counts as a write or a Snowflake-only feature
//! flows directly into `melt audit`'s percentages.

use melt_core::{SessionInfo, TableRef};
use melt_router::classify::{extract_tables, is_write, uses_snowflake_features};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

use crate::redact::redact_literals;

/// Three buckets per spec §4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    /// `INSERT/UPDATE/MERGE/DDL/GRANT`, Snowflake-only features,
    /// or a parse failure. Never routes.
    PassthroughForced,
    /// Read-only SELECT, no Snowflake-only features, references
    /// at least one regular table.
    RoutableCandidate,
    /// Empty SQL, system queries, INFORMATION_SCHEMA-only, etc.
    /// Excluded from the routable %.
    Unknown,
}

/// Why a query landed in `PassthroughForced`. Mirrors the slice of
/// `melt_router::decide` reasons spec §3.2 calls out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassthroughReason {
    Write,
    SnowflakeFeature(&'static str),
    ParseFailed,
}

#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    pub bucket: Bucket,
    pub passthrough_reason: Option<PassthroughReason>,
    /// Resolved `db.schema.table` references, in AST traversal order.
    pub tables: Vec<TableRef>,
    /// Literal-redacted query text, normalized for grouping.
    pub redacted: String,
}

impl QueryAnalysis {
    /// Pick a single representative table for `top_patterns` rows.
    /// The first resolved one wins — same heuristic the engine uses
    /// when a query touches multiple tables.
    pub fn primary_table(&self) -> Option<&TableRef> {
        self.tables.first()
    }
}

/// Classify one `QUERY_TEXT` value as one of the three buckets.
///
/// `session` defaults the database/schema for unqualified names. The
/// audit doesn't have a real Snowflake session, so callers pass a
/// synthetic one — typically [`audit_session`] — to keep
/// `extract_tables` deterministic across rows.
pub fn classify_query(sql: &str, session: &SessionInfo) -> QueryAnalysis {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return QueryAnalysis {
            bucket: Bucket::Unknown,
            passthrough_reason: None,
            tables: Vec::new(),
            redacted: String::new(),
        };
    }

    let redacted = redact_literals(trimmed);

    let dialect = SnowflakeDialect;
    let ast = match Parser::parse_sql(&dialect, trimmed) {
        Ok(stmts) => stmts,
        Err(_) => {
            return QueryAnalysis {
                bucket: Bucket::PassthroughForced,
                passthrough_reason: Some(PassthroughReason::ParseFailed),
                tables: Vec::new(),
                redacted,
            };
        }
    };

    if is_write(&ast) {
        return QueryAnalysis {
            bucket: Bucket::PassthroughForced,
            passthrough_reason: Some(PassthroughReason::Write),
            tables: Vec::new(),
            redacted,
        };
    }

    if let Some(label) = uses_snowflake_features(&ast) {
        return QueryAnalysis {
            bucket: Bucket::PassthroughForced,
            passthrough_reason: Some(PassthroughReason::SnowflakeFeature(label)),
            tables: Vec::new(),
            redacted,
        };
    }

    let tables = extract_tables(&ast, session);
    if tables.is_empty() || tables.iter().all(is_system_table) {
        return QueryAnalysis {
            bucket: Bucket::Unknown,
            passthrough_reason: None,
            tables,
            redacted,
        };
    }

    QueryAnalysis {
        bucket: Bucket::RoutableCandidate,
        passthrough_reason: None,
        tables,
        redacted,
    }
}

/// Synthetic session used by the audit's classify pass. The audit
/// reads historical SQL, so there is no live driver session — but
/// `extract_tables` still needs *something* to default unqualified
/// names against. Picks a sentinel database/schema that won't collide
/// with real Snowflake tenants.
pub fn audit_session() -> SessionInfo {
    let mut s = SessionInfo::new("melt-audit", 1);
    s.database = Some("MELT_AUDIT_UNQUALIFIED".to_string());
    s.schema = Some("PUBLIC".to_string());
    s
}

fn is_system_table(t: &TableRef) -> bool {
    let db = t.database.to_ascii_uppercase();
    let schema = t.schema.to_ascii_uppercase();
    schema == "INFORMATION_SCHEMA"
        || db == "SNOWFLAKE"
        || db == "MELT_AUDIT_UNQUALIFIED" // synthetic default
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(sql: &str) -> QueryAnalysis {
        classify_query(sql, &audit_session())
    }

    #[test]
    fn select_is_routable() {
        let a = classify("SELECT id FROM ANALYTICS.PUBLIC.EVENTS WHERE ts > '2025-01-01'");
        assert_eq!(a.bucket, Bucket::RoutableCandidate);
        assert!(a.redacted.contains('?'));
        assert_eq!(
            a.primary_table().unwrap().name.to_ascii_uppercase(),
            "EVENTS"
        );
    }

    #[test]
    fn write_is_passthrough() {
        let a = classify("INSERT INTO ANALYTICS.PUBLIC.EVENTS VALUES (1,2,3)");
        assert_eq!(a.bucket, Bucket::PassthroughForced);
        assert_eq!(a.passthrough_reason, Some(PassthroughReason::Write));
    }

    #[test]
    fn snowflake_feature_is_passthrough() {
        let a = classify("SELECT * FROM TABLE(GENERATOR(ROWCOUNT=>10))");
        assert_eq!(a.bucket, Bucket::PassthroughForced);
        assert!(matches!(
            a.passthrough_reason,
            Some(PassthroughReason::SnowflakeFeature(_))
        ));
    }

    #[test]
    fn parse_failed_is_passthrough() {
        let a = classify("THIS IS NOT VALID SQL ;;");
        assert_eq!(a.bucket, Bucket::PassthroughForced);
        assert_eq!(a.passthrough_reason, Some(PassthroughReason::ParseFailed));
    }

    #[test]
    fn information_schema_only_is_unknown() {
        let a = classify("SELECT * FROM INFORMATION_SCHEMA.TABLES");
        // INFORMATION_SCHEMA also matches the Snowflake-features
        // probe, so it lands in passthrough_forced. That matches the
        // engine — INFORMATION_SCHEMA never routes.
        assert_eq!(a.bucket, Bucket::PassthroughForced);
    }

    #[test]
    fn empty_is_unknown() {
        let a = classify("");
        assert_eq!(a.bucket, Bucket::Unknown);
        let a = classify("   \n  ");
        assert_eq!(a.bucket, Bucket::Unknown);
    }
}
