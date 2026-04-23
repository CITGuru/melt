//! View-bootstrap helpers shared by both backends.
//!
//! Two layers:
//!
//! 1. [`classify_view_body`] parses a Snowflake view body and tells the
//!    caller which bootstrap path is viable:
//!    - [`ViewBodyClassification::StreamCompatible`] — body is clean
//!      enough that `CREATE STREAM … ON VIEW` will work. All such
//!      bodies are also decomposable.
//!    - [`ViewBodyClassification::DecomposableOnly`] — body uses
//!      constructs Snowflake forbids for stream-on-view (aggregates,
//!      `DISTINCT`, `QUALIFY`, `LIMIT`, correlated subqueries,
//!      non-deterministic functions) but DuckDB can still evaluate,
//!      so decomposition stays on the table.
//!    - [`ViewBodyClassification::Unsupported`] — body is beyond what
//!      either path can handle (e.g. a `CREATE` in the body, which
//!      would only show up in a malformed `GET_DDL` output).
//! 2. [`translate_view_body`] is a thin shim over the shared
//!    `melt_core::translate::translate_body`. Kept in this module so
//!    callers don't need to depend on `melt_core` directly.
//!
//! The dependency-graph BFS lives in the sync crates (DuckLake /
//! Iceberg) because it needs access to the Snowflake client — this
//! module only owns the dialect-parse pieces.

use melt_core::Result;
use sqlparser::ast::{
    visit_expressions, visit_statements, Expr, GroupByExpr, Query, SelectItem, SetExpr, Statement,
};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

/// Result of inspecting a view body for bootstrap viability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewBodyClassification {
    /// Body is pure relational algebra. Either path works;
    /// stream-on-view is preferred when decomposition fails for other
    /// reasons (excluded dep, untranslatable functions).
    StreamCompatible,
    /// Body uses constructs DuckDB can evaluate but Snowflake
    /// disallows for stream-on-view. Only the decomposition path
    /// applies.
    DecomposableOnly(ViewBodyReason),
    /// Body couldn't be parsed, doesn't contain a `SELECT`, or hits
    /// a construct neither path supports.
    Unsupported(ViewBodyReason),
}

/// Machine-readable reason string; attached to quarantine messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewBodyReason(pub String);

impl ViewBodyReason {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ViewBodyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Translate a Snowflake-dialect view body to DuckDB dialect. Thin
/// re-export of `melt_core::translate::translate_body` so callers only
/// depend on this crate.
pub fn translate_view_body(body: &str) -> Result<String> {
    melt_core::translate::translate_body(body)
}

/// Parse a Snowflake view body (the full `CREATE VIEW ...` DDL
/// returned by `GET_DDL`) and decide which bootstrap paths apply.
///
/// Accepts two inputs:
/// 1. The full `CREATE [OR REPLACE] [SECURE] VIEW x AS <select>`
///    statement (what `GET_DDL('VIEW', …)` returns).
/// 2. Just the `<select>` body (for callers who've already extracted
///    it).
///
/// Returns a classification plus the extracted SELECT body text; the
/// body is what gets fed into DuckLake's `CREATE OR REPLACE VIEW`.
pub fn classify_view_body(input: &str) -> (ViewBodyClassification, Option<String>) {
    let ast = match Parser::parse_sql(&SnowflakeDialect {}, input) {
        Ok(a) => a,
        Err(e) => {
            return (
                ViewBodyClassification::Unsupported(ViewBodyReason::new(format!(
                    "parse_failed: {e}"
                ))),
                None,
            )
        }
    };
    let Some(query) = extract_query(&ast) else {
        return (
            ViewBodyClassification::Unsupported(ViewBodyReason::new(
                "no SELECT in view body".to_string(),
            )),
            None,
        );
    };

    // Collect every disqualifier; first quarantine message names a concrete feature.
    let mut decompose_only: Vec<&'static str> = Vec::new();
    // Reserved hook for constructs neither path handles.
    let unsupported: Vec<&'static str> = Vec::new();

    if query.limit.is_some() {
        decompose_only.push("LIMIT");
    }
    if query.order_by.is_some() {
        decompose_only.push("ORDER BY");
    }

    match &*query.body {
        SetExpr::Select(select) => {
            if select.distinct.is_some() {
                decompose_only.push("DISTINCT");
            }
            if group_by_is_present(&select.group_by) {
                decompose_only.push("GROUP BY");
            }
            if select.having.is_some() {
                decompose_only.push("HAVING");
            }
            if select.qualify.is_some() {
                decompose_only.push("QUALIFY");
            }
            if !select.cluster_by.is_empty() {
                decompose_only.push("CLUSTER BY");
            }
            if !select.distribute_by.is_empty() {
                decompose_only.push("DISTRIBUTE BY");
            }
            if !select.sort_by.is_empty() {
                decompose_only.push("SORT BY");
            }
            if projection_has_aggregate(&select.projection) {
                decompose_only.push("aggregate in projection");
            }
            if projection_has_non_deterministic(&select.projection) {
                decompose_only.push("non-deterministic function");
            }
        }
        SetExpr::Query(_) => {
            decompose_only.push("nested set expression");
        }
        SetExpr::SetOperation {
            op, set_quantifier, ..
        } => {
            use sqlparser::ast::{SetOperator, SetQuantifier};
            // UNION ALL is the only set op stream-on-view allows
            // (Snowflake parses bare `UNION` as `Union` + `None`).
            let allow =
                matches!(op, SetOperator::Union) && matches!(set_quantifier, SetQuantifier::All);
            if !allow {
                decompose_only.push("set operation other than UNION ALL");
            }
        }
        _ => {
            decompose_only.push("unsupported set expression");
        }
    }

    // Whole-tree walks for "anywhere in the body" disqualifiers.
    if contains_non_from_subquery(&ast) {
        decompose_only.push("subquery outside FROM");
    }

    if !unsupported.is_empty() {
        return (
            ViewBodyClassification::Unsupported(ViewBodyReason::new(unsupported.join("; "))),
            Some(query.to_string()),
        );
    }

    let classification = if decompose_only.is_empty() {
        ViewBodyClassification::StreamCompatible
    } else {
        ViewBodyClassification::DecomposableOnly(ViewBodyReason::new(decompose_only.join("; ")))
    };
    (classification, Some(query.to_string()))
}

/// `group_by` is always present on `Select` (as the enum
/// `GroupByExpr`). "No GROUP BY" = `GroupByExpr::Expressions(vec![], vec![])`.
fn group_by_is_present(gb: &GroupByExpr) -> bool {
    match gb {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(items, modifiers) => !items.is_empty() || !modifiers.is_empty(),
    }
}

/// Extract the `SELECT` body from either a `CREATE VIEW ... AS <q>`
/// or a bare `<q>`.
fn extract_query(ast: &[Statement]) -> Option<&Query> {
    if ast.is_empty() {
        return None;
    }
    match &ast[0] {
        Statement::CreateView { query, .. } => Some(query.as_ref()),
        Statement::Query(q) => Some(q.as_ref()),
        _ => None,
    }
}

fn projection_has_aggregate(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_aggregate(e)
        }
        _ => false,
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    let mut hit = false;
    let _ = visit_expressions(expr, |e: &Expr| {
        if let Expr::Function(f) = e {
            if let Some(name) = f.name.0.last() {
                if is_aggregate_name(&name.value) {
                    hit = true;
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    hit
}

fn is_aggregate_name(n: &str) -> bool {
    matches!(
        n.to_ascii_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VAR_POP"
            | "VAR_SAMP"
            | "VARIANCE"
            | "CORR"
            | "COVAR_POP"
            | "COVAR_SAMP"
            | "MEDIAN"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "ANY_VALUE"
            | "APPROX_COUNT_DISTINCT"
            | "LISTAGG"
            | "ARRAY_AGG"
            | "OBJECT_AGG"
            | "STRING_AGG"
            | "BITAND_AGG"
            | "BITOR_AGG"
            | "BITXOR_AGG"
            | "BOOLAND_AGG"
            | "BOOLOR_AGG"
    )
}

fn projection_has_non_deterministic(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_non_deterministic(e)
        }
        _ => false,
    })
}

fn expr_has_non_deterministic(expr: &Expr) -> bool {
    let mut hit = false;
    let _ = visit_expressions(expr, |e: &Expr| {
        if let Expr::Function(f) = e {
            if let Some(name) = f.name.0.last() {
                if is_non_deterministic_name(&name.value) {
                    hit = true;
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    hit
}

fn is_non_deterministic_name(n: &str) -> bool {
    matches!(
        n.to_ascii_uppercase().as_str(),
        "CURRENT_TIMESTAMP"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "LOCALTIMESTAMP"
            | "LOCALTIME"
            | "SYSDATE"
            | "RANDOM"
            | "RANDSTR"
            | "UUID_STRING"
            | "SEQ1"
            | "SEQ2"
            | "SEQ4"
            | "SEQ8"
    )
}

/// Walk the whole statement tree and return true when a non-FROM
/// subquery is discovered (a subquery expression inside WHERE, a
/// projected column, etc.). FROM-subqueries (derived tables) are the
/// only form Snowflake allows for stream-on-view; everything else is
/// decompose-only.
fn contains_non_from_subquery(ast: &[Statement]) -> bool {
    let mut hit = false;
    for stmt in ast {
        let _ = visit_statements(stmt, |s: &Statement| {
            if let Statement::Query(q) = s {
                let _ = visit_expressions(q, |e: &Expr| {
                    if matches!(
                        e,
                        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
                    ) {
                        hit = true;
                        return ControlFlow::Break(());
                    }
                    ControlFlow::<()>::Continue(())
                });
            }
            ControlFlow::<()>::Continue(())
        });
        if hit {
            break;
        }
    }
    hit
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(sql: &str) -> ViewBodyClassification {
        classify_view_body(sql).0
    }

    #[test]
    fn plain_select_is_stream_compatible() {
        let c = classify("SELECT a, b FROM analytics.public.orders WHERE region = 'US'");
        assert_eq!(c, ViewBodyClassification::StreamCompatible);
    }

    #[test]
    fn inner_and_cross_join_are_stream_compatible() {
        let c = classify(
            "SELECT o.id, c.name FROM analytics.public.orders o \
             JOIN analytics.public.customers c ON c.id = o.customer_id",
        );
        assert_eq!(c, ViewBodyClassification::StreamCompatible);
    }

    #[test]
    fn union_all_is_stream_compatible() {
        let c = classify("SELECT a FROM t1 UNION ALL SELECT a FROM t2");
        assert_eq!(c, ViewBodyClassification::StreamCompatible);
    }

    #[test]
    fn group_by_is_decomposable_only() {
        let c = classify("SELECT region, COUNT(*) FROM analytics.public.orders GROUP BY region");
        match c {
            ViewBodyClassification::DecomposableOnly(r) => {
                assert!(
                    r.as_str().contains("GROUP BY") || r.as_str().contains("aggregate"),
                    "reason was {r}"
                );
            }
            other => panic!("expected DecomposableOnly, got {other:?}"),
        }
    }

    #[test]
    fn limit_is_decomposable_only() {
        let c = classify("SELECT * FROM orders LIMIT 100");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn distinct_is_decomposable_only() {
        let c = classify("SELECT DISTINCT region FROM orders");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn qualify_is_decomposable_only() {
        let c =
            classify("SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn FROM orders QUALIFY rn = 1");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn correlated_subquery_is_decomposable_only() {
        let c = classify(
            "SELECT o.*, (SELECT name FROM customers c WHERE c.id = o.customer_id) name \
             FROM orders o",
        );
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn random_in_projection_is_decomposable_only() {
        let c = classify("SELECT id, RANDOM() FROM orders");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn garbage_is_unsupported() {
        let c = classify("not actually sql at all");
        match c {
            ViewBodyClassification::Unsupported(r) => {
                assert!(r.as_str().starts_with("parse_failed"));
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn create_view_wrapper_is_unwrapped() {
        // `GET_DDL('VIEW', …)` returns the full CREATE VIEW statement.
        let (c, body) = classify_view_body(
            "CREATE OR REPLACE VIEW analytics.public.orders_us AS \
             SELECT * FROM analytics.public.orders WHERE region = 'US'",
        );
        assert_eq!(c, ViewBodyClassification::StreamCompatible);
        let body = body.expect("body text extracted");
        assert!(
            body.to_ascii_uppercase().contains("SELECT"),
            "body looked like {body:?}"
        );
    }

    #[test]
    fn translate_view_body_rewrites_snowflake_functions() {
        // `IFF` → CASE is part of the `functions` pass; exercising
        // it here proves the shim reaches `melt_core::translate`.
        let duck =
            translate_view_body("SELECT id, IFF(status = 'ok', 1, 0) AS ok_flag FROM orders")
                .expect("translate ok");
        assert!(
            duck.to_ascii_uppercase().contains("CASE WHEN"),
            "translated body was {duck}"
        );
        assert!(
            !duck.to_ascii_uppercase().contains("IFF("),
            "IFF still present in {duck}"
        );
    }

    #[test]
    fn translate_view_body_errors_on_unparseable_input() {
        let err = translate_view_body("this is not sql").unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("parse") || msg.contains("translate") || msg.contains("sql"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn order_by_is_decomposable_only() {
        let c = classify("SELECT * FROM orders ORDER BY id");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn union_other_than_all_is_decomposable_only() {
        let c = classify("SELECT a FROM t1 UNION SELECT a FROM t2");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }

    #[test]
    fn current_timestamp_in_projection_is_decomposable_only() {
        let c = classify("SELECT id, CURRENT_TIMESTAMP() FROM orders");
        assert!(matches!(c, ViewBodyClassification::DecomposableOnly(_)));
    }
}
