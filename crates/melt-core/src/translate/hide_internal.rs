//! Hide sync-internal columns from client-facing `SELECT *` by
//! rewriting wildcards as `SELECT * EXCLUDE (__row_id)`. CTE-output
//! wildcards and single-ident references that may shadow a CTE alias
//! are left alone — DuckDB errors on EXCLUDE for a missing column.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    ExcludeSelectItem, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    WildcardAdditionalOptions,
};

use super::TranslateResult;

/// Column name we materialise on every lake table. Kept in sync with
/// `melt-snowflake/src/client.rs::read_stream_since` (the CTAS that
/// promotes `METADATA$ROW_ID` to a real column) and
/// `melt-ducklake/src/sync/apply.rs::write_changes` (which writes it).
const ROW_ID_COLUMN: &str = "__row_id";

pub fn rewrite(stmt: &mut Statement) -> TranslateResult<()> {
    visit_stmt(stmt);
    Ok(())
}

/// Walk every `Query` embedded in `stmt` with an empty CTE scope.
fn visit_stmt(stmt: &mut Statement) {
    let mut ctx = CteScope::new();
    match stmt {
        Statement::Query(q) => walk_query(q, &mut ctx),
        Statement::Insert(ins) => {
            if let Some(source) = &mut ins.source {
                walk_query(source, &mut ctx);
            }
        }
        Statement::CreateView { query, .. } => walk_query(query, &mut ctx),
        _ => {}
    }
}

/// Tracks CTE names that are "in scope" as we descend. A
/// `TableFactor::Table` whose first-ident name matches a tracked
/// CTE is NOT a real base table — rewriting its wildcard with
/// `EXCLUDE (__row_id)` would attempt to strip a column that doesn't
/// exist on the CTE's output, and DuckDB errors on a non-existent
/// exclude target.
///
/// We model this as a stack of hash sets (each `WITH` introduces
/// a new frame, popped when we leave the query) so that nested
/// queries don't leak CTE names up into siblings.
#[derive(Default)]
struct CteScope {
    frames: Vec<HashSet<String>>,
}

impl CteScope {
    fn new() -> Self {
        Self::default()
    }

    fn push_frame(&mut self) {
        self.frames.push(HashSet::new());
    }

    fn pop_frame(&mut self) {
        self.frames.pop();
    }

    fn register(&mut self, name: &str) {
        if let Some(f) = self.frames.last_mut() {
            f.insert(name.to_ascii_uppercase());
        }
    }

    fn is_cte(&self, name: &str) -> bool {
        let upper = name.to_ascii_uppercase();
        self.frames.iter().any(|f| f.contains(&upper))
    }
}

fn walk_query(q: &mut Query, scope: &mut CteScope) {
    let has_with = q.with.is_some();
    if has_with {
        scope.push_frame();
    }

    // Register CTE names first so siblings can see each other (Snowflake/DuckDB allow forward refs).
    if let Some(with) = &q.with {
        for cte in &with.cte_tables {
            scope.register(&cte.alias.name.value);
        }
    }
    if let Some(with) = &mut q.with {
        for cte in &mut with.cte_tables {
            walk_query(&mut cte.query, scope);
        }
    }

    walk_set_expr(&mut q.body, scope);

    if has_with {
        scope.pop_frame();
    }
}

fn walk_set_expr(set_expr: &mut SetExpr, scope: &mut CteScope) {
    match set_expr {
        SetExpr::Select(select) => walk_select(select.as_mut(), scope),
        SetExpr::Query(inner) => walk_query(inner.as_mut(), scope),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left.as_mut(), scope);
            walk_set_expr(right.as_mut(), scope);
        }
        _ => {}
    }
}

fn walk_select(select: &mut Select, scope: &mut CteScope) {
    // Walk FROM first so subqueries get rewritten; then gate THIS
    // select's wildcards on a non-CTE base table being present.
    for table_with_joins in &mut select.from {
        walk_table_factor(&mut table_with_joins.relation, scope);
        for join in &mut table_with_joins.joins {
            walk_table_factor(&mut join.relation, scope);
        }
    }

    if !has_base_table(&select.from, scope) {
        return;
    }

    for item in &mut select.projection {
        match item {
            SelectItem::Wildcard(opts) | SelectItem::QualifiedWildcard(_, opts) => {
                add_row_id_exclude(opts);
            }
            _ => {}
        }
    }
}

fn walk_table_factor(tf: &mut TableFactor, scope: &mut CteScope) {
    match tf {
        TableFactor::Derived { subquery, .. } => walk_query(subquery.as_mut(), scope),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_factor(&mut table_with_joins.relation, scope);
            for join in &mut table_with_joins.joins {
                walk_table_factor(&mut join.relation, scope);
            }
        }
        _ => {}
    }
}

fn has_base_table(from: &[sqlparser::ast::TableWithJoins], scope: &CteScope) -> bool {
    let is_real_base = |tf: &TableFactor| match tf {
        TableFactor::Table { name, .. } => {
            // Single-ident name may shadow a CTE; multi-part names are always physical.
            if name.0.len() == 1 {
                !scope.is_cte(&name.0[0].value)
            } else {
                true
            }
        }
        _ => false,
    };

    for tw in from {
        if is_real_base(&tw.relation) {
            return true;
        }
        for join in &tw.joins {
            if is_real_base(&join.relation) {
                return true;
            }
        }
    }
    false
}

/// Attach `EXCLUDE (__row_id)` to a wildcard projection, merging with
/// any existing EXCLUDE the caller already wrote.
///
/// Cases:
/// * no existing EXCLUDE → `EXCLUDE (__row_id)`
/// * `EXCLUDE a` → `EXCLUDE (a, __row_id)`
/// * `EXCLUDE (a, b)` → `EXCLUDE (a, b, __row_id)`
/// * existing EXCLUDE already contains `__row_id` → no-op
fn add_row_id_exclude(opts: &mut WildcardAdditionalOptions) {
    let row_id = Ident::new(ROW_ID_COLUMN);
    let already_present = |cols: &[Ident]| {
        cols.iter()
            .any(|i| i.value.eq_ignore_ascii_case(ROW_ID_COLUMN))
    };

    opts.opt_exclude = Some(match opts.opt_exclude.take() {
        None => ExcludeSelectItem::Single(row_id),
        Some(ExcludeSelectItem::Single(existing)) => {
            if existing.value.eq_ignore_ascii_case(ROW_ID_COLUMN) {
                ExcludeSelectItem::Single(existing)
            } else {
                ExcludeSelectItem::Multiple(vec![existing, row_id])
            }
        }
        Some(ExcludeSelectItem::Multiple(mut cols)) => {
            if !already_present(&cols) {
                cols.push(row_id);
            }
            ExcludeSelectItem::Multiple(cols)
        }
    });
}

// `_` silences unused warning when `visit_statements_mut` pattern
// isn't used here; we walk manually above.
const _: fn() -> ControlFlow<()> = || ControlFlow::Continue(());

#[cfg(test)]
mod tests {
    use super::rewrite;
    use sqlparser::dialect::SnowflakeDialect;
    use sqlparser::parser::Parser;

    fn roundtrip(sql: &str) -> String {
        let mut ast = Parser::parse_sql(&SnowflakeDialect {}, sql).expect("parse");
        for s in ast.iter_mut() {
            rewrite(s).expect("rewrite");
        }
        ast.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(";\n")
    }

    #[test]
    fn bare_wildcard_on_base_table() {
        let out = roundtrip("SELECT * FROM TDM_BANK.MODELED.PROFILE_CUSTOMERS");
        // sqlparser 0.52 renders `ExcludeSelectItem::Single` as
        // `EXCLUDE col` (no parens). That's DuckDB-legal.
        assert!(out.contains("EXCLUDE __row_id"), "got: {out}");
    }

    #[test]
    fn qualified_wildcard_on_aliased_table() {
        let out = roundtrip("SELECT t.* FROM TDM_BANK.MODELED.PROFILE_CUSTOMERS t");
        assert!(out.contains("t.* EXCLUDE __row_id"), "got: {out}");
    }

    #[test]
    fn explicit_columns_are_untouched() {
        let out = roundtrip("SELECT id, name FROM T");
        assert!(!out.contains("EXCLUDE"), "got: {out}");
    }

    #[test]
    fn count_star_is_untouched() {
        // COUNT(*) is a function call, not a SelectItem::Wildcard; it
        // must survive the pass without picking up an EXCLUDE.
        let out = roundtrip("SELECT COUNT(*) FROM T");
        assert!(!out.contains("EXCLUDE"), "got: {out}");
    }

    #[test]
    fn existing_exclude_is_merged() {
        let out = roundtrip("SELECT * EXCLUDE (secret) FROM T");
        assert!(out.contains("secret"), "got: {out}");
        assert!(out.contains("__row_id"), "got: {out}");
    }

    #[test]
    fn double_run_is_idempotent() {
        let once = roundtrip("SELECT * FROM T");
        let twice = roundtrip(&once);
        // Should still mention __row_id, but not as a duplicate.
        assert!(twice.contains("__row_id"), "got: {twice}");
        assert_eq!(twice.matches("__row_id").count(), 1, "got: {twice}");
    }

    #[test]
    fn pure_cte_select_is_not_touched_outer_but_inner_is() {
        let out = roundtrip("WITH c AS (SELECT * FROM T) SELECT * FROM c");
        // Inner SELECT reads a base table → gets EXCLUDE.
        // Outer SELECT reads c (not a base table) → no EXCLUDE, else
        // DuckDB rejects "column __row_id not in output".
        let occurrences = out.matches("EXCLUDE").count();
        assert_eq!(occurrences, 1, "expected exactly one EXCLUDE, got: {out}");
    }

    #[test]
    fn nested_subquery_inner_only() {
        let out = roundtrip("SELECT * FROM (SELECT * FROM T) AS sub");
        // Outer wildcard reads the derived subquery (no base table in
        // its direct FROM) → skip. Inner wildcard reads base T →
        // EXCLUDE.
        let occurrences = out.matches("EXCLUDE").count();
        assert_eq!(occurrences, 1, "expected exactly one EXCLUDE, got: {out}");
    }

    #[test]
    fn joins_trigger_exclude() {
        let out = roundtrip("SELECT * FROM A JOIN B ON A.id = B.id");
        assert!(out.contains("EXCLUDE __row_id"), "got: {out}");
    }
}
