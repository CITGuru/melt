//! AST → [`HybridPlan`] builder.
//!
//! Heuristic-based v1: see the module-level doc in `mod.rs` for the
//! algorithm. Returns `BuildOutcome::NotHybrid` for queries that don't
//! reference any Remote tables, `BuildOutcome::Bail(reason)` for
//! shapes v1 can't handle (caller falls through to today's
//! Lake/Snowflake decision), and `BuildOutcome::Plan(plan)` on the
//! happy path.

use std::ops::ControlFlow;
use std::sync::Arc;

use melt_core::config::RouterConfig;
use melt_core::{
    AttachRewrite, HybridPlan, HybridReason, NodeKind, Placement, PlanNode, RemoteFragment,
    SessionInfo, TableRef, TableSourceRegistry,
};
use sqlparser::ast::{
    visit_relations_mut, Expr, Ident, ObjectName, Query, SetExpr, Statement, TableFactor,
    TableWithJoins,
};

use crate::hybrid::strategy::{build_chain_from_config, CollapseDecision, StrategyContext};
use crate::parse::unparse;
use crate::translate::translate_ast;

/// What [`build_hybrid_plan`] produced.
pub enum BuildOutcome {
    /// No Remote tables in the query. Caller should run today's
    /// Lake/Snowflake decision unchanged.
    NotHybrid,
    /// The shape isn't supported in v1. Caller should fall through to
    /// `Route::Snowflake { TranslationFailed }` (or the default
    /// passthrough). The string is for log + metric labels.
    Bail(String),
    /// Hybrid plan built successfully. Carries the per-table byte
    /// estimates (in input order) so the caller can apply the
    /// Materialize size cap before deciding whether to emit the route.
    Plan {
        plan: Arc<HybridPlan>,
        reason: HybridReason,
    },
}

/// Build a [`HybridPlan`] from an already-parsed Snowflake-dialect
/// AST. Mutates the input AST when applying Attach rewrites; the
/// result's `local_sql` is what should actually run on DuckDB.
///
/// `tables_in_order` is the same list `classify::extract_tables`
/// produces — passed in to avoid re-walking the AST.
///
/// `per_table_bytes` carries the `StorageBackend::estimate_scan_bytes`
/// result aligned with `tables_in_order`, used to compute the
/// `estimated_remote_bytes` field on the plan.
pub fn build_hybrid_plan(
    ast: &mut [Statement],
    session: &SessionInfo,
    tables_in_order: &[TableRef],
    per_table_bytes: &[u64],
    per_table_rows: &[u64],
    registry: &TableSourceRegistry,
    cfg: &RouterConfig,
) -> BuildOutcome {
    debug_assert_eq!(tables_in_order.len(), per_table_bytes.len());
    debug_assert_eq!(tables_in_order.len(), per_table_rows.len());

    // Bail-out: any statement mentioning a window function over a
    // Remote table goes to passthrough. Detected as a string probe
    // because precise AST matching for window-over-remote is fiddly
    // and v1's safe degradation is "punt to Snowflake."
    if mentions_window_over_remote(ast, registry, session) {
        return BuildOutcome::Bail("window_over_remote".into());
    }

    // Bail-out: set ops anywhere. UNION / INTERSECT / EXCEPT need
    // per-branch placement which v1 doesn't model.
    if mentions_set_op(ast) {
        return BuildOutcome::Bail("set_op".into());
    }

    // Partition referenced tables.
    let (remote_tables, _local_tables): (Vec<TableRef>, Vec<TableRef>) = tables_in_order
        .iter()
        .cloned()
        .partition(|t| registry.is_remote(t));

    if remote_tables.is_empty() {
        return BuildOutcome::NotHybrid;
    }

    // Per-table bytes for Remote tables, used for the Materialize cap.
    let remote_bytes_total: u64 = tables_in_order
        .iter()
        .zip(per_table_bytes.iter())
        .filter(|(t, _)| registry.is_remote(t))
        .map(|(_, b)| *b)
        .sum();

    // ── Strategy decision ────────────────────────────────────────
    //
    // v1 algorithm (aligns with §4.7 `choose_strategy` + the Python
    // regression variants):
    //
    // 1. Walk the AST looking for all-remote SUBQUERIES with 2+
    //    scans — those collapse into one Materialize fragment each.
    //    Single-scan subqueries fall through to the Attach step
    //    below (DuckDB's extension does predicate pushdown better
    //    than we'd approximate with a Materialize fragment).
    //
    // 2. If the OUTER query is all-remote AND has 2+ remote scans
    //    not already consumed by subquery collapse, collapse the
    //    whole statement into one Materialize fragment.
    //
    // 3. Every Remote ObjectName still standing after steps 1-2
    //    gets an Attach rewrite.
    let mut remote_fragments: Vec<RemoteFragment> = Vec::new();
    let mut attach_rewrites: Vec<AttachRewrite> = Vec::new();

    // Strategy chain decides the collapse floor for this query based
    // on its configured chain (default `["heuristic"]` ⇒ today's
    // behaviour exactly). When `["cost", "heuristic"]` is configured,
    // the cost strategy may flip the floor for single-table queries
    // when its cost equations show a clear advantage; otherwise the
    // heuristic answers and behaviour is unchanged.
    //
    // Strategy is consulted ONCE per build, against the top-level
    // remote totals. Inner subqueries inherit the same floor — for
    // v1 this is acceptable because the cost strategy abstains for
    // multi-table subtrees anyway, and inner all-remote subqueries
    // are typically the same size class as the outer query.
    let remote_per_table_rows: Vec<u64> = tables_in_order
        .iter()
        .zip(per_table_rows.iter())
        .filter(|(t, _)| registry.is_remote(t))
        .map(|(_, r)| *r)
        .collect();
    let remote_per_table_bytes: Vec<u64> = tables_in_order
        .iter()
        .zip(per_table_bytes.iter())
        .filter(|(t, _)| registry.is_remote(t))
        .map(|(_, b)| *b)
        .collect();
    let strategy_chain = build_chain_from_config(&cfg.hybrid_strategy);
    let strategy_ctx = StrategyContext {
        scanned_tables: &remote_tables,
        per_table_rows: &remote_per_table_rows,
        per_table_bytes: &remote_per_table_bytes,
        attach_runtime_enabled: cfg.hybrid_attach_enabled,
    };
    let chain_member_names: Vec<String> = strategy_chain
        .member_names()
        .into_iter()
        .map(String::from)
        .collect();
    let (decision, decided_by) = strategy_chain.decide(&strategy_ctx);
    metrics::counter!(
        melt_metrics::HYBRID_STRATEGY_DECISIONS,
        melt_metrics::LABEL_STRATEGY => decided_by,
        "decision" => match decision {
            CollapseDecision::Collapse => "collapse",
            CollapseDecision::Skip => "skip",
        },
    )
    .increment(1);
    let collapse_floor = match decision {
        CollapseDecision::Collapse => 1,
        CollapseDecision::Skip => 2,
    };
    collapse_all_remote_subqueries(
        ast,
        session,
        registry,
        &mut remote_fragments,
        collapse_floor,
        decided_by,
    );
    try_collapse_top_level_statements(
        ast,
        session,
        registry,
        &mut remote_fragments,
        collapse_floor,
        decided_by,
    );
    if let Err(e) = rewrite_attach_in_place(
        ast,
        registry,
        session,
        &mut attach_rewrites,
        cfg,
        decided_by,
    ) {
        return BuildOutcome::Bail(format!("attach_rewrite: {e}"));
    }

    // Translate the rewritten AST to DuckDB dialect for `local_sql`.
    if let Err(e) = translate_ast(ast) {
        return BuildOutcome::Bail(format!("translate_ast: {e}"));
    }
    let local_sql = unparse(ast);

    // Pick a HybridReason. Phase 0 only fires `RemoteByConfig` because
    // the trigger-case toggles (`hybrid_allow_bootstrapping`,
    // `hybrid_allow_oversize`) aren't wired into the router yet (Phase 4).
    let reason = HybridReason::RemoteByConfig;

    // Build a minimal annotated tree for EXPLAIN / observability.
    // v1 keeps it shallow — a root that lists every Remote scan as a
    // child. Sufficient for the `melt route` text output; richer
    // tree-building lands when `pushdown_federable_subplans` does.
    let root = build_explain_tree(&remote_tables, &remote_fragments, &attach_rewrites);

    let plan = Arc::new(HybridPlan {
        root,
        remote_fragments,
        attach_rewrites,
        local_sql,
        estimated_remote_bytes: remote_bytes_total,
        strategy_chain: chain_member_names,
        chain_decided_by: decided_by.to_string(),
    });

    BuildOutcome::Plan { plan, reason }
}

/// Build a shallow PlanNode tree for `melt route` EXPLAIN output.
/// Root is a Project node with one child per Remote scan, annotated
/// with its strategy via the placement label.
fn build_explain_tree(
    remote_tables: &[TableRef],
    fragments: &[RemoteFragment],
    rewrites: &[AttachRewrite],
) -> PlanNode {
    let mut id = 1u32;
    let mut children: Vec<PlanNode> = Vec::new();
    for frag in fragments {
        let node = PlanNode::new(
            id,
            NodeKind::RemoteSql {
                sql: frag.snowflake_sql.clone(),
                tables: frag.scanned_tables.clone(),
            },
            Placement::Remote,
        );
        id += 1;
        children.push(node);
    }
    for rw in rewrites {
        let node = PlanNode::new(
            id,
            NodeKind::Scan {
                table: rw.original.clone(),
            },
            Placement::Remote,
        );
        id += 1;
        children.push(node);
    }
    let _ = remote_tables; // referenced for symmetry with the doc; the
                           // actual scan list is reconstructed from
                           // fragments + rewrites above.
    PlanNode::new(
        0,
        NodeKind::Project {
            columns: vec!["<root>".into()],
        },
        Placement::Local,
    )
    .with_children(children)
}

/// Walk every `Query` subtree (subqueries + CTE bodies + main). For
/// each one whose every table reference is Remote, render it as a
/// Materialize fragment and replace its body with `SELECT * FROM
/// __remote_N`.
///
/// `min_scans` is the per-subquery scan-count threshold — 2 in the
/// default mixed-strategy mode (single-scan subqueries are left for
/// the Attach-rewrite pass), or 1 when the runtime has disabled
/// Attach (`hybrid_attach_enabled = false` or the pool reports
/// `sf_link_available() == false`). With `min_scans = 1` every
/// all-remote subquery becomes a Materialize fragment, producing a
/// pure-Materialize plan that doesn't need `sf_link.*` aliases at
/// execute-time.
fn collapse_all_remote_subqueries(
    ast: &mut [Statement],
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    for stmt in ast.iter_mut() {
        if let Statement::Query(q) = stmt {
            collapse_in_query(q, session, registry, fragments, min_scans, decided_by);
        }
    }
}

fn collapse_in_query(
    q: &mut Query,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    // Top-down: visit children first, then check the current Query.
    // Top-down means we collapse the OUTERMOST all-remote subquery,
    // which is the right call — collapsing inside-out would bind us
    // to the smallest subtree and lose merge opportunities.
    //
    // Apart from CTEs (which we descend into), the structure we walk
    // is `q.body: SetExpr`. For a plain Select, we look at WHERE /
    // SELECT-list expressions for `Expr::Subquery` and
    // `Expr::InSubquery`.
    if let SetExpr::Select(select) = q.body.as_mut() {
        for item in select.projection.iter_mut() {
            walk_select_item_for_subqueries(
                item,
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            );
        }
        if let Some(where_expr) = select.selection.as_mut() {
            walk_expr_for_subqueries(
                where_expr,
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            );
        }
        for join_or_table in select.from.iter_mut() {
            walk_table_with_joins_for_subqueries(
                join_or_table,
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            );
        }
    }

    if let Some(with) = q.with.as_mut() {
        for cte in with.cte_tables.iter_mut() {
            collapse_in_query(
                cte.query.as_mut(),
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            );
        }
    }
}

fn walk_select_item_for_subqueries(
    item: &mut sqlparser::ast::SelectItem,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            walk_expr_for_subqueries(e, session, registry, fragments, min_scans, decided_by);
        }
        _ => {}
    }
}

fn walk_table_with_joins_for_subqueries(
    twj: &mut TableWithJoins,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    walk_table_factor_for_subqueries(
        &mut twj.relation,
        session,
        registry,
        fragments,
        min_scans,
        decided_by,
    );
    for join in twj.joins.iter_mut() {
        walk_table_factor_for_subqueries(
            &mut join.relation,
            session,
            registry,
            fragments,
            min_scans,
            decided_by,
        );
    }
}

fn walk_table_factor_for_subqueries(
    tf: &mut TableFactor,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    if let TableFactor::Derived { subquery, .. } = tf {
        if try_collapse_query(
            subquery.as_mut(),
            session,
            registry,
            fragments,
            min_scans,
            decided_by,
        ) {
            return;
        }
        collapse_in_query(
            subquery.as_mut(),
            session,
            registry,
            fragments,
            min_scans,
            decided_by,
        );
    }
}

fn walk_expr_for_subqueries(
    expr: &mut Expr,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            if !try_collapse_query(
                q.as_mut(),
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            ) {
                collapse_in_query(
                    q.as_mut(),
                    session,
                    registry,
                    fragments,
                    min_scans,
                    decided_by,
                );
            }
        }
        Expr::InSubquery {
            subquery,
            expr: inner,
            ..
        } => {
            walk_expr_for_subqueries(inner, session, registry, fragments, min_scans, decided_by);
            if !try_collapse_query(
                subquery.as_mut(),
                session,
                registry,
                fragments,
                min_scans,
                decided_by,
            ) {
                collapse_in_query(
                    subquery.as_mut(),
                    session,
                    registry,
                    fragments,
                    min_scans,
                    decided_by,
                );
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr_for_subqueries(left, session, registry, fragments, min_scans, decided_by);
            walk_expr_for_subqueries(right, session, registry, fragments, min_scans, decided_by);
        }
        Expr::UnaryOp { expr: e, .. } => {
            walk_expr_for_subqueries(e, session, registry, fragments, min_scans, decided_by)
        }
        Expr::Nested(e) => {
            walk_expr_for_subqueries(e, session, registry, fragments, min_scans, decided_by)
        }
        Expr::Cast { expr: e, .. } => {
            walk_expr_for_subqueries(e, session, registry, fragments, min_scans, decided_by)
        }
        Expr::Function(f) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            if let FunctionArguments::List(args) = &mut f.args {
                for a in args.args.iter_mut() {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = a
                    {
                        walk_expr_for_subqueries(
                            e,
                            session,
                            registry,
                            fragments,
                            min_scans,
                            decided_by,
                        );
                    }
                }
            }
        }
        _ => {}
    }
}

/// If `q` references only Remote tables AND has 2+ distinct scans,
/// replace its body with `SELECT * FROM __remote_N` and push the
/// rewritten SQL (sf_link aliases + passed through `translate_ast`)
/// into `fragments`. Returns `true` if collapsed.
///
/// Single-scan all-remote subqueries are left alone — the outer
/// Attach rewrite handles them more efficiently (DuckDB extension's
/// predicate pushdown beats a whole-subquery materialization). The
/// `BridgeStrategy` selector (§4.7) formalizes this: tables.len() == 1
/// → Attach, tables.len() >= 2 → Materialize.
///
/// **Fragment SQL dialect note.** v1 stores the fragment in the form
/// the DuckDB connection (with `sf_link` attached) will execute —
/// table refs prefixed with `sf_link.<db>.<schema>.<table>` and
/// Snowflake-specific functions already translated to DuckDB. This
/// lets `execute_hybrid` stage it directly as a `CREATE TEMP TABLE
/// __remote_N AS <sql>` without a second rewrite pass. Parity
/// sampler (§12) strips the `sf_link.` prefix when it needs the
/// original Snowflake-dialect form for replay.
fn try_collapse_query(
    q: &mut Query,
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) -> bool {
    let scanned = relations_in_query(q, session);
    if scanned.len() < min_scans {
        return false;
    }
    if !scanned.iter().all(|t| registry.is_remote(t)) {
        return false;
    }

    // Rewrite ObjectNames in the subtree to sf_link form so the
    // DuckDB-side execution resolves them through the attached
    // Snowflake catalog. Then translate to DuckDB dialect.
    let mut fragment_ast: Vec<Statement> = vec![Statement::Query(Box::new(q.clone()))];
    let mut discarded_rewrites: Vec<AttachRewrite> = Vec::new();
    if let Err(e) = rewrite_attach_in_place(
        &mut fragment_ast,
        registry,
        session,
        &mut discarded_rewrites,
        // cfg unused inside; pass defaults
        &RouterConfig::default(),
        decided_by,
    ) {
        tracing::debug!(error = %e, "hybrid fragment rewrite failed; skipping collapse");
        return false;
    }
    if let Err(e) = translate_ast(&mut fragment_ast) {
        tracing::debug!(error = %e, "hybrid fragment translate failed; skipping collapse");
        return false;
    }
    let snowflake_sql = unparse(&fragment_ast);
    let placeholder = format!("__remote_{}", fragments.len());

    fragments.push(RemoteFragment {
        placeholder: placeholder.clone(),
        snowflake_sql,
        scanned_tables: scanned,
        decided_by_strategy: decided_by.to_string(),
    });

    // Replace the Query body with `SELECT * FROM __remote_N` and
    // clear the WITH / ORDER BY / LIMIT (they were folded into the
    // fragment SQL).
    *q = parse_placeholder_query(&placeholder);
    true
}

/// After `collapse_all_remote_subqueries`, also try to collapse the
/// top-level Query in each statement. Handles the "SELECT FROM A JOIN
/// B" all-remote case that has no inner subqueries but should still
/// materialize as one fragment (variants 52/53/55/57).
fn try_collapse_top_level_statements(
    ast: &mut [Statement],
    session: &SessionInfo,
    registry: &TableSourceRegistry,
    fragments: &mut Vec<RemoteFragment>,
    min_scans: usize,
    decided_by: &str,
) {
    for stmt in ast.iter_mut() {
        if let Statement::Query(q) = stmt {
            try_collapse_query(q, session, registry, fragments, min_scans, decided_by);
        }
    }
}

fn parse_placeholder_query(placeholder: &str) -> Query {
    let sql = format!("SELECT * FROM {placeholder}");
    let mut ast =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::SnowflakeDialect {}, &sql)
            .expect("parse_placeholder_query: synthesized SQL must parse");
    match ast.remove(0) {
        Statement::Query(q) => *q,
        _ => unreachable!("synthesized statement is a Query"),
    }
}

/// All resolved table refs reachable from `q`, **preserving
/// duplicates** — a self-join `FROM t a JOIN t b` returns `[t, t]`.
/// This is critical for the strategy selector: a self-join counts
/// as 2 scans and should emerge as a Materialize fragment, not an
/// Attach rewrite that would lose the Snowflake-side join semantics.
fn relations_in_query(q: &Query, session: &SessionInfo) -> Vec<TableRef> {
    let stmt = vec![Statement::Query(Box::new(q.clone()))];
    let mut out: Vec<TableRef> = Vec::new();
    let _ = sqlparser::ast::visit_relations(&stmt, |obj: &ObjectName| {
        if let Some(t) = resolve(obj, session) {
            out.push(t);
        }
        ControlFlow::<()>::Continue(())
    });
    out
}

/// Rewrite every Remote ObjectName in `ast` to its Attach alias
/// (`sf_link.<db>.<schema>.<table>`). Records each rewrite in
/// `rewrites`. Skips `__remote_N` placeholders (1-part names) and
/// non-Remote tables.
///
/// Per-Attach-scan size cap (`hybrid_max_attach_scan_bytes`) is
/// enforced upstream by the caller before this function runs; v1 does
/// not silently downgrade an over-cap Attach to Materialize here
/// because the design's `phase1-rewrite-remote` checkpoint deliberately
/// keeps that check in the orchestrator (caller can decide whether to
/// bail to Snowflake or build a fresh plan with the over-cap node
/// forced to Materialize). v1 of this builder just does the rewrite.
fn rewrite_attach_in_place(
    ast: &mut [Statement],
    registry: &TableSourceRegistry,
    session: &SessionInfo,
    rewrites: &mut Vec<AttachRewrite>,
    _cfg: &RouterConfig,
    decided_by: &str,
) -> Result<(), String> {
    let mut owned = ast.to_vec();
    let res = visit_relations_mut(&mut owned, |obj: &mut ObjectName| {
        let parts: Vec<String> = obj.0.iter().map(|p| p.value.clone()).collect();

        // Skip the `__remote_N` placeholders we just inserted —
        // they're 1-part names that look like local tables.
        if parts.len() == 1 && parts[0].starts_with("__remote_") {
            return ControlFlow::<()>::Continue(());
        }

        let resolved = match resolve(obj, session) {
            Some(t) => t,
            None => return ControlFlow::Continue(()),
        };
        if !registry.is_remote(&resolved) {
            return ControlFlow::Continue(());
        }

        // Build the 4-part `sf_link.<db>.<schema>.<table>` name.
        // The plan §7 calls out that this MUST be 4-part so that
        // `strip_database` (which only touches 3-part names) leaves
        // it alone.
        let alias_reference = format!(
            "sf_link.{}.{}.{}",
            resolved.database, resolved.schema, resolved.name
        );
        let aliased = ObjectName(vec![
            Ident::new("sf_link"),
            Ident::new(&resolved.database),
            Ident::new(&resolved.schema),
            Ident::new(&resolved.name),
        ]);
        // Record once per unique table; subsequent scans share the
        // rewrite entry.
        if !rewrites.iter().any(|r| r.original == resolved) {
            rewrites.push(AttachRewrite {
                original: resolved,
                alias_reference,
                decided_by_strategy: decided_by.to_string(),
            });
        }
        *obj = aliased;
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(_) = res {
        return Err("relation walk aborted".into());
    }
    for (slot, new) in ast.iter_mut().zip(owned.into_iter()) {
        *slot = new;
    }
    Ok(())
}

fn resolve(obj: &ObjectName, session: &SessionInfo) -> Option<TableRef> {
    let parts: Vec<String> = obj
        .0
        .iter()
        .map(|p| p.value.clone())
        .filter(|s| !s.is_empty())
        .collect();
    match parts.len() {
        1 => Some(TableRef::new(
            session.database.clone()?,
            session.schema.clone()?,
            parts[0].clone(),
        )),
        2 => Some(TableRef::new(
            session.database.clone()?,
            parts[0].clone(),
            parts[1].clone(),
        )),
        3 => Some(TableRef::new(
            parts[0].clone(),
            parts[1].clone(),
            parts[2].clone(),
        )),
        _ => None,
    }
}

/// String-probe for window functions over Remote tables.
/// V1 over-conservative: any presence of `OVER (` or `QUALIFY` AND any
/// presence of a Remote table FQN string in the rendered AST → bail.
/// Catches variant 64 and similar shapes.
fn mentions_window_over_remote(
    ast: &[Statement],
    registry: &TableSourceRegistry,
    session: &SessionInfo,
) -> bool {
    let lowered = unparse(ast).to_lowercase();
    if !lowered.contains("over (") && !lowered.contains("qualify") {
        return false;
    }
    // Window present. Bail if any referenced table is Remote — we
    // don't know how to safely place the window above a Bridge.
    let tables = relations_in_statements(ast, session);
    tables.iter().any(|t| registry.is_remote(t))
}

fn relations_in_statements(ast: &[Statement], session: &SessionInfo) -> Vec<TableRef> {
    let stmt = ast.to_vec();
    let mut out: Vec<TableRef> = Vec::new();
    let _ = sqlparser::ast::visit_relations(&stmt, |obj: &ObjectName| {
        if let Some(t) = resolve(obj, session) {
            if !out.iter().any(|x| x == &t) {
                out.push(t);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    out
}

fn mentions_set_op(ast: &[Statement]) -> bool {
    for stmt in ast {
        if let Statement::Query(q) = stmt {
            if has_set_op(&q.body) {
                return true;
            }
        }
    }
    false
}

fn has_set_op(body: &SetExpr) -> bool {
    match body {
        SetExpr::SetOperation { .. } => true,
        SetExpr::Query(q) => has_set_op(&q.body),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse;
    use melt_core::SessionInfo;

    fn session() -> SessionInfo {
        let mut s = SessionInfo::new("test", 1);
        s.database = Some("D".into());
        s.schema = Some("S".into());
        s
    }

    fn registry(remote: &[(&str, &str, &str)]) -> TableSourceRegistry {
        TableSourceRegistry::from_iter(remote.iter().map(|(d, s, n)| TableRef::new(*d, *s, *n)))
    }

    fn cfg() -> RouterConfig {
        RouterConfig {
            hybrid_execution: true,
            ..RouterConfig::default()
        }
    }

    fn run_build(sql: &str, remote: &[(&str, &str, &str)]) -> BuildOutcome {
        let mut ast = parse(sql).unwrap();
        let session = session();
        let reg = registry(remote);
        let tables = crate::classify::extract_tables(&ast, &session);
        let bytes = vec![1u64; tables.len()];
        let rows = vec![1u64; tables.len()];
        build_hybrid_plan(&mut ast, &session, &tables, &bytes, &rows, &reg, &cfg())
    }

    #[test]
    fn no_remote_returns_not_hybrid() {
        let outcome = run_build("SELECT * FROM D.S.T", &[]);
        assert!(matches!(outcome, BuildOutcome::NotHybrid));
    }

    #[test]
    fn single_remote_scan_picks_attach() {
        let outcome = run_build(
            "SELECT * FROM REMOTE.PUB.USERS",
            &[("REMOTE", "PUB", "USERS")],
        );
        let (plan, _) = match outcome {
            BuildOutcome::Plan { plan, reason } => (plan, reason),
            other => panic!("expected Plan, got {:?}", outcome_label(&other)),
        };
        // Single-scan remote → Attach rewrite (matches `choose_strategy`
        // in emit.rs and the variant 50/70 regression expectation).
        // No Materialize fragment — DuckDB's extension handles the
        // single scan with native predicate pushdown.
        assert_eq!(plan.remote_fragments.len(), 0);
        assert_eq!(plan.attach_rewrites.len(), 1);
        assert!(
            plan.local_sql.contains("sf_link"),
            "Attach alias expected in local_sql; got {}",
            plan.local_sql
        );
    }

    #[test]
    fn all_remote_two_tables_collapses_to_one_fragment() {
        let outcome = run_build(
            "SELECT a.x, b.y FROM REMOTE.PUB.A a JOIN REMOTE.PUB.B b ON a.id = b.id",
            &[("REMOTE", "PUB", "A"), ("REMOTE", "PUB", "B")],
        );
        let plan = match outcome {
            BuildOutcome::Plan { plan, .. } => plan,
            _ => panic!("expected Plan"),
        };
        assert_eq!(plan.remote_fragments.len(), 1);
        assert_eq!(plan.attach_rewrites.len(), 0);
        let frag = &plan.remote_fragments[0];
        assert!(
            frag.snowflake_sql.contains("REMOTE.PUB.A") || frag.snowflake_sql.contains("\"A\"")
        );
        assert!(plan.local_sql.contains("__remote_0"), "{}", plan.local_sql);
    }

    #[test]
    fn mixed_local_and_remote_uses_attach() {
        let outcome = run_build(
            "SELECT * FROM D.S.LOCAL_T l JOIN REMOTE.PUB.USERS u ON u.id = l.id",
            &[("REMOTE", "PUB", "USERS")],
        );
        let plan = match outcome {
            BuildOutcome::Plan { plan, .. } => plan,
            _ => panic!("expected Plan"),
        };
        // 1 remote + 1 local, mixed → Attach for the remote
        assert_eq!(plan.remote_fragments.len(), 0);
        assert_eq!(plan.attach_rewrites.len(), 1);
        let rw = &plan.attach_rewrites[0];
        assert_eq!(rw.original, TableRef::new("REMOTE", "PUB", "USERS"));
        assert!(plan.local_sql.contains("sf_link"), "{}", plan.local_sql);
    }

    #[test]
    fn all_remote_subquery_in_in_collapses_to_materialize() {
        // Mixed query with an all-remote IN subquery — the subquery
        // collapses to Materialize, the outer Local table stays Local.
        let outcome = run_build(
            "SELECT * FROM D.S.LOCAL_T \
             WHERE id IN (SELECT id FROM REMOTE.PUB.A JOIN REMOTE.PUB.B USING (id))",
            &[("REMOTE", "PUB", "A"), ("REMOTE", "PUB", "B")],
        );
        let plan = match outcome {
            BuildOutcome::Plan { plan, .. } => plan,
            _ => panic!("expected Plan"),
        };
        assert_eq!(
            plan.remote_fragments.len(),
            1,
            "expected one Materialize fragment for the IN subquery"
        );
        assert_eq!(
            plan.attach_rewrites.len(),
            0,
            "no leftover Remote scans should remain after subquery collapse"
        );
    }

    #[test]
    fn set_op_bails() {
        let outcome = run_build(
            "SELECT * FROM REMOTE.PUB.A UNION SELECT * FROM REMOTE.PUB.B",
            &[("REMOTE", "PUB", "A"), ("REMOTE", "PUB", "B")],
        );
        match outcome {
            BuildOutcome::Bail(reason) => assert!(reason.contains("set_op")),
            _ => panic!("expected Bail"),
        }
    }

    #[test]
    fn window_function_over_remote_bails() {
        let outcome = run_build(
            "SELECT u.* FROM REMOTE.PUB.USERS u QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1",
            &[("REMOTE", "PUB", "USERS")],
        );
        match outcome {
            BuildOutcome::Bail(reason) => assert!(reason.contains("window")),
            _ => panic!("expected Bail"),
        }
    }

    #[test]
    fn attach_rewrite_survives_strip_database() {
        // Critical correctness invariant from §7 of the design doc:
        // the Attach rewrite must produce 4-part names so
        // `strip_database` (which drops the leading segment of any
        // 3-part ObjectName) doesn't silently shorten them.
        let outcome = run_build(
            "SELECT * FROM D.S.LOCAL_T l JOIN REMOTE.PUB.USERS u ON u.id = l.id",
            &[("REMOTE", "PUB", "USERS")],
        );
        let plan = match outcome {
            BuildOutcome::Plan { plan, .. } => plan,
            _ => panic!("expected Plan"),
        };
        // The local_sql here has already been through translate_ast,
        // including strip_database. If the Attach rewrite produced
        // 3-part `sf_link.PUB.USERS`, strip_database would have left
        // us with `PUB.USERS` (the database segment dropped) — exactly
        // the silent-correctness-bug §7 warns about.
        assert!(
            plan.local_sql.contains("sf_link") || plan.local_sql.contains("\"sf_link\""),
            "Attach reference must survive strip_database; got: {}",
            plan.local_sql
        );
    }

    #[test]
    fn materialize_placeholder_survives_strip_database() {
        // Same invariant for `__remote_N` (1-part names that
        // strip_database also leaves alone).
        let outcome = run_build(
            "SELECT * FROM REMOTE.PUB.A JOIN REMOTE.PUB.B USING (id)",
            &[("REMOTE", "PUB", "A"), ("REMOTE", "PUB", "B")],
        );
        let plan = match outcome {
            BuildOutcome::Plan { plan, .. } => plan,
            _ => panic!("expected Plan"),
        };
        assert!(
            plan.local_sql.contains("__remote_0"),
            "Materialize placeholder must survive strip_database; got: {}",
            plan.local_sql
        );
    }

    fn outcome_label(o: &BuildOutcome) -> &'static str {
        match o {
            BuildOutcome::NotHybrid => "NotHybrid",
            BuildOutcome::Bail(_) => "Bail",
            BuildOutcome::Plan { .. } => "Plan",
        }
    }
}
