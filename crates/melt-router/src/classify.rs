use std::ops::ControlFlow;

use melt_core::{SessionInfo, TableRef};
use sqlparser::ast::{visit_relations, ObjectName, Statement};

/// Returns true if the statement set contains anything that mutates
/// state. We forward all writes to Snowflake — Melt's lake copy is
/// read-only from the proxy's perspective.
pub fn is_write(ast: &[Statement]) -> bool {
    ast.iter().any(|s| {
        matches!(
            s,
            Statement::Insert { .. }
                | Statement::Update { .. }
                | Statement::Delete { .. }
                | Statement::Merge { .. }
                | Statement::Truncate { .. }
                | Statement::CreateTable { .. }
                | Statement::CreateView { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateDatabase { .. }
                | Statement::AlterTable { .. }
                | Statement::Drop { .. }
                | Statement::Grant { .. }
                | Statement::Revoke { .. }
        )
    })
}

/// Detect Snowflake-only constructs we don't yet translate. Returns
/// the marketing name to label `melt_router_decisions_total`.
///
/// This is intentionally conservative — anything that smells like
/// Snowflake-specific magic short-circuits to passthrough.
pub fn uses_snowflake_features(ast: &[Statement]) -> Option<&'static str> {
    let lowered: String = ast
        .iter()
        .map(|s| s.to_string().to_lowercase())
        .collect::<Vec<_>>()
        .join(" ");

    const PROBES: &[(&str, &str)] = &[
        ("call_udf", "Snowpark"),
        ("call_sproc", "Snowpark"),
        ("time_slice(", "TIME_SLICE"),
        ("table(generator(", "GENERATOR"),
        ("flatten(", "FLATTEN"),
        ("any_value(", "ANY_VALUE"),
        ("information_schema", "INFORMATION_SCHEMA"),
        ("at(timestamp", "Time Travel"),
        ("before(timestamp", "Time Travel"),
        ("at(offset", "Time Travel"),
        ("at(statement", "Time Travel"),
    ];
    PROBES.iter().find_map(|(needle, label)| {
        if lowered.contains(needle) {
            Some(*label)
        } else {
            None
        }
    })
}

/// Walk every relation in the AST and resolve unqualified names
/// against the session's default database/schema.
///
/// Resolution rules:
/// 1. `db.schema.name` → used as-is.
/// 2. `schema.name`     → `(session.database, schema, name)`.
/// 3. `name`            → `(session.database, session.schema, name)`.
pub fn extract_tables(ast: &[Statement], session: &SessionInfo) -> Vec<TableRef> {
    let mut out: Vec<TableRef> = Vec::new();
    let _ = visit_relations(&ast.to_vec(), |obj: &ObjectName| {
        if let Some(t) = resolve(obj, session) {
            if !out.iter().any(|x| x == &t) {
                out.push(t);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    out
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
