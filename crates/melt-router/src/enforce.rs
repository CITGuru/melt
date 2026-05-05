//! `PolicyMode::Enforce` AST rewrite.
//!
//! For each `(table, view_name)` pair the sync subsystem advertised,
//! rewrite every reference in the parsed statement set to point at
//! the filtered view instead. The `view_name` already includes the
//! schema (and is shell-quoted) so we replace the whole `ObjectName`
//! and let the unparse step render it cleanly.

use std::ops::ControlFlow;

use melt_core::TableRef;
use sqlparser::ast::{visit_relations_mut, Ident, ObjectName, Statement};

/// Walk every relation in `ast`. If the resolved table matches one
/// of `tables` and we have a corresponding `Some(view)` entry, swap
/// the relation's `ObjectName` for the view's name.
///
/// Returns `Err(detail)` only on a structural failure — translation
/// passes still get to run after.
pub fn rewrite_views(
    ast: &mut [Statement],
    tables: &[TableRef],
    views: &[Option<String>],
) -> Result<(), String> {
    debug_assert_eq!(tables.len(), views.len());

    let mut owned = ast.to_vec();
    let res = visit_relations_mut(&mut owned, |obj: &mut ObjectName| {
        if let Some(view) = match_view(obj, tables, views) {
            *obj = parse_object_name(&view);
        }
        ControlFlow::<()>::Continue(())
    });
    if let ControlFlow::Break(_) = res {
        return Err("enforce: relation walk aborted".into());
    }
    for (slot, new) in ast.iter_mut().zip(owned) {
        *slot = new;
    }
    Ok(())
}

fn match_view(obj: &ObjectName, tables: &[TableRef], views: &[Option<String>]) -> Option<String> {
    let parts: Vec<String> = obj.0.iter().map(|p| p.value.clone()).collect();
    for (i, t) in tables.iter().enumerate() {
        let matches = match parts.as_slice() {
            [name] => name == &t.name,
            [schema, name] => schema == &t.schema && name == &t.name,
            [db, schema, name] => db == &t.database && schema == &t.schema && name == &t.name,
            _ => false,
        };
        if matches {
            if let Some(v) = views.get(i).and_then(|x| x.clone()) {
                return Some(v);
            }
        }
    }
    None
}

/// Parse `"schema"."view"` (with optional shell-quotes) back into an
/// `ObjectName`. Forgiving: anything we don't understand becomes a
/// single Ident so the unparse round-trip still produces valid SQL.
fn parse_object_name(s: &str) -> ObjectName {
    let parts: Vec<Ident> = s
        .split('.')
        .map(|p| {
            let trimmed = p.trim_matches('"');
            Ident {
                value: trimmed.to_string(),
                quote_style: Some('"'),
            }
        })
        .collect();
    ObjectName(if parts.is_empty() {
        vec![Ident::new(s)]
    } else {
        parts
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{parse, unparse};

    #[test]
    fn rewrites_qualified_ref() {
        let mut ast = parse("SELECT * FROM analytics.public.orders").unwrap();
        let tables = vec![TableRef::new("analytics", "public", "orders")];
        let views = vec![Some("\"public\".\"orders__melt_filtered\"".to_string())];
        rewrite_views(&mut ast, &tables, &views).unwrap();
        let out = unparse(&ast);
        assert!(out.contains("orders__melt_filtered"), "got {out}");
    }

    #[test]
    fn unmatched_left_alone() {
        let mut ast = parse("SELECT * FROM analytics.public.events").unwrap();
        let tables = vec![TableRef::new("analytics", "public", "orders")];
        let views = vec![Some("\"public\".\"orders__melt_filtered\"".to_string())];
        rewrite_views(&mut ast, &tables, &views).unwrap();
        let out = unparse(&ast);
        assert!(out.contains("events"), "got {out}");
        assert!(!out.contains("orders"), "got {out}");
    }
}
