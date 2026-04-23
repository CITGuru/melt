//! Semi-structured (VARIANT / OBJECT / ARRAY) — Snowflake → DuckDB.
//!
//! - `PARSE_JSON(s)` → `s::JSON` (DuckDB's CAST to JSON parses on
//!   ingestion just like Snowflake's PARSE_JSON returns a VARIANT).
//! - `TO_JSON(x)` → `to_json(x)` (case-only; both engines use this).
//! - `OBJECT_CONSTRUCT(k, v, ...)` → `json_object(k, v, ...)`.
//! - `ARRAY_CONSTRUCT(...)` → `[ ... ]` (DuckDB's list literal).
//! - `GET_PATH(json, 'a.b.c')` → `json_extract(json, '$.a.b.c')`.
//!
//! For the GET / GET_PATH function family DuckDB's `json_extract`
//! returns JSON — usually fine for downstream, but if the caller
//! depends on the variant cast Snowflake exposes, the user can
//! continue to passthrough by adding the function name to
//! `classify::uses_snowflake_features`.

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions_mut, Array, CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, Statement, Value,
};

use super::TranslateResult;

pub fn rewrite(stmt: &mut Statement) -> TranslateResult<()> {
    let _ = visit_expressions_mut(stmt, |expr: &mut Expr| {
        if let Some(rewritten) = rewrite_expr(expr) {
            *expr = rewritten;
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(())
}

fn rewrite_expr(expr: &Expr) -> Option<Expr> {
    let Expr::Function(f) = expr else {
        return None;
    };
    let name = f.name.0.last()?.value.to_ascii_uppercase();
    let args = unnamed_args(f)?;

    match name.as_str() {
        "PARSE_JSON" if args.len() == 1 => Some(Expr::Cast {
            kind: CastKind::DoubleColon,
            expr: Box::new(args[0].clone()),
            data_type: DataType::JSON,
            format: None,
        }),

        "OBJECT_CONSTRUCT" if !args.is_empty() && args.len() % 2 == 0 => {
            Some(call("json_object", args))
        }

        "ARRAY_CONSTRUCT" => Some(Expr::Array(Array {
            elem: args,
            named: false,
        })),

        "GET_PATH" if args.len() == 2 => {
            // Convert `'a.b.c'` → `'$.a.b.c'`. Anything else stays as-is.
            let path = match &args[1] {
                Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
                _ => None,
            }?;
            let normalized = if path.starts_with('$') {
                path
            } else {
                format!("$.{}", path.trim_start_matches('.'))
            };
            Some(call(
                "json_extract",
                vec![
                    args[0].clone(),
                    Expr::Value(Value::SingleQuotedString(normalized)),
                ],
            ))
        }

        _ => None,
    }
}

fn unnamed_args(f: &Function) -> Option<Vec<Expr>> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    list.args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    use sqlparser::ast::FunctionArgumentList;
    let list = FunctionArgumentList {
        duplicate_treatment: None,
        args: args
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        clauses: vec![],
    };
    Expr::Function(Function {
        name: ObjectName(vec![Ident::new(name)]),
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(list),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
