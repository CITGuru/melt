//! Snowflake → DuckDB function rewrites.
//!
//! - `IFF(c, a, b)` → `CASE WHEN c THEN a ELSE b END`
//! - `DECODE(expr, val1, res1, ..., default?)` → `CASE` chain
//! - `EQUAL_NULL(a, b)` / `NULLIFEQUAL(a, b)` → `a IS NOT DISTINCT FROM b`
//! - `BOOLAND_AGG` / `BOOLOR_AGG` → `bool_and` / `bool_or`
//! - `ZEROIFNULL(x)` → `COALESCE(x, 0)`
//! - `NVL(a, b)` → `COALESCE(a, b)`
//! - `NVL2(a, b, c)` → `CASE WHEN a IS NOT NULL THEN b ELSE c END`
//!
//! Each rule mutates the AST in place via `visit_expressions_mut`.

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions_mut, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    Statement,
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
    let name = function_name(f)?;
    let args = unnamed_args(f)?;

    match name.to_ascii_uppercase().as_str() {
        "IFF" if args.len() == 3 => Some(Expr::Case {
            operand: None,
            conditions: vec![args[0].clone()],
            results: vec![args[1].clone()],
            else_result: Some(Box::new(args[2].clone())),
        }),

        "DECODE" if args.len() >= 3 => {
            let mut conditions = Vec::new();
            let mut results = Vec::new();
            let mut i = 1;
            while i + 1 < args.len() {
                conditions.push(Expr::IsNotDistinctFrom(
                    Box::new(args[0].clone()),
                    Box::new(args[i].clone()),
                ));
                results.push(args[i + 1].clone());
                i += 2;
            }
            let else_result = if i < args.len() {
                Some(Box::new(args[i].clone()))
            } else {
                None
            };
            Some(Expr::Case {
                operand: None,
                conditions,
                results,
                else_result,
            })
        }

        "EQUAL_NULL" | "NULLIFEQUAL" if args.len() == 2 => Some(Expr::IsNotDistinctFrom(
            Box::new(args[0].clone()),
            Box::new(args[1].clone()),
        )),

        "BOOLAND_AGG" if args.len() == 1 => Some(rename_function(f, "bool_and")),
        "BOOLOR_AGG" if args.len() == 1 => Some(rename_function(f, "bool_or")),

        "ZEROIFNULL" if args.len() == 1 => Some(call("COALESCE", vec![args[0].clone(), int(0)])),
        "NVL" if args.len() == 2 => Some(call("COALESCE", args.to_vec())),
        "NVL2" if args.len() == 3 => Some(Expr::Case {
            operand: None,
            conditions: vec![Expr::IsNotNull(Box::new(args[0].clone()))],
            results: vec![args[1].clone()],
            else_result: Some(Box::new(args[2].clone())),
        }),

        _ => None,
    }
}

fn function_name(f: &Function) -> Option<String> {
    Some(f.name.0.last()?.value.clone())
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

fn rename_function(f: &Function, new_name: &str) -> Expr {
    let mut new = f.clone();
    new.name = sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new(new_name)]);
    Expr::Function(new)
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    use sqlparser::ast::{FunctionArgumentList, Ident, ObjectName};
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

fn int(n: i64) -> Expr {
    Expr::Value(sqlparser::ast::Value::Number(n.to_string(), false))
}
