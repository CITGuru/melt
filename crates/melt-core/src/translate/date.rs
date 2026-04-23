//! Snowflake date/time → DuckDB rewrites.
//!
//! Snowflake accepts unit identifiers as bare keywords:
//!     `DATEADD(day, 7, x)`     `DATEDIFF(month, a, b)`
//!     `DATE_TRUNC(quarter, x)`
//!
//! DuckDB requires a string literal:
//!     `DATEADD('day', 7, x)`   `DATEDIFF('month', a, b)`
//!     `DATE_TRUNC('quarter', x)`
//!
//! We rewrite the leading bare-identifier argument into a string
//! literal. `TIMESTAMPADD` / `TIMESTAMPDIFF` follow the same shape.
//!
//! `CONVERT_TIMEZONE(tz, ts)` → `ts AT TIME ZONE tz`. The two-arg form
//! is the common case from BI tools; the three-arg form
//! `CONVERT_TIMEZONE(src, dst, ts)` is left alone (less common, more
//! semantics to preserve).

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions_mut, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    Statement, Value,
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

    match name.as_str() {
        "DATEADD" | "DATEDIFF" | "TIMESTAMPADD" | "TIMESTAMPDIFF" | "DATE_TRUNC" => {
            stringify_first_arg(f)
        }
        "CONVERT_TIMEZONE" => convert_timezone(f),
        _ => None,
    }
}

fn stringify_first_arg(f: &Function) -> Option<Expr> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    if list.args.is_empty() {
        return None;
    }

    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(_)))) =
        &list.args[0]
    {
        return None;
    }

    // Promote bare-keyword date part (Snowflake form) to a string literal.
    let unit = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident { value, .. }))) => {
            Some(value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            parts.last().map(|p| p.value.clone())
        }
        _ => None,
    }?;

    let mut new = f.clone();
    if let FunctionArguments::List(list) = &mut new.args {
        list.args[0] = FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            Value::SingleQuotedString(unit),
        )));
    }
    Some(Expr::Function(new))
}

fn convert_timezone(f: &Function) -> Option<Expr> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    if list.args.len() != 2 {
        return None;
    }
    let tz = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e.clone(),
        _ => return None,
    };
    let ts = match &list.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e.clone(),
        _ => return None,
    };
    Some(Expr::AtTimeZone {
        timestamp: Box::new(ts),
        time_zone: Box::new(tz),
    })
}
