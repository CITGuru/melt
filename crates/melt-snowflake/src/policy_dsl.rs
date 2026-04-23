//! Snowflake row-access / masking policy DSL → DuckDB expression
//! translator. Powers `PolicyMode::Enforce`.

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
    Value,
};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

#[derive(Debug, thiserror::Error)]
pub enum UnsupportedPolicy {
    #[error("unsupported function: {0}")]
    UnsupportedFunction(String),
    #[error("unsupported expression: {0}")]
    Unsupported(&'static str),
    #[error("policy DSL parse failed: {0}")]
    Parse(String),
}

/// Parse a Snowflake policy body and emit a DuckDB-compatible
/// expression. The returned string can be embedded as the `WHERE`
/// clause of a filtered view.
///
/// # Example
/// ```ignore
/// let duck = translate("CURRENT_ROLE() = 'ANALYST' AND IS_ROLE_IN_SESSION('US')")?;
/// assert!(duck.contains("current_setting('melt.role')"));
/// ```
pub fn translate(snowflake_expr: &str) -> Result<String, UnsupportedPolicy> {
    // Wrap in `SELECT (...)` so sqlparser parses it as an expression.
    let wrapped = format!("SELECT ({snowflake_expr})");
    let mut ast = Parser::parse_sql(&SnowflakeDialect {}, &wrapped)
        .map_err(|e| UnsupportedPolicy::Parse(e.to_string()))?;
    let expr = ast
        .pop()
        .and_then(|stmt| match stmt {
            sqlparser::ast::Statement::Query(q) => Some(q),
            _ => None,
        })
        .and_then(|q| match *q.body {
            sqlparser::ast::SetExpr::Select(sel) => {
                sel.projection.into_iter().next().and_then(|p| match p {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => Some(e),
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                    _ => None,
                })
            }
            _ => None,
        })
        .ok_or(UnsupportedPolicy::Unsupported(
            "policy body did not produce a single expression",
        ))?;
    render(&unwrap_nested(expr))
}

fn unwrap_nested(mut e: Expr) -> Expr {
    while let Expr::Nested(inner) = e {
        e = *inner;
    }
    e
}

fn render(e: &Expr) -> Result<String, UnsupportedPolicy> {
    match e {
        Expr::Identifier(id) => Ok(quote_ident(&id.value)),
        Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(|p| quote_ident(&p.value))
            .collect::<Vec<_>>()
            .join(".")),
        Expr::Value(v) => Ok(render_literal(v)),
        Expr::Nested(inner) => Ok(format!("({})", render(inner)?)),
        Expr::UnaryOp { op, expr } => {
            let inner = render(expr)?;
            Ok(match op {
                UnaryOperator::Not => format!("NOT ({inner})"),
                UnaryOperator::Minus => format!("-({inner})"),
                UnaryOperator::Plus => format!("+({inner})"),
                _ => return Err(UnsupportedPolicy::Unsupported("unary operator")),
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let l = render(left)?;
            let r = render(right)?;
            let op_str = match op {
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                _ => return Err(UnsupportedPolicy::Unsupported("binary operator")),
            };
            Ok(format!("({l}) {op_str} ({r})"))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let lhs = render(expr)?;
            let parts: Result<Vec<_>, _> = list.iter().map(render).collect();
            let inner = parts?.join(", ");
            Ok(if *negated {
                format!("({lhs}) NOT IN ({inner})")
            } else {
                format!("({lhs}) IN ({inner})")
            })
        }
        Expr::IsNull(inner) => Ok(format!("({}) IS NULL", render(inner)?)),
        Expr::IsNotNull(inner) => Ok(format!("({}) IS NOT NULL", render(inner)?)),
        Expr::Function(f) => render_function(f),
        _ => Err(UnsupportedPolicy::Unsupported("expression form")),
    }
}

fn render_function(f: &Function) -> Result<String, UnsupportedPolicy> {
    let name = f
        .name
        .0
        .last()
        .map(|p| p.value.to_ascii_uppercase())
        .unwrap_or_default();
    let args = unnamed_args(f);

    match name.as_str() {
        "CURRENT_ROLE" => Ok("current_setting('melt.role')".to_string()),
        "CURRENT_USER" => Ok("current_setting('melt.user')".to_string()),
        "IS_ROLE_IN_SESSION" => {
            let target = args.as_ref().and_then(|a| a.first().cloned()).ok_or(
                UnsupportedPolicy::Unsupported("IS_ROLE_IN_SESSION needs 1 arg"),
            )?;
            let rendered = render(&target)?;
            Ok(format!(
                "({rendered}) = ANY(string_split(current_setting('melt.session_roles'), ','))"
            ))
        }
        // No clean Lake analogue.
        "IS_DATABASE_ROLE_IN_SESSION" | "INVOKER_ROLE" | "INVOKER_SHARE" => {
            Err(UnsupportedPolicy::UnsupportedFunction(name))
        }
        "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "UPPER" | "LOWER" | "TRIM" | "LENGTH"
        | "SUBSTRING" => {
            let parts: Result<Vec<_>, _> = args.unwrap_or_default().iter().map(render).collect();
            Ok(format!("{}({})", name.to_lowercase(), parts?.join(", ")))
        }
        _ => Err(UnsupportedPolicy::UnsupportedFunction(name)),
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

fn render_literal(v: &Value) -> String {
    match v {
        Value::SingleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Number(n, _) => n.clone(),
        Value::Boolean(b) => b.to_string().to_uppercase(),
        Value::Null => "NULL".into(),
        _ => format!("{v}"),
    }
}

fn quote_ident(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_role_translates() {
        let out = translate("CURRENT_ROLE() = 'ANALYST'").unwrap();
        assert!(out.contains("current_setting('melt.role')"));
        assert!(out.contains("'ANALYST'"));
    }

    #[test]
    fn is_role_in_session_translates() {
        let out = translate("IS_ROLE_IN_SESSION('US')").unwrap();
        assert!(out.contains("string_split"));
        assert!(out.contains("'US'"));
    }

    #[test]
    fn boolean_ops_compose() {
        let out = translate("CURRENT_ROLE() = 'A' AND IS_ROLE_IN_SESSION('B')").unwrap();
        assert!(out.contains("AND"));
        assert!(out.contains("current_setting('melt.role')"));
        assert!(out.contains("string_split"));
    }

    #[test]
    fn unsupported_udf_errors() {
        let err = translate("MY_UDF(x) = 1").unwrap_err();
        assert!(matches!(err, UnsupportedPolicy::UnsupportedFunction(_)));
    }

    #[test]
    fn database_role_is_unsupported() {
        let err = translate("IS_DATABASE_ROLE_IN_SESSION('R')").unwrap_err();
        assert!(matches!(err, UnsupportedPolicy::UnsupportedFunction(_)));
    }

    #[test]
    fn in_list_translates() {
        let out = translate("CURRENT_ROLE() IN ('A', 'B', 'C')").unwrap();
        assert!(out.contains("IN ('A', 'B', 'C')"));
    }

    #[test]
    fn parens_dont_break_anything() {
        let out = translate("(CURRENT_ROLE() = 'X')").unwrap();
        assert!(out.contains("current_setting('melt.role')"));
    }
}
