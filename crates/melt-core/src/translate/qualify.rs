//! `QUALIFY` → subquery rewrite. DuckDB now supports `QUALIFY`
//! natively in recent versions, so this pass is a no-op when the
//! target backend does too. We keep the module so a fallback can be
//! added when running against an older DuckDB.

use sqlparser::ast::Statement;

use super::TranslateResult;

pub fn rewrite(_stmt: &mut Statement) -> TranslateResult<()> {
    Ok(())
}
