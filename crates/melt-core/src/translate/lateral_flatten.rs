//! `LATERAL FLATTEN(...)` → `unnest(...)`. Stub today.

use sqlparser::ast::Statement;

use super::TranslateResult;

pub fn rewrite(_stmt: &mut Statement) -> TranslateResult<()> {
    Ok(())
}
