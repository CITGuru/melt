//! Snowflake → DuckDB dialect rewriting.
//!
//! Thin re-export of `melt_core::translate`. The passes used to live
//! here before `melt-snowflake` needed to reuse them for view-body
//! translation; moving them to `melt-core` avoids a crate cycle since
//! `melt-router` already depends on `melt-snowflake`. Router callers
//! see no behaviour change.

pub use melt_core::translate::{
    bind, date, functions, lateral_flatten, qualify, semi_structured, translate_ast,
    translate_body, TranslateError, TranslateResult,
};
