//! `melt-audit` — local-only `$/savings` projection from
//! Snowflake `ACCOUNT_USAGE.QUERY_HISTORY`.
//!
//! See `docs/internal/` and the spec on `POWA-89` for the full
//! product brief. This crate is the implementation half of the
//! "no execution, no upload by default" audit binary.
//!
//! Library surface is what `crates/melt-audit/src/main.rs` (the
//! `melt-audit` binary) and `crates/melt-cli` (the `melt audit`
//! subcommand wrapper) drive. The Snowflake live-pull lives in
//! [`snowflake`]; the local-processing pipeline (classify →
//! aggregate → render) is fixture-testable end-to-end without
//! ever opening a network connection.

pub mod aggregate;
pub mod classify;
pub mod cli;
pub mod credit;
pub mod fixture;
pub mod grants;
pub mod model;
pub mod output;
pub mod redact;
pub mod share;
pub mod snowflake;

pub use aggregate::build_audit_output;
pub use classify::{classify_query, Bucket, PassthroughReason, QueryAnalysis};
pub use cli::{run as run_cli, AuditArgs};
pub use credit::{credits_per_hour, credits_used, dollars};
pub use grants::GRANTS_SQL;
pub use model::{
    AuditOutput, Disclaimers, PassthroughBreakdown, PatternRow, QueryHistoryRow, RoutableSummary,
    Window, SCHEMA_VERSION,
};
pub use output::{render_json, render_stdout_table, render_talkingpoints};
pub use redact::redact_literals;
pub use share::{redact_for_share, ShareArgs, DEFAULT_SHARE_ENDPOINT};

/// Default `--credit-price` in USD per credit. Snowflake on-demand list
/// price as of the launch window; operators override with the flag.
pub const DEFAULT_CREDIT_PRICE_USD: f64 = 3.00;

/// Default `--top-n` for the conservative routable rate. Spec §4.
pub const DEFAULT_TOP_N: usize = 20;

/// Default `--window` in days. Spec §2.
pub const DEFAULT_WINDOW_DAYS: u32 = 30;

/// Default warehouse used for the audit query itself. Spec §2.
pub const DEFAULT_WAREHOUSE: &str = "XSMALL";

/// `--window` accepts only this set in v1 (spec §2). Anything else
/// rejects with a remediation message — keeps the talking-points
/// math (annualization × 365 / N) honest.
pub const SUPPORTED_WINDOW_DAYS: &[u32] = &[30, 60, 90];

/// Confidence band, percent. Surfaced on every output.
pub const CONFIDENCE_BAND_PCT: u32 = 20;
