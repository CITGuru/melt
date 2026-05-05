//! On-disk JSON shape for `melt-audit-<account>-<date>.json` and the
//! shared in-memory types the rest of the crate operates on.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Bumped any time the JSON schema changes in a way external
/// consumers can detect.
pub const SCHEMA_VERSION: u32 = 1;

/// One row from `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` — the
/// minimum set of columns the audit needs. The fixture path
/// in [`crate::fixture`] loads these from CSV; the live path
/// in [`crate::snowflake`] will populate them from the JSON
/// rowset Snowflake returns.
#[derive(Debug, Clone)]
pub struct QueryHistoryRow {
    pub query_id: String,
    pub query_text: String,
    pub start_time: DateTime<Utc>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Warehouse size label as reported by Snowflake — e.g.
    /// `X-Small`, `SMALL`, `LARGE`. Spec §4: drives credit math.
    /// `None` means no warehouse (compile-only / metadata).
    pub warehouse_size: Option<String>,
    /// Bytes scanned, used as the rough proxy for hot-table
    /// ranking when QUERY_HISTORY exposes it. Zero if missing.
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoutableSummary {
    pub count: u64,
    /// Percentage of `total_queries` (0.0–100.0).
    pub pct: f64,
    /// `$/query` after Melt: routable queries assumed at $0,
    /// non-routable unchanged from baseline.
    pub dollar_per_query_post: f64,
    /// `total_spend_usd - dollar_per_query_post * total_queries`,
    /// floored at 0.
    pub dollars_saved: f64,
    /// `dollars_saved * 365 / window.days`.
    pub annualized: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PatternRow {
    pub rank: u32,
    pub freq: u64,
    pub avg_ms: u64,
    pub table_fqn: String,
    pub pattern_redacted: String,
    pub est_dollars_in_window: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct PassthroughBreakdown {
    #[serde(default)]
    pub writes: u64,
    #[serde(default)]
    pub snowflake_features: u64,
    #[serde(default)]
    pub parse_failed: u64,
    #[serde(default)]
    pub no_tables: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Disclaimers(pub Vec<String>);

impl Disclaimers {
    /// Default human-readable strings printed below the §1 mockup
    /// table and dropped into the JSON `disclaimers` array.
    pub fn default_lines() -> Self {
        Self(vec![
            format!("confidence band ±{}%", crate::CONFIDENCE_BAND_PCT),
            "static analysis only — no DuckDB execution".to_string(),
            "cloud-services credits ignored".to_string(),
            "warehouse credit pricing assumed flat at --credit-price".to_string(),
        ])
    }
}

/// Top-level on-disk shape (`schema_version: 1`). Field set comes
/// straight from spec §3.2.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditOutput {
    pub schema_version: u32,
    pub account: String,
    pub window: Window,
    pub total_queries: u64,
    pub total_spend_usd: f64,
    pub dollar_per_query_baseline: f64,
    pub routable_static: RoutableSummary,
    pub routable_conservative: RoutableSummary,
    pub top_patterns: Vec<PatternRow>,
    pub passthrough_reasons_breakdown: PassthroughBreakdown,
    pub confidence_band_pct: u32,
    pub disclaimers: Vec<String>,
}
