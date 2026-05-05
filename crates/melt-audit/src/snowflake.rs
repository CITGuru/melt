//! Live Snowflake pull. **Stub for [POWA-140](#) — implementation
//! lands in the follow-up commit on this branch.**
//!
//! The fixture-based snapshot test (the "smallest test that proves
//! it" in the issue) does not need this module to be functional —
//! the local-processing pipeline is fully testable from CSV. This
//! file fixes the public surface so the binary can `--print-grants`
//! and reject unsupported `--window` values today; the HTTP path
//! comes online before we open the PR.

use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::model::QueryHistoryRow;

/// Bound on the rows the audit query asks for in a single run.
/// Keeps memory predictable on accounts with multi-million-query
/// 30-day windows. Surfaced in the JSON output as a disclaimer if
/// hit.
pub const DEFAULT_LIMIT_ROWS: u64 = 5_000_000;

#[derive(Debug, Clone)]
pub struct PullPlan {
    pub account: String,
    pub window_days: u32,
    pub warehouse: String,
    pub credit_price_usd: f64,
    pub limit_rows: u64,
}

#[derive(Debug, Clone)]
pub struct PullResult {
    pub rows: Vec<QueryHistoryRow>,
    pub min_start_time: Option<DateTime<Utc>>,
    pub max_start_time: Option<DateTime<Utc>>,
    pub total_pull_duration: Duration,
}

/// Pull `QUERY_HISTORY` + `WAREHOUSE_METERING_HISTORY` for the
/// configured window.
///
/// Returns `Err` if grants are missing on either view; callers map
/// that into the spec §2 remediation message.
pub async fn run_pull(_plan: &PullPlan) -> Result<PullResult> {
    Err(anyhow!(
        "live Snowflake pull is not yet wired — use the fixture-based path; \
         tracked under POWA-140 follow-up commit on feat/melt_audit_binary"
    ))
}

/// SQL emitted to Snowflake. Public so the upcoming integration test
/// can pin the exact text the audit will issue.
pub fn audit_query(window_days: u32, limit_rows: u64) -> String {
    format!(
        "SELECT \
            QUERY_ID, QUERY_TEXT, START_TIME, EXECUTION_TIME, \
            WAREHOUSE_SIZE, BYTES_SCANNED \
         FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY \
         WHERE START_TIME >= DATEADD('day', -{window_days}, CURRENT_TIMESTAMP()) \
           AND ERROR_CODE IS NULL \
         ORDER BY START_TIME DESC \
         LIMIT {limit_rows}"
    )
}
