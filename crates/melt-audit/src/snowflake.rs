//! Live Snowflake pull. Drains `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
//! through [`melt_snowflake::SnowflakeClient`], reusing the shared
//! HTTP client / token cache used by the proxy passthrough and the
//! sync subsystems.
//!
//! Translates upstream errors into a remediation hint that names the
//! `MELT_AUDIT` role and the `IMPORTED PRIVILEGES` grant operators
//! provision via `--print-grants` (audit acceptance #5).

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use melt_snowflake::statements::{execute_json_paginated, JsonStatementResult, StatementRequest};
use melt_snowflake::{SnowflakeClient, SnowflakeConfig};
use serde_json::Value;

use crate::model::QueryHistoryRow;

/// Bound on the rows the audit query asks for in a single run.
/// Keeps memory predictable on accounts with multi-million-query
/// 30-day windows. Surfaced in the JSON output as a disclaimer if
/// hit.
pub const DEFAULT_LIMIT_ROWS: u64 = 5_000_000;

/// Fixed Snowflake role the audit runs under. Provisioned by the
/// snippet from spec §2 (`--print-grants`). Hard-coded so the
/// remediation hint always matches what operators see.
pub const AUDIT_ROLE: &str = "MELT_AUDIT";

/// Statement timeout sent to Snowflake (seconds). Five minutes is
/// enough for a cold XSMALL warehouse to spin up + scan a 90-day
/// QUERY_HISTORY window for typical accounts. Beyond this we surface
/// `WarehouseColdStart` so the operator can re-run on a warm
/// warehouse instead of letting the binary block forever.
pub const STATEMENT_TIMEOUT_SECS: u64 = 300;

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

/// Service-auth choices supported by the live pull. Mirrors the
/// `--token | --private-key` half of `AuditArgs`; `--password` is
/// rejected upstream in `cli::run_live` because Snowflake's REST API
/// has no password flow.
#[derive(Debug, Clone)]
pub enum AuditAuth {
    /// Programmatic Access Token (`--token`). Used as the `/api/v2/statements`
    /// bearer directly.
    Pat(String),
    /// PEM-encoded RSA private key + Snowflake login name
    /// (`--private-key` + `--user`). Signs a fresh JWT, exchanges it
    /// for a session token via `melt-snowflake`'s shared cache.
    KeyPair { pem_bytes: Vec<u8>, user: String },
}

/// Resolve audit-mode flags into a `SnowflakeClient`. Public so the
/// CLI layer can surface auth-config errors before any HTTP traffic.
pub fn build_client(account: &str, auth: AuditAuth) -> Result<SnowflakeClient> {
    let mut cfg = SnowflakeConfig {
        account: account.to_string(),
        ..Default::default()
    };
    match auth {
        AuditAuth::Pat(token) => {
            if token.trim().is_empty() {
                return Err(anyhow!("--token must be a non-empty PAT"));
            }
            cfg.pat = token;
        }
        AuditAuth::KeyPair { pem_bytes, user } => {
            if user.trim().is_empty() {
                return Err(anyhow!(
                    "--user is required when --private-key is set (JWT signing names the service user)"
                ));
            }
            cfg.private_key = String::from_utf8(pem_bytes)
                .map_err(|e| anyhow!("--private-key must be UTF-8 PEM: {e}"))?;
            cfg.user = user;
        }
    }
    Ok(SnowflakeClient::new(cfg))
}

/// Pull `QUERY_HISTORY` for the configured window through the live
/// Snowflake REST API. Returns one [`QueryHistoryRow`] per row in the
/// rowset; tail partitions are walked transparently.
///
/// Errors carry the spec §2 remediation hint — `MELT_AUDIT` role +
/// `IMPORTED PRIVILEGES on DATABASE SNOWFLAKE` — so the binary can
/// `eprintln!` them verbatim without losing context (acceptance #5).
pub async fn run_pull(plan: &PullPlan, client: &SnowflakeClient) -> Result<PullResult> {
    let started = Instant::now();
    let token = client
        .service_token()
        .await
        .map_err(|e| remediation_error("authenticate to Snowflake", e))?;

    let sql = audit_query(plan.window_days, plan.limit_rows);
    let warehouse = if plan.warehouse.trim().is_empty() {
        None
    } else {
        Some(plan.warehouse.as_str())
    };
    let req = StatementRequest {
        statement: &sql,
        timeout: STATEMENT_TIMEOUT_SECS,
        warehouse,
        database: Some("SNOWFLAKE"),
        schema: Some("ACCOUNT_USAGE"),
        role: Some(AUDIT_ROLE),
    };

    let resp = execute_json_paginated(client, token.as_str(), &req)
        .await
        .map_err(|e| remediation_error("query SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", e))?;

    let rows = parse_rows(&resp)?;
    let min_start_time = rows.iter().map(|r| r.start_time).min();
    let max_start_time = rows.iter().map(|r| r.start_time).max();
    Ok(PullResult {
        rows,
        min_start_time,
        max_start_time,
        total_pull_duration: started.elapsed(),
    })
}

/// SQL emitted to Snowflake. Public so callers (and the future
/// integration test) can pin the exact text the audit will issue.
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

/// Translate any upstream error into a single human-readable string
/// that names the role + grants operators provisioned via
/// `--print-grants`. The CLI prints this verbatim on stderr so a
/// Snowflake DBA can see what to fix without digging through stack
/// traces.
fn remediation_error<E: std::fmt::Display>(stage: &str, err: E) -> anyhow::Error {
    anyhow!(
        "failed to {stage}: {err}. \
         The audit runs under the {AUDIT_ROLE} role and needs \
         IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE \
         (read access to ACCOUNT_USAGE.QUERY_HISTORY and \
         WAREHOUSE_METERING_HISTORY) plus USAGE on the audit \
         warehouse. Re-run `melt audit --print-grants` to print \
         the role-creation snippet from spec §2 and have a Snowflake \
         admin run it before retrying."
    )
}

fn parse_rows(resp: &JsonStatementResult) -> Result<Vec<QueryHistoryRow>> {
    let col_index = build_column_index(&resp.row_type)?;
    let mut out = Vec::with_capacity(resp.rows.len());
    for (lineno, row) in resp.rows.iter().enumerate() {
        let cell = |name: &str| -> Option<&Value> { col_index.get(name).and_then(|i| row.get(*i)) };
        let query_id = cell_str(cell("QUERY_ID")).unwrap_or_default();
        let query_text = cell_str(cell("QUERY_TEXT")).unwrap_or_default();
        let start_time_str = cell_str(cell("START_TIME")).ok_or_else(|| {
            anyhow!(
                "row {} missing START_TIME — expected ISO timestamp from \
                 ACCOUNT_USAGE.QUERY_HISTORY",
                lineno
            )
        })?;
        let start_time = parse_snowflake_timestamp(&start_time_str).ok_or_else(|| {
            anyhow!(
                "row {} START_TIME `{start_time_str}` is not a recognized \
                 Snowflake timestamp; expected RFC3339 or epoch-millis seconds",
                lineno
            )
        })?;
        let execution_time_ms = cell_str(cell("EXECUTION_TIME"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let warehouse_size = cell_str(cell("WAREHOUSE_SIZE")).filter(|s| !s.trim().is_empty());
        let bytes_scanned = cell_str(cell("BYTES_SCANNED"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        out.push(QueryHistoryRow {
            query_id,
            query_text,
            start_time,
            execution_time_ms,
            warehouse_size,
            bytes_scanned,
        });
    }
    Ok(out)
}

fn build_column_index(row_type: &[Value]) -> Result<std::collections::HashMap<String, usize>> {
    if row_type.is_empty() {
        return Err(anyhow!(
            "Snowflake response missing rowType metadata; cannot map columns to QueryHistoryRow"
        ));
    }
    let mut idx = std::collections::HashMap::new();
    for (i, col) in row_type.iter().enumerate() {
        if let Some(name) = col.get("name").and_then(|v| v.as_str()) {
            idx.insert(name.to_ascii_uppercase(), i);
        }
    }
    Ok(idx)
}

fn cell_str(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn parse_snowflake_timestamp(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    // Snowflake JSON encodes TIMESTAMP_LTZ as `<seconds>.<nanos>` since
    // the epoch (e.g. `1714512345.123456789`). Parse the whole / fractional
    // halves separately so we don't lose sub-second precision.
    let mut parts = s.splitn(2, '.');
    let secs_str = parts.next()?;
    let secs: i64 = secs_str.parse().ok()?;
    let nanos: u32 = match parts.next() {
        None => 0,
        Some(frac) => {
            let mut buf = String::with_capacity(9);
            for c in frac.chars().take(9) {
                if !c.is_ascii_digit() {
                    return None;
                }
                buf.push(c);
            }
            while buf.len() < 9 {
                buf.push('0');
            }
            buf.parse().ok()?
        }
    };
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row_type() -> Vec<Value> {
        vec![
            json!({"name": "QUERY_ID"}),
            json!({"name": "QUERY_TEXT"}),
            json!({"name": "START_TIME"}),
            json!({"name": "EXECUTION_TIME"}),
            json!({"name": "WAREHOUSE_SIZE"}),
            json!({"name": "BYTES_SCANNED"}),
        ]
    }

    #[test]
    fn parses_rfc3339_timestamp() {
        let resp = JsonStatementResult {
            row_type: row_type(),
            rows: vec![vec![
                json!("q1"),
                json!("SELECT 1"),
                json!("2026-04-01T12:00:00Z"),
                json!("12345"),
                json!("X-Small"),
                json!("1024"),
            ]],
            statement_handle: None,
        };
        let rows = parse_rows(&resp).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.query_id, "q1");
        assert_eq!(r.query_text, "SELECT 1");
        assert_eq!(r.execution_time_ms, 12345);
        assert_eq!(r.warehouse_size.as_deref(), Some("X-Small"));
        assert_eq!(r.bytes_scanned, 1024);
        assert_eq!(r.start_time.format("%Y-%m-%d").to_string(), "2026-04-01");
    }

    #[test]
    fn parses_epoch_seconds_with_fraction() {
        let resp = JsonStatementResult {
            row_type: row_type(),
            rows: vec![vec![
                json!("q2"),
                json!("SELECT 2"),
                json!("1714512345.500000000"),
                json!("0"),
                Value::Null,
                Value::Null,
            ]],
            statement_handle: None,
        };
        let rows = parse_rows(&resp).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.warehouse_size, None);
        assert_eq!(r.bytes_scanned, 0);
        assert_eq!(r.execution_time_ms, 0);
        assert_eq!(
            r.start_time,
            DateTime::<Utc>::from_timestamp(1714512345, 500_000_000).unwrap()
        );
    }

    #[test]
    fn reports_missing_start_time() {
        let resp = JsonStatementResult {
            row_type: row_type(),
            rows: vec![vec![
                json!("q3"),
                json!("SELECT 3"),
                Value::Null,
                json!("0"),
                Value::Null,
                Value::Null,
            ]],
            statement_handle: None,
        };
        let err = parse_rows(&resp).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing START_TIME"), "got: {msg}");
    }

    #[test]
    fn requires_row_type_metadata() {
        let resp = JsonStatementResult {
            row_type: vec![],
            rows: vec![],
            statement_handle: None,
        };
        let err = parse_rows(&resp).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("rowType"), "got: {msg}");
    }

    #[test]
    fn remediation_error_names_role_and_grants() {
        let err = remediation_error(
            "authenticate to Snowflake",
            "401 Unauthorized: invalid bearer",
        );
        let msg = format!("{err}");
        assert!(msg.contains("MELT_AUDIT"), "missing role: {msg}");
        assert!(
            msg.contains("IMPORTED PRIVILEGES"),
            "missing grants hint: {msg}"
        );
        assert!(
            msg.contains("--print-grants"),
            "missing remediation pointer: {msg}"
        );
    }

    #[test]
    fn audit_query_pins_view_and_filters() {
        let sql = audit_query(30, 5);
        assert!(sql.contains("ACCOUNT_USAGE.QUERY_HISTORY"));
        assert!(sql.contains("DATEADD('day', -30,"));
        assert!(sql.contains("LIMIT 5"));
    }

    #[test]
    fn build_client_rejects_empty_token() {
        // `SnowflakeClient` doesn't impl Debug, so we can't unwrap_err
        // directly — match on the result instead.
        match build_client("ACME-DEMO", AuditAuth::Pat("".into())) {
            Ok(_) => panic!("empty PAT should fail validation"),
            Err(e) => assert!(format!("{e}").contains("non-empty PAT")),
        }
    }

    #[test]
    fn build_client_keypair_requires_user() {
        match build_client(
            "ACME-DEMO",
            AuditAuth::KeyPair {
                pem_bytes: b"-----BEGIN PRIVATE KEY-----\n...\n".to_vec(),
                user: "".into(),
            },
        ) {
            Ok(_) => panic!("empty --user should fail validation"),
            Err(e) => assert!(format!("{e}").contains("--user is required")),
        }
    }
}
