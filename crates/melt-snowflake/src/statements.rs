//! Wrapper around Snowflake's `/api/v2/statements` REST surface used
//! by sync subsystems and the policy refresher. The proxy uses
//! `passthrough` instead because it forwards driver requests verbatim.

use melt_core::{MeltError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::SnowflakeClient;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatementRequest<'a> {
    pub statement: &'a str,
    pub timeout: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<&'a str>,
}

pub async fn execute_json(
    client: &SnowflakeClient,
    token: &str,
    req: &StatementRequest<'_>,
) -> Result<Value> {
    let url = format!("{}/api/v2/statements", client.config().base_url());
    let resp = client
        .http
        .post(&url)
        .bearer_auth(token)
        .json(req)
        .send()
        .await
        .map_err(|e| MeltError::Http(format!("statements send: {e}")))?;
    if !resp.status().is_success() {
        return Err(MeltError::BackendUnavailable(format!(
            "statements upstream {}",
            resp.status()
        )));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| MeltError::Http(format!("statements parse: {e}")))
}

/// Result rows from a paginated JSON statement call. `row_type` is the
/// `resultSetMetaData.rowType` array (column names + Snowflake type
/// metadata); `rows` is the concatenation of partition 0 (inline) and
/// any tail partitions fetched with `?partition=k`. Each cell is the
/// JSON representation Snowflake emits — strings for most types,
/// `null` for SQL NULL.
#[derive(Debug, Clone)]
pub struct JsonStatementResult {
    pub row_type: Vec<Value>,
    pub rows: Vec<Vec<Value>>,
    pub statement_handle: Option<String>,
}

/// `execute_json`, but walks every partition in the response so the
/// caller sees the full result set instead of silently truncating at
/// partition 0. Used by `melt-audit` to drain `ACCOUNT_USAGE.QUERY_HISTORY`
/// without re-implementing the partition walker for a JSON body.
///
/// Synchronous execution only — Snowflake responds with HTTP 202 +
/// `statementStatusUrl` when a statement exceeds `req.timeout`. We
/// surface that as `BackendUnavailable` so the audit binary can
/// translate to a "warehouse cold-start; retry on a warm warehouse"
/// remediation hint instead of polling forever.
pub async fn execute_json_paginated(
    client: &SnowflakeClient,
    token: &str,
    req: &StatementRequest<'_>,
) -> Result<JsonStatementResult> {
    let base_url = client.config().base_url();
    let initial_url = format!("{base_url}/api/v2/statements");
    let resp = client
        .http
        .post(&initial_url)
        .bearer_auth(token)
        .json(req)
        .send()
        .await
        .map_err(|e| MeltError::Http(format!("statements send: {e}")))?;

    let status = resp.status();
    if status.as_u16() == 202 {
        return Err(MeltError::BackendUnavailable(format!(
            "statements upstream still running after {}s; retry on a warm warehouse",
            req.timeout
        )));
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(MeltError::BackendUnavailable(format!(
            "statements upstream {status}: {}",
            preview_body(&body)
        )));
    }
    let value: Value = resp
        .json()
        .await
        .map_err(|e| MeltError::Http(format!("statements parse: {e}")))?;

    let row_type: Vec<Value> = value
        .pointer("/resultSetMetaData/rowType")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let statement_handle = value
        .get("statementHandle")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let partition_info: Vec<Value> = value
        .pointer("/resultSetMetaData/partitionInfo")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows: Vec<Vec<Value>> = Vec::new();
    extract_data_rows(&value, &mut rows);

    let total_partitions = partition_info.len().max(1);
    if total_partitions > 1 {
        let Some(handle) = statement_handle.as_ref() else {
            return Err(MeltError::backend(
                "execute_json_paginated: multi-partition response without statementHandle",
            ));
        };
        for p in 1..total_partitions {
            let part_url = format!("{base_url}/api/v2/statements/{handle}?partition={p}");
            let part_resp = client
                .http
                .get(&part_url)
                .bearer_auth(token)
                .header("Accept", "application/json")
                .send()
                .await
                .map_err(|e| MeltError::Http(format!("partition {p} send: {e}")))?;
            if !part_resp.status().is_success() {
                let body = part_resp.text().await.unwrap_or_default();
                return Err(MeltError::BackendUnavailable(format!(
                    "statements upstream partition {p}: {}",
                    preview_body(&body)
                )));
            }
            let part_value: Value = part_resp
                .json()
                .await
                .map_err(|e| MeltError::Http(format!("partition {p} parse: {e}")))?;
            extract_data_rows(&part_value, &mut rows);
        }
    }

    Ok(JsonStatementResult {
        row_type,
        rows,
        statement_handle,
    })
}

fn extract_data_rows(value: &Value, sink: &mut Vec<Vec<Value>>) {
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return;
    };
    for row in data {
        if let Some(arr) = row.as_array() {
            sink.push(arr.clone());
        }
    }
}

/// Trim an upstream error body so a 4KB Snowflake HTML page doesn't
/// drown out the actionable bits. Keeps the first non-empty line up to
/// 200 chars; collapses whitespace so downstream `eprintln!` output
/// stays on a single line.
fn preview_body(body: &str) -> String {
    let mut buf = String::with_capacity(200);
    let mut last_space = false;
    for ch in body.chars() {
        if ch.is_whitespace() {
            if !last_space && !buf.is_empty() {
                buf.push(' ');
                last_space = true;
            }
            continue;
        }
        last_space = false;
        if ch == '<' {
            // Strip HTML tag bodies: cheap heuristic so a Snowflake 404
            // page (which is wrapped in a giant `<html>…</html>` block)
            // doesn't leak markup into the operator-facing message.
            // Skip up to the matching `>`; bail out if the page is
            // raw text instead.
            // We only need a coarse strip — the operator gets the
            // remediation hint either way.
            break;
        }
        buf.push(ch);
        if buf.len() >= 200 {
            buf.push('…');
            break;
        }
    }
    let trimmed = buf.trim();
    if trimmed.is_empty() {
        format!("<{} bytes upstream body, see network trace>", body.len())
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::preview_body;

    #[test]
    fn preview_body_strips_html_404_to_byte_summary() {
        let body = "<!DOCTYPE html><html><body><h1>404 Not Found</h1></body></html>";
        let got = preview_body(body);
        assert!(
            got.starts_with('<') || got.contains("upstream body"),
            "expected fallback summary, got: {got}"
        );
    }

    #[test]
    fn preview_body_collapses_whitespace_in_short_text() {
        let body = "  invalid PAT\n  user does not exist\n";
        let got = preview_body(body);
        assert_eq!(got, "invalid PAT user does not exist");
    }

    #[test]
    fn preview_body_truncates_long_text() {
        let body = "x".repeat(500);
        let got = preview_body(&body);
        assert!(got.ends_with('…'));
        // Account for the trailing ellipsis (3-byte UTF-8) plus the
        // 200 chars we kept.
        assert!(got.len() <= 200 + 3);
    }
}
