//! Snowpipe ingestion adapter (REST `insertFiles` + load history).
//!
//! Melt syncs OUT of Snowflake; this surface is exposed for parity
//! with operators who mirror lake tables back into Snowflake. The
//! Snowpipe Streaming gRPC variant is Java-only and out of scope.

use melt_core::{MeltError, Result};
use serde::{Deserialize, Serialize};

use crate::client::SnowflakeClient;

#[derive(Debug, Serialize)]
pub struct InsertFilesRequest<'a> {
    pub pipe_name: &'a str,
    pub files: Vec<&'a str>,
}

#[derive(Debug, Deserialize)]
pub struct InsertFilesResponse {
    #[serde(rename = "requestId", default)]
    pub request_id: String,
}

pub async fn insert_files(
    client: &SnowflakeClient,
    token: &str,
    req: InsertFilesRequest<'_>,
) -> Result<InsertFilesResponse> {
    let url = format!(
        "{}/v1/data/pipes/{}/insertFiles",
        client.config().base_url(),
        req.pipe_name,
    );
    let body: Vec<serde_json::Value> = req
        .files
        .iter()
        .map(|f| serde_json::json!({ "path": f }))
        .collect();
    let resp = client
        .http
        .post(&url)
        .bearer_auth(token)
        .json(&serde_json::json!({ "files": body }))
        .send()
        .await
        .map_err(|e| MeltError::Http(format!("snowpipe insertFiles send: {e}")))?;
    if !resp.status().is_success() {
        return Err(MeltError::BackendUnavailable(format!(
            "snowpipe insertFiles upstream {}",
            resp.status()
        )));
    }
    resp.json::<InsertFilesResponse>()
        .await
        .map_err(|e| MeltError::Http(format!("snowpipe parse: {e}")))
}

#[derive(Debug, Deserialize)]
pub struct LoadHistoryEntry {
    #[serde(rename = "fileName", default)]
    pub file_name: String,
    #[serde(rename = "status", default)]
    pub status: String,
    #[serde(rename = "rowsInserted", default)]
    pub rows_inserted: u64,
    #[serde(rename = "rowsParsed", default)]
    pub rows_parsed: u64,
    #[serde(rename = "errorsSeen", default)]
    pub errors_seen: u64,
}

#[derive(Debug, Deserialize)]
pub struct LoadHistoryResponse {
    pub files: Vec<LoadHistoryEntry>,
}

pub async fn load_history(
    client: &SnowflakeClient,
    token: &str,
    pipe_name: &str,
    start_iso: &str,
    end_iso: &str,
) -> Result<LoadHistoryResponse> {
    let url = format!(
        "{}/v1/data/pipes/{}/loadHistoryScan?startTimeInclusive={}&endTimeExclusive={}",
        client.config().base_url(),
        pipe_name,
        start_iso,
        end_iso,
    );
    let resp = client
        .http
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| MeltError::Http(format!("loadHistoryScan send: {e}")))?;
    if !resp.status().is_success() {
        return Err(MeltError::BackendUnavailable(format!(
            "loadHistoryScan upstream {}",
            resp.status()
        )));
    }
    resp.json::<LoadHistoryResponse>()
        .await
        .map_err(|e| MeltError::Http(format!("loadHistoryScan parse: {e}")))
}
