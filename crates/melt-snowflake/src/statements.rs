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
