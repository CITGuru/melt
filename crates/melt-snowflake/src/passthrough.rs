//! Passthrough helpers — small wrappers used by `melt-proxy` to forward
//! arbitrary statement / session requests to Snowflake.

use bytes::Bytes;
use futures::Stream;
use http::Method;
use melt_core::Result;

use crate::client::SnowflakeClient;

pub async fn forward(
    client: &SnowflakeClient,
    method: Method,
    path: &str,
    token: &str,
    body: Bytes,
) -> Result<impl Stream<Item = Result<Bytes>> + Send + Unpin + 'static> {
    client.passthrough(method, path, token, body).await
}
