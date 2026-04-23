use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use futures::StreamExt;
use melt_core::{MeltError, RouteKind};
use serde::Deserialize;
use uuid::Uuid;

use crate::handlers::session::extract_bearer;
use crate::handlers::statement::error_response;
use crate::response::{PartitionInfo, ResultSetMetaData, StatementResponse};
use crate::server::ProxyState;

#[derive(Deserialize)]
pub struct PartitionQuery {
    #[serde(default)]
    pub partition: Option<u32>,
}

/// `GET /api/v2/statements/{handle}` and
/// `GET /api/v2/statements/{handle}?partition=N`. Lake-routed handles
/// pull from the in-memory `ResultStore`. Unknown handles map to
/// Snowflake error 391918 so drivers re-execute.
pub async fn poll(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    Path(handle_str): Path<String>,
    Query(q): Query<PartitionQuery>,
) -> Response {
    // Bearer is required on every poll for Lake-path handles — without
    // it the caller can't prove ownership of the session that opened
    // the handle, and a leaked UUID would otherwise be a free pass.
    // Snowflake-path handles ALSO require a bearer (forwarded to
    // Snowflake for authorization).
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };

    let Ok(handle) = Uuid::parse_str(&handle_str) else {
        // Snowflake-issued handles aren't UUIDs — forward to Snowflake.
        return forward_to_snowflake(state, handle_str, q.partition, &token).await;
    };

    // Resolve the caller's session. Missing / expired → pretend the
    // handle doesn't exist so enumeration attempts are indistinguishable
    // from legitimate "handle gone" replies.
    let Some(session) = state.sessions.lookup(&token) else {
        return error_response(MeltError::HandleNotFound);
    };

    match state.results.lookup_route_for_session(handle, &session.id) {
        Some(RouteKind::Lake) => {}
        Some(RouteKind::Snowflake) => {
            return forward_to_snowflake(state, handle.to_string(), q.partition, &token).await;
        }
        None => return error_response(MeltError::HandleNotFound),
    }

    let partition = q.partition.unwrap_or(0);
    match state
        .results
        .poll_partition(handle, partition, &session.id)
        .await
    {
        Ok(page) => {
            let resp = StatementResponse {
                metadata: ResultSetMetaData {
                    num_rows: page.rows.len(),
                    format: "jsonv2".to_string(),
                    row_type: page.row_type.clone(),
                    partition_info: vec![PartitionInfo {
                        row_count: page.rows.len(),
                        uncompressed_size: 0,
                    }],
                },
                data: page.rows,
                statement_handle: handle.to_string(),
                status_url: format!("/api/v2/statements/{handle}"),
                created_on: Utc::now().timestamp_millis(),
                code: if page.has_more {
                    "333334".to_string() // Snowflake "result chunk available"
                } else {
                    "090001".to_string() // success / done
                },
                message: if page.has_more {
                    "Partition available".to_string()
                } else {
                    "End of result".to_string()
                },
            };
            axum::Json(resp).into_response()
        }
        Err(e) => error_response(e),
    }
}

/// `POST /api/v2/statements/{handle}/cancel` — forward to Snowflake
/// for passthrough handles, drop the entry for Lake handles. Either
/// way return Snowflake-shaped success.
pub async fn cancel(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    Path(handle_str): Path<String>,
) -> Response {
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };

    let Ok(handle) = Uuid::parse_str(&handle_str) else {
        return forward_to_snowflake_cancel(state, handle_str, &token).await;
    };

    let Some(session) = state.sessions.lookup(&token) else {
        return error_response(MeltError::HandleNotFound);
    };

    match state.results.lookup_route_for_session(handle, &session.id) {
        Some(RouteKind::Lake) => {
            let _ = state.results.cancel(handle, &session.id);
            (StatusCode::OK, "{}").into_response()
        }
        Some(RouteKind::Snowflake) => {
            forward_to_snowflake_cancel(state, handle.to_string(), &token).await
        }
        None => error_response(MeltError::HandleNotFound),
    }
}

async fn forward_to_snowflake(
    state: ProxyState,
    handle_str: String,
    partition: Option<u32>,
    token: &str,
) -> Response {
    let path = match partition {
        Some(p) => format!("/api/v2/statements/{handle_str}?partition={p}"),
        None => format!("/api/v2/statements/{handle_str}"),
    };
    match state
        .snowflake
        .passthrough(Method::GET, &path, token, axum::body::Bytes::new())
        .await
    {
        Ok(stream) => collect_to_response(stream).await,
        Err(e) => error_response(e),
    }
}

async fn forward_to_snowflake_cancel(
    state: ProxyState,
    handle_str: String,
    token: &str,
) -> Response {
    let path = format!("/api/v2/statements/{handle_str}/cancel");
    match state
        .snowflake
        .passthrough(Method::POST, &path, token, axum::body::Bytes::new())
        .await
    {
        Ok(stream) => collect_to_response(stream).await,
        Err(e) => error_response(e),
    }
}

async fn collect_to_response<S>(stream: S) -> Response
where
    S: futures::Stream<Item = melt_core::Result<bytes::Bytes>> + Send + Unpin + 'static,
{
    let mut s = Box::pin(stream);
    let mut buf = Vec::new();
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(b) => buf.extend_from_slice(&b),
            Err(e) => return error_response(e),
        }
    }
    (StatusCode::OK, [("content-type", "application/json")], buf).into_response()
}
