//! `POST /api/v2/statements` — the Snowflake REST-API v2 execute
//! surface.
//!
//! Everything interesting (auth, concurrency, routing, fallback,
//! metrics) lives in [`crate::execution`]; this file only parses the
//! v2 request shape and builds the v2 response envelope. When you
//! need to change how queries are routed or executed, edit
//! `execution.rs`; when you need to fix the wire response shape,
//! edit here.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use melt_core::{MeltError, RouteKind, SessionInfo};
use melt_snowflake::PassthroughResponse;
use metrics::{counter, histogram};
use serde_json::Value;

use crate::execution::{self, Executed, LakeExecution, RouteInput};
use crate::handlers::session::extract_bearer;
use crate::response::{ErrorResponse, PartitionInfo, ResultSetMetaData, StatementResponse};
use crate::server::ProxyState;

/// `POST /api/v2/statements` — execute a query.
///
/// Wraps the inner work in `tokio::time::timeout` honouring
/// `[proxy.limits].request_timeout`. On expiry the response future
/// is dropped and a Snowflake 604 ("operation cancelled") is
/// returned. The DuckDB query itself runs to completion in the
/// background — the duckdb crate doesn't surface
/// `Connection::interrupt()`, so we accept the resource cost and
/// rely on result-store eviction / idle-TTL to recycle memory.
pub async fn execute(
    State(state): State<ProxyState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    counter!(melt_metrics::PROXY_REQUESTS).increment(1);
    let timeout = state.request_timeout;

    let work = execute_inner(state, query, headers, body);
    let resp = match tokio::time::timeout(timeout, work).await {
        Ok(r) => r,
        Err(_) => {
            counter!(
                melt_metrics::PROXY_TIMEOUTS,
                melt_metrics::LABEL_OUTCOME => melt_metrics::OUTCOME_CANCELLED
            )
            .increment(1);
            return error_response(MeltError::Timeout);
        }
    };
    histogram!(melt_metrics::PROXY_LATENCY).record(start.elapsed().as_secs_f64());
    resp
}

async fn execute_inner(
    state: ProxyState,
    query: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };
    let Some(session) = state.sessions.lookup(&token) else {
        return error_response(MeltError::Unauthorized);
    };

    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error_response(MeltError::parse(format!("body: {e}"))),
    };
    let sql = parsed
        .get("statement")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    if sql.is_empty() {
        return error_response(MeltError::parse("missing 'statement' field"));
    }

    let result = execution::run(RouteInput {
        state: &state,
        session: session.clone(),
        token,
        sql,
        body: body.clone(),
        query: query.as_deref(),
        headers: &headers,
        passthrough_path: "/api/v2/statements",
        api: "v2",
        // v2 streams the result via the partition-polling API —
        // eagerly pull the first batch and hand the continuation to
        // ResultStore below.
        drain_full: false,
    })
    .await;

    match result {
        Ok(Executed::Lake(lake)) => build_v2_lake_response(&state, &session, lake),
        Ok(Executed::Passthrough(resp)) => forward_passthrough(resp),
        Err(e) => error_response(e),
    }
}

/// Shape a v2 response envelope from a Lake execution, including the
/// `ResultStore`-backed continuation handle so the driver can poll
/// partitions via `GET /api/v2/statements/{handle}?partition=N`.
fn build_v2_lake_response(
    state: &ProxyState,
    session: &Arc<SessionInfo>,
    lake: LakeExecution,
) -> Response {
    let LakeExecution {
        schema,
        eager_batches,
        continuation,
        outcome: _,
    } = lake;

    // Defensive empty stream if continuation missing.
    let tail_stream: melt_core::RecordBatchStream = continuation.unwrap_or_else(|| {
        Box::pin(futures::stream::empty::<
            melt_core::Result<arrow::record_batch::RecordBatch>,
        >())
    });

    let handle = state.results.insert(
        tail_stream,
        session.id.clone(),
        RouteKind::Lake,
        schema.clone(),
    );

    let first = eager_batches.first();
    let row_type = first
        .map(|b| schema_to_columns(&b.schema()))
        .unwrap_or_default();
    let data = first
        .map(|b| crate::response::batches_to_partition(std::slice::from_ref(b)))
        .unwrap_or_default();

    let resp = StatementResponse {
        metadata: ResultSetMetaData {
            num_rows: data.len(),
            format: "jsonv2".to_string(),
            row_type,
            partition_info: vec![PartitionInfo {
                row_count: data.len(),
                uncompressed_size: 0,
            }],
        },
        data,
        statement_handle: handle.to_string(),
        status_url: format!("/api/v2/statements/{handle}"),
        created_on: Utc::now().timestamp_millis(),
        code: "090001".to_string(),
        message: "Statement executed successfully".to_string(),
    };
    axum::Json(resp).into_response()
}

/// Replay upstream status + headers + body to the driver. Shared
/// byte-for-byte with the v1 handler via the same upstream
/// `PassthroughResponse` plumbing.
fn forward_passthrough(resp: PassthroughResponse) -> Response {
    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        for (name, value) in resp.headers.iter() {
            let name_str = name.as_str();
            if matches!(
                name_str,
                "connection" | "transfer-encoding" | "content-length" | "keep-alive" | "upgrade"
            ) {
                continue;
            }
            if let (Ok(n), Ok(v)) = (
                axum::http::HeaderName::from_bytes(name_str.as_bytes()),
                axum::http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                headers.append(n, v);
            }
        }
    }
    match builder.body(axum::body::Body::from_stream(resp.body)) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "v2 passthrough: response build failed");
            error_response(MeltError::backend(format!("response build: {e}")))
        }
    }
}

fn schema_to_columns(schema: &arrow_schema::Schema) -> Vec<crate::response::ColumnMeta> {
    schema
        .fields()
        .iter()
        .map(|f| crate::response::ColumnMeta {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            nullable: f.is_nullable(),
        })
        .collect()
}

pub fn error_response(e: MeltError) -> Response {
    let code = melt_snowflake::snowflake_code(&e);
    let status = melt_snowflake::errors::http_status(&e);
    let body = ErrorResponse::snowflake(code, e.to_string());
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        axum::Json(body),
    )
        .into_response()
}
