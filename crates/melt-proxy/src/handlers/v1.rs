//! Snowflake v1 wire-protocol endpoints.
//!
//! The official drivers (snowflake-connector-python default path, the
//! JDBC driver's legacy code paths, and the Go driver's compatibility
//! fallback) execute queries against the v1 API surface, not v2. The
//! request body shape (`sqlText` vs `statement`), the response body
//! shape (`data.rowtype`/`data.rowset` vs `resultSetMetaData`/`data`),
//! and the passthrough URL all differ — but the work in the middle
//! (auth, concurrency, routing, fallback, metrics) is shared with v2
//! via [`crate::execution`]. This module is just the wire-edge
//! adapter.
//!
//! | Endpoint                               | Handler             | Strategy |
//! |----------------------------------------|---------------------|----------|
//! | `POST /queries/v1/query-request`       | [`query_request`]   | Route via `execution::run`; build v1 response envelope on Lake, replay upstream on Snowflake |
//! | `POST /queries/v1/abort-request`       | [`abort_request`]   | Passthrough (no routing; server-side cancel) |
//! | `GET  /queries/{id}/result`            | [`result_get`]      | Passthrough (async result fetch — Lake results are synchronous, so Melt doesn't issue Lake handles drivers would poll here) |
//! | `GET  /monitoring/queries/{id}`        | [`monitoring_query`]| Passthrough (informational status) |

use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use melt_core::{MeltError, SessionInfo};
use melt_snowflake::PassthroughResponse;
use metrics::{counter, histogram};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::execution::{self, Executed, RouteInput};
use crate::handlers::session::extract_bearer;
use crate::handlers::statement::error_response;
use crate::server::ProxyState;

/// `POST /queries/v1/query-request` — the v1 execute endpoint.
///
/// Body shape (only fields we consume):
///
/// ```json
/// { "sqlText": "SELECT 1", "asyncExec": false, "parameters": {} }
/// ```
///
/// `asyncExec: true` asks Snowflake to queue the query and return a
/// handle the driver polls via `/queries/{id}/result`. For the Lake
/// path we always execute synchronously (DuckDB is local; async is
/// meaningless); for the Snowflake path we passthrough so the
/// upstream contract is whatever Snowflake decides.
pub async fn query_request(
    State(state): State<ProxyState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    counter!(melt_metrics::PROXY_REQUESTS).increment(1);
    let timeout = state.request_timeout;

    let work = query_request_inner(state, query, headers, body);
    let resp = match tokio::time::timeout(timeout, work).await {
        Ok(r) => r,
        Err(_) => {
            counter!(
                melt_metrics::PROXY_TIMEOUTS,
                melt_metrics::LABEL_OUTCOME => melt_metrics::OUTCOME_CANCELLED,
            )
            .increment(1);
            return error_response(MeltError::Timeout);
        }
    };
    histogram!(melt_metrics::PROXY_LATENCY).record(start.elapsed().as_secs_f64());
    resp
}

async fn query_request_inner(
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
        Err(e) => return error_response(MeltError::parse(format!("v1 body: {e}"))),
    };
    let sql = parsed
        .get("sqlText")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if sql.is_empty() {
        return error_response(MeltError::parse("v1: missing 'sqlText'"));
    }

    let result = execution::run(RouteInput {
        state: &state,
        session: session.clone(),
        token,
        sql,
        body: body.clone(),
        query: query.as_deref(),
        headers: &headers,
        passthrough_path: "/queries/v1/query-request",
        api: "v1",
        // v1 has no partition-polling convention — drivers expect
        // the full result inline. Drain the whole stream.
        drain_full: true,
    })
    .await;

    match result {
        Ok(Executed::Lake(lake)) => {
            build_v1_local_response(&session, lake.schema, lake.eager_batches)
        }
        Ok(Executed::Hybrid(hy)) => {
            // v1 has no partition-polling convention; the full hybrid
            // result was already drained inline by `execute_hybrid`.
            // Same response shape as Lake.
            build_v1_local_response(&session, hy.schema, hy.eager_batches)
        }
        Ok(Executed::Passthrough(resp)) => forward_passthrough_response(resp),
        Err(e) => error_response(e),
    }
}

/// Pack a fully-drained local execution (Lake or Hybrid) into the v1
/// response envelope every driver expects.
fn build_v1_local_response(
    session: &Arc<SessionInfo>,
    schema: Option<arrow_schema::SchemaRef>,
    eager_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Response {
    let rowtype = schema
        .as_deref()
        .map(schema_to_v1_columns)
        .unwrap_or_default();
    let rowset = crate::response::batches_to_partition(&eager_batches);
    let total = rowset.len() as u64;

    let resp = V1QueryResponse {
        data: V1ResponseData {
            rowtype,
            rowset,
            total,
            returned: total,
            query_id: Uuid::new_v4().to_string(),
            query_result_format: "json",
            final_database_name: session.database.clone(),
            final_schema_name: session.schema.clone(),
            final_warehouse_name: session.warehouse.clone(),
            final_role_name: session.role.clone(),
            parameters: Vec::new(),
            query_context: V1QueryContext {
                entries: Vec::new(),
            },
            // SELECT — the only statement type we synthesise locally.
            statement_type_id: 4096,
            version: 1,
            send_result_time: Utc::now().timestamp_millis(),
        },
        message: None,
        code: None,
        success: true,
    };
    axum::Json(resp).into_response()
}

/// `POST /queries/v1/abort-request` — driver cancels an in-flight
/// query. No v1-side Lake handle registry (Lake results are
/// synchronous and exposed inline), so abort is always passthrough.
/// We still authenticate so anonymous abuse doesn't reach upstream.
pub async fn abort_request(
    State(state): State<ProxyState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };
    match state
        .snowflake
        .passthrough_full(
            Method::POST,
            "/queries/v1/abort-request",
            query.as_deref(),
            &headers,
            &token,
            body,
        )
        .await
    {
        Ok(resp) => forward_passthrough_response(resp),
        Err(e) => error_response(e),
    }
}

/// `GET /queries/{query_id}/result` — async result poll. Lake-executed
/// queries are synchronous and inline the full result in the initial
/// `query_request` response, so drivers don't hit this for Lake
/// handles. Snowflake-issued queryIds pass through verbatim.
pub async fn result_get(
    State(state): State<ProxyState>,
    Path(query_id): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };
    let path = format!("/queries/{query_id}/result");
    match state
        .snowflake
        .passthrough_full(
            Method::GET,
            &path,
            query.as_deref(),
            &headers,
            &token,
            Bytes::new(),
        )
        .await
    {
        Ok(resp) => forward_passthrough_response(resp),
        Err(e) => error_response(e),
    }
}

/// `GET /monitoring/queries/{query_id}` — status lookup. Always
/// passthrough; drivers use it for informational UI and Melt has no
/// equivalent to project Lake queries into.
pub async fn monitoring_query(
    State(state): State<ProxyState>,
    Path(query_id): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(token) = extract_bearer(&headers) else {
        return error_response(MeltError::Unauthorized);
    };
    let path = format!("/monitoring/queries/{query_id}");
    match state
        .snowflake
        .passthrough_full(
            Method::GET,
            &path,
            query.as_deref(),
            &headers,
            &token,
            Bytes::new(),
        )
        .await
    {
        Ok(resp) => forward_passthrough_response(resp),
        Err(e) => error_response(e),
    }
}

/// Replay a full upstream response (status + headers + body) back to
/// the driver. Critical to keep `Content-Encoding`, `content-type`,
/// and the actual HTTP status untouched — drivers decompress based
/// on the former, pick a JSON parser based on the latter, and branch
/// error handling on the status. Hop-by-hop and proxy-controlled
/// headers (`Connection`, `Transfer-Encoding`, `content-length`)
/// are dropped so axum can resynthesise them for our socket.
fn forward_passthrough_response(resp: PassthroughResponse) -> Response {
    let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);
    if let Some(builder_headers) = builder.headers_mut() {
        for (name, value) in resp.headers.iter() {
            let name_str = name.as_str();
            if matches!(
                name_str,
                "connection" | "transfer-encoding" | "content-length" | "keep-alive" | "upgrade"
            ) {
                continue;
            }
            if let (Ok(name), Ok(value)) = (
                axum::http::HeaderName::from_bytes(name_str.as_bytes()),
                axum::http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                builder_headers.append(name, value);
            }
        }
    }
    match builder.body(axum::body::Body::from_stream(resp.body)) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "v1 passthrough: response build failed");
            error_response(MeltError::backend(format!("response build: {e}")))
        }
    }
}

// V1 structs omit Snowflake fields we can't populate from Lake.

#[derive(Serialize)]
struct V1QueryResponse {
    data: V1ResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    success: bool,
}

#[derive(Serialize)]
struct V1ResponseData {
    rowtype: Vec<V1Column>,
    rowset: Vec<Vec<Option<String>>>,
    total: u64,
    returned: u64,
    #[serde(rename = "queryId")]
    query_id: String,
    #[serde(rename = "queryResultFormat")]
    query_result_format: &'static str,
    #[serde(rename = "finalDatabaseName", skip_serializing_if = "Option::is_none")]
    final_database_name: Option<String>,
    #[serde(rename = "finalSchemaName", skip_serializing_if = "Option::is_none")]
    final_schema_name: Option<String>,
    #[serde(rename = "finalWarehouseName", skip_serializing_if = "Option::is_none")]
    final_warehouse_name: Option<String>,
    #[serde(rename = "finalRoleName", skip_serializing_if = "Option::is_none")]
    final_role_name: Option<String>,
    /// Session parameters to apply on the driver side. We don't
    /// synthesise any for Lake responses — drivers tolerate empty
    /// lists, they just mean "no server-initiated setting changes".
    parameters: Vec<Value>,
    #[serde(rename = "queryContext")]
    query_context: V1QueryContext,
    #[serde(rename = "statementTypeId")]
    statement_type_id: u32,
    version: u32,
    #[serde(rename = "sendResultTime")]
    send_result_time: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct V1QueryContext {
    entries: Vec<Value>,
}

#[derive(Serialize)]
struct V1Column {
    name: String,
    /// Snowflake type family. Lowercase, driver-visible. We pick from
    /// the reduced set the connectors actually parse; exotic Arrow
    /// types fall through to `"text"` so the connector never blows up
    /// on an unrecognised `type` string.
    #[serde(rename = "type")]
    type_: &'static str,
    nullable: bool,
    #[serde(rename = "byteLength")]
    byte_length: u32,
    length: u32,
    precision: u32,
    scale: u32,
    database: &'static str,
    schema: &'static str,
    table: &'static str,
    collation: Option<&'static str>,
}

fn schema_to_v1_columns(schema: &arrow_schema::Schema) -> Vec<V1Column> {
    schema
        .fields()
        .iter()
        .map(|f| {
            use arrow_schema::DataType::*;
            let (type_, byte_length, length) = match f.data_type() {
                Utf8 | LargeUtf8 => ("text", 16_777_216, 16_777_216),
                Boolean => ("boolean", 1, 1),
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => ("fixed", 8, 8),
                Float16 | Float32 | Float64 | Decimal128(_, _) | Decimal256(_, _) => ("real", 8, 8),
                Date32 | Date64 => ("date", 4, 4),
                Time32(_) | Time64(_) => ("time", 8, 8),
                Timestamp(_, None) => ("timestamp_ntz", 8, 8),
                Timestamp(_, Some(_)) => ("timestamp_ltz", 8, 8),
                Binary | LargeBinary | FixedSizeBinary(_) => ("binary", 8_388_608, 8_388_608),
                _ => ("text", 16_777_216, 16_777_216),
            };
            V1Column {
                name: f.name().clone(),
                type_,
                nullable: f.is_nullable(),
                byte_length,
                length,
                precision: 0,
                scale: 0,
                database: "",
                schema: "",
                table: "",
                collation: None,
            }
        })
        .collect()
}
