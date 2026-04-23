use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use melt_core::{MeltError, SessionInfo};
use serde_json::Value;
use std::sync::Arc;

use crate::server::ProxyState;

/// `POST /session/v1/login-request` — forwarded verbatim. We sniff
/// the response body just enough to extract the token Snowflake
/// returns so we can register a session for downstream statement
/// handling, then we replay the body to the caller unchanged.
///
/// The query string matters: Snowflake drivers pin the session
/// context (`databaseName`, `schemaName`, `warehouse`, `roleName`)
/// and the client-generated `request_id`/`request_guid` on the URL,
/// not the body. Drop them and Snowflake answers HTTP 400 on every
/// real driver login while naive curl probes without params still
/// work. We forward the raw query through `forward_login_with_query`
/// so nothing about the URL contract differs between Melt and direct
/// calls.
///
/// Error responses intentionally carry generic messages — detailed
/// reqwest / upstream failures stay in `tracing::warn!` logs so we
/// don't leak upstream hostnames, JWT kid values, or TLS peer detail
/// to callers.
pub async fn login_request(
    State(state): State<ProxyState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let resp = match state
        .snowflake
        .forward_login_with_query(query.as_deref(), &headers, body)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "login forward failed");
            return crate::handlers::statement::error_response(MeltError::BackendUnavailable(
                "upstream login unavailable".into(),
            ));
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let headers = resp.headers().clone();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "login body read failed");
            return crate::handlers::statement::error_response(MeltError::BackendUnavailable(
                "upstream login body unreadable".into(),
            ));
        }
    };

    if let Some(parsed) = parse_login_response(&bytes) {
        register_from_login(&state, parsed);
    }

    let mut response = (status, bytes).into_response();
    forward_headers(&mut response, &headers);
    response
}

/// `POST /session/v1/token-request` — refresh. Forwarded; we update
/// the session record on success and re-cache the new bearer token.
/// Same query-string forwarding contract as `login_request` — some
/// drivers attach `request_id` / `request_guid` on refreshes too.
pub async fn token_request(
    State(state): State<ProxyState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match state
        .snowflake
        .forward_login_with_query(query.as_deref(), &headers, body)
        .await
    {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
            let headers = resp.headers().clone();
            let bytes = resp.bytes().await.unwrap_or_default();
            if let Some(parsed) = parse_login_response(&bytes) {
                register_from_login(&state, parsed);
            }
            let mut response = (status, bytes).into_response();
            forward_headers(&mut response, &headers);
            response
        }
        Err(e) => {
            tracing::warn!(error = %e, "token refresh failed");
            crate::handlers::statement::error_response(MeltError::BackendUnavailable(
                "upstream token refresh unavailable".into(),
            ))
        }
    }
}

/// `POST /session?delete=true` — close session. We forget the token
/// locally but do not block on Snowflake's response (best-effort).
pub async fn close_session(State(state): State<ProxyState>, headers: HeaderMap) -> Response {
    if let Some(token) = bearer(&headers) {
        state.sessions.close(&token);
    }
    (StatusCode::OK, "{}").into_response()
}

/// `GET /session/heartbeat` — keepalive ping.
pub async fn heartbeat() -> Response {
    (StatusCode::OK, "{}").into_response()
}

fn forward_headers(resp: &mut Response, headers: &http::HeaderMap) {
    let dst = resp.headers_mut();
    for (k, v) in headers.iter() {
        let name_str = k.as_str();
        if matches!(
            name_str,
            "transfer-encoding" | "content-length" | "connection"
        ) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name_str.as_bytes()),
            axum::http::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            dst.insert(name, value);
        }
    }
}

/// Sniffed fields from a Snowflake login / token response. Anything
/// the proxy needs to populate `SessionInfo` lives here; everything
/// else stays untouched in the body forwarded to the driver.
struct ParsedLogin {
    token: String,
    role: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    expires_in_secs: Option<u64>,
}

fn parse_login_response(bytes: &[u8]) -> Option<ParsedLogin> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let data = value.get("data")?;
    let token = data
        .get("token")
        .or_else(|| data.get("sessionToken"))
        .and_then(|t| t.as_str())?
        .to_string();
    let session_info = data.get("sessionInfo");
    let pick = |key: &str| -> Option<String> {
        let from_session = session_info
            .and_then(|s| s.get(key))
            .and_then(|v| v.as_str());
        let from_root = data.get(key).and_then(|v| v.as_str());
        from_session
            .or(from_root)
            .map(str::to_string)
            .filter(|s| !s.is_empty())
    };
    let expires_in_secs = data
        .get("validityInSeconds")
        .or_else(|| data.get("masterValidityInSeconds"))
        .and_then(|v| v.as_u64());
    Some(ParsedLogin {
        token,
        role: pick("roleName").or_else(|| pick("role")),
        warehouse: pick("warehouseName").or_else(|| pick("warehouse")),
        database: pick("databaseName").or_else(|| pick("database")),
        schema: pick("schemaName").or_else(|| pick("schema")),
        expires_in_secs,
    })
}

fn register_from_login(state: &ProxyState, parsed: ParsedLogin) -> Arc<SessionInfo> {
    let info = state.sessions.register(parsed.token);
    state.sessions.update(&info.token, |s| {
        s.role = parsed.role.clone();
        s.warehouse = parsed.warehouse.clone();
        s.database = parsed.database.clone();
        s.schema = parsed.schema.clone();
        if let Some(ttl) = parsed.expires_in_secs {
            s.expires_at = std::time::Instant::now() + std::time::Duration::from_secs(ttl);
        }
    });
    info
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("authorization")?.to_str().ok()?;
    // Accept both `Bearer <token>` (REST API) and Snowflake's
    // `Snowflake Token="..."` form drivers send on legacy paths.
    if let Some(rest) = raw.strip_prefix("Bearer ") {
        return Some(rest.trim().to_string());
    }
    if let Some(rest) = raw.strip_prefix("Snowflake Token=") {
        let cleaned = rest.trim_matches('"').trim().to_string();
        return Some(cleaned);
    }
    None
}

pub(crate) fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    bearer(headers)
}
