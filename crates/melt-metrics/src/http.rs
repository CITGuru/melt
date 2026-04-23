use std::future::Future;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;

use crate::{registry, AdminHooks, MetricsConfig, Result};

#[derive(Clone)]
struct AdminState {
    hooks: AdminHooks,
    /// Admin bearer token. `None` means unauthenticated. Evaluated
    /// for every request; missing token + non-loopback listen is
    /// rejected at `serve` time so this is never `None` on a public
    /// socket.
    token: Option<String>,
}

pub async fn serve<F>(cfg: &MetricsConfig, hooks: AdminHooks, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let Some(addr) = cfg.listen else {
        tracing::info!("metrics admin server disabled (no listen address configured)");
        // Honour the shutdown signal even when we're not actually
        // serving — otherwise this branch would hang forever inside
        // the CLI's `tokio::try_join!`, defeating the whole point of
        // cooperative shutdown.
        shutdown.await;
        return Ok(());
    };

    let token = crate::resolve_admin_token(cfg)?;
    let is_loopback = addr.ip().is_loopback();
    if token.is_none() && !is_loopback {
        return Err(crate::MetricsError::Init(format!(
            "admin server bound to non-loopback {addr} but no admin token \
             configured — set [metrics].admin_token_file"
        )));
    }

    let state = AdminState { hooks, token };
    let mut app = axum::Router::new()
        .route("/metrics", get(prometheus_handler))
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz_handler));

    if state.hooks.reload.is_some() {
        app = app.route("/admin/reload", post(reload_handler));
    }

    let app = app.with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "metrics admin server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| crate::MetricsError::Init(format!("axum serve: {e}")))?;
    tracing::info!(%addr, "metrics admin server stopped");
    Ok(())
}

async fn prometheus_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        registry::render(),
    )
}

async fn readyz_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let ready = match state.hooks.readiness.as_ref() {
        Some(p) => p.check().await,
        None => true,
    };
    if ready {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn reload_handler(State(state): State<AdminState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(err) = authorize(&state, &headers) {
        return err.into_response();
    }
    let Some(reload) = state.hooks.reload.as_ref() else {
        return (StatusCode::NOT_IMPLEMENTED, "reload handler not wired").into_response();
    };
    let resp = (reload)().await;
    let code = if resp.ok {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    (code, Json(resp)).into_response()
}

fn authorize(state: &AdminState, headers: &HeaderMap) -> std::result::Result<(), AuthError> {
    let Some(expected) = state.token.as_ref() else {
        return Ok(());
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(tok) if constant_time_eq(tok.as_bytes(), expected.as_bytes()) => Ok(()),
        Some(_) => Err(AuthError::Forbidden),
        None => Err(AuthError::Unauthorized),
    }
}

enum AuthError {
    Unauthorized,
    Forbidden,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AuthError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "missing or malformed Authorization header",
            )
                .into_response(),
            AuthError::Forbidden => (StatusCode::FORBIDDEN, "admin token mismatch").into_response(),
        }
    }
}

/// Constant-time slice equality — prevents the trivial string-compare
/// timing oracle when validating the admin token.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
