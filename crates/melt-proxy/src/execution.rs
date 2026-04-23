//! Shared routing + execution core for every SQL-bearing surface the
//! proxy exposes.
//!
//! The v1 (`/queries/v1/query-request`) and v2 (`/api/v2/statements`)
//! wire formats differ at the edges — request field names, response
//! envelopes, passthrough URLs — but the work in the middle is
//! identical: auth, concurrency caps, router decision, backend
//! execute (with fallback), metrics. Duplicating that across handlers
//! is how feature asymmetries creep in (e.g. v1 had a Lake→Snowflake
//! fallback that v2 didn't). Everything interesting lives in this
//! module; handlers are tiny adapters.
//!
//! ## Contract
//!
//! Handlers call [`run`] with a [`RouteInput`] that names:
//! * the extracted SQL string (wire-format-specific parse),
//! * the original request body (so passthrough can replay it verbatim),
//! * the passthrough URL that matches the caller's wire format,
//! * a `drain_full` flag — `true` drains the whole DuckDB stream into
//!   the initial response (v1 has no partition-polling convention),
//!   `false` pulls only the first batch and hands the continuation
//!   stream back for the caller to store and expose via the v2
//!   partition-polling API.
//!
//! Everything else — concurrency acquisition, routing, the
//! "fall back to Snowflake before any byte goes out" rule, metric
//! bumps, and the `statement_complete` log line — is handled inside
//! [`run`].

use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use futures::StreamExt;
use melt_core::{MeltError, QueryContext, RecordBatchStream, Result, Route, SessionInfo};
use melt_router::RouteOutcome;
use melt_snowflake::PassthroughResponse;
use metrics::{counter, histogram};

use crate::server::ProxyState;

/// Every piece of state a handler feeds into [`run`]. Lifetimes are
/// scoped to the single request — nothing here outlives the await.
pub struct RouteInput<'a> {
    pub state: &'a ProxyState,
    pub session: Arc<SessionInfo>,
    /// Session bearer — required for the Snowflake passthrough path
    /// (`Authorization: Snowflake Token="..."`). Handlers extract
    /// this via `session::extract_bearer` before calling.
    pub token: String,
    /// SQL text the driver sent. v1 pulled this from `sqlText`, v2
    /// from `statement`; `run` doesn't care which.
    pub sql: String,
    /// Original request body, replayed verbatim on the passthrough
    /// path. This is what carries driver-specific framing (compressed
    /// or not, any extra fields like `bindings`, `parameters`).
    pub body: Bytes,
    /// Raw query string from the inbound request. Forwarded to
    /// upstream alongside the body so `request_id`, `databaseName`,
    /// `roleName`, etc. stay intact.
    pub query: Option<&'a str>,
    /// Caller headers to replay on upstream (compression, accept,
    /// user-agent). Curated allowlist is applied inside
    /// `SnowflakeClient::passthrough_full`.
    pub headers: &'a HeaderMap,
    /// Upstream URL for the passthrough leg — `/api/v2/statements`
    /// for v2, `/queries/v1/query-request` for v1.
    pub passthrough_path: &'a str,
    /// API label used in the `statement_complete` log + metrics. Lets
    /// operators separate v1 and v2 traffic without reading handler
    /// source. `"v1"` / `"v2"`.
    pub api: &'static str,
    /// See module-level doc. `true` for v1 (drain everything into the
    /// initial response), `false` for v2 (pull first batch, hand the
    /// rest back as `Executed::Lake.continuation`).
    pub drain_full: bool,
}

/// What [`run`] produced. Handlers pattern-match on this to build
/// their wire-specific response envelope.
pub enum Executed {
    Lake(LakeExecution),
    Passthrough(PassthroughResponse),
}

pub struct LakeExecution {
    /// Route outcome from the router — kept so handlers can pull
    /// `translated_sql`, reason labels, etc. if they need to surface
    /// any of that in the response.
    pub outcome: RouteOutcome,
    /// Schema of the first batch. `None` when the query returned
    /// zero rows and zero batches (DuckDB omits an empty schema
    /// sometimes).
    pub schema: Option<arrow_schema::SchemaRef>,
    /// Batches already materialised into memory. For `drain_full=true`
    /// this is every batch the backend produced; for
    /// `drain_full=false` it's just the first one (so the caller can
    /// inspect the schema + cells for the initial response body).
    pub eager_batches: Vec<arrow::record_batch::RecordBatch>,
    /// Stream with the remaining batches. Always `None` when
    /// `drain_full=true`; `Some` when `drain_full=false` AND the
    /// stream wasn't exhausted by the eager-first-batch pull. The
    /// caller is expected to hand this to `ResultStore` and expose
    /// a continuation handle.
    pub continuation: Option<RecordBatchStream>,
}

/// Run one SQL request end-to-end: acquire concurrency, route,
/// execute, apply the fallback rule, emit metrics + a
/// `statement_complete` log. Returns an [`Executed`] the handler
/// shapes into its wire-format response envelope.
///
/// Fallback rule (same on v1 and v2 now that this is shared):
///
/// * If the router returns `Lake`, execute on the backend.
/// * If anything fails before the first response byte leaves the
///   proxy — `backend.execute()` errored, or the first batch came
///   back as `Err` — fall back to a Snowflake passthrough. The
///   driver never sees a 500 on a query the upstream would have
///   accepted.
/// * Once a first batch is successfully materialised, we're past the
///   fallback window; subsequent stream errors bubble up as normal
///   execution failures and the driver gets whatever partial result
///   we already sent.
pub async fn run(input: RouteInput<'_>) -> Result<Executed> {
    let Ok(_global) = input.state.sessions.global.clone().try_acquire_owned() else {
        return Err(MeltError::TooManyStatements);
    };
    let Ok(_per_session) = input.session.concurrency.clone().try_acquire_owned() else {
        return Err(MeltError::TooManyStatements);
    };

    let route_start = Instant::now();
    let loaded_matcher = input.state.sync_matcher.load_full();
    let matcher_ref: Option<&melt_core::SyncTableMatcher> =
        loaded_matcher.as_ref().as_ref().map(|a| a.as_ref());
    let outcome = melt_router::route(
        &input.sql,
        &input.session,
        input.state.backend.as_ref(),
        &input.state.router_cfg,
        &input.state.snowflake_cfg,
        &input.state.router_cache,
        matcher_ref,
        input.state.discovery.as_ref(),
    )
    .await;
    histogram!(melt_metrics::ROUTER_DECISION_LATENCY).record(route_start.elapsed().as_secs_f64());

    let exec_start = Instant::now();
    let result: Result<Executed> = match &outcome.route {
        Route::Lake { .. } => match execute_lake(&input, outcome.clone()).await {
            Ok(lake) => Ok(Executed::Lake(lake)),
            Err(e) => {
                // Pre-first-byte fallback to Snowflake.
                counter!(melt_metrics::PROXY_FALLBACKS).increment(1);
                tracing::warn!(
                    error = %e,
                    api = input.api,
                    "lake path failed before first byte; falling back to Snowflake",
                );
                execute_passthrough(&input).await.map(Executed::Passthrough)
            }
        },
        Route::Snowflake { .. } => execute_passthrough(&input).await.map(Executed::Passthrough),
    };
    histogram!(
        melt_metrics::BACKEND_EXEC_LATENCY,
        melt_metrics::LABEL_BACKEND => input.state.backend.kind().as_str()
    )
    .record(exec_start.elapsed().as_secs_f64());

    let (outcome_label, final_route_label) = match &result {
        Ok(Executed::Lake(_)) => (melt_metrics::OUTCOME_OK, outcome.route.as_str()),
        // Passthrough may be planned or fallback; PROXY_FALLBACKS metric distinguishes.
        Ok(Executed::Passthrough(_)) => (melt_metrics::OUTCOME_OK, "snowflake"),
        Err(_) => (melt_metrics::OUTCOME_ERR, outcome.route.as_str()),
    };
    tracing::info!(
        route = final_route_label,
        backend = input.state.backend.kind().as_str(),
        outcome = outcome_label,
        api = input.api,
        "statement_complete",
    );

    result
}

/// Lake-path execution — shared by v1 and v2. Pulls batches until
/// `drain_full` is satisfied; returns whatever was collected plus
/// (maybe) a continuation stream.
async fn execute_lake(input: &RouteInput<'_>, outcome: RouteOutcome) -> Result<LakeExecution> {
    let translated = outcome
        .translated_sql
        .as_deref()
        .ok_or_else(|| MeltError::backend("router returned Lake without translated SQL"))?;

    let ctx = QueryContext::from_session(&input.session);
    let mut stream = input
        .state
        .backend
        .execute(translated, &ctx)
        .await
        .map_err(|e| {
            tracing::warn!(
                error = %e,
                translated = %translated.chars().take(200).collect::<String>(),
                api = input.api,
                "lake: backend refused translated SQL",
            );
            e
        })?;

    let mut eager = Vec::new();
    let mut schema: Option<arrow_schema::SchemaRef> = None;

    // Pull the first batch regardless of drain mode — it anchors the
    // fallback window (any error here is recoverable) and the schema
    // (needed for the response metadata).
    match stream.as_mut().next().await {
        Some(Ok(batch)) => {
            schema = Some(batch.schema());
            eager.push(batch);
        }
        Some(Err(e)) => {
            tracing::warn!(error = %e, api = input.api, "lake: first batch failed");
            return Err(e);
        }
        None => {
            // Empty result set — legal. Schema will be None; handlers
            // must be prepared to emit an empty `rowset` / `data`.
        }
    }

    if input.drain_full {
        // v1: finish draining before responding. Errors here are
        // past the fallback window by design — a partial result is
        // still more informative than a 500 with no context.
        while let Some(batch) = stream.as_mut().next().await {
            let batch = batch.map_err(|e| {
                tracing::warn!(error = %e, api = input.api, "lake: mid-stream error");
                e
            })?;
            eager.push(batch);
        }
        Ok(LakeExecution {
            outcome,
            schema,
            eager_batches: eager,
            continuation: None,
        })
    } else {
        // v2: hand the remainder back for the caller to store in
        // `ResultStore`. We don't know whether the stream is
        // exhausted without polling; leave that decision to the
        // caller so this function stays oblivious to ResultStore
        // internals.
        Ok(LakeExecution {
            outcome,
            schema,
            eager_batches: eager,
            continuation: Some(stream),
        })
    }
}

/// Snowflake passthrough — forwards the original body, query string,
/// and curated request headers to upstream, and returns the full
/// [`PassthroughResponse`] so the handler can replay
/// `Content-Encoding`, `content-type`, etc. on the outbound response.
async fn execute_passthrough(input: &RouteInput<'_>) -> Result<PassthroughResponse> {
    input
        .state
        .snowflake
        .passthrough_full(
            Method::POST,
            input.passthrough_path,
            input.query,
            input.headers,
            &input.token,
            input.body.clone(),
        )
        .await
}
