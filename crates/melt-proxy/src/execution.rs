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
use melt_core::{
    HybridPlan, MeltError, QueryContext, RecordBatchStream, Result, Route, SessionInfo,
};
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
    /// Dual-execution result. Same shape as [`LakeExecution`] for
    /// envelope-construction purposes — handlers can reuse their
    /// `build_v*_lake_response` codepaths with minimal branching.
    /// The carried [`HybridPlan`] is kept for EXPLAIN / observability.
    Hybrid(HybridExecution),
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

pub struct HybridExecution {
    pub outcome: RouteOutcome,
    pub schema: Option<arrow_schema::SchemaRef>,
    pub eager_batches: Vec<arrow::record_batch::RecordBatch>,
    pub continuation: Option<RecordBatchStream>,
    /// The hybrid plan that produced this result. Carried for
    /// EXPLAIN-style logging and the `melt route` CLI output.
    pub plan: Arc<HybridPlan>,
}

/// Categorised failure for the hybrid execution path. Used by [`run`]
/// to decide whether to fall back to a Snowflake passthrough or
/// surface the error to the caller. Mirrors OpenDuck's
/// `HybridError::is_fallback_eligible` policy: only `Unavailable`
/// (transport-class) errors fall back; backend SQL errors and
/// auth failures surface to the client so translation/emission
/// bugs aren't masked.
#[derive(Debug)]
pub enum HybridError {
    /// Transport / connection failure to Snowflake (timeout,
    /// DNS, TCP reset, 5xx). Eligible for fallback to passthrough.
    Unavailable(MeltError),
    /// Snowflake returned a SQL error on a Materialize fragment, or
    /// the local DuckDB execution failed for a reason other than
    /// transport. Surface to the client — masking these would hide
    /// translation regressions.
    Backend(MeltError),
}

impl HybridError {
    /// `true` when the caller may safely fall back to a full
    /// Snowflake passthrough.
    pub fn is_fallback_eligible(&self) -> bool {
        matches!(self, HybridError::Unavailable(_))
    }

    fn into_melt(self) -> MeltError {
        match self {
            HybridError::Unavailable(e) | HybridError::Backend(e) => e,
        }
    }
}

impl std::fmt::Display for HybridError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HybridError::Unavailable(e) => write!(f, "hybrid unavailable: {e}"),
            HybridError::Backend(e) => write!(f, "hybrid backend: {e}"),
        }
    }
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
        Route::Hybrid { plan, .. } => {
            let plan = plan.clone();
            match execute_hybrid(&input, outcome.clone(), plan).await {
                Ok(hy) => Ok(Executed::Hybrid(hy)),
                Err(e) if e.is_fallback_eligible() => {
                    counter!(melt_metrics::PROXY_FALLBACKS).increment(1);
                    counter!(
                        melt_metrics::HYBRID_FALLBACKS,
                        melt_metrics::LABEL_REASON => "unavailable",
                    )
                    .increment(1);
                    tracing::warn!(
                        error = %e,
                        api = input.api,
                        "hybrid path failed before first byte; falling back to Snowflake",
                    );
                    execute_passthrough(&input).await.map(Executed::Passthrough)
                }
                Err(e) => {
                    counter!(
                        melt_metrics::HYBRID_REMOTE_ERRORS,
                        melt_metrics::LABEL_REASON => "backend",
                    )
                    .increment(1);
                    Err(e.into_melt())
                }
            }
        }
        Route::Snowflake { .. } => execute_passthrough(&input).await.map(Executed::Passthrough),
    };
    histogram!(
        melt_metrics::BACKEND_EXEC_LATENCY,
        melt_metrics::LABEL_BACKEND => input.state.backend.kind().as_str()
    )
    .record(exec_start.elapsed().as_secs_f64());

    let (outcome_label, final_route_label) = match &result {
        Ok(Executed::Lake(_)) | Ok(Executed::Hybrid(_)) => {
            (melt_metrics::OUTCOME_OK, outcome.route.as_str())
        }
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

/// Hybrid (dual-execution) path. Handles three sub-shapes uniformly:
///
/// - **Attach-only** plans: `local_sql` already references
///   `sf_link.<db>.<schema>.<t>` for every node. We just call the
///   standard backend `execute()` (the same path that powers Lake)
///   and shape the result into a [`HybridExecution`] envelope. The
///   community Snowflake DuckDB extension handles predicate /
///   projection pushdown automatically.
/// - **Materialize-only** plans: each `RemoteFragment` stages via
///   `CREATE TEMP TABLE __remote_N AS <fragment_sql>` against the
///   backend (which has `sf_link` attached, so the fragment runs
///   through the extension and the data lands in DuckDB without an
///   out-of-band HTTP path). Then `local_sql` references the staged
///   placeholders.
/// - **Mixed** plans: stage the Materialize fragments first, then
///   run `local_sql` which references both `__remote_N` temp tables
///   and `sf_link.*` aliases. DuckDB sees no difference — both look
///   like ordinary catalog entries to the executor.
///
/// Optional pre-step: when the proxy's hybrid result cache is
/// configured and an identical (database, schema, sql) was served
/// recently, return cached batches and skip the entire backend
/// pipeline.
async fn execute_hybrid(
    input: &RouteInput<'_>,
    outcome: RouteOutcome,
    plan: Arc<HybridPlan>,
) -> std::result::Result<HybridExecution, HybridError> {
    let ctx = QueryContext::from_session(&input.session);

    // Hybrid result cache lookup (statement-level). When enabled
    // and a hit is present, we skip the entire fragment-staging +
    // local_sql pipeline and return cached batches directly. See
    // `crates/melt-proxy/src/hybrid_cache.rs` for the design.
    if let Some(cache) = &input.state.hybrid_cache {
        let key = crate::hybrid_cache::FragmentCache::key_for(
            &input.sql,
            input.session.database.as_deref().unwrap_or(""),
            input.session.schema.as_deref().unwrap_or(""),
        );
        if let Some((schema, batches)) = cache.get(&key) {
            tracing::info!(
                api = input.api,
                rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64,
                cached_at_ttl = ?input.state.router_cfg.hybrid_fragment_cache_ttl,
                "hybrid: cache hit; skipping Snowflake roundtrip",
            );
            return Ok(HybridExecution {
                outcome,
                schema,
                eager_batches: batches,
                continuation: None,
                plan,
            });
        }
    }

    // Step 1 — Materialize: stage each RemoteFragment into a
    // `TEMP TABLE __remote_i` on the backend. The fragment SQL is
    // already in DuckDB-dialect form with `sf_link.<...>` aliases
    // (the builder applied the translate + attach-alias rewrite),
    // so the backend executes it directly through the attached
    // Snowflake catalog. Snowflake does the join natively; DuckDB
    // materializes the result into a temp table.
    //
    // v1 uses `CREATE TEMP TABLE AS` rather than a direct Arrow IPC
    // fetch + Appender because it keeps the materialization entirely
    // inside DuckDB (no separate HTTP path) and reuses the same
    // sf_link extension that's already loaded for Attach. When/if
    // the Arrow-IPC path lands (e.g. to side-step the extension
    // being flaky), it slots in as a second branch here.
    if !plan.remote_fragments.is_empty() {
        let materialize_start = Instant::now();
        for frag in &plan.remote_fragments {
            let stage_sql = format!(
                "CREATE TEMP TABLE {name} AS {body}",
                name = frag.placeholder,
                body = frag.snowflake_sql,
            );
            // Streaming execute returns a RecordBatchStream; for DDL
            // we only need it to run to completion. Drain so the
            // backend actually commits the work.
            let mut stream = input
                .state
                .backend
                .execute(&stage_sql, &ctx)
                .await
                .map_err(|e| {
                    tracing::warn!(
                        error = %e,
                        api = input.api,
                        placeholder = %frag.placeholder,
                        fragment = %frag.snowflake_sql.chars().take(200).collect::<String>(),
                        "hybrid: fragment staging failed",
                    );
                    HybridError::Unavailable(e)
                })?;
            while let Some(batch) = stream.as_mut().next().await {
                batch.map_err(|e| {
                    tracing::warn!(
                        error = %e,
                        api = input.api,
                        placeholder = %frag.placeholder,
                        "hybrid: fragment drain errored",
                    );
                    HybridError::Unavailable(e)
                })?;
            }
        }
        let elapsed = materialize_start.elapsed().as_secs_f64();
        histogram!(melt_metrics::HYBRID_MATERIALIZE_LATENCY).record(elapsed);
        tracing::info!(
            api = input.api,
            fragments = plan.remote_fragments.len(),
            elapsed_seconds = elapsed,
            "hybrid: fragments staged",
        );
    }

    // Step 2 — Execute local_sql. It already references the staged
    // `__remote_i` temp tables (Materialize) and `sf_link.<...>`
    // aliases (Attach). DuckDB runs the final join / projection.
    let translated = &plan.local_sql;

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
                "hybrid attach: backend refused local_sql",
            );
            // Backend setup failures are likely the sf_link extension
            // not being loaded — recoverable via passthrough.
            HybridError::Unavailable(e)
        })?;

    let mut eager = Vec::new();
    let mut schema: Option<arrow_schema::SchemaRef> = None;

    match stream.as_mut().next().await {
        Some(Ok(batch)) => {
            schema = Some(batch.schema());
            eager.push(batch);
        }
        Some(Err(e)) => {
            tracing::warn!(error = %e, api = input.api, "hybrid: first batch failed");
            return Err(HybridError::Unavailable(e));
        }
        None => {}
    }

    let continuation = if input.drain_full {
        while let Some(batch) = stream.as_mut().next().await {
            let batch = batch.map_err(|e| {
                tracing::warn!(error = %e, api = input.api, "hybrid: mid-stream error");
                // Mid-stream failures are past the fallback window —
                // surface as Backend so the caller errors instead of
                // silently passthrough'ing a partial result.
                HybridError::Backend(e)
            })?;
            eager.push(batch);
        }
        None
    } else {
        Some(stream)
    };

    // Offer a parity sample if the harness is up. The sampler's
    // `try_send` handles the probability roll + drop-on-full
    // backpressure internally — this call is cheap and non-blocking.
    //
    // Plan summary is built on the proxy side (vs. inside the harness)
    // so the WARN log is fully self-describing without the harness
    // needing to depend on `melt-router::route` types. The eager
    // batches are only cloned in `Hash` compare mode — `RowCount` mode
    // ignores them, so we save the clone in the common case.
    if let Some(parity) = &input.state.parity {
        let row_count: u64 = eager.iter().map(|b| b.num_rows() as u64).sum();
        let compare_mode = parity.compare_mode();
        let eager_for_sample = if matches!(compare_mode, melt_core::HybridParityCompareMode::Hash,)
        {
            eager.clone()
        } else {
            Vec::new()
        };
        let plan_summary = crate::hybrid_parity::PlanSummary {
            strategy: plan.strategy_label().to_string(),
            reason: format_hybrid_reason(&outcome.route),
            fragments: plan.remote_fragments.len(),
            attach_rewrites: plan.attach_rewrites.len(),
            remote_table_count: count_remote_tables(plan.as_ref()),
            estimated_remote_bytes: plan.estimated_remote_bytes,
            chain_decided_by: plan.chain_decided_by.clone(),
        };
        let sample = crate::hybrid_parity::ParitySample {
            query_id: uuid::Uuid::new_v4().to_string(),
            query_hash: crate::hybrid_parity::hash_query(&input.sql),
            original_sql: input.sql.clone(),
            token: input.token.clone(),
            hybrid_row_count: row_count,
            hybrid_eager_batches: eager_for_sample,
            plan_summary,
            compare_mode,
            sample_rate: parity.sample_rate(),
        };
        let _ = parity.sample(sample);
    }

    // Profiler tap. When `router.hybrid_profile_attach_queries = true`
    // AND this plan has Attach nodes, run `EXPLAIN ANALYZE local_sql`
    // and log every line carrying a `query_string` (the field DuckDB's
    // community Snowflake extension annotates `snowflake_scan`
    // operators with). The doubled query cost is why this is opt-in
    // and only triggers on Attach plans — Materialize plans expose
    // the outgoing SQL via `RemoteFragment::snowflake_sql` already.
    //
    // Failures are non-fatal — the main query already returned;
    // profiling is observability not correctness.
    if input.state.router_cfg.hybrid_profile_attach_queries && !plan.attach_rewrites.is_empty() {
        match input
            .state
            .backend
            .analyze_query(&plan.local_sql, &ctx)
            .await
        {
            Ok(plan_text) if !plan_text.is_empty() => {
                let attach_lines: Vec<&str> = plan_text
                    .lines()
                    .filter(|l| l.contains("snowflake_scan") || l.contains("query_string"))
                    .collect();
                tracing::info!(
                    api = input.api,
                    attach_node_count = plan.attach_rewrites.len(),
                    snowflake_operator_lines = attach_lines.len(),
                    profile = %attach_lines.join(" | "),
                    "hybrid attach profiler",
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "hybrid attach profiler EXPLAIN ANALYZE failed; ignoring",
                );
            }
        }
    }

    // Cache write — only when we have the full result set (drain_full)
    // AND the cache is enabled. Partial results (continuation = Some)
    // would corrupt the cache because we'd return cached "first batch
    // + cached continuation pointer" later when the cached
    // continuation no longer exists.
    if let (Some(cache), true) = (&input.state.hybrid_cache, input.drain_full) {
        let key = crate::hybrid_cache::FragmentCache::key_for(
            &input.sql,
            input.session.database.as_deref().unwrap_or(""),
            input.session.schema.as_deref().unwrap_or(""),
        );
        let exec_for_tables = HybridExecution {
            outcome: outcome.clone(),
            schema: schema.clone(),
            eager_batches: eager.clone(),
            continuation: None,
            plan: plan.clone(),
        };
        let scanned = crate::hybrid_cache::cache_write_from_execution(&exec_for_tables);
        cache.insert(key, scanned, schema.clone(), eager.clone());
    }

    Ok(HybridExecution {
        outcome,
        schema,
        eager_batches: eager,
        continuation,
        plan,
    })
}

/// Render the routing reason for the parity log line. Hybrid plans
/// always carry a [`HybridReason`]; falls through to a generic label
/// for non-hybrid routes (which the parity sampler should never see
/// today, but the helper is total to keep the call site simple).
fn format_hybrid_reason(route: &Route) -> String {
    match route {
        Route::Hybrid { reason, .. } => reason.to_string(),
        Route::Lake { .. } => "Lake".to_string(),
        Route::Snowflake { .. } => "Snowflake".to_string(),
    }
}

/// Distinct table count across both Materialize fragments and Attach
/// rewrites. Used purely for the parity log line so operators see
/// fan-out without us having to log table names (PII risk).
fn count_remote_tables(plan: &melt_core::HybridPlan) -> usize {
    let mut seen = std::collections::HashSet::new();
    let key = |t: &melt_core::TableRef| format!("{}.{}.{}", t.database, t.schema, t.name);
    for frag in &plan.remote_fragments {
        for t in &frag.scanned_tables {
            seen.insert(key(t));
        }
    }
    for rw in &plan.attach_rewrites {
        seen.insert(key(&rw.original));
    }
    seen.len()
}
