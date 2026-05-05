//! Centralized metric & label names. Misspellings become compile errors.

pub const ROUTER_DECISIONS: &str = "melt_router_decisions_total";
pub const ROUTER_POLICY_PASSTHROUGH: &str = "melt_router_policy_passthrough_total";
pub const PROXY_REQUESTS: &str = "melt_proxy_requests_total";
pub const PROXY_FALLBACKS: &str = "melt_proxy_lake_fallbacks_total";
pub const PROXY_TIMEOUTS: &str = "melt_proxy_timeouts_total";
pub const SYNC_ROWS_APPLIED: &str = "melt_sync_rows_applied_total";
pub const SYNC_POLICY_REFRESHES: &str = "melt_sync_policy_refreshes_total";
pub const SYNC_BOOTSTRAP_FAILURES: &str = "melt_sync_bootstrap_failures_total";
pub const SYNC_STREAM_STALE: &str = "melt_sync_stream_stale_total";
pub const SYNC_VIEWS: &str = "melt_sync_views_total";
pub const ROUTER_VIEW_HITS: &str = "melt_router_view_hits_total";
pub const ADMIN_RELOADS: &str = "melt_admin_reloads_total";

pub const PROXY_LATENCY: &str = "melt_proxy_request_duration_seconds";
pub const ROUTER_DECISION_LATENCY: &str = "melt_router_decision_duration_seconds";
pub const BACKEND_EXEC_LATENCY: &str = "melt_backend_execute_duration_seconds";
pub const SNOWFLAKE_PASSTHROUGH: &str = "melt_snowflake_passthrough_duration_seconds";
pub const TRANSLATE_LATENCY: &str = "melt_translate_duration_seconds";
pub const SYNC_LAG_SECONDS: &str = "melt_sync_lag_seconds";
pub const SYNC_BOOTSTRAP_DURATION_SECONDS: &str = "melt_sync_bootstrap_duration_seconds";

pub const DUCKDB_POOL_IN_USE: &str = "melt_duckdb_pool_in_use";
pub const DUCKDB_POOL_IDLE: &str = "melt_duckdb_pool_idle";
pub const RESULT_STORE_BYTES: &str = "melt_result_store_bytes";
pub const ACTIVE_SESSIONS: &str = "melt_active_sessions";
pub const SYNC_TABLES_BY_STATE: &str = "melt_sync_tables";

pub const LABEL_ROUTE: &str = "route";
pub const LABEL_BACKEND: &str = "backend";
pub const LABEL_TABLE: &str = "table";
pub const LABEL_OUTCOME: &str = "outcome";
pub const LABEL_REASON: &str = "reason";
pub const LABEL_STRATEGY: &str = "strategy";

pub const OUTCOME_OK: &str = "ok";
pub const OUTCOME_ERR: &str = "err";
pub const OUTCOME_FALLBACK: &str = "fallback";
pub const OUTCOME_CANCELLED: &str = "cancelled";

// ── Hybrid (dual-execution) router metrics ───────────────────────
// `melt_router_decisions_total{route="hybrid"}` is auto-covered by
// the existing ROUTER_DECISIONS counter.
pub const HYBRID_REASONS: &str = "melt_router_hybrid_reasons_total";
pub const HYBRID_STRATEGY: &str = "melt_hybrid_strategy_total";
pub const HYBRID_PUSHDOWN_COLLAPSED: &str = "melt_hybrid_pushdown_collapsed_total";
pub const HYBRID_FALLBACKS: &str = "melt_hybrid_fallbacks_total";
pub const HYBRID_REMOTE_ERRORS: &str = "melt_hybrid_remote_errors_total";
pub const HYBRID_ATTACH_UNAVAILABLE: &str = "melt_hybrid_attach_unavailable_total";
pub const HYBRID_PARITY_MISMATCHES: &str = "melt_hybrid_parity_mismatches_total";
pub const HYBRID_PARITY_SAMPLE_DROPS: &str = "melt_hybrid_parity_sample_drops_total";

pub const HYBRID_REMOTE_SCAN_BYTES: &str = "melt_hybrid_remote_scan_bytes";
pub const HYBRID_MATERIALIZE_LATENCY: &str = "melt_hybrid_materialize_latency_seconds";
pub const HYBRID_ATTACH_NODES_PER_QUERY: &str = "melt_hybrid_attach_nodes_per_query";
pub const HYBRID_MATERIALIZE_NODES_PER_QUERY: &str = "melt_hybrid_materialize_nodes_per_query";

/// Strategy chain decision counter — labels: `strategy=<name>` (the
/// chain member that answered, e.g. `cost`, `heuristic`, `fallback`)
/// and `decision=collapse|skip`. Lets operators see WHO is making
/// each routing decision and the distribution between Attach and
/// Materialize at decide-time.
pub const HYBRID_STRATEGY_DECISIONS: &str = "melt_hybrid_strategy_decisions_total";
