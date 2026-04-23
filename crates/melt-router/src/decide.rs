use std::sync::Arc;

use bytesize::ByteSize;
use melt_core::config::RouterConfig;
use melt_core::{
    DiscoveryCatalog, LakeReason, MatchOutcome, PassthroughReason, PolicyMode, Route, SessionInfo,
    StorageBackend, SyncSource, SyncState, SyncTableMatcher, TableRef,
};
use melt_snowflake::SnowflakeConfig;
use metrics::counter;

use crate::classify;
use crate::enforce;
use crate::parse;
use crate::stats::Cache;
use crate::translate;

#[derive(Clone, Debug)]
pub struct RouteOutcome {
    pub route: Route,
    pub translated_sql: Option<String>,
}

impl RouteOutcome {
    pub fn passthrough(reason: PassthroughReason) -> Self {
        Self {
            route: Route::Snowflake { reason },
            translated_sql: None,
        }
    }

    pub fn lake(reason: LakeReason, translated: String) -> Self {
        Self {
            route: Route::Lake { reason },
            translated_sql: Some(translated),
        }
    }
}

/// The router decision. Concurrency-friendly: every catalog round
/// trip goes through the shared `Cache`.
#[allow(clippy::too_many_arguments)]
pub async fn route(
    sql: &str,
    session: &SessionInfo,
    backend: &dyn StorageBackend,
    cfg: &RouterConfig,
    sf_cfg: &SnowflakeConfig,
    cache: &Arc<Cache>,
    matcher: Option<&SyncTableMatcher>,
    discovery: Option<&Arc<dyn DiscoveryCatalog>>,
) -> RouteOutcome {
    let outcome = decide_inner(
        sql, session, backend, cfg, sf_cfg, cache, matcher, discovery,
    )
    .await;
    counter!(
        melt_metrics::ROUTER_DECISIONS,
        melt_metrics::LABEL_ROUTE => outcome.route.as_str(),
        melt_metrics::LABEL_BACKEND => backend.kind().as_str(),
    )
    .increment(1);
    if let Route::Snowflake { reason } = &outcome.route {
        if matches!(reason, PassthroughReason::PolicyProtected { .. }) {
            counter!(
                melt_metrics::ROUTER_POLICY_PASSTHROUGH,
                melt_metrics::LABEL_REASON => "policy_protected",
            )
            .increment(1);
        } else if matches!(reason, PassthroughReason::NotInAllowList { .. }) {
            counter!(
                melt_metrics::ROUTER_POLICY_PASSTHROUGH,
                melt_metrics::LABEL_REASON => "not_in_allowlist",
            )
            .increment(1);
        }
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn decide_inner(
    sql: &str,
    session: &SessionInfo,
    backend: &dyn StorageBackend,
    cfg: &RouterConfig,
    sf_cfg: &SnowflakeConfig,
    cache: &Arc<Cache>,
    matcher: Option<&SyncTableMatcher>,
    discovery: Option<&Arc<dyn DiscoveryCatalog>>,
) -> RouteOutcome {
    let mut ast = match parse::parse(sql) {
        Ok(ast) => ast,
        Err(_) => return RouteOutcome::passthrough(PassthroughReason::ParseFailed),
    };

    if classify::is_write(&ast) {
        return RouteOutcome::passthrough(PassthroughReason::WriteStatement);
    }
    if let Some(name) = classify::uses_snowflake_features(&ast) {
        return RouteOutcome::passthrough(PassthroughReason::UsesSnowflakeFeature(name));
    }

    let tables = classify::extract_tables(&ast, session);

    // No tables touched → cheap pure expression. Send to lake (DuckDB
    // can compute it locally) or to Snowflake — both are correct, we
    // pick lake to avoid the network hop.
    if tables.is_empty() {
        return finish_lake(&mut ast, 0).await;
    }

    // Sync-state gate when matcher+catalog wired; legacy tables_exist
    // path otherwise (e.g. `melt route` lazy CLI).
    if let (Some(matcher), Some(catalog)) = (matcher, discovery) {
        if let Some(reason) = check_sync_state(matcher, catalog, &tables).await {
            return RouteOutcome::passthrough(reason);
        }
    } else {
        let exists = cache.tables_exist(backend, &tables).await;
        if let Some((idx, _)) = exists.iter().enumerate().find(|(_, e)| !**e) {
            return RouteOutcome::passthrough(PassthroughReason::TableMissing(tables[idx].clone()));
        }
    }

    let (markers, bytes) = tokio::join!(
        cache.policy_markers(backend, &tables),
        cache.estimate_bytes(backend, &tables),
    );

    match &sf_cfg.policy.mode {
        PolicyMode::Passthrough => {
            if let Some((idx, name)) = markers
                .iter()
                .enumerate()
                .find_map(|(i, m)| m.clone().map(|n| (i, n)))
            {
                return RouteOutcome::passthrough(PassthroughReason::PolicyProtected {
                    table: tables[idx].clone(),
                    policy_name: name,
                });
            }
        }
        PolicyMode::AllowList { tables: allowed } => {
            if let Some(t) = tables.iter().find(|t| !allowed.contains(t)) {
                return RouteOutcome::passthrough(PassthroughReason::NotInAllowList {
                    table: t.clone(),
                });
            }
        }
        PolicyMode::Enforce => {
            // Filtered views are exposed by sync — rewrite refs to
            // them. Tables without a view stay protected by the
            // marker check above (markers_to_write contains the
            // un-translatable subset).
            let views = cache.policy_views(backend, &tables).await;
            if let Err(e) = enforce::rewrite_views(&mut ast, &tables, &views) {
                return RouteOutcome::passthrough(PassthroughReason::TranslationFailed {
                    detail: e,
                });
            }
        }
    }

    let bytes = bytes.unwrap_or(u64::MAX);
    let limit = cfg.lake_max_scan_bytes.as_u64();
    if bytes > limit {
        return RouteOutcome::passthrough(PassthroughReason::AboveThreshold {
            estimated_bytes: bytes,
            limit,
        });
    }

    finish_lake(&mut ast, bytes).await
}

async fn finish_lake(ast: &mut [sqlparser::ast::Statement], estimated_bytes: u64) -> RouteOutcome {
    if let Err(e) = translate::translate_ast(ast) {
        return RouteOutcome::passthrough(PassthroughReason::TranslationFailed {
            detail: e.to_string(),
        });
    }
    let translated = parse::unparse(ast);
    RouteOutcome::lake(LakeReason::UnderThreshold { estimated_bytes }, translated)
}

/// Convenience for `melt route <sql>` — runs the cheap classification
/// path without consulting any backend. The CLI command handles the
/// "I cannot tell you about TableMissing/AboveThreshold without a
/// live backend" caveat in its own output.
pub fn lazy_classify(sql: &str, session: &SessionInfo, sf_cfg: &SnowflakeConfig) -> RouteOutcome {
    let mut ast = match parse::parse(sql) {
        Ok(ast) => ast,
        Err(_) => return RouteOutcome::passthrough(PassthroughReason::ParseFailed),
    };

    if classify::is_write(&ast) {
        return RouteOutcome::passthrough(PassthroughReason::WriteStatement);
    }
    if let Some(name) = classify::uses_snowflake_features(&ast) {
        return RouteOutcome::passthrough(PassthroughReason::UsesSnowflakeFeature(name));
    }

    let tables = classify::extract_tables(&ast, session);
    if let PolicyMode::AllowList { tables: allowed } = &sf_cfg.policy.mode {
        if let Some(t) = tables.iter().find(|t| !allowed.contains(t)) {
            return RouteOutcome::passthrough(PassthroughReason::NotInAllowList {
                table: t.clone(),
            });
        }
    }

    if let Err(e) = translate::translate_ast(&mut ast) {
        return RouteOutcome::passthrough(PassthroughReason::TranslationFailed {
            detail: e.to_string(),
        });
    }
    RouteOutcome::lake(
        LakeReason::UnderThreshold { estimated_bytes: 0 },
        parse::unparse(&ast),
    )
}

/// Format the routing threshold for human-readable display.
pub fn fmt_bytes(b: u64) -> String {
    ByteSize::b(b).to_string()
}

/// Partition tables by matcher outcome, upsert or query-stamp rows
/// as appropriate, and return a passthrough reason if any table is
/// not `active`. Returns `None` when every table is `active`.
///
/// The two lookup paths are optimized for the common shapes:
///
/// * Tables matching `include` (or auto-discovered) go through
///   `ensure_discovered`, which writes a row if absent and bumps
///   `last_queried_at` in one round-trip.
/// * Tables matching only `exclude` force passthrough immediately
///   without any catalog I/O — they're outside Melt's sync scope.
/// * Tables matching nothing (when `auto_discover = false`) force
///   passthrough as well, but we still want to bump `last_queried_at`
///   on rows that were previously tracked so the CLI can show
///   recency — best-effort, failures logged not returned.
async fn check_sync_state(
    matcher: &SyncTableMatcher,
    discovery: &Arc<dyn DiscoveryCatalog>,
    tables: &[TableRef],
) -> Option<PassthroughReason> {
    let mut to_register_include: Vec<TableRef> = Vec::new();
    let mut to_register_discover: Vec<TableRef> = Vec::new();
    let mut to_query_stamp: Vec<TableRef> = Vec::new();

    for t in tables {
        match matcher.classify(t) {
            MatchOutcome::Excluded => {
                return Some(PassthroughReason::TableMissing(t.clone()));
            }
            MatchOutcome::Included => to_register_include.push(t.clone()),
            MatchOutcome::NotMatched => {
                if matcher.auto_discover() {
                    to_register_discover.push(t.clone());
                } else {
                    to_query_stamp.push(t.clone());
                }
            }
        }
    }

    // Upserts (include first, so include wins over discovered for
    // source-promotion purposes per ControlCatalog::ensure_discovered
    // semantics).
    let include_states = if !to_register_include.is_empty() {
        match discovery
            .ensure_discovered(&to_register_include, SyncSource::Include)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "router: ensure_discovered(include) failed");
                return Some(PassthroughReason::BackendUnavailable);
            }
        }
    } else {
        Vec::new()
    };
    let discover_states = if !to_register_discover.is_empty() {
        match discovery
            .ensure_discovered(&to_register_discover, SyncSource::Discovered)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "router: ensure_discovered(discovered) failed");
                return Some(PassthroughReason::BackendUnavailable);
            }
        }
    } else {
        Vec::new()
    };

    // Non-fatal: update recency for rows the operator's explicitly
    // opted out of auto-discovering. Lookup also tells us whether
    // they're tracked at all — if not, it's a TableMissing.
    let stamp_states = if !to_query_stamp.is_empty() {
        if let Err(e) = discovery.mark_queried(&to_query_stamp).await {
            tracing::debug!(error = %e, "router: mark_queried best-effort failed");
        }
        match discovery.state_batch(&to_query_stamp).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "router: state_batch(not_matched) failed");
                return Some(PassthroughReason::BackendUnavailable);
            }
        }
    } else {
        Vec::new()
    };

    // First-non-active wins for atomic routing. Iterate by original
    // table order so the error names the same FQN the driver sent.
    let mut inc_iter = include_states.into_iter();
    let mut dsc_iter = discover_states.into_iter();
    let mut stp_iter = stamp_states.into_iter();

    for t in tables {
        match matcher.classify(t) {
            MatchOutcome::Excluded => unreachable!("excluded returned early above"),
            MatchOutcome::Included => {
                let s = inc_iter.next().unwrap_or(SyncState::Pending);
                if let Some(reason) = state_to_reason(t, s) {
                    return Some(reason);
                }
            }
            MatchOutcome::NotMatched if matcher.auto_discover() => {
                let s = dsc_iter.next().unwrap_or(SyncState::Pending);
                if let Some(reason) = state_to_reason(t, s) {
                    return Some(reason);
                }
            }
            MatchOutcome::NotMatched => match stp_iter.next().unwrap_or(None) {
                None => return Some(PassthroughReason::TableMissing(t.clone())),
                Some(s) => {
                    if let Some(reason) = state_to_reason(t, s) {
                        return Some(reason);
                    }
                }
            },
        }
    }

    None
}

fn state_to_reason(t: &TableRef, state: SyncState) -> Option<PassthroughReason> {
    match state {
        SyncState::Active => None,
        SyncState::Pending | SyncState::Bootstrapping => {
            Some(PassthroughReason::BootstrappingTable {
                table: t.clone(),
                state: state.as_str(),
            })
        }
        SyncState::Quarantined => Some(PassthroughReason::TableQuarantined {
            table: t.clone(),
            reason: "bootstrap failed — see `melt sync status` / \
                     melt_table_stats.bootstrap_error"
                .to_string(),
        }),
    }
}
