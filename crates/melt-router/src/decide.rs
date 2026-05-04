use std::sync::Arc;

use bytesize::ByteSize;
use melt_core::config::RouterConfig;
use melt_core::{
    DiscoveryCatalog, HybridPlan, HybridReason, LakeReason, MatchOutcome, PassthroughReason,
    PolicyMode, Route, SessionInfo, StorageBackend, SyncSource, SyncState, SyncTableMatcher,
    TableRef, TableSourceRegistry,
};
use melt_snowflake::SnowflakeConfig;
use metrics::counter;

use crate::classify;
use crate::enforce;
use crate::hybrid;
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

    pub fn hybrid(plan: Arc<HybridPlan>, reason: HybridReason) -> Self {
        let estimated_remote_bytes = plan.estimated_remote_bytes;
        Self {
            translated_sql: Some(plan.local_sql.clone()),
            route: Route::Hybrid {
                plan,
                reason,
                estimated_remote_bytes,
            },
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
    // Hint pre-pass. Comment hints (`/*+ melt_route(snowflake) */` &
    // friends) override the normal decision tree. We parse before
    // anything else so an explicit `melt_route(snowflake)` skips the
    // whole evaluator. See `crates/melt-router/src/hints.rs`.
    let hints = crate::hints::parse_hints(sql);
    if matches!(hints.route, Some(crate::hints::RouteHint::Snowflake)) {
        return RouteOutcome::passthrough(PassthroughReason::OperatorHint);
    }

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
    // pick lake to avoid the network hop. Honor an explicit hint
    // either way.
    if tables.is_empty() {
        return finish_lake(&mut ast, 0).await;
    }

    // ── Hybrid partition ────────────────────────────────────────────
    //
    // Three trigger cases promote a table to the remote pool:
    // 1. `[sync].remote` glob (`MatchOutcome::Remote`) — operator
    //    declared "never sync this".
    // 2. Sync state is Pending/Bootstrapping AND
    //    `cfg.hybrid_allow_bootstrapping = true`.
    //    Lets queries serve via Snowflake while the lake bootstraps,
    //    instead of falling back to full passthrough.
    // 3. Per-table estimate exceeds `lake_max_scan_bytes` AND
    //    `cfg.hybrid_allow_oversize = true`. Lets the proxy serve
    //    the small lake tables locally and federate just the big one.
    //
    // Cases 2 and 3 fire only when their toggle is on; default-off
    // preserves today's behaviour (Phase 0 backwards-compat).
    let (remote_tables, non_remote_tables): (Vec<TableRef>, Vec<TableRef>) =
        if let Some(m) = matcher {
            tables
                .iter()
                .cloned()
                .partition(|t| matches!(m.classify(t), MatchOutcome::Remote))
        } else {
            (Vec::new(), tables.clone())
        };

    let mut remote_tables = remote_tables;
    let mut non_remote_tables = non_remote_tables;
    // `/*+ melt_route(lake) */` — operator override that says "I know
    // this query, run it locally; ignore `[sync].remote` matches and
    // bootstrapping/oversize promotions." Move every Remote table
    // back to the local pool. The lake decision below will then
    // either succeed or surface the actual missing-table error
    // instead of falling back to passthrough silently.
    if matches!(hints.route, Some(crate::hints::RouteHint::Lake)) {
        non_remote_tables.append(&mut remote_tables);
    }
    let mut promotion_reasons: Vec<HybridReason> = Vec::new();
    if !remote_tables.is_empty() {
        promotion_reasons.push(HybridReason::RemoteByConfig);
    }

    // ── Case 2: bootstrapping promotion ──────────────────────────
    if cfg.hybrid_execution && cfg.hybrid_allow_bootstrapping {
        if let (Some(_m), Some(catalog)) = (matcher, discovery) {
            let states_result = catalog.state_batch(&non_remote_tables).await;
            if let Ok(states) = states_result {
                let mut promoted: Vec<TableRef> = Vec::new();
                let mut kept: Vec<TableRef> = Vec::new();
                for (t, state) in non_remote_tables.iter().cloned().zip(states.iter()) {
                    let is_bootstrapping = matches!(
                        state,
                        Some(SyncState::Pending) | Some(SyncState::Bootstrapping)
                    );
                    if is_bootstrapping {
                        promoted.push(t);
                    } else {
                        kept.push(t);
                    }
                }
                if !promoted.is_empty() {
                    promotion_reasons.push(HybridReason::RemoteBootstrapping);
                    remote_tables.extend(promoted);
                    non_remote_tables = kept;
                }
            } else if let Err(e) = states_result {
                tracing::debug!(
                    error = %e,
                    "case 2 (bootstrapping promotion) lookup failed; \
                     falling back to non-promotion path"
                );
            }
        }
    }

    // ── Case 3: oversize promotion ──────────────────────────────
    if cfg.hybrid_execution && cfg.hybrid_allow_oversize && !non_remote_tables.is_empty() {
        let per_table_bytes_local = cache
            .estimate_bytes_per_table(backend, &non_remote_tables)
            .await
            .unwrap_or_else(|| vec![0; non_remote_tables.len()]);
        let limit = cfg.lake_max_scan_bytes.as_u64();
        let mut promoted: Vec<TableRef> = Vec::new();
        let mut kept: Vec<TableRef> = Vec::new();
        for (t, b) in non_remote_tables
            .iter()
            .cloned()
            .zip(per_table_bytes_local.iter())
        {
            if *b > limit {
                promoted.push(t);
            } else {
                kept.push(t);
            }
        }
        if !promoted.is_empty() {
            promotion_reasons.push(HybridReason::RemoteOversize);
            remote_tables.extend(promoted);
            non_remote_tables = kept;
        }
    }

    if !remote_tables.is_empty() {
        // Shadow log so operators can audit hybrid eligibility even
        // when `hybrid_execution = false`.
        tracing::info!(
            remote_tables = ?remote_tables.iter().map(|t| t.fqn()).collect::<Vec<_>>(),
            local_tables  = ?non_remote_tables.iter().map(|t| t.fqn()).collect::<Vec<_>>(),
            hybrid_execution = cfg.hybrid_execution,
            "hybrid candidate"
        );

        if !cfg.hybrid_execution {
            // Feature flag off — passthrough on first Remote table
            // (today's safe default; operator hasn't opted in).
            return RouteOutcome::passthrough(PassthroughReason::TableMissing(
                remote_tables[0].clone(),
            ));
        }

        // Policy guardrail (§10.1): hybrid never federates over a
        // policy-protected table. Service-role auth on the Snowflake
        // bridges would silently bypass row-access / masking policies.
        // Check ALL referenced tables (local + remote), not just
        // remote — a policy on the local side still matters because
        // the proxy used to forward the whole query to Snowflake on
        // a marker hit, and that contract must hold.
        let policy_check_tables = tables.clone();
        let markers = cache.policy_markers(backend, &policy_check_tables).await;
        if let PolicyMode::Passthrough = &sf_cfg.policy.mode {
            if let Some((idx, name)) = markers
                .iter()
                .enumerate()
                .find_map(|(i, m)| m.clone().map(|n| (i, n)))
            {
                return RouteOutcome::passthrough(PassthroughReason::PolicyProtected {
                    table: policy_check_tables[idx].clone(),
                    policy_name: name,
                });
            }
        }
        if let PolicyMode::AllowList { tables: allowed } = &sf_cfg.policy.mode {
            if let Some(t) = policy_check_tables.iter().find(|t| !allowed.contains(t)) {
                return RouteOutcome::passthrough(PassthroughReason::NotInAllowList {
                    table: t.clone(),
                });
            }
        }

        // Per-table byte estimates: needed for the Materialize cap
        // and (future) the oversize trigger case.
        let per_table_bytes = cache
            .estimate_bytes_per_table(backend, &policy_check_tables)
            .await
            .unwrap_or_else(|| vec![0; policy_check_tables.len()]);

        // Build the hybrid plan. The builder owns the AST mutation
        // (Attach rewrites + Materialize fragment extraction) and
        // returns either a plan, a Bail (fall through to passthrough),
        // or NotHybrid (defensive — shouldn't happen because we
        // already know remote_tables is non-empty).
        //
        // Runtime gate: if the backend's pool reports `sf_link` is
        // unavailable (extension load or ATTACH failed), force the
        // builder into all-Materialize mode by overriding
        // `hybrid_attach_enabled = false` on a local cfg copy. The
        // public `RouterConfig` is unchanged. We also bump the
        // `melt_hybrid_attach_unavailable_total` counter so the
        // degradation surfaces on dashboards instead of being a
        // silent log line at startup.
        //
        // Hint gate: `/*+ melt_strategy(materialize) */` forces the
        // same all-Materialize mode (operator opt-in to the safer
        // path); `melt_strategy(attach)` is a no-op here because
        // Attach is already the default for single-scan nodes — the
        // strategy selector decides per-node.
        let registry = TableSourceRegistry::from_iter(remote_tables.iter().cloned());
        let attach_runtime_available = backend.hybrid_attach_available();
        let force_materialize_by_hint = matches!(
            hints.strategy,
            Some(crate::hints::StrategyHint::Materialize)
        );
        let effective_cfg: RouterConfig;
        let cfg_for_builder: &RouterConfig = if cfg.hybrid_attach_enabled
            && !attach_runtime_available
        {
            counter!(melt_metrics::HYBRID_ATTACH_UNAVAILABLE).increment(1);
            tracing::warn!(
                "hybrid: sf_link not available at runtime; forcing all-Materialize plan"
            );
            effective_cfg = RouterConfig {
                hybrid_attach_enabled: false,
                ..cfg.clone()
            };
            &effective_cfg
        } else if force_materialize_by_hint {
            tracing::info!("hybrid: forcing all-Materialize via /*+ melt_strategy(materialize) */");
            effective_cfg = RouterConfig {
                hybrid_attach_enabled: false,
                ..cfg.clone()
            };
            &effective_cfg
        } else {
            cfg
        };
        let outcome = hybrid::build_hybrid_plan(
            &mut ast,
            session,
            &policy_check_tables,
            &per_table_bytes,
            &registry,
            cfg_for_builder,
        );
        match outcome {
            hybrid::BuildOutcome::Plan {
                plan,
                reason: builder_reason,
            } => {
                // Prefer the trigger-case reason from `promotion_reasons`
                // over the builder's per-fragment reason. The builder
                // can't see whether the promotion came from
                // `[sync].remote` vs case 2 vs case 3 — that's
                // decide_inner's job.
                let reason = if promotion_reasons.len() >= 2 {
                    HybridReason::MixedReasons
                } else {
                    promotion_reasons.first().copied().unwrap_or(builder_reason)
                };
                // Size caps. `/*+ melt_route(hybrid) */` bypasses
                // them — operator escape hatch for "I know the
                // estimate is wrong / I want this query to run via
                // hybrid no matter what." Counter-intuitively this
                // is safer than a manual passthrough because hybrid
                // still respects policy markers and doesn't expose
                // the lake bytes to Snowflake.
                let bypass_caps = matches!(hints.route, Some(crate::hints::RouteHint::Hybrid));
                if !bypass_caps {
                    let limit = cfg.hybrid_max_remote_scan_bytes.as_u64();
                    if plan.estimated_remote_bytes > limit {
                        return RouteOutcome::passthrough(PassthroughReason::AboveThreshold {
                            estimated_bytes: plan.estimated_remote_bytes,
                            limit,
                        });
                    }
                    let frag_limit = cfg.hybrid_max_fragment_bytes.as_u64();
                    for frag in &plan.remote_fragments {
                        let frag_bytes: u64 = frag
                            .scanned_tables
                            .iter()
                            .filter_map(|t| {
                                policy_check_tables
                                    .iter()
                                    .position(|x| x == t)
                                    .and_then(|i| per_table_bytes.get(i).copied())
                            })
                            .sum();
                        if frag_bytes > frag_limit {
                            return RouteOutcome::passthrough(PassthroughReason::AboveThreshold {
                                estimated_bytes: frag_bytes,
                                limit: frag_limit,
                            });
                        }
                    }
                }
                // Per-Attach-scan cap. Each Attach rewrite represents
                // one streaming scan through the community Snowflake
                // extension. Without bulk materialization, the only
                // backpressure on a runaway Attach scan is this cap;
                // unlike Materialize the bytes pulled may not get
                // joined-down before they hit DuckDB. Defaults to
                // 10 GiB (intentionally permissive); tighten per-
                // tenant if you observe single Attach scans crowding
                // out lake QPS. Over-cap → passthrough — the operator
                // declared the table remote so passthrough is the
                // safe behavior; we don't silently downgrade to
                // Materialize because the Materialize cap is tighter
                // and would just refuse anyway.
                if !bypass_caps {
                    let attach_limit = cfg.hybrid_max_attach_scan_bytes.as_u64();
                    for rw in &plan.attach_rewrites {
                        let scan_bytes = policy_check_tables
                            .iter()
                            .position(|t| *t == rw.original)
                            .and_then(|i| per_table_bytes.get(i).copied())
                            .unwrap_or(0);
                        if scan_bytes > attach_limit {
                            return RouteOutcome::passthrough(PassthroughReason::AboveThreshold {
                                estimated_bytes: scan_bytes,
                                limit: attach_limit,
                            });
                        }
                    }
                }
                // Phase 1 metrics (§11 in the design doc).
                counter!(
                    melt_metrics::HYBRID_REASONS,
                    melt_metrics::LABEL_REASON => reason.label(),
                )
                .increment(1);
                counter!(
                    melt_metrics::HYBRID_STRATEGY,
                    melt_metrics::LABEL_STRATEGY => plan.strategy_label(),
                )
                .increment(1);
                // Pushdown collapse counter — increments per Materialize
                // fragment that covers 2+ tables (the optimization
                // that makes hybrid actually pay off vs N separate
                // Attach scans). Direct proxy for that win.
                let collapsed = plan
                    .remote_fragments
                    .iter()
                    .filter(|f| f.scanned_tables.len() >= 2)
                    .count();
                if collapsed > 0 {
                    counter!(melt_metrics::HYBRID_PUSHDOWN_COLLAPSED).increment(collapsed as u64);
                }
                // Per-query strategy distribution histograms.
                metrics::histogram!(melt_metrics::HYBRID_ATTACH_NODES_PER_QUERY)
                    .record(plan.attach_rewrites.len() as f64);
                metrics::histogram!(melt_metrics::HYBRID_MATERIALIZE_NODES_PER_QUERY)
                    .record(plan.remote_fragments.len() as f64);
                // Per-fragment / per-Attach-scan estimated bytes. Lets
                // operators see the actual transfer-volume distribution
                // rather than only the per-query node count. Labeled by
                // strategy so the two paths don't get aggregated into a
                // single histogram. Per-fragment Materialize bytes
                // reuses the same scanned_tables→bytes lookup above.
                for frag in &plan.remote_fragments {
                    let bytes: u64 = frag
                        .scanned_tables
                        .iter()
                        .filter_map(|t| {
                            policy_check_tables
                                .iter()
                                .position(|x| x == t)
                                .and_then(|i| per_table_bytes.get(i).copied())
                        })
                        .sum();
                    metrics::histogram!(
                        melt_metrics::HYBRID_REMOTE_SCAN_BYTES,
                        melt_metrics::LABEL_STRATEGY => "materialize",
                    )
                    .record(bytes as f64);
                }
                for rw in &plan.attach_rewrites {
                    let bytes = policy_check_tables
                        .iter()
                        .position(|t| *t == rw.original)
                        .and_then(|i| per_table_bytes.get(i).copied())
                        .unwrap_or(0);
                    metrics::histogram!(
                        melt_metrics::HYBRID_REMOTE_SCAN_BYTES,
                        melt_metrics::LABEL_STRATEGY => "attach",
                    )
                    .record(bytes as f64);
                }
                tracing::info!(
                    fragments = plan.remote_fragments.len(),
                    attach_rewrites = plan.attach_rewrites.len(),
                    estimated_remote_bytes = plan.estimated_remote_bytes,
                    strategy = plan.strategy_label(),
                    reason = reason.label(),
                    "hybrid_plan emitted",
                );
                return RouteOutcome::hybrid(plan, reason);
            }
            hybrid::BuildOutcome::Bail(detail) => {
                // V1 doesn't know how to safely federate this shape.
                // Fall through to Snowflake passthrough; the bail
                // reason rides along on the metric label.
                return RouteOutcome::passthrough(PassthroughReason::TranslationFailed {
                    detail: format!("hybrid_bail: {detail}"),
                });
            }
            hybrid::BuildOutcome::NotHybrid => {
                // Defensive: should never happen — we already gated on
                // !remote_tables.is_empty(). Fall through to today's
                // passthrough rather than crash the proxy.
                tracing::warn!(
                    "hybrid::build returned NotHybrid for non-empty remote set; \
                     this is a bug — please file with the failing SQL"
                );
                return RouteOutcome::passthrough(PassthroughReason::TableMissing(
                    remote_tables[0].clone(),
                ));
            }
        }
    }

    // Sync-state gate when matcher+catalog wired; legacy tables_exist
    // path otherwise (e.g. `melt route` lazy CLI). Operates only on
    // non-Remote tables (Remote was partitioned out above).
    if let (Some(matcher), Some(catalog)) = (matcher, discovery) {
        if let Some(reason) = check_sync_state(matcher, catalog, &non_remote_tables).await {
            return RouteOutcome::passthrough(reason);
        }
    } else {
        let exists = cache.tables_exist(backend, &non_remote_tables).await;
        if let Some((idx, _)) = exists.iter().enumerate().find(|(_, e)| !**e) {
            return RouteOutcome::passthrough(PassthroughReason::TableMissing(
                non_remote_tables[idx].clone(),
            ));
        }
    }

    let (markers, bytes) = tokio::join!(
        cache.policy_markers(backend, &non_remote_tables),
        cache.estimate_bytes(backend, &non_remote_tables),
    );

    match &sf_cfg.policy.mode {
        PolicyMode::Passthrough => {
            if let Some((idx, name)) = markers
                .iter()
                .enumerate()
                .find_map(|(i, m)| m.clone().map(|n| (i, n)))
            {
                return RouteOutcome::passthrough(PassthroughReason::PolicyProtected {
                    table: non_remote_tables[idx].clone(),
                    policy_name: name,
                });
            }
        }
        PolicyMode::AllowList { tables: allowed } => {
            if let Some(t) = non_remote_tables.iter().find(|t| !allowed.contains(t)) {
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
            let views = cache.policy_views(backend, &non_remote_tables).await;
            if let Err(e) = enforce::rewrite_views(&mut ast, &non_remote_tables, &views) {
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
///
/// Backwards-compatible shim: matcher-aware callers should use
/// [`lazy_classify_with_matcher`] instead. Without a matcher the
/// lazy classifier can't see `[sync].remote` globs and therefore
/// never emits `Route::Hybrid` — it behaves as it did pre-hybrid.
pub fn lazy_classify(sql: &str, session: &SessionInfo, sf_cfg: &SnowflakeConfig) -> RouteOutcome {
    lazy_classify_with_matcher(sql, session, sf_cfg, None, &RouterConfig::default())
}

/// Matcher-aware offline classifier. `melt route` loads
/// [`SyncTableMatcher::from_config`] from the operator's `melt.toml`
/// and passes it here so `[sync].remote` globs drive a real
/// `Route::Hybrid` decision — visible without a running proxy.
///
/// When `cfg.hybrid_execution = true` and the matcher classifies any
/// referenced table as `Remote`, this emits `Route::Hybrid` with the
/// full plan (attach rewrites + materialize fragments) exactly as
/// `decide_inner` would on the live path. When the flag is off, the
/// function falls back to today's lazy behaviour so pre-Phase-1
/// callers see no change.
pub fn lazy_classify_with_matcher(
    sql: &str,
    session: &SessionInfo,
    sf_cfg: &SnowflakeConfig,
    matcher: Option<&SyncTableMatcher>,
    cfg: &RouterConfig,
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
    if let PolicyMode::AllowList { tables: allowed } = &sf_cfg.policy.mode {
        if let Some(t) = tables.iter().find(|t| !allowed.contains(t)) {
            return RouteOutcome::passthrough(PassthroughReason::NotInAllowList {
                table: t.clone(),
            });
        }
    }

    // Hybrid-aware short-circuit: if the matcher is wired AND
    // `hybrid_execution = true` AND any referenced table is
    // Remote-classified, build a hybrid plan and return it. This
    // makes the offline classifier's verdict match what the live
    // router emits for the same query — so the Python regression
    // variants can be evaluated without spinning up a proxy.
    if let Some(m) = matcher {
        let remote_tables: Vec<TableRef> = tables
            .iter()
            .filter(|t| matches!(m.classify(t), MatchOutcome::Remote))
            .cloned()
            .collect();
        if !remote_tables.is_empty() && cfg.hybrid_execution {
            let registry = TableSourceRegistry::from_iter(remote_tables.iter().cloned());
            // The lazy path has no backend, so we can't get real
            // per-table byte estimates. Use zeros — size caps aren't
            // relevant for the offline classifier, the live decide_inner
            // enforces them separately.
            let per_table_bytes = vec![0u64; tables.len()];
            let outcome = hybrid::build_hybrid_plan(
                &mut ast,
                session,
                &tables,
                &per_table_bytes,
                &registry,
                cfg,
            );
            match outcome {
                hybrid::BuildOutcome::Plan { plan, reason } => {
                    return RouteOutcome::hybrid(plan, reason);
                }
                hybrid::BuildOutcome::Bail(detail) => {
                    return RouteOutcome::passthrough(PassthroughReason::TranslationFailed {
                        detail: format!("hybrid_bail: {detail}"),
                    });
                }
                hybrid::BuildOutcome::NotHybrid => {
                    // Fall through to the legacy lake path below.
                }
            }
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
            // Remote tables are partitioned out by `decide_inner`
            // before this function is called — they have no sync
            // state to consult.
            MatchOutcome::Remote => unreachable!(
                "Remote tables must be partitioned out by decide_inner \
                 before check_sync_state is called"
            ),
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
            MatchOutcome::Remote => unreachable!(
                "Remote tables must be partitioned out by decide_inner \
                 before check_sync_state is called"
            ),
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
