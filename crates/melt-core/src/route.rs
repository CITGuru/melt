use std::sync::Arc;

use crate::hybrid::HybridPlan;
use crate::table::TableRef;

/// The router's decision for a single statement. Every variant carries
/// the *reason* so `melt route <sql>` can explain itself and metrics
/// can label by cause.
#[derive(Clone, Debug)]
pub enum Route {
    Lake {
        reason: LakeReason,
    },
    Snowflake {
        reason: PassthroughReason,
    },
    /// Hybrid (dual-execution) — query touches both lake-resident and
    /// Snowflake-resident tables. The `plan` carries the annotated
    /// PlanNode tree, the Materialize fragments to fetch, the Attach
    /// rewrites that have already been baked into `plan.local_sql`,
    /// and the rendered DuckDB-dialect `local_sql`.
    Hybrid {
        plan: Arc<HybridPlan>,
        reason: HybridReason,
        estimated_remote_bytes: u64,
    },
}

#[derive(Clone, Debug)]
pub enum LakeReason {
    UnderThreshold { estimated_bytes: u64 },
}

#[derive(Clone, Debug)]
pub enum PassthroughReason {
    ParseFailed,
    TranslationFailed {
        detail: String,
    },
    WriteStatement,
    UsesSnowflakeFeature(&'static str),
    TableMissing(TableRef),
    AboveThreshold {
        estimated_bytes: u64,
        limit: u64,
    },
    BackendUnavailable,

    // Security-related — see PolicyMode in melt-core::policy.
    PolicyProtected {
        table: TableRef,
        policy_name: String,
    },
    NotInAllowList {
        table: TableRef,
    },

    // Sync state-machine (see melt-control::SyncState).
    /// Table is tracked but not yet `active` — sync's bootstrap
    /// (initial snapshot + stream creation) is still in progress.
    BootstrappingTable {
        table: TableRef,
        state: &'static str,
    },
    /// Bootstrap failed permanently and won't retry without an
    /// explicit `melt sync refresh`. Error detail lives in
    /// `melt_table_stats.bootstrap_error`.
    TableQuarantined {
        table: TableRef,
        reason: String,
    },
    /// `/*+ melt_route(snowflake) */` comment hint forced
    /// passthrough. See `crates/melt-router/src/hints.rs`.
    OperatorHint,
}

/// Why the router emitted `Route::Hybrid`. Used by metrics
/// (`melt_router_hybrid_reasons_total{reason=…}`) and by the
/// `melt route` CLI to explain the decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HybridReason {
    /// At least one referenced table matched a `[sync].remote` glob
    /// (operator-declared never-synced).
    RemoteByConfig,
    /// At least one referenced table is in `pending` or `bootstrapping`
    /// state and `hybrid_allow_bootstrapping = true` lets us federate
    /// while the lake catches up.
    RemoteBootstrapping,
    /// At least one referenced table's per-table estimate exceeds
    /// `lake_max_scan_bytes` and `hybrid_allow_oversize = true` lets
    /// us federate just that table.
    RemoteOversize,
    /// More than one of the above triggers fired across different
    /// tables in the same query.
    MixedReasons,
}

impl HybridReason {
    /// Stable label suitable for metric labels.
    pub fn label(&self) -> &'static str {
        match self {
            HybridReason::RemoteByConfig => "remote_by_config",
            HybridReason::RemoteBootstrapping => "remote_bootstrapping",
            HybridReason::RemoteOversize => "remote_oversize",
            HybridReason::MixedReasons => "mixed",
        }
    }
}

impl std::fmt::Display for HybridReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            HybridReason::RemoteByConfig => "RemoteByConfig",
            HybridReason::RemoteBootstrapping => "RemoteBootstrapping",
            HybridReason::RemoteOversize => "RemoteOversize",
            HybridReason::MixedReasons => "MixedReasons",
        })
    }
}

/// Coarse-grained route kind, stored alongside `ResultStore` entries
/// so cancel / poll handlers know whether to interrupt DuckDB or
/// forward to Snowflake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteKind {
    Lake,
    Snowflake,
    Hybrid,
}

impl Route {
    pub fn as_str(&self) -> &'static str {
        match self {
            Route::Lake { .. } => "lake",
            Route::Snowflake { .. } => "snowflake",
            Route::Hybrid { .. } => "hybrid",
        }
    }

    pub fn kind(&self) -> RouteKind {
        match self {
            Route::Lake { .. } => RouteKind::Lake,
            Route::Snowflake { .. } => RouteKind::Snowflake,
            Route::Hybrid { .. } => RouteKind::Hybrid,
        }
    }
}

impl PassthroughReason {
    /// Stable label suitable for `melt_router_decisions_total{reason=...}`.
    pub fn label(&self) -> &'static str {
        match self {
            PassthroughReason::ParseFailed => "parse_failed",
            PassthroughReason::TranslationFailed { .. } => "translation_failed",
            PassthroughReason::WriteStatement => "write_statement",
            PassthroughReason::UsesSnowflakeFeature(_) => "uses_snowflake_feature",
            PassthroughReason::TableMissing(_) => "table_missing",
            PassthroughReason::AboveThreshold { .. } => "above_threshold",
            PassthroughReason::BackendUnavailable => "backend_unavailable",
            PassthroughReason::PolicyProtected { .. } => "policy_protected",
            PassthroughReason::NotInAllowList { .. } => "not_in_allowlist",
            PassthroughReason::BootstrappingTable { .. } => "bootstrapping_table",
            PassthroughReason::TableQuarantined { .. } => "table_quarantined",
            PassthroughReason::OperatorHint => "operator_hint",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid::{NodeKind, Placement, PlanNode};

    fn placeholder_plan() -> Arc<HybridPlan> {
        Arc::new(HybridPlan {
            root: PlanNode::new(
                0,
                NodeKind::Scan {
                    table: TableRef::new("d", "s", "n"),
                },
                Placement::Local,
            ),
            remote_fragments: Vec::new(),
            attach_rewrites: Vec::new(),
            local_sql: String::new(),
            estimated_remote_bytes: 0,
            strategy_chain: vec!["heuristic".into()],
            chain_decided_by: "heuristic".into(),
        })
    }

    #[test]
    fn hybrid_route_as_str() {
        let route = Route::Hybrid {
            plan: placeholder_plan(),
            reason: HybridReason::RemoteByConfig,
            estimated_remote_bytes: 0,
        };
        assert_eq!(route.as_str(), "hybrid");
        assert_eq!(route.kind(), RouteKind::Hybrid);
    }

    #[test]
    fn hybrid_reason_labels_are_stable_metric_strings() {
        // Metrics consumers depend on these strings; flag any rename.
        assert_eq!(HybridReason::RemoteByConfig.label(), "remote_by_config");
        assert_eq!(
            HybridReason::RemoteBootstrapping.label(),
            "remote_bootstrapping"
        );
        assert_eq!(HybridReason::RemoteOversize.label(), "remote_oversize");
        assert_eq!(HybridReason::MixedReasons.label(), "mixed");
    }
}
