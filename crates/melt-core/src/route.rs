use crate::table::TableRef;

/// The router's decision for a single statement. Every variant carries
/// the *reason* so `melt route <sql>` can explain itself and metrics
/// can label by cause.
#[derive(Clone, Debug)]
pub enum Route {
    Lake { reason: LakeReason },
    Snowflake { reason: PassthroughReason },
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
}

/// Coarse-grained route kind, stored alongside `ResultStore` entries
/// so cancel / poll handlers know whether to interrupt DuckDB or
/// forward to Snowflake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteKind {
    Lake,
    Snowflake,
}

impl Route {
    pub fn as_str(&self) -> &'static str {
        match self {
            Route::Lake { .. } => "lake",
            Route::Snowflake { .. } => "snowflake",
        }
    }

    pub fn kind(&self) -> RouteKind {
        match self {
            Route::Lake { .. } => RouteKind::Lake,
            Route::Snowflake { .. } => RouteKind::Snowflake,
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
        }
    }
}
