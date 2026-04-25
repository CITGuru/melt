//! Sync config + pattern matching shared by the router (discovery)
//! and the sync loop (bootstrap picker).

use std::time::Duration;

use async_trait::async_trait;
use bytesize::ByteSize;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};

use crate::error::{MeltError, Result};
use crate::table::TableRef;

/// Life-cycle state of a table Melt is mirroring.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncState {
    Pending,
    Bootstrapping,
    Active,
    Quarantined,
}

impl SyncState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncState::Pending => "pending",
            SyncState::Bootstrapping => "bootstrapping",
            SyncState::Active => "active",
            SyncState::Quarantined => "quarantined",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "pending" => SyncState::Pending,
            "bootstrapping" => SyncState::Bootstrapping,
            "quarantined" => SyncState::Quarantined,
            _ => SyncState::Active,
        }
    }

    /// True only for `Active`. All other states force Snowflake
    /// passthrough.
    pub fn is_routable(&self) -> bool {
        matches!(self, SyncState::Active)
    }
}

/// Provenance of a `melt_table_stats` row. Drives demotion — only
/// `Discovered` rows are candidates for the idle-days sweep.
/// `ViewDependency` rows are demoted only when no `active` parent
/// view still references them (see
/// `ControlCatalog::idle_discovered`). `Remote` rows are operator-
/// declared never-synced — they're tracked only so the dual-execution
/// router can record query recency and reason about hybrid eligibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncSource {
    Include,
    Discovered,
    /// Pulled in transitively because an `active` view decomposed
    /// onto it. Carried as its own source so demotion can ref-count
    /// through `melt_view_dependencies`.
    ViewDependency,
    /// Matched a `[sync].remote` glob — never synced to the lake;
    /// always read from Snowflake via the dual-execution router. Has
    /// no `melt_table_stats` lifecycle (no bootstrap, no demotion).
    /// Kept here so future "track query recency for remote tables"
    /// work has a clean provenance label.
    Remote,
}

impl SyncSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncSource::Include => "include",
            SyncSource::Discovered => "discovered",
            SyncSource::ViewDependency => "view_dependency",
            SyncSource::Remote => "remote",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "include" => SyncSource::Include,
            "view_dependency" => SyncSource::ViewDependency,
            "remote" => SyncSource::Remote,
            _ => SyncSource::Discovered,
        }
    }
}

/// Kind of Snowflake object a `melt_table_stats` row corresponds to.
/// Drives bootstrap dispatch: base tables take the existing
/// `CREATE STREAM ... ON TABLE` path, views try decomposition
/// then fall back to `CREATE STREAM ... ON VIEW`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    BaseTable,
    View,
    SecureView,
    MaterializedView,
    ExternalTable,
    Unknown,
}

impl ObjectKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectKind::BaseTable => "base_table",
            ObjectKind::View => "view",
            ObjectKind::SecureView => "secure_view",
            ObjectKind::MaterializedView => "materialized_view",
            ObjectKind::ExternalTable => "external_table",
            ObjectKind::Unknown => "unknown",
        }
    }

    pub fn from_db(s: &str) -> Self {
        match s {
            "view" => ObjectKind::View,
            "secure_view" => ObjectKind::SecureView,
            "materialized_view" => ObjectKind::MaterializedView,
            "external_table" => ObjectKind::ExternalTable,
            "unknown" => ObjectKind::Unknown,
            _ => ObjectKind::BaseTable,
        }
    }

    /// Translate a raw Snowflake `TABLE_TYPE` (`INFORMATION_SCHEMA.TABLES`)
    /// plus the companion `IS_SECURE` flag into our enum.
    ///
    /// `table_type` values Snowflake returns today:
    ///   - `BASE TABLE`
    ///   - `VIEW`
    ///   - `MATERIALIZED VIEW`
    ///   - `EXTERNAL TABLE`
    ///
    /// Secure views surface as `TABLE_TYPE = 'VIEW'` with
    /// `IS_SECURE = 'YES'` — callers check both columns.
    pub fn from_snowflake(table_type: &str, is_secure: bool) -> Self {
        match table_type.to_ascii_uppercase().as_str() {
            "BASE TABLE" | "LOCAL TEMPORARY" | "TEMPORARY TABLE" => ObjectKind::BaseTable,
            "VIEW" if is_secure => ObjectKind::SecureView,
            "VIEW" => ObjectKind::View,
            "MATERIALIZED VIEW" => ObjectKind::MaterializedView,
            "EXTERNAL TABLE" => ObjectKind::ExternalTable,
            _ => ObjectKind::Unknown,
        }
    }

    /// True for the two kinds we know how to bootstrap via the
    /// existing `ON TABLE` stream path.
    pub fn is_base_tablelike(&self) -> bool {
        matches!(self, ObjectKind::BaseTable | ObjectKind::ExternalTable)
    }
}

/// How a view is being mirrored to the lake.
/// - `Decomposed`: the view itself has no rows in the lake; a DuckDB
///   view over the synced base tables resolves queries at read time.
/// - `StreamOnView`: the view's output is materialized at its FQN
///   via `CREATE STREAM ... ON VIEW`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewStrategy {
    Decomposed,
    StreamOnView,
}

impl ViewStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            ViewStrategy::Decomposed => "decomposed",
            ViewStrategy::StreamOnView => "stream_on_view",
        }
    }

    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "decomposed" => Some(ViewStrategy::Decomposed),
            "stream_on_view" => Some(ViewStrategy::StreamOnView),
            _ => None,
        }
    }
}

/// Minimal control-catalog interface the router needs. Implemented
/// by `melt_control::ControlCatalog` via a thin blanket wrapper
/// (see `melt-control/src/discovery.rs`). Defined here so
/// `melt-router` can depend on the trait without pulling in the
/// Postgres client crate.
#[async_trait]
pub trait DiscoveryCatalog: Send + Sync {
    /// Upsert a `pending` row for each unknown table and bump
    /// `last_queried_at`. Returns the resolved state of each table.
    async fn ensure_discovered(
        &self,
        tables: &[TableRef],
        source: SyncSource,
    ) -> Result<Vec<SyncState>>;

    /// Look up the current state without mutating anything. `None`
    /// means the table isn't tracked.
    async fn state_batch(&self, tables: &[TableRef]) -> Result<Vec<Option<SyncState>>>;

    /// Bump `last_queried_at` without inserting unknown rows. Used
    /// when `auto_discover = false` and the operator wants query
    /// recency visibility without side-effecting the catalog.
    async fn mark_queried(&self, tables: &[TableRef]) -> Result<()>;
}

/// Top-level `[sync]` block. Drives both the router's discovery path
/// and the sync loop's bootstrap picker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncConfig {
    /// When true, any table a query touches that isn't already tracked
    /// gets inserted into `melt_table_stats` with state `pending`. Sync
    /// then bootstraps it on the next tick.
    #[serde(default = "SyncConfig::default_auto_discover")]
    pub auto_discover: bool,

    /// Glob patterns that are always synced regardless of query
    /// activity. Patterns match `DB.SCHEMA.TABLE` case-insensitively.
    /// Example: `"ANALYTICS.MARTS.*"`, `"DATA_*.STAGING.*"`.
    #[serde(default)]
    pub include: Vec<String>,

    /// Glob patterns that are never synced. Wins over `include` and
    /// over auto-discovery. Use to protect `SNOWFLAKE.*`,
    /// `*.INFORMATION_SCHEMA.*`, etc.
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Glob patterns the dual-execution router should treat as
    /// always-remote — never synced, always read from Snowflake via
    /// the hybrid bridges (Attach for single-scan, Materialize for
    /// collapsed multi-scan subtrees). See
    /// `docs/internal/DUAL_EXECUTION.md`.
    ///
    /// Precedence: `exclude` > `remote` > `include` > not_matched.
    /// Exclude wins over remote so operators can still block
    /// `SNOWFLAKE.*` even under a broad `remote = ["*.*.*"]` glob.
    #[serde(default)]
    pub remote: Vec<String>,

    /// Tunables for the discovery path.
    #[serde(default)]
    pub lazy: LazyDiscoverConfig,

    /// Tunables for view-aware sync. See [`ViewsConfig`].
    #[serde(default)]
    pub views: ViewsConfig,
}

/// Tunables that only apply when `auto_discover = true`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LazyDiscoverConfig {
    /// Reject auto-discovered tables larger than this on bootstrap —
    /// row lands in `quarantined` with a clear error. Does NOT apply
    /// to tables in `[sync].include` (operator explicitly asked for
    /// them, so we try regardless of size).
    #[serde(default = "LazyDiscoverConfig::default_max_initial_bytes")]
    pub max_initial_bytes: ByteSize,

    /// Drop auto-discovered tables that haven't been queried in this
    /// many days. `include` tables are immortal.
    #[serde(default = "LazyDiscoverConfig::default_demotion_idle_days")]
    pub demotion_idle_days: u32,

    /// Cap on how many pending tables sync will bootstrap in parallel.
    #[serde(default = "LazyDiscoverConfig::default_max_concurrent_bootstraps")]
    pub max_concurrent_bootstraps: u32,

    /// When true, Melt issues `ALTER TABLE … SET CHANGE_TRACKING =
    /// TRUE` on bootstrap if the source table doesn't have it yet.
    /// Requires `GRANT APPLY CHANGE TRACKING ON SCHEMA … TO ROLE
    /// MELT_SYNC_ROLE`. Default is off — safer to let the operator
    /// flip it manually and fail loudly when a grant's missing.
    #[serde(default = "LazyDiscoverConfig::default_false")]
    pub auto_enable_change_tracking: bool,

    /// Apply a built-in exclude list (`SNOWFLAKE.*`,
    /// `*.INFORMATION_SCHEMA.*`, `*.*._STAGE*`) on top of the
    /// operator-supplied `exclude`. Default on.
    #[serde(default = "LazyDiscoverConfig::default_true")]
    pub exclude_system_schemas: bool,

    /// How often sync sweeps idle tables. Separate from the CDC
    /// iteration interval so huge deployments can demote infrequently
    /// without slowing CDC.
    #[serde(
        with = "humantime_serde",
        default = "LazyDiscoverConfig::default_demotion_interval"
    )]
    pub demotion_interval: Duration,
}

impl SyncConfig {
    fn default_auto_discover() -> bool {
        true
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            auto_discover: Self::default_auto_discover(),
            include: Vec::new(),
            exclude: Vec::new(),
            remote: Vec::new(),
            lazy: LazyDiscoverConfig::default(),
            views: ViewsConfig::default(),
        }
    }
}

/// `[sync.views]` block. All fields optional with sensible defaults so
/// existing deployments pick up view-aware sync automatically.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ViewsConfig {
    /// When true, a view's base-table dependencies are auto-registered
    /// as `SyncSource::ViewDependency` during bootstrap. Turn off to
    /// force operators to add every base table explicitly in `include`.
    #[serde(default = "ViewsConfig::default_true")]
    pub auto_include_dependencies: bool,

    /// Skip decomposition and go straight to `CREATE STREAM ... ON VIEW`.
    /// Useful when operators know their views use a Snowflake-only
    /// function we can't translate and don't want the extra catalog
    /// round-trip per bootstrap.
    #[serde(default = "ViewsConfig::default_false")]
    pub prefer_stream_on_view: bool,

    /// Cap on how deep the dependency resolver follows views-on-views.
    /// Beyond this depth we bail with `view_body_unsupported` so a
    /// pathological graph never turns into a stack blow-up.
    #[serde(default = "ViewsConfig::default_max_dependency_depth")]
    pub max_dependency_depth: u32,
}

impl ViewsConfig {
    fn default_true() -> bool {
        true
    }
    fn default_false() -> bool {
        false
    }
    fn default_max_dependency_depth() -> u32 {
        4
    }
}

impl Default for ViewsConfig {
    fn default() -> Self {
        Self {
            auto_include_dependencies: Self::default_true(),
            prefer_stream_on_view: Self::default_false(),
            max_dependency_depth: Self::default_max_dependency_depth(),
        }
    }
}

impl LazyDiscoverConfig {
    fn default_max_initial_bytes() -> ByteSize {
        ByteSize::gb(50)
    }
    fn default_demotion_idle_days() -> u32 {
        30
    }
    fn default_max_concurrent_bootstraps() -> u32 {
        2
    }
    fn default_true() -> bool {
        true
    }
    fn default_false() -> bool {
        false
    }
    fn default_demotion_interval() -> Duration {
        Duration::from_secs(60 * 60)
    }
}

impl Default for LazyDiscoverConfig {
    fn default() -> Self {
        Self {
            max_initial_bytes: Self::default_max_initial_bytes(),
            demotion_idle_days: Self::default_demotion_idle_days(),
            max_concurrent_bootstraps: Self::default_max_concurrent_bootstraps(),
            auto_enable_change_tracking: Self::default_false(),
            exclude_system_schemas: Self::default_true(),
            demotion_interval: Self::default_demotion_interval(),
        }
    }
}

/// Outcome of matching a single `TableRef` against a `[sync]` block.
///
/// Precedence (most-restrictive wins): `Excluded` > `Remote` >
/// `Included` > `NotMatched`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchOutcome {
    /// Matches an exclude glob (or a built-in system exclude). Router
    /// should force Snowflake passthrough and NOT register the table.
    Excluded,
    /// Matches at least one `[sync].remote` glob — operator-declared
    /// never-synced. Router emits `Route::Hybrid` for queries that
    /// reference this table (gated on `router.hybrid_execution`).
    /// Always wins over `include` so a permissive `include = [...]`
    /// can't accidentally start syncing a remote-declared table.
    Remote,
    /// Matches at least one include glob. Router should register the
    /// table with `SyncSource::Include`.
    Included,
    /// Doesn't match include or exclude. Router should register only
    /// if `auto_discover = true`, with `SyncSource::Discovered`.
    NotMatched,
}

/// Compiled glob-set triple derived from a [`SyncConfig`]. Built once
/// at startup (and on hot-reload), then cheap to match per query.
#[derive(Debug)]
pub struct SyncTableMatcher {
    /// `[sync].remote` patterns. Checked after `exclude`, before
    /// `include`. Drives the dual-execution router's `Remote` outcome.
    remote: GlobSet,
    include: GlobSet,
    exclude: GlobSet,
    auto_discover: bool,
}

impl SyncTableMatcher {
    /// Built-in exclude patterns. Anything matching these is never
    /// synced — Snowflake's own metadata schemas, temporary stages,
    /// and the account_usage views.
    const SYSTEM_EXCLUDES: &'static [&'static str] =
        &["SNOWFLAKE.*.*", "*.INFORMATION_SCHEMA.*", "*.*._STAGE_*"];

    pub fn from_config(cfg: &SyncConfig) -> Result<Self> {
        let include = compile(&cfg.include, "include")?;
        let remote = compile(&cfg.remote, "remote")?;
        let mut exclude_patterns = cfg.exclude.clone();
        if cfg.lazy.exclude_system_schemas {
            for p in Self::SYSTEM_EXCLUDES {
                exclude_patterns.push((*p).to_string());
            }
        }
        let exclude = compile(&exclude_patterns, "exclude")?;

        // Warn loudly if the same FQN matches both — exclude wins but
        // the operator probably didn't intend it. Emit once at build
        // time so the warning appears on startup / reload.
        for inc in &cfg.include {
            if let Ok(g) = Glob::new(&normalize_pattern(inc)) {
                // Tests only the include pattern against exclude set,
                // using the pattern's literal form as the candidate.
                // Cheap, imperfect; misses include globs vs. exclude
                // globs overlap that don't share a concrete string.
                let candidate = g.glob();
                if exclude.is_match(candidate) {
                    tracing::warn!(
                        pattern = %inc,
                        "[sync] include pattern is shadowed by an exclude \
                         pattern; exclude wins."
                    );
                }
            }
        }

        // Same shadowing warning for `remote` patterns matching either
        // an exclude (silently drops the table from hybrid eligibility)
        // or an include (which would be operator confusion — both flags
        // would otherwise apply to the same FQN).
        for rem in &cfg.remote {
            if let Ok(g) = Glob::new(&normalize_pattern(rem)) {
                let candidate = g.glob();
                if exclude.is_match(candidate) {
                    tracing::warn!(
                        pattern = %rem,
                        "[sync] remote pattern is shadowed by an exclude \
                         pattern; exclude wins."
                    );
                }
                if include.is_match(candidate) {
                    tracing::warn!(
                        pattern = %rem,
                        "[sync] remote pattern overlaps an include pattern; \
                         remote wins (table will be federated, not synced)."
                    );
                }
            }
        }

        Ok(Self {
            remote,
            include,
            exclude,
            auto_discover: cfg.auto_discover,
        })
    }

    /// Classify a single table. Precedence:
    /// `Excluded` > `Remote` > `Included` > `NotMatched`.
    pub fn classify(&self, t: &TableRef) -> MatchOutcome {
        let fqn = format!("{}.{}.{}", t.database, t.schema, t.name).to_uppercase();
        if self.exclude.is_match(&fqn) {
            return MatchOutcome::Excluded;
        }
        if self.remote.is_match(&fqn) {
            return MatchOutcome::Remote;
        }
        if self.include.is_match(&fqn) {
            return MatchOutcome::Included;
        }
        MatchOutcome::NotMatched
    }

    /// True when auto-discovery should register a `NotMatched` table
    /// on first query.
    pub fn auto_discover(&self) -> bool {
        self.auto_discover
    }
}

fn compile(patterns: &[String], kind: &'static str) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        let normalized = normalize_pattern(p);
        let glob = Glob::new(&normalized)
            .map_err(|e| MeltError::config(format!("[sync].{kind}: invalid glob `{p}`: {e}")))?;
        b.add(glob);
    }
    b.build()
        .map_err(|e| MeltError::config(format!("[sync].{kind}: build failed: {e}")))
}

/// Normalize a glob pattern for matching. Uppercases letters so the
/// case-insensitive match against `DB.SCHEMA.TABLE` works reliably.
fn normalize_pattern(p: &str) -> String {
    p.to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(db: &str, sc: &str, n: &str) -> TableRef {
        TableRef::new(db.to_string(), sc.to_string(), n.to_string())
    }

    fn matcher(include: &[&str], exclude: &[&str], system: bool) -> SyncTableMatcher {
        matcher_full(include, exclude, &[], system)
    }

    fn matcher_full(
        include: &[&str],
        exclude: &[&str],
        remote: &[&str],
        system: bool,
    ) -> SyncTableMatcher {
        SyncTableMatcher::from_config(&SyncConfig {
            auto_discover: true,
            include: include.iter().map(|s| s.to_string()).collect(),
            exclude: exclude.iter().map(|s| s.to_string()).collect(),
            remote: remote.iter().map(|s| s.to_string()).collect(),
            lazy: LazyDiscoverConfig {
                exclude_system_schemas: system,
                ..Default::default()
            },
            views: ViewsConfig::default(),
        })
        .expect("valid patterns")
    }

    #[test]
    fn include_exact_fqn_matches() {
        let m = matcher(&["ANALYTICS.PUBLIC.ORDERS"], &[], false);
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "ORDERS")),
            MatchOutcome::Included
        );
    }

    #[test]
    fn include_is_case_insensitive() {
        let m = matcher(&["analytics.public.orders"], &[], false);
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "ORDERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("analytics", "public", "orders")),
            MatchOutcome::Included
        );
    }

    #[test]
    fn include_glob_wildcards_within_segments_only() {
        let m = matcher(&["ANALYTICS.PUBLIC.*"], &[], false);
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "ORDERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "CUSTOMERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("ANALYTICS", "MARTS", "ORDERS")),
            MatchOutcome::NotMatched
        );
    }

    #[test]
    fn multi_segment_globs_work() {
        let m = matcher(&["DATA_*.STAGING.*"], &[], false);
        assert_eq!(
            m.classify(&t("DATA_STAGING", "STAGING", "ORDERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("DATA_PROD", "STAGING", "CUSTOMERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("RAW", "STAGING", "ORDERS")),
            MatchOutcome::NotMatched
        );
    }

    #[test]
    fn exclude_wins_over_include() {
        let m = matcher(&["ANALYTICS.*.*"], &["ANALYTICS.LEGACY.*"], false);
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "ORDERS")),
            MatchOutcome::Included
        );
        assert_eq!(
            m.classify(&t("ANALYTICS", "LEGACY", "ORDERS")),
            MatchOutcome::Excluded
        );
    }

    #[test]
    fn system_excludes_filter_snowflake_schemas() {
        let m = matcher(&[], &[], true);
        assert_eq!(
            m.classify(&t("SNOWFLAKE", "ACCOUNT_USAGE", "QUERY_HISTORY")),
            MatchOutcome::Excluded
        );
        assert_eq!(
            m.classify(&t("ANALYTICS", "INFORMATION_SCHEMA", "TABLES")),
            MatchOutcome::Excluded
        );
    }

    #[test]
    fn system_excludes_off_allows_through() {
        let m = matcher(&[], &[], false);
        assert_eq!(
            m.classify(&t("SNOWFLAKE", "ACCOUNT_USAGE", "QUERY_HISTORY")),
            MatchOutcome::NotMatched
        );
    }

    #[test]
    fn not_matched_is_not_excluded() {
        let m = matcher(&["ANALYTICS.*.*"], &["RAW.*.*"], false);
        assert_eq!(
            m.classify(&t("STAGING", "PUBLIC", "ORDERS")),
            MatchOutcome::NotMatched
        );
    }

    #[test]
    fn invalid_glob_errors() {
        let cfg = SyncConfig {
            include: vec!["[broken".to_string()],
            ..Default::default()
        };
        let err = SyncTableMatcher::from_config(&cfg).unwrap_err();
        match err {
            MeltError::Config(msg) => {
                assert!(msg.contains("include"));
                assert!(msg.contains("[broken"));
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn parses_from_toml() {
        let raw = r#"
            auto_discover = false
            include = ["ANALYTICS.PUBLIC.ORDERS", "DATA_*.STAGING.*"]
            exclude = ["SNOWFLAKE.*"]

            [lazy]
            max_initial_bytes          = "100GB"
            demotion_idle_days         = 14
            auto_enable_change_tracking = true
            demotion_interval          = "2h"
        "#;
        let cfg: SyncConfig = toml::from_str(raw).unwrap();
        assert!(!cfg.auto_discover);
        assert_eq!(cfg.include.len(), 2);
        assert_eq!(cfg.lazy.max_initial_bytes, ByteSize::gb(100));
        assert_eq!(cfg.lazy.demotion_idle_days, 14);
        assert!(cfg.lazy.auto_enable_change_tracking);
        assert_eq!(cfg.lazy.demotion_interval, Duration::from_secs(2 * 60 * 60));
    }

    #[test]
    fn defaults_are_sensible() {
        let cfg = SyncConfig::default();
        assert!(cfg.auto_discover);
        assert!(cfg.include.is_empty());
        assert!(cfg.exclude.is_empty());
        assert!(cfg.remote.is_empty());
        assert!(!cfg.lazy.auto_enable_change_tracking);
        assert!(cfg.lazy.exclude_system_schemas);
        assert!(cfg.views.auto_include_dependencies);
        assert!(!cfg.views.prefer_stream_on_view);
        assert_eq!(cfg.views.max_dependency_depth, 4);
    }

    #[test]
    fn remote_glob_classifies_remote() {
        let m = matcher_full(&[], &[], &["WAREHOUSE.*.*"], false);
        assert_eq!(
            m.classify(&t("WAREHOUSE", "PUBLIC", "USERS")),
            MatchOutcome::Remote
        );
        assert_eq!(
            m.classify(&t("ANALYTICS", "PUBLIC", "ORDERS")),
            MatchOutcome::NotMatched
        );
    }

    #[test]
    fn remote_wins_over_include() {
        // The matcher's documented precedence is exclude > remote >
        // include. A table matching both `include` and `remote` must
        // resolve as Remote (federate, don't sync) — otherwise the
        // operator's intent to never sync would be silently ignored.
        let m = matcher_full(&["WAREHOUSE.*.*"], &[], &["WAREHOUSE.RESTRICTED.*"], false);
        assert_eq!(
            m.classify(&t("WAREHOUSE", "RESTRICTED", "USERS")),
            MatchOutcome::Remote,
            "remote should win over include for the same FQN",
        );
        // A table covered by `include` but NOT `remote` stays Included.
        assert_eq!(
            m.classify(&t("WAREHOUSE", "PUBLIC", "ORDERS")),
            MatchOutcome::Included,
        );
    }

    #[test]
    fn exclude_wins_over_remote() {
        // A defensive `exclude = ["SNOWFLAKE.*"]` should still bite
        // even under a permissive `remote = ["*.*.*"]`.
        let m = matcher_full(&[], &["SNOWFLAKE.*.*"], &["*.*.*"], false);
        assert_eq!(
            m.classify(&t("SNOWFLAKE", "ACCOUNT_USAGE", "QUERY_HISTORY")),
            MatchOutcome::Excluded,
        );
        assert_eq!(
            m.classify(&t("WAREHOUSE", "PUBLIC", "USERS")),
            MatchOutcome::Remote,
        );
    }

    #[test]
    fn remote_glob_invalid_pattern_errors_with_remote_label() {
        let cfg = SyncConfig {
            remote: vec!["[broken".to_string()],
            ..Default::default()
        };
        let err = SyncTableMatcher::from_config(&cfg).unwrap_err();
        match err {
            MeltError::Config(msg) => {
                assert!(msg.contains("remote"), "got {msg}");
                assert!(msg.contains("[broken"), "got {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn views_block_parses_from_toml() {
        let raw = r#"
            [views]
            auto_include_dependencies = false
            prefer_stream_on_view     = true
            max_dependency_depth      = 8
        "#;
        let cfg: SyncConfig = toml::from_str(raw).unwrap();
        assert!(!cfg.views.auto_include_dependencies);
        assert!(cfg.views.prefer_stream_on_view);
        assert_eq!(cfg.views.max_dependency_depth, 8);
    }

    #[test]
    fn sync_source_round_trips_view_dependency() {
        let s = SyncSource::ViewDependency;
        assert_eq!(s.as_str(), "view_dependency");
        assert_eq!(
            SyncSource::from_db("view_dependency"),
            SyncSource::ViewDependency
        );
        // Unknown strings fall back to Discovered (preserving the
        // pre-existing behaviour from before we added the variant).
        assert_eq!(SyncSource::from_db("other"), SyncSource::Discovered);
    }

    #[test]
    fn object_kind_maps_snowflake_types() {
        assert_eq!(
            ObjectKind::from_snowflake("BASE TABLE", false),
            ObjectKind::BaseTable
        );
        assert_eq!(ObjectKind::from_snowflake("VIEW", false), ObjectKind::View);
        assert_eq!(
            ObjectKind::from_snowflake("VIEW", true),
            ObjectKind::SecureView
        );
        assert_eq!(
            ObjectKind::from_snowflake("MATERIALIZED VIEW", false),
            ObjectKind::MaterializedView
        );
        assert_eq!(
            ObjectKind::from_snowflake("EXTERNAL TABLE", false),
            ObjectKind::ExternalTable
        );
        assert_eq!(
            ObjectKind::from_snowflake("some gibberish", false),
            ObjectKind::Unknown
        );
    }
}
