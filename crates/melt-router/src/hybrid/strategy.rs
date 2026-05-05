//! Pluggable strategy decision interface for the dual-execution router.
//!
//! Replaces the static "1 table → Attach, 2+ → Materialize" heuristic
//! with a chain of trait-based strategies. Each strategy can either
//! return a concrete decision OR abstain (`None`), letting the next
//! strategy in the chain decide. Default chain is `[heuristic]` so
//! current behavior is preserved; flipping to `[cost, heuristic]`
//! adds cost-driven decisions with heuristic fallback for ties /
//! missing stats.
//!
//! See `docs/internal/DUAL_EXECUTION.md` §13.1 for the design.
//!
//! ## Why a chain instead of a single picker
//!
//! 1. **Graceful degradation.** When stats are missing (cold catalog,
//!    Iceberg backend, view-decomposition edges), the cost strategy
//!    abstains and the heuristic answers. No error path needed; no
//!    "is the cost model trustworthy?" boolean.
//! 2. **Tie-break stability.** When cost differences are below the
//!    operator's [`CostStrategyConfig::min_advantage_ratio`] threshold,
//!    the cost strategy abstains so the heuristic decides — this
//!    prevents routing flapping on near-ties caused by stale stats.
//! 3. **Forward-compatible.** Future strategies (`HintStrategy` for
//!    `/*+ melt_strategy(...) */`, `BanditStrategy` for online
//!    learning, `TenantStrategy` for SaaS overrides) drop into the
//!    chain at the right priority without modifying the existing
//!    members.
//!
//! ## What strategies decide today
//!
//! The trait exposes one decision point — [`PlacementStrategy::should_collapse`]
//! — that controls whether an all-Remote subtree gets collapsed into
//! a Materialize fragment OR left for per-table Attach rewrite. That
//! single decision drives the only crossover the heuristic
//! historically gets wrong (single-table queries large enough that
//! Materialize beats Attach despite the temp-table overhead).
//!
//! Once the plan-tree refactor lands (§13.2.3), the trait will gain
//! a `choose_placement` method for cross-engine boundaries; the
//! current single-method shape lets us land cost-driven decisions
//! incrementally without that bigger refactor.

use crate::hybrid::cost::CostModel;
use melt_core::config::{CostStrategyConfig, HybridStrategyConfig};
use melt_core::TableRef;

/// Whether to collapse an all-Remote subtree into a Materialize
/// fragment. The "other" outcome is leaving the subtree for the
/// builder's attach-rewrite pass (every Remote scan becomes its own
/// `sf_link.<...>` reference).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollapseDecision {
    /// Collapse the subtree — emits a `RemoteFragment` whose
    /// Snowflake-side SQL covers every scanned table at once.
    Collapse,
    /// Leave the subtree alone — every Remote scan in it gets an
    /// individual Attach rewrite.
    Skip,
}

/// Inputs to a strategy decision. Lifetime-borrowed from the
/// builder; cheap to construct per-decision.
pub struct StrategyContext<'a> {
    /// Tables referenced by the candidate subtree (deduped, in
    /// AST order). The strategy uses this both for the size check
    /// and to look up per-table stats.
    pub scanned_tables: &'a [TableRef],
    /// Per-table row-count estimates parallel to `scanned_tables`.
    /// Zero means "no stats available" and signals strategies to
    /// defer.
    pub per_table_rows: &'a [u64],
    /// Per-table byte estimates parallel to `scanned_tables`.
    /// Same zero-means-missing convention.
    pub per_table_bytes: &'a [u64],
    /// Whether the operator (or pool startup probe) has disabled
    /// Attach. When false, every strategy MUST return
    /// [`CollapseDecision::Collapse`] — Attach isn't usable.
    pub attach_runtime_enabled: bool,
}

impl<'a> StrategyContext<'a> {
    /// Sum of per-table rows. Zero when any input is unknown
    /// (we can't make a sound size-driven decision with partial
    /// stats).
    pub fn total_rows(&self) -> u64 {
        if self.per_table_rows.iter().any(|r| *r == 0) {
            return 0;
        }
        self.per_table_rows.iter().sum()
    }

    /// Sum of per-table bytes. Same partial-stats handling.
    pub fn total_bytes(&self) -> u64 {
        if self.per_table_bytes.iter().any(|b| *b == 0) {
            return 0;
        }
        self.per_table_bytes.iter().sum()
    }
}

/// Decision interface. `None` returns are "no opinion — defer to
/// the next strategy in the chain." A chain whose strategies all
/// return `None` falls back to a hard-coded safe default
/// ([`CollapseDecision::Skip`] when Attach is enabled,
/// [`CollapseDecision::Collapse`] when it's not), enforced by
/// [`ChainStrategy`].
pub trait PlacementStrategy: Send + Sync + 'static {
    /// Stable name for metrics / logs. e.g. `"heuristic"`, `"cost"`.
    fn name(&self) -> &'static str;

    /// Decide whether to collapse the candidate subtree to a
    /// Materialize fragment. Return `None` to defer.
    fn should_collapse(&self, ctx: &StrategyContext<'_>) -> Option<CollapseDecision>;
}

/// Today's heuristic, lifted into a strategy. Behavior-preserving.
///
/// **Decision semantics.** [`CollapseDecision`] here is interpreted
/// by the builder as a `collapse_floor` override:
///   - `Skip` ⇒ floor = 2 (today's default — 2+-table subtrees
///     collapse, 1-table subtrees stay as Attach).
///   - `Collapse` ⇒ floor = 1 (more aggressive — every all-Remote
///     subtree collapses, including 1-table ones).
///
/// The heuristic returns `Collapse` only when Attach is disabled
/// at runtime (extension load failure or operator kill-switch); in
/// every other case it returns `Skip`, leaving the per-subtree
/// floor=2 logic to do the right thing — multi-table subtrees
/// collapse naturally at that floor, 1-table subtrees stay Attach.
/// The cost strategy is the one that, when its math says so, can
/// flip 1-table queries to `Collapse`.
///
/// Always returns a decision — never abstains. Designed to sit at
/// the END of any chain as the safety net. If you put HeuristicStrategy
/// in the middle of a chain, anything after it never runs.
pub struct HeuristicStrategy;

impl PlacementStrategy for HeuristicStrategy {
    fn name(&self) -> &'static str {
        "heuristic"
    }

    fn should_collapse(&self, ctx: &StrategyContext<'_>) -> Option<CollapseDecision> {
        if !ctx.attach_runtime_enabled {
            // Attach unusable ⇒ force everything to Materialize.
            return Some(CollapseDecision::Collapse);
        }
        // Default floor is fine for both 1-table (stays Attach) and
        // multi-table (collapses naturally at floor=2). No override.
        Some(CollapseDecision::Skip)
    }
}

/// Cost-driven strategy. Compares estimated Attach vs Materialize
/// costs from [`CostModel`] and picks the cheaper if it beats the
/// other by [`CostStrategyConfig::min_advantage_ratio`]. Otherwise
/// abstains so the heuristic decides.
///
/// Defers when:
/// - `attach_runtime_enabled = false` (heuristic forces collapse
///   anyway; no need for cost work)
/// - any input table has zero rows OR zero bytes (no stats)
/// - cost difference is below the advantage threshold
/// - subtree has 2+ tables (heuristic always picks Materialize and
///   the cost model has nothing to add — Snowflake-side joins are
///   strictly cheaper than ferrying multiple Attach scans through
///   DuckDB; cost strategy intentionally has no opinion here)
pub struct CostStrategy {
    pub model: CostModel,
    pub min_advantage_ratio: f64,
    /// When the same fragment is expected to be scanned more than
    /// once locally (e.g. small dimension joined repeatedly), the
    /// cost equations amortize the temp-table-write cost. v1 uses
    /// a single static value; future builders that detect re-scan
    /// patterns will pass per-subtree counts.
    pub default_local_scan_count: u32,
}

impl CostStrategy {
    pub fn new(model: CostModel, min_advantage_ratio: f64) -> Self {
        Self {
            model,
            min_advantage_ratio: min_advantage_ratio.max(1.0),
            default_local_scan_count: 1,
        }
    }
}

impl PlacementStrategy for CostStrategy {
    fn name(&self) -> &'static str {
        "cost"
    }

    fn should_collapse(&self, ctx: &StrategyContext<'_>) -> Option<CollapseDecision> {
        // Multi-table subtrees: the strategy has no opinion. Today's
        // heuristic always picks Collapse here and the cost model
        // doesn't disagree (Snowflake-side join always wins the
        // network round-trip math). Defer to keep the surface narrow.
        if ctx.scanned_tables.len() != 1 {
            return None;
        }
        // Attach disabled at runtime: heuristic forces Collapse;
        // we have nothing to add.
        if !ctx.attach_runtime_enabled {
            return None;
        }
        // Need both row + byte stats to estimate either side.
        let rows = ctx.total_rows();
        let bytes = ctx.total_bytes();
        if rows == 0 || bytes == 0 {
            return None;
        }
        let attach_cost = self.model.cost_attach(rows, bytes)?;
        let materialize_cost =
            self.model
                .cost_materialize(rows, bytes, self.default_local_scan_count)?;
        // Pick whichever wins by the advantage threshold; if neither
        // wins by enough, abstain. (`is_cheaper_than` already
        // applies the threshold.)
        if materialize_cost.is_cheaper_than(&attach_cost, self.min_advantage_ratio) {
            Some(CollapseDecision::Collapse)
        } else if attach_cost.is_cheaper_than(&materialize_cost, self.min_advantage_ratio) {
            Some(CollapseDecision::Skip)
        } else {
            None
        }
    }
}

/// Walks strategies in order; returns the first concrete decision.
/// If every member abstains, falls back to a built-in default that
/// matches today's behavior (Skip when Attach is on, Collapse when
/// it's off). The fallback exists so the chain is total: callers
/// can always rely on a decision being returned.
pub struct ChainStrategy {
    members: Vec<Box<dyn PlacementStrategy>>,
}

impl ChainStrategy {
    pub fn new(members: Vec<Box<dyn PlacementStrategy>>) -> Self {
        Self { members }
    }

    /// Walk the chain and return both the decision AND the name
    /// of the strategy that produced it. The name powers the
    /// `melt_hybrid_strategy_decisions_total{strategy=…}` metric.
    pub fn decide(&self, ctx: &StrategyContext<'_>) -> (CollapseDecision, &'static str) {
        for s in &self.members {
            if let Some(d) = s.should_collapse(ctx) {
                return (d, s.name());
            }
        }
        // Fallback: behave like the legacy heuristic. Skip = leave
        // floor=2 (default behaviour); Collapse = force floor=1.
        // Only attach-disabled needs the Collapse override; multi-
        // table subtrees collapse naturally at floor=2.
        let fallback = if !ctx.attach_runtime_enabled {
            CollapseDecision::Collapse
        } else {
            CollapseDecision::Skip
        };
        (fallback, "fallback")
    }
}

/// Build a [`ChainStrategy`] from `[router.hybrid_strategy]`. Unknown
/// strategy names are LOGGED as warnings and skipped — the chain
/// stays valid (it has the built-in fallback) so a typo in config
/// degrades to today's behaviour rather than crashing the proxy.
pub fn build_chain_from_config(cfg: &HybridStrategyConfig) -> ChainStrategy {
    let mut members: Vec<Box<dyn PlacementStrategy>> = Vec::new();
    for name in &cfg.chain {
        match name.as_str() {
            "heuristic" => members.push(Box::new(HeuristicStrategy)),
            "cost" => members.push(Box::new(cost_strategy_from_cfg(&cfg.cost))),
            other => {
                tracing::warn!(
                    name = other,
                    "unknown hybrid strategy in chain; ignoring (valid: heuristic, cost)",
                );
            }
        }
    }
    ChainStrategy::new(members)
}

fn cost_strategy_from_cfg(cfg: &CostStrategyConfig) -> CostStrategy {
    let mut s = CostStrategy::new(
        CostModel {
            network_bytes_per_sec: cfg.network_bytes_per_sec,
            attach_rows_per_sec: cfg.attach_rows_per_sec,
            materialize_scan_rows_per_sec: cfg.materialize_scan_rows_per_sec,
            materialize_write_rows_per_sec: cfg.materialize_write_rows_per_sec,
            materialize_setup_seconds: cfg.materialize_setup_seconds,
        },
        cfg.min_advantage_ratio,
    );
    s.default_local_scan_count = 1;
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(name: &str) -> TableRef {
        TableRef::new("D", "S", name)
    }

    fn ctx<'a>(
        tables: &'a [TableRef],
        rows: &'a [u64],
        bytes: &'a [u64],
        attach: bool,
    ) -> StrategyContext<'a> {
        StrategyContext {
            scanned_tables: tables,
            per_table_rows: rows,
            per_table_bytes: bytes,
            attach_runtime_enabled: attach,
        }
    }

    // ── HeuristicStrategy ────────────────────────────────────────────

    #[test]
    fn heuristic_single_table_skips_collapse_floor() {
        // Skip = leave floor=2 in place (default behaviour, 1-table
        // stays Attach).
        let h = HeuristicStrategy;
        let ts = vec![t("A")];
        let c = ctx(&ts, &[1000], &[1_000_000], true);
        assert_eq!(h.should_collapse(&c), Some(CollapseDecision::Skip));
    }

    #[test]
    fn heuristic_multi_table_skips_collapse_floor() {
        // Multi-table subtrees collapse naturally at floor=2 — no
        // override needed. Skip preserves today's behaviour.
        let h = HeuristicStrategy;
        let ts = vec![t("A"), t("B")];
        let c = ctx(&ts, &[1000, 2000], &[1_000_000, 2_000_000], true);
        assert_eq!(h.should_collapse(&c), Some(CollapseDecision::Skip));
    }

    #[test]
    fn heuristic_attach_disabled_collapses_aggressively() {
        // Attach unusable ⇒ force floor=1 so single-table queries
        // also Materialize.
        let h = HeuristicStrategy;
        let ts = vec![t("A")];
        let c = ctx(&ts, &[1000], &[1_000_000], false);
        assert_eq!(h.should_collapse(&c), Some(CollapseDecision::Collapse));
    }

    // ── CostStrategy ─────────────────────────────────────────────────

    #[test]
    fn cost_defers_for_multi_table() {
        let s = CostStrategy::new(CostModel::default(), 1.5);
        let ts = vec![t("A"), t("B")];
        let c = ctx(&ts, &[1000, 2000], &[1_000_000, 2_000_000], true);
        assert_eq!(s.should_collapse(&c), None);
    }

    #[test]
    fn cost_defers_when_attach_disabled() {
        let s = CostStrategy::new(CostModel::default(), 1.5);
        let ts = vec![t("A")];
        let c = ctx(&ts, &[1000], &[1_000_000], false);
        assert_eq!(s.should_collapse(&c), None);
    }

    #[test]
    fn cost_defers_when_stats_missing() {
        let s = CostStrategy::new(CostModel::default(), 1.5);
        let ts = vec![t("A")];
        // Zero rows ⇒ defer
        let c = ctx(&ts, &[0], &[1_000_000], true);
        assert_eq!(s.should_collapse(&c), None);
        // Zero bytes ⇒ defer
        let c = ctx(&ts, &[1000], &[0], true);
        assert_eq!(s.should_collapse(&c), None);
    }

    #[test]
    fn cost_picks_skip_for_small_query() {
        let s = CostStrategy::new(CostModel::default(), 1.5);
        let ts = vec![t("A")];
        // 100 rows, 10 KB — Attach beats Materialize handily.
        let c = ctx(&ts, &[100], &[10_000], true);
        assert_eq!(s.should_collapse(&c), Some(CollapseDecision::Skip));
    }

    #[test]
    fn cost_picks_collapse_when_constants_favour_materialize() {
        // The default constants make Attach win for almost all
        // single-table cases — that's REALITY: Materialize's
        // amortization advantage only kicks in when the staged
        // temp table is rescanned, and DuckDB rarely rescans for
        // a single hash join. To prove the cost path can flip a
        // decision, configure constants where Materialize is
        // strictly cheaper.
        let model = CostModel {
            // Attach throughput crippled to 100K rows/sec
            // (simulating a flaky extension or high per-batch
            // overhead).
            attach_rows_per_sec: 100_000.0,
            ..CostModel::default()
        };
        let s = CostStrategy::new(model, 1.5);
        let ts = vec![t("A")];
        let c = ctx(&ts, &[10_000_000], &[100_000_000], true);
        assert_eq!(s.should_collapse(&c), Some(CollapseDecision::Collapse));
    }

    #[test]
    fn cost_picks_skip_for_typical_small_dimension() {
        // Typical dimension fragment: small bytes, low rows. With
        // a modest 1.2x advantage threshold Attach clears it on
        // setup-cost alone (Materialize pays the temp-table setup
        // overhead even on tiny inputs).
        let s = CostStrategy::new(CostModel::default(), 1.2);
        let ts = vec![t("A")];
        let c = ctx(&ts, &[10_000], &[1_000_000], true);
        assert_eq!(s.should_collapse(&c), Some(CollapseDecision::Skip));
    }

    #[test]
    fn cost_defers_on_near_tie() {
        // Tighten the model so attach and materialize are nearly even.
        // With min_advantage_ratio=10 the strategy should defer.
        let s = CostStrategy::new(CostModel::default(), 10.0);
        let ts = vec![t("A")];
        let c = ctx(&ts, &[10_000], &[1_000_000], true);
        let decision = s.should_collapse(&c);
        // We don't assert which side — only that a 10x advantage
        // isn't reachable for any reasonable mid-sized query.
        assert_eq!(
            decision, None,
            "10x advantage threshold should make near-ties defer"
        );
    }

    // ── ChainStrategy ────────────────────────────────────────────────

    #[test]
    fn chain_returns_first_concrete_decision() {
        let chain = ChainStrategy::new(vec![
            Box::new(CostStrategy::new(CostModel::default(), 1.5)),
            Box::new(HeuristicStrategy),
        ]);
        let ts = vec![t("A")];
        let c = ctx(&ts, &[100], &[10_000], true);
        let (decision, name) = chain.decide(&c);
        assert_eq!(decision, CollapseDecision::Skip);
        assert_eq!(name, "cost", "cost strategy should answer first");
    }

    #[test]
    fn chain_falls_through_to_heuristic_when_cost_defers() {
        let chain = ChainStrategy::new(vec![
            Box::new(CostStrategy::new(CostModel::default(), 1.5)),
            Box::new(HeuristicStrategy),
        ]);
        let ts = vec![t("A"), t("B")]; // multi-table ⇒ cost defers
        let c = ctx(&ts, &[1000, 2000], &[1_000_000, 2_000_000], true);
        let (decision, name) = chain.decide(&c);
        // Heuristic answers Skip — multi-table collapses naturally
        // at floor=2; no override needed.
        assert_eq!(decision, CollapseDecision::Skip);
        assert_eq!(name, "heuristic");
    }

    #[test]
    fn chain_falls_through_to_default_when_all_abstain() {
        // Chain of just the cost strategy, which abstains for
        // multi-table. Built-in fallback should match the heuristic's
        // logic: floor=2 default ⇒ Skip.
        let chain =
            ChainStrategy::new(vec![Box::new(CostStrategy::new(CostModel::default(), 1.5))]);
        let ts = vec![t("A"), t("B")];
        let c = ctx(&ts, &[1000, 2000], &[1_000_000, 2_000_000], true);
        let (decision, name) = chain.decide(&c);
        assert_eq!(decision, CollapseDecision::Skip);
        assert_eq!(name, "fallback");
    }

    #[test]
    fn chain_with_only_heuristic_preserves_today_behaviour() {
        // The default chain in production until operator opts in
        // to cost. Both single and multi-table return Skip — the
        // builder's floor=2 then handles the per-subtree decision
        // (1-table stays Attach, 2+-table collapses).
        let chain = ChainStrategy::new(vec![Box::new(HeuristicStrategy)]);
        let single = vec![t("A")];
        let multi = vec![t("A"), t("B")];
        let ctx_single = ctx(&single, &[100], &[10_000], true);
        let ctx_multi = ctx(&multi, &[100, 200], &[10_000, 20_000], true);
        assert_eq!(chain.decide(&ctx_single).0, CollapseDecision::Skip);
        assert_eq!(chain.decide(&ctx_multi).0, CollapseDecision::Skip);
    }
}
