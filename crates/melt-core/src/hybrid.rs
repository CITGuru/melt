//! Data types for the hybrid (dual-execution) router path.
//!
//! These are pure data structures shared by [`Route::Hybrid`](crate::Route)
//! (defined in this crate) and the algorithm crate `melt-router::hybrid`
//! (which builds, transforms, and emits them). Keeping the types here
//! avoids a circular dep — `Route::Hybrid` can carry a concrete
//! `Arc<HybridPlan>` without needing dynamic dispatch into `melt-router`.
//!
//! Algorithms — `resolve_auto_with_registry`, `pushdown_federable_subplans`,
//! `insert_bridges`, `choose_strategy`, the AST→PlanNode builder, and the
//! Snowflake-dialect emitter — live in `melt-router::hybrid` because they
//! depend on `sqlparser`. This crate stays sqlparser-free.

use std::collections::HashSet;
use std::fmt;

use crate::table::TableRef;

/// Where a `PlanNode` should execute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Placement {
    /// Execute in the local DuckDB connection (lake side).
    Local,
    /// Execute on Snowflake — either via the attached `sf_link` catalog
    /// (Attach strategy) or as a `RemoteFragment` materialized into a
    /// `TEMP TABLE __remote_i` (Materialize strategy).
    Remote,
    /// Unresolved during AST → PlanNode building. Filled in by
    /// `resolve_auto_with_registry`.
    Auto,
}

impl Placement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Placement::Local => "LOCAL",
            Placement::Remote => "REMOTE",
            Placement::Auto => "AUTO",
        }
    }
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Direction a `Bridge` operator moves data. v1 only emits
/// `RemoteToLocal` (Snowflake → DuckDB via Arrow IPC). `LocalToRemote`
/// is reserved for future dynamic-filter synthesis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BridgeDirection {
    RemoteToLocal,
    LocalToRemote,
}

impl BridgeDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            BridgeDirection::RemoteToLocal => "R→L",
            BridgeDirection::LocalToRemote => "L→R",
        }
    }
}

impl fmt::Display for BridgeDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Operator types in the hybrid plan tree.
#[derive(Clone, Debug)]
pub enum NodeKind {
    /// Table / index scan. `table` is the resolved fully-qualified name.
    Scan { table: TableRef },
    /// Hash join. `condition` is the on-clause text (best-effort —
    /// only used for EXPLAIN-style output).
    HashJoin { condition: String },
    /// Aggregate (GROUP BY + agg fns).
    Aggregate { keys: Vec<String> },
    /// Projection (SELECT list).
    Project { columns: Vec<String> },
    /// Filter predicate (WHERE / HAVING).
    Filter { predicate: String },
    /// Bridge inserted at a placement boundary by `insert_bridges`.
    Bridge { direction: BridgeDirection },
    /// Collapsed subtree produced by `pushdown_federable_subplans`.
    /// `sql` is the Snowflake-dialect SQL fragment that, when run via
    /// the Materialize path, produces the same rows the subtree would.
    /// `tables` lists every underlying scan inside the collapsed
    /// subtree — `tables.len()` drives `BridgeStrategy` selection
    /// (1 → Attach, 2+ → Materialize).
    RemoteSql { sql: String, tables: Vec<TableRef> },
}

/// A node in the hybrid execution plan.
#[derive(Clone, Debug)]
pub struct PlanNode {
    pub id: u32,
    pub kind: NodeKind,
    pub placement: Placement,
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(id: u32, kind: NodeKind, placement: Placement) -> Self {
        Self {
            id,
            kind,
            placement,
            children: Vec::new(),
        }
    }

    pub fn with_children(mut self, children: Vec<PlanNode>) -> Self {
        self.children = children;
        self
    }

    /// Highest `id` in this subtree. Used by `insert_bridges` to mint
    /// fresh ids for inserted nodes.
    pub fn max_id(&self) -> u32 {
        let mut m = self.id;
        for c in &self.children {
            m = m.max(c.max_id());
        }
        m
    }

    /// Walk the tree depth-first, calling `f` on every node.
    pub fn walk<F: FnMut(&PlanNode)>(&self, f: &mut F) {
        f(self);
        for c in &self.children {
            c.walk(f);
        }
    }
}

/// Strategy for executing a single `RemoteSql` node. Selected at
/// emit time from the node's `tables.len()` and the runtime
/// `hybrid_attach_enabled` gauge — see `melt-router::hybrid::strategy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BridgeStrategy {
    /// Single-scan remote node. Rewrite the scan's `ObjectName` to
    /// `sf_link.<db>.<schema>.<table>`; let DuckDB's optimizer push
    /// predicates/projections through the community Snowflake
    /// extension. Pipelined; no temp-table materialization.
    Attach,
    /// Multi-scan remote node (or any single-scan node when Attach is
    /// unavailable). Emit as a `RemoteFragment`; SnowflakeClient runs
    /// it, Arrow batches stage into a `TEMP TABLE __remote_i`. Snowflake
    /// executes the join inside the fragment natively.
    Materialize,
}

impl BridgeStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            BridgeStrategy::Attach => "attach",
            BridgeStrategy::Materialize => "materialize",
        }
    }
}

/// One Snowflake-dialect SQL query that the Materialize path will
/// execute against Snowflake and bulk-load into DuckDB as a TEMP TABLE.
#[derive(Clone, Debug)]
pub struct RemoteFragment {
    /// The DuckDB TEMP TABLE name `local_sql` references for this
    /// fragment. Convention: `__remote_0`, `__remote_1`, …
    pub placeholder: String,
    /// Snowflake-dialect SQL — sent verbatim to `SnowflakeClient`.
    pub snowflake_sql: String,
    /// Tables this fragment scans. ≥ 1; 2+ when pushdown collapsed a
    /// subtree.
    pub scanned_tables: Vec<TableRef>,
}

/// Recorded for each `RemoteSql` node that chose `Attach`. Mostly used
/// for EXPLAIN-style logging — the actual rewrite is already baked
/// into `local_sql` by the plan emitter.
#[derive(Clone, Debug)]
pub struct AttachRewrite {
    /// Original Snowflake-resident table reference.
    pub original: TableRef,
    /// The 4-part alias the rewrite produced
    /// (`sf_link.<db>.<schema>.<table>`). Carried as a string because
    /// SQL renderers may shell-quote individual segments.
    pub alias_reference: String,
}

/// Output of the hybrid plan pipeline. Carried by `Route::Hybrid` and
/// consumed by `melt-proxy::execution::execute_hybrid`.
#[derive(Clone, Debug)]
pub struct HybridPlan {
    /// Annotated tree (placements + bridges + RemoteSql) — kept for
    /// EXPLAIN, parity sampler, and observability.
    pub root: PlanNode,
    /// `RemoteSql` nodes that chose `Materialize`. One TEMP TABLE
    /// each at execute time.
    pub remote_fragments: Vec<RemoteFragment>,
    /// `RemoteSql` nodes that chose `Attach`. For logging only —
    /// `local_sql` already references `sf_link.…` aliases for these.
    pub attach_rewrites: Vec<AttachRewrite>,
    /// Final SQL run on the local DuckDB connection. Already
    /// `translate_ast`-translated to DuckDB dialect; references both
    /// `__remote_i` placeholders (Materialize) and `sf_link.…` aliases
    /// (Attach).
    pub local_sql: String,
    /// Estimated bytes the Materialize path will pull from Snowflake
    /// across all fragments. Used by the size-cap guardrail.
    pub estimated_remote_bytes: u64,
}

impl HybridPlan {
    /// Total `RemoteSql` node count (Materialize + Attach).
    pub fn remote_node_count(&self) -> usize {
        self.remote_fragments.len() + self.attach_rewrites.len()
    }

    /// `true` if the plan has at least one Materialize fragment.
    pub fn has_materialize(&self) -> bool {
        !self.remote_fragments.is_empty()
    }

    /// `true` if the plan has at least one Attach rewrite.
    pub fn has_attach(&self) -> bool {
        !self.attach_rewrites.is_empty()
    }

    /// The query's overall strategy label for metrics:
    /// `attach` | `materialize` | `mixed`.
    pub fn strategy_label(&self) -> &'static str {
        match (self.has_attach(), self.has_materialize()) {
            (true, true) => "mixed",
            (true, false) => "attach",
            (false, true) => "materialize",
            (false, false) => "none", // shouldn't happen for a Hybrid route
        }
    }
}

/// Registry of remote-classified tables. Built once per `decide_inner`
/// call from the matcher's classification. Used by
/// `resolve_auto_with_registry` and `pushdown_federable_subplans` in
/// `melt-router::hybrid`.
///
/// Melt-specific simplification of OpenDuck's per-`compute_context`
/// registry: every remote table targets the one fixed Snowflake, so
/// `is_remote(t)` is the only question and `are_co_located(a, b)` is
/// just `is_remote(a) && is_remote(b)`. This makes the pushdown rule's
/// precondition fire more often than in OpenDuck's multi-worker
/// setting.
#[derive(Clone, Debug, Default)]
pub struct TableSourceRegistry {
    remote: HashSet<TableRef>,
}

impl TableSourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, table: TableRef) {
        self.remote.insert(table);
    }

    pub fn is_remote(&self, t: &TableRef) -> bool {
        self.remote.contains(t)
    }

    pub fn are_co_located(&self, a: &TableRef, b: &TableRef) -> bool {
        self.is_remote(a) && self.is_remote(b)
    }

    pub fn is_empty(&self) -> bool {
        self.remote.is_empty()
    }

    pub fn len(&self) -> usize {
        self.remote.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TableRef> {
        self.remote.iter()
    }
}

impl FromIterator<TableRef> for TableSourceRegistry {
    fn from_iter<I: IntoIterator<Item = TableRef>>(iter: I) -> Self {
        Self {
            remote: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(db: &str, sc: &str, n: &str) -> TableRef {
        TableRef::new(db, sc, n)
    }

    #[test]
    fn placement_display_matches_explain_format() {
        // EXPLAIN-style output relies on these labels — see §13 in the
        // dual-execution doc for the exact format.
        assert_eq!(Placement::Local.to_string(), "LOCAL");
        assert_eq!(Placement::Remote.to_string(), "REMOTE");
        assert_eq!(Placement::Auto.to_string(), "AUTO");
    }

    #[test]
    fn bridge_direction_arrows() {
        // The arrow glyphs are user-facing (in `melt route` output).
        assert_eq!(BridgeDirection::RemoteToLocal.to_string(), "R→L");
        assert_eq!(BridgeDirection::LocalToRemote.to_string(), "L→R");
    }

    #[test]
    fn plannode_max_id_walks_subtree() {
        let leaf = PlanNode::new(
            5,
            NodeKind::Scan {
                table: t("d", "s", "n"),
            },
            Placement::Local,
        );
        let parent = PlanNode::new(
            2,
            NodeKind::HashJoin {
                condition: "a = b".into(),
            },
            Placement::Local,
        )
        .with_children(vec![leaf]);
        assert_eq!(parent.max_id(), 5);
    }

    #[test]
    fn registry_co_location_collapses_to_both_remote() {
        let a = t("a", "s", "n");
        let b = t("b", "s", "n");
        let local = t("c", "s", "n");
        let reg = TableSourceRegistry::from_iter([a.clone(), b.clone()]);
        assert!(reg.are_co_located(&a, &b));
        assert!(!reg.are_co_located(&a, &local));
        assert!(!reg.are_co_located(&local, &local));
    }

    #[test]
    fn strategy_label_matches_design_doc() {
        let placeholder_node = PlanNode::new(
            0,
            NodeKind::Scan {
                table: t("d", "s", "n"),
            },
            Placement::Local,
        );

        let attach_only = HybridPlan {
            root: placeholder_node.clone(),
            remote_fragments: vec![],
            attach_rewrites: vec![AttachRewrite {
                original: t("d", "s", "n"),
                alias_reference: "sf_link.d.s.n".into(),
            }],
            local_sql: String::new(),
            estimated_remote_bytes: 0,
        };
        assert_eq!(attach_only.strategy_label(), "attach");

        let materialize_only = HybridPlan {
            root: placeholder_node.clone(),
            remote_fragments: vec![RemoteFragment {
                placeholder: "__remote_0".into(),
                snowflake_sql: "SELECT 1".into(),
                scanned_tables: vec![t("d", "s", "n")],
            }],
            attach_rewrites: vec![],
            local_sql: String::new(),
            estimated_remote_bytes: 0,
        };
        assert_eq!(materialize_only.strategy_label(), "materialize");

        let mixed = HybridPlan {
            root: placeholder_node,
            remote_fragments: vec![RemoteFragment {
                placeholder: "__remote_0".into(),
                snowflake_sql: "SELECT 1".into(),
                scanned_tables: vec![t("d", "s", "n")],
            }],
            attach_rewrites: vec![AttachRewrite {
                original: t("d2", "s", "n"),
                alias_reference: "sf_link.d2.s.n".into(),
            }],
            local_sql: String::new(),
            estimated_remote_bytes: 0,
        };
        assert_eq!(mixed.strategy_label(), "mixed");
    }
}
