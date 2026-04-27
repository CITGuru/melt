//! Strategy selection per `RemoteSql` node.
//!
//! v1's `choose_strategy` is the algorithm the design doc calls out:
//! 1-table → Attach (DuckDB extension's pushdown wins for single
//! scans), 2+-table → Materialize (Snowflake-side join collapse wins
//! for multi-table subtrees). When `hybrid_attach_enabled = false`
//! (set by the pool startup code on extension load failure, or by the
//! operator as a kill switch), every node falls back to Materialize.
//!
//! The `BridgeStrategy` type itself lives in `melt-core::hybrid` so
//! `RouterConfig` can reference it without depending on this crate.

use melt_core::config::RouterConfig;
use melt_core::{BridgeStrategy, NodeKind, PlanNode};

/// Pick a strategy for a single `RemoteSql` node. Panics if called on
/// any other node kind — strategy selection is by definition only
/// applied to nodes that survived `pushdown_federable_subplans`.
pub fn choose_strategy(node: &PlanNode, cfg: &RouterConfig) -> BridgeStrategy {
    match &node.kind {
        NodeKind::RemoteSql { tables, .. } if tables.len() == 1 => {
            if cfg.hybrid_attach_enabled {
                BridgeStrategy::Attach
            } else {
                // Operator kill-switch (or extension load failure).
                // Forcing Materialize keeps hybrid working without the
                // community Snowflake extension; the per-fragment cap
                // still applies.
                BridgeStrategy::Materialize
            }
        }
        NodeKind::RemoteSql { .. } => BridgeStrategy::Materialize,
        _ => unreachable!(
            "choose_strategy must only be called on RemoteSql nodes; got {:?}",
            node.kind
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melt_core::{Placement, TableRef};

    fn t(name: &str) -> TableRef {
        TableRef::new("D", "S", name)
    }

    fn remote_node(tables: Vec<TableRef>) -> PlanNode {
        PlanNode::new(
            0,
            NodeKind::RemoteSql {
                sql: String::new(),
                tables,
            },
            Placement::Remote,
        )
    }

    #[test]
    fn one_table_picks_attach() {
        let node = remote_node(vec![t("X")]);
        let cfg = RouterConfig::default();
        assert_eq!(choose_strategy(&node, &cfg), BridgeStrategy::Attach);
    }

    #[test]
    fn two_tables_picks_materialize() {
        let node = remote_node(vec![t("X"), t("Y")]);
        let cfg = RouterConfig::default();
        assert_eq!(choose_strategy(&node, &cfg), BridgeStrategy::Materialize);
    }

    #[test]
    fn attach_disabled_forces_materialize() {
        let node = remote_node(vec![t("X")]);
        let mut cfg = RouterConfig::default();
        cfg.hybrid_attach_enabled = false;
        assert_eq!(choose_strategy(&node, &cfg), BridgeStrategy::Materialize);
    }
}
