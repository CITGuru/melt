//! Dual-execution router: AST → `HybridPlan`.
//!
//! Builds the [`HybridPlan`] (defined in `melt-core::hybrid`) from a
//! parsed Snowflake-dialect AST + a [`TableSourceRegistry`] of
//! Remote-classified tables. Implements the strategy selector (Attach
//! for single-scan nodes; Materialize for collapsed multi-scan subtrees)
//! and emits the final DuckDB-dialect `local_sql` with `sf_link.…`
//! aliases and `__remote_i` placeholders in place.
//!
//! ## v1 plan-builder approach
//!
//! Rather than constructing a full physical plan tree (the OpenDuck
//! port the plan describes), v1 takes a pragmatic heuristic-based
//! approach that handles the common shapes the regression variants
//! exercise:
//!
//! 1. Walk the AST. Identify every `ObjectName` that resolves to a
//!    Remote-classified [`TableRef`].
//! 2. Look for **all-remote subqueries** — `Query` blocks (inside
//!    `Subquery` / CTE bodies / IN-subqueries / EXISTS) whose every
//!    table reference is Remote. These collapse into one
//!    `RemoteFragment` each (Materialize strategy) and the AST is
//!    rewritten in-place to reference the `__remote_i` placeholder.
//! 3. The remaining (non-collapsed) Remote scans get individually
//!    rewritten to `sf_link.<db>.<schema>.<table>` (Attach strategy)
//!    and recorded as [`AttachRewrite`]s.
//! 4. Render the rewritten AST as `local_sql`, run `translate_ast` to
//!    DuckDB dialect, and assemble the [`HybridPlan`].
//!
//! Bail-out shapes that immediately return `None` (caller passes
//! through):
//! - Set operations (`UNION`/`INTERSECT`/`EXCEPT`) anywhere in the
//!   query.
//! - Window functions (`QUALIFY`, `OVER (...)`) referencing a Remote
//!   table.
//! - Anything we don't know how to safely rewrite.
//!
//! Future work (deferred from v1):
//! - Full physical plan tree for cost-based placement.
//! - `pushdown_federable_subplans` over arbitrary join shapes (today
//!   we only collapse within a single `Query` block).
//! - `Bridge(L→R)` for dynamic-filter synthesis.

mod builder;
mod emit;

pub use builder::{build_hybrid_plan, BuildOutcome};
pub use emit::choose_strategy;
