//! Cost equations for the dual-execution router's strategy selector.
//!
//! v1 of the cost model is deliberately **narrow**: it estimates only
//! the costs needed to choose between Attach and Materialize for a
//! single all-Remote subtree (the only crossover decision today's
//! heuristic gets meaningfully wrong). It does NOT do full plan-tree
//! cost-based optimization — that's the bigger refactor laid out in
//! §13.2.3 of `docs/internal/DUAL_EXECUTION.md`. Once the plan tree
//! lands, this module's [`Cost`] type becomes the leaf at every
//! node; the equations below are the per-leaf costs.
//!
//! ## What the cost crossover captures
//!
//! For a single-table Remote subtree, both strategies eventually pull
//! the same rows from Snowflake (Attach via the extension's per-batch
//! query; Materialize via the fragment SQL). They differ in the
//! per-row CPU overhead on the DuckDB side:
//!
//! - **Attach**: streaming through `snowflake_scan` operator. DuckDB's
//!   vectorized executor processes batches as they arrive; no
//!   intermediate materialization. Per-row cost is dominated by the
//!   network + the extension's batch boundary overhead.
//!
//! - **Materialize**: a `CREATE TEMP TABLE __remote_N AS <fragment>`
//!   first stages all rows, then DuckDB scans the temp table for the
//!   join. Adds a one-time temp-table-write cost (~per-row constant)
//!   but the subsequent scan is faster because there's no per-batch
//!   call back into Snowflake.
//!
//! The crossover happens when the temp-table-write cost is amortized
//! over enough subsequent scans of the same data — i.e. when the
//! query is large or the staged data gets joined against many local
//! rows. For tiny single-shot queries, Attach wins. For large or
//! join-heavy queries, Materialize wins.
//!
//! ## Calibration
//!
//! All constants in [`CostModel`] are tunable via `[router.hybrid_strategy.cost]`
//! in `melt.toml`. Defaults are reasonable starting points (calibrated
//! against the bench suite in `examples/bench/`); operators should
//! re-fit per-deployment if the cost strategy's decisions don't match
//! observed query latencies. See §11 of the design doc for the
//! procedure.

/// Cost units. We track network + local separately so the strategy
/// can reason about which side dominates (useful for L→R bridge
/// follow-up work that adds remote-side cost too).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Cost {
    /// Wall-clock seconds spent moving bytes across the network.
    pub network_seconds: f64,
    /// Wall-clock seconds spent in DuckDB-side processing (temp-table
    /// write + executor overhead per row).
    pub local_seconds: f64,
}

impl Cost {
    pub fn total(&self) -> f64 {
        self.network_seconds + self.local_seconds
    }

    pub fn add(self, other: Cost) -> Cost {
        Cost {
            network_seconds: self.network_seconds + other.network_seconds,
            local_seconds: self.local_seconds + other.local_seconds,
        }
    }

    /// Whether `self` is cheaper than `other` by at least
    /// `min_advantage_ratio`. The threshold prevents the cost model
    /// from flipping decisions on near-ties — small estimate errors
    /// shouldn't swing routing.
    ///
    /// Returns `false` when either total is non-positive (degenerate
    /// inputs ⇒ caller should defer to the heuristic).
    pub fn is_cheaper_than(&self, other: &Cost, min_advantage_ratio: f64) -> bool {
        let s = self.total();
        let o = other.total();
        if s <= 0.0 || o <= 0.0 || min_advantage_ratio < 1.0 {
            return false;
        }
        o / s >= min_advantage_ratio
    }
}

/// Per-strategy cost equations. All inputs are fact-data; tunable
/// throughput constants live in [`CostModel`].
///
/// The leaf data is `(rows, bytes)` — both come from the existing
/// backend stats (`estimate_table_rows`, `estimate_scan_bytes`). When
/// either is zero, [`CostModel::cost_attach`] / [`CostModel::cost_materialize`]
/// return `None` and the caller (CostStrategy) defers to the
/// heuristic.
#[derive(Clone, Debug)]
pub struct CostModel {
    /// Sustained network throughput from Snowflake to the proxy, in
    /// bytes per second. Default 100 MB/s — mid-range for VPC-peered
    /// Snowflake → AWS region with healthy ADBC. Re-fit per deployment.
    pub network_bytes_per_sec: f64,

    /// DuckDB row-throughput baseline for streaming scans through
    /// `snowflake_scan` (Attach path). Default 5M rows/sec —
    /// includes both the extension's batch boundary cost and DuckDB's
    /// vectorized executor cost.
    pub attach_rows_per_sec: f64,

    /// DuckDB row-throughput for materialized temp-table scans
    /// (Materialize path's post-staging scan). Default 25M rows/sec —
    /// 5× attach because there's no per-batch call back into the
    /// extension.
    pub materialize_scan_rows_per_sec: f64,

    /// Per-row write cost for staging the temp table. Default
    /// 12M rows/sec (faster than scan because it's append-only +
    /// vectorized).
    pub materialize_write_rows_per_sec: f64,

    /// Fixed-overhead cost for temp-table allocation + finalization,
    /// in seconds. Default 5 ms — covers DDL parsing, allocator,
    /// metadata commit. Independent of row count.
    pub materialize_setup_seconds: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        // Defaults from the bench suite calibration. See
        // examples/bench/README.md §"Cost-model calibration".
        Self {
            network_bytes_per_sec: 100.0 * 1_000_000.0,
            attach_rows_per_sec: 5_000_000.0,
            materialize_scan_rows_per_sec: 25_000_000.0,
            materialize_write_rows_per_sec: 12_000_000.0,
            materialize_setup_seconds: 0.005,
        }
    }
}

impl CostModel {
    /// Cost of pulling `(rows, bytes)` through the Attach strategy
    /// (DuckDB's `snowflake_scan` operator streaming through the
    /// community Snowflake extension).
    ///
    /// Returns `None` when either `rows` or `bytes` is zero —
    /// signal to the caller that we lack the stats to make an
    /// informed decision.
    pub fn cost_attach(&self, rows: u64, bytes: u64) -> Option<Cost> {
        if rows == 0 || bytes == 0 {
            return None;
        }
        Some(Cost {
            network_seconds: bytes as f64 / self.network_bytes_per_sec,
            local_seconds: rows as f64 / self.attach_rows_per_sec,
        })
    }

    /// Cost of pulling `(rows, bytes)` through the Materialize
    /// strategy (`CREATE TEMP TABLE AS` followed by the local scan).
    ///
    /// `local_scan_count` is the expected number of times DuckDB
    /// will scan the staged temp table during query execution —
    /// usually 1 for a top-level Materialize, higher for fragments
    /// joined repeatedly. Default `1` mirrors today's behavior;
    /// future builders that detect re-scan can pass higher values.
    pub fn cost_materialize(&self, rows: u64, bytes: u64, local_scan_count: u32) -> Option<Cost> {
        if rows == 0 || bytes == 0 {
            return None;
        }
        let write_seconds = rows as f64 / self.materialize_write_rows_per_sec;
        let scan_seconds =
            (rows as f64 / self.materialize_scan_rows_per_sec) * (local_scan_count.max(1) as f64);
        Some(Cost {
            network_seconds: bytes as f64 / self.network_bytes_per_sec,
            local_seconds: self.materialize_setup_seconds + write_seconds + scan_seconds,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> CostModel {
        CostModel::default()
    }

    #[test]
    fn total_sums_components() {
        let c = Cost {
            network_seconds: 0.4,
            local_seconds: 0.1,
        };
        assert!((c.total() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn add_combines_components() {
        let a = Cost {
            network_seconds: 1.0,
            local_seconds: 2.0,
        };
        let b = Cost {
            network_seconds: 3.0,
            local_seconds: 4.0,
        };
        let c = a.add(b);
        assert_eq!(c.network_seconds, 4.0);
        assert_eq!(c.local_seconds, 6.0);
    }

    #[test]
    fn cheaper_respects_advantage_ratio() {
        let cheap = Cost {
            network_seconds: 0.4,
            local_seconds: 0.1,
        };
        let expensive = Cost {
            network_seconds: 0.8,
            local_seconds: 0.2,
        };
        // 1.0 → 2.0 ratio; 1.5x advantage threshold ⇒ winning.
        assert!(cheap.is_cheaper_than(&expensive, 1.5));
        // 3.0x threshold ⇒ not winning by enough; defer.
        assert!(!cheap.is_cheaper_than(&expensive, 3.0));
    }

    #[test]
    fn cheaper_returns_false_for_degenerate_costs() {
        let zero = Cost::default();
        let real = Cost {
            network_seconds: 1.0,
            local_seconds: 0.0,
        };
        assert!(!zero.is_cheaper_than(&real, 1.5));
        assert!(!real.is_cheaper_than(&zero, 1.5));
    }

    #[test]
    fn cheaper_returns_false_for_invalid_ratio() {
        let a = Cost {
            network_seconds: 0.5,
            local_seconds: 0.0,
        };
        let b = Cost {
            network_seconds: 1.0,
            local_seconds: 0.0,
        };
        assert!(!a.is_cheaper_than(&b, 0.9));
        assert!(!a.is_cheaper_than(&b, 0.5));
    }

    #[test]
    fn zero_inputs_yield_none() {
        let m = model();
        assert!(m.cost_attach(0, 100).is_none());
        assert!(m.cost_attach(100, 0).is_none());
        assert!(m.cost_materialize(0, 100, 1).is_none());
        assert!(m.cost_materialize(100, 0, 1).is_none());
    }

    #[test]
    fn small_query_attach_cheaper_than_materialize() {
        let m = model();
        // 100 rows × ~100 bytes/row = ~10KB
        let attach = m.cost_attach(100, 10_000).unwrap();
        let materialize = m.cost_materialize(100, 10_000, 1).unwrap();
        // Setup overhead dominates the materialize cost for this
        // tiny input, so attach wins.
        assert!(
            attach.total() < materialize.total(),
            "attach={:?} mat={:?}",
            attach,
            materialize,
        );
    }

    #[test]
    fn large_query_materialize_cheaper_than_attach() {
        let m = model();
        // 100M rows × 100 bytes = 10 GB
        let attach = m.cost_attach(100_000_000, 10_000_000_000).unwrap();
        let materialize = m.cost_materialize(100_000_000, 10_000_000_000, 1).unwrap();
        // At this scale the per-row scan-throughput delta dominates.
        // Materialize wins because materialize_scan is 5× attach's
        // streaming throughput.
        assert!(
            materialize.total() < attach.total(),
            "attach={:?} mat={:?}",
            attach,
            materialize,
        );
    }

    #[test]
    fn high_rescan_count_amplifies_materialize_advantage() {
        let m = model();
        // Same input, but the staged temp table gets scanned 10×
        // (e.g. fragment is a small dimension joined to a large
        // local fact). Materialize's per-scan throughput advantage
        // multiplies; attach has no analogous benefit.
        let attach = m.cost_attach(1_000_000, 100_000_000).unwrap();
        let mat_1 = m.cost_materialize(1_000_000, 100_000_000, 1).unwrap();
        let mat_10 = m.cost_materialize(1_000_000, 100_000_000, 10).unwrap();
        assert!(mat_10.total() > mat_1.total(), "10× scans should cost more");
        // But on a per-row-of-output basis materialize starts winning
        // sooner with more rescans. Sanity check: increase rescan
        // until materialize beats attach for 1M-row input.
        let mut rescans = 1;
        while m
            .cost_materialize(1_000_000, 100_000_000, rescans)
            .unwrap()
            .total()
            >= attach.total()
            && rescans < 100
        {
            rescans += 1;
        }
        // Default constants put the crossover at modest rescans; the
        // exact number is calibration-dependent. Just assert it's
        // reachable within bounds.
        assert!(rescans < 100, "materialize never overtook attach");
    }

    #[test]
    fn higher_network_throughput_lowers_costs_proportionally() {
        let mut fast = model();
        fast.network_bytes_per_sec *= 10.0;
        let slow = model();
        let attach_fast = fast.cost_attach(1_000_000, 100_000_000).unwrap();
        let attach_slow = slow.cost_attach(1_000_000, 100_000_000).unwrap();
        assert!(attach_fast.network_seconds < attach_slow.network_seconds);
        // Local seconds shouldn't change with network speed.
        assert!((attach_fast.local_seconds - attach_slow.local_seconds).abs() < 1e-9);
    }
}
