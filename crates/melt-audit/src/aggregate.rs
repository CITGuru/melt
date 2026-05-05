//! Local aggregation pipeline: classify each row, bucket it,
//! roll the per-row spend into the [`AuditOutput`] shape `output.rs`
//! renders. Pure function — no I/O, no clock, no network — so the
//! fixture-based snapshot test drives this end-to-end.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::classify::{audit_session, classify_query, Bucket, PassthroughReason, QueryAnalysis};
use crate::credit::{credits_used, dollars};
use crate::model::{
    AuditOutput, Disclaimers, PassthroughBreakdown, PatternRow, QueryHistoryRow, RoutableSummary,
    Window, SCHEMA_VERSION,
};

/// Cap on `top_patterns` per spec §3.2.
const TOP_PATTERNS_CAP: usize = 10;

#[derive(Debug, Clone)]
pub struct AggregateConfig {
    pub account: String,
    pub credit_price_usd: f64,
    pub top_n: usize,
    pub window_days: u32,
    /// Override window bounds — used by the live path to pin the
    /// `MIN/MAX(START_TIME)` Snowflake actually returned even when
    /// no rows came back. Fixture path leaves this `None` and lets
    /// the bounds flow from the data.
    pub explicit_window_bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

/// Run the local-processing pipeline end-to-end:
/// classify → bucket → aggregate → produce [`AuditOutput`]. The
/// fixture snapshot test drives the whole audit through this single
/// function.
pub fn build_audit_output(rows: &[QueryHistoryRow], cfg: &AggregateConfig) -> AuditOutput {
    let session = audit_session();

    let mut total_queries: u64 = 0;
    let mut total_spend_usd: f64 = 0.0;
    let mut routable_count: u64 = 0;
    let mut passthrough_breakdown = PassthroughBreakdown::default();

    let mut spend_per_table: BTreeMap<String, f64> = BTreeMap::new();
    let mut pattern_groups: BTreeMap<(String, String), PatternAccum> = BTreeMap::new();

    let mut min_start = None::<DateTime<Utc>>;
    let mut max_start = None::<DateTime<Utc>>;

    let mut analyses: Vec<(QueryAnalysis, &QueryHistoryRow, f64)> = Vec::with_capacity(rows.len());

    for row in rows {
        total_queries += 1;
        min_start = Some(min_start.map_or(row.start_time, |t| t.min(row.start_time)));
        max_start = Some(max_start.map_or(row.start_time, |t| t.max(row.start_time)));

        let credits = credits_used(row.execution_time_ms, row.warehouse_size.as_deref());
        let row_dollars = dollars(credits, cfg.credit_price_usd);
        total_spend_usd += row_dollars;

        let analysis = classify_query(&row.query_text, &session);

        match analysis.bucket {
            Bucket::RoutableCandidate => routable_count += 1,
            Bucket::PassthroughForced => match analysis.passthrough_reason {
                Some(PassthroughReason::Write) => passthrough_breakdown.writes += 1,
                Some(PassthroughReason::SnowflakeFeature(_)) => {
                    passthrough_breakdown.snowflake_features += 1;
                }
                Some(PassthroughReason::ParseFailed) => passthrough_breakdown.parse_failed += 1,
                None => {}
            },
            Bucket::Unknown => {
                if analysis.tables.is_empty() {
                    passthrough_breakdown.no_tables += 1;
                }
            }
        }

        if matches!(analysis.bucket, Bucket::RoutableCandidate) {
            if let Some(table) = analysis.primary_table() {
                let fqn = format_fqn(table);
                *spend_per_table.entry(fqn.clone()).or_default() += row_dollars;
                let key = (fqn, analysis.redacted.clone());
                let entry = pattern_groups.entry(key).or_default();
                entry.freq += 1;
                entry.total_ms += row.execution_time_ms;
                entry.total_dollars += row_dollars;
            }
        }

        analyses.push((analysis, row, row_dollars));
    }

    let dollar_per_query_baseline = if total_queries == 0 {
        0.0
    } else {
        total_spend_usd / total_queries as f64
    };

    let top_n_tables: BTreeSet<String> = top_n_by_spend(&spend_per_table, cfg.top_n);

    let mut conservative_count: u64 = 0;
    for (analysis, _row, _) in &analyses {
        if !matches!(analysis.bucket, Bucket::RoutableCandidate) {
            continue;
        }
        if let Some(table) = analysis.primary_table() {
            if top_n_tables.contains(&format_fqn(table)) {
                conservative_count += 1;
            }
        }
    }

    let routable_static = summarize_routable(
        routable_count,
        total_queries,
        total_spend_usd,
        dollar_per_query_baseline,
        cfg.window_days,
    );
    let routable_conservative = summarize_routable(
        conservative_count,
        total_queries,
        total_spend_usd,
        dollar_per_query_baseline,
        cfg.window_days,
    );

    let top_patterns = top_patterns(&pattern_groups);
    let window = build_window(
        cfg.window_days,
        cfg.explicit_window_bounds,
        min_start,
        max_start,
    );

    AuditOutput {
        schema_version: SCHEMA_VERSION,
        account: cfg.account.clone(),
        window,
        total_queries,
        total_spend_usd: round2(total_spend_usd),
        dollar_per_query_baseline: round4(dollar_per_query_baseline),
        routable_static,
        routable_conservative,
        top_patterns,
        passthrough_reasons_breakdown: passthrough_breakdown,
        confidence_band_pct: crate::CONFIDENCE_BAND_PCT,
        disclaimers: Disclaimers::default_lines().0,
    }
}

#[derive(Default, Debug, Clone)]
struct PatternAccum {
    freq: u64,
    total_ms: u64,
    total_dollars: f64,
}

fn format_fqn(t: &melt_core::TableRef) -> String {
    format!("{}.{}.{}", t.database, t.schema, t.name).to_ascii_uppercase()
}

fn top_n_by_spend(spend_per_table: &BTreeMap<String, f64>, n: usize) -> BTreeSet<String> {
    let mut by_spend: Vec<(&String, &f64)> = spend_per_table.iter().collect();
    by_spend.sort_by(|a, b| {
        b.1.partial_cmp(a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    by_spend
        .into_iter()
        .take(n)
        .map(|(k, _)| k.clone())
        .collect()
}

fn summarize_routable(
    routable_count: u64,
    total_queries: u64,
    total_spend_usd: f64,
    dollar_per_query_baseline: f64,
    window_days: u32,
) -> RoutableSummary {
    let pct = if total_queries == 0 {
        0.0
    } else {
        (routable_count as f64 / total_queries as f64) * 100.0
    };
    let post_total =
        (total_queries.saturating_sub(routable_count)) as f64 * dollar_per_query_baseline;
    let dollar_per_query_post = if total_queries == 0 {
        0.0
    } else {
        post_total / total_queries as f64
    };
    let dollars_saved = (total_spend_usd - post_total).max(0.0);
    let annualized = if window_days == 0 {
        0.0
    } else {
        dollars_saved * 365.0 / window_days as f64
    };
    RoutableSummary {
        count: routable_count,
        pct: round2(pct),
        dollar_per_query_post: round4(dollar_per_query_post),
        dollars_saved: round2(dollars_saved),
        annualized: round2(annualized),
    }
}

fn top_patterns(groups: &BTreeMap<(String, String), PatternAccum>) -> Vec<PatternRow> {
    let mut rows: Vec<PatternRow> = groups
        .iter()
        .map(|((fqn, pattern), acc)| PatternRow {
            rank: 0,
            freq: acc.freq,
            avg_ms: acc.total_ms.checked_div(acc.freq).unwrap_or(0),
            table_fqn: fqn.clone(),
            pattern_redacted: pattern.clone(),
            est_dollars_in_window: round2(acc.total_dollars),
        })
        .collect();
    rows.sort_by(|a, b| {
        b.freq
            .cmp(&a.freq)
            .then_with(|| {
                b.est_dollars_in_window
                    .partial_cmp(&a.est_dollars_in_window)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.table_fqn.cmp(&b.table_fqn))
    });
    rows.truncate(TOP_PATTERNS_CAP);
    for (i, r) in rows.iter_mut().enumerate() {
        r.rank = (i + 1) as u32;
    }
    rows
}

fn build_window(
    window_days: u32,
    explicit: Option<(DateTime<Utc>, DateTime<Utc>)>,
    min_start: Option<DateTime<Utc>>,
    max_start: Option<DateTime<Utc>>,
) -> Window {
    let (start, end) = match (explicit, min_start, max_start) {
        (Some((s, e)), _, _) => (s, e),
        (None, Some(s), Some(e)) => (s, e),
        _ => {
            let now = Utc::now();
            (now, now)
        }
    };
    Window {
        start,
        end,
        days: window_days,
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}
