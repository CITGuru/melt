//! `stdout` table, JSON, and talking-points renderers from spec §3.
//!
//! The aggregation that produces an [`AuditOutput`] from raw rows
//! lives in [`crate::aggregate`]. This module is a pure render
//! layer: takes a finalized [`AuditOutput`], emits bytes.

use chrono::{DateTime, Datelike, Utc};
use serde_json::to_string_pretty;

use crate::model::AuditOutput;

// Re-export so existing callers (the binary, the integration test)
// can keep importing `melt_audit::output::{aggregate, AggregateConfig}`
// without caring that the implementation moved next door.
pub use crate::aggregate::{build_audit_output, build_audit_output as aggregate, AggregateConfig};

/// JSON formatter for `melt-audit-<account>-<date>.json`. Pretty-
/// printed for human review; deterministic field order via serde
/// derive.
pub fn render_json(out: &AuditOutput) -> String {
    to_string_pretty(out).expect("AuditOutput is always serializable")
}

/// Stdout block from spec §1. ANSI green for the savings cell, gray
/// for the disclaimer footer. Falls through to plain text when
/// `color = false`.
pub fn render_stdout_table(out: &AuditOutput, color: bool) -> String {
    let g = |s: &str| {
        if color {
            format!("\x1b[32m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    };
    let dim = |s: &str| {
        if color {
            format!("\x1b[90m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    };

    let mut s = String::new();
    s.push_str(&format!(
        "\nMelt audit — savings projection ({}d window)\n",
        out.window.days
    ));
    s.push_str("─────────────────────────────────────────────────────────────────────\n");
    s.push_str(&format!(
        "  Total queries                    {}\n",
        format_int(out.total_queries)
    ));
    s.push_str(&format!(
        "  Total Snowflake spend            ${}  ({}d)  · $/query  ${:.4}\n",
        format_money(out.total_spend_usd),
        out.window.days,
        out.dollar_per_query_baseline
    ));
    s.push_str(&format!(
        "  Routable to lake (static)        {}   ({:.1}%)\n",
        format_int(out.routable_static.count),
        out.routable_static.pct
    ));
    s.push_str(&format!(
        "  Routable to lake (conservative)  {}   ({:.1}%)   ← top-{} tables\n",
        format_int(out.routable_conservative.count),
        out.routable_conservative.pct,
        conservative_top_n_label(out)
    ));
    s.push_str("─────────────────────────────────────────────────────────────────────\n");
    s.push_str(&format!(
        "  Projected $/query post-Melt           ${:.4}  (static)\n",
        out.routable_static.dollar_per_query_post
    ));
    s.push_str(&format!(
        "                                        ${:.4}  (conservative)\n",
        out.routable_conservative.dollar_per_query_post
    ));
    s.push_str(&format!(
        "  Projected $ saved ({}d)             {} – {}\n",
        out.window.days,
        g(&format!(
            "${}",
            format_money(out.routable_conservative.dollars_saved)
        )),
        g(&format!(
            "${}",
            format_money(out.routable_static.dollars_saved)
        )),
    ));
    s.push_str(&format!(
        "  Projected $ saved (annualized)     {} – {}\n",
        g(&format!(
            "${}",
            format_money(out.routable_conservative.annualized)
        )),
        g(&format!(
            "${}",
            format_money(out.routable_static.annualized)
        )),
    ));
    s.push_str("─────────────────────────────────────────────────────────────────────\n\n");

    if !out.top_patterns.is_empty() {
        s.push_str(&format!(
            "Top routable patterns ({} of {})\n",
            out.top_patterns.len(),
            out.top_patterns.len()
        ));
        s.push_str("  rank  freq      avg ms    table                              pattern\n");
        for p in &out.top_patterns {
            s.push_str(&format!(
                "  {:<4}  {:<8}  {:<8}  {:<34}  {}\n",
                p.rank,
                format_int(p.freq),
                p.avg_ms,
                truncate(&p.table_fqn, 34),
                truncate(&p.pattern_redacted, 80),
            ));
        }
        s.push('\n');
    }

    s.push_str(&dim(&format!(
        "Disclaimers: {}\n",
        out.disclaimers.join("; ")
    )));
    s
}

fn conservative_top_n_label(out: &AuditOutput) -> u32 {
    // We don't store the operator's `--top-n` in the JSON shape, so
    // we recover the label by counting the distinct tables that
    // appear in `top_patterns`. Bounded below by 1 so the empty
    // case still reads `top-1 tables` instead of `top-0 tables`.
    let distinct: std::collections::BTreeSet<&String> =
        out.top_patterns.iter().map(|p| &p.table_fqn).collect();
    distinct.len().max(1) as u32
}

fn format_int(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

fn format_money(n: f64) -> String {
    format_int(n.round() as u64)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

/// Talking-points markdown from spec §3.3. Headline framing is
/// gated on POWA-139 (GTM positioning); until that lands, the
/// conservative number is the headline and we leave a TODO at the
/// top of the file the GTM agent can flip.
pub fn render_talkingpoints(out: &AuditOutput) -> String {
    let mut s = String::new();
    s.push_str("<!-- TODO(POWA-139): confirm headline framing -->\n");
    s.push_str(&format!(
        "- Audit window: {} → {} ({} days)\n",
        out.window.start.format("%Y-%m-%d"),
        out.window.end.format("%Y-%m-%d"),
        out.window.days,
    ));
    s.push_str(&format!(
        "- Snowflake spend in window: **${}** ($/query: ${:.4})\n",
        format_money(out.total_spend_usd),
        out.dollar_per_query_baseline,
    ));
    s.push_str(&format!(
        "- Statically routable to a Melt lake: **{:.1}%** of queries\n",
        out.routable_static.pct
    ));
    s.push_str(&format!(
        "- Conservative (top-N tables synced): **{:.1}%** of queries\n",
        out.routable_conservative.pct
    ));
    s.push_str(&format!(
        "- Projected {}-day savings: **${}–${}**\n",
        out.window.days,
        format_money(out.routable_conservative.dollars_saved),
        format_money(out.routable_static.dollars_saved),
    ));
    s.push_str(&format!(
        "- Projected annualized: **${}–${}**\n",
        format_money(out.routable_conservative.annualized),
        format_money(out.routable_static.annualized),
    ));
    if let Some(top) = out.top_patterns.first() {
        s.push_str(&format!(
            "- Top routable table pattern: `{}` ({} queries, avg {}ms)\n",
            top.table_fqn,
            format_int(top.freq),
            top.avg_ms,
        ));
    }
    s.push_str(&format!(
        "- Confidence band: ±{}%; static analysis only, no execution.\n",
        out.confidence_band_pct,
    ));
    s
}

/// Suggested filename stem used by the binary for the JSON +
/// talking-points artifacts: `melt-audit-<account>-<date>`.
pub fn output_stem(account: &str, when: DateTime<Utc>) -> String {
    format!(
        "melt-audit-{}-{:04}-{:02}-{:02}",
        sanitize_account(account),
        when.year(),
        when.month(),
        when.day()
    )
}

fn sanitize_account(a: &str) -> String {
    a.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect()
}
