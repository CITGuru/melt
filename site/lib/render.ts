// Render the savings projection from a stored AuditOutput as a
// self-contained HTML page. Uses **only** the uploaded JSON — no DB
// enrichment, no Melt-internal joins. The page is intentionally
// shareable: a prospect should be able to drop the URL into a POC ask
// and the recipient should see the same numbers.

import type { AuditOutput, StoredAudit } from "./types.js";

const fmtUsd = (n: number) =>
  n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const fmtUsd2 = (n: number) =>
  n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
const fmtInt = (n: number) => n.toLocaleString("en-US");
const fmtPct = (n: number) => `${n.toFixed(1)}%`;

export interface RenderOpts {
  /** Render with `<meta name="robots" content="noindex">`. */
  noindex?: boolean;
}

export function renderAuditPage(record: StoredAudit, opts: RenderOpts = {}): string {
  const a = record.payload;
  const title = `Melt audit — ${esc(a.account)} — ${a.window.days}-day projection`;
  return [
    "<!doctype html>",
    '<html lang="en">',
    "<head>",
    '<meta charset="utf-8" />',
    '<meta name="viewport" content="width=device-width, initial-scale=1" />',
    `<title>${esc(title)}</title>`,
    `<meta name="description" content="${esc(metaDescription(a))}" />`,
    opts.noindex ? '<meta name="robots" content="noindex" />' : "",
    `<meta property="og:title" content="${esc(title)}" />`,
    `<meta property="og:description" content="${esc(metaDescription(a))}" />`,
    '<meta property="og:type" content="website" />',
    '<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' viewBox=\'0 0 32 32\'%3E%3Ccircle cx=\'16\' cy=\'16\' r=\'14\' fill=\'%23ff6a3d\'/%3E%3C/svg%3E" />',
    "<style>",
    css(),
    "</style>",
    "</head>",
    "<body>",
    '<main class="wrap">',
    header(a),
    summary(a),
    routableTable(a),
    patternsTable(a),
    methodologyBlock(a),
    footer(record),
    "</main>",
    "</body>",
    "</html>",
  ]
    .filter(Boolean)
    .join("\n");
}

function metaDescription(a: AuditOutput): string {
  return (
    `Melt savings projection for ${a.account} over ${a.window.days} days: ` +
    `${fmtUsd(a.routable_conservative.dollars_saved)}–${fmtUsd(a.routable_static.dollars_saved)} ` +
    `(annualized ${fmtUsd(a.routable_conservative.annualized)}–${fmtUsd(a.routable_static.annualized)}). ` +
    `Confidence band ±${a.confidence_band_pct}%.`
  );
}

function header(a: AuditOutput): string {
  return [
    '<header class="hero">',
    '<div class="kicker">Melt audit</div>',
    `<h1>${esc(a.account)}</h1>`,
    `<p class="window">Window: ${esc(a.window.start)} → ${esc(a.window.end)} (${a.window.days} days)</p>`,
    "</header>",
  ].join("\n");
}

function summary(a: AuditOutput): string {
  return [
    '<section class="summary">',
    '<div class="cards">',
    card("Total queries", fmtInt(a.total_queries)),
    card("Total spend", fmtUsd2(a.total_spend_usd)),
    card("$/query baseline", fmtUsd2(a.dollar_per_query_baseline)),
    "</div>",
    "</section>",
  ].join("\n");
}

function card(label: string, value: string): string {
  return `<div class="card"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div></div>`;
}

function routableTable(a: AuditOutput): string {
  return [
    '<section class="block">',
    "<h2>Routable savings</h2>",
    "<table>",
    "<thead><tr>",
    "<th>Profile</th><th>Queries</th><th>Share</th><th>$/query post</th><th>Dollars saved</th><th>Annualized</th>",
    "</tr></thead>",
    "<tbody>",
    routableRow("Static (max routable)", a.routable_static),
    routableRow("Conservative", a.routable_conservative),
    "</tbody>",
    "</table>",
    `<p class="band">Confidence band ±${a.confidence_band_pct}% — bracket the saved range as a planning input, not a quote.</p>`,
    "</section>",
  ].join("\n");
}

function routableRow(label: string, r: AuditOutput["routable_static"]): string {
  return [
    "<tr>",
    `<td>${esc(label)}</td>`,
    `<td>${fmtInt(r.count)}</td>`,
    `<td>${fmtPct(r.pct)}</td>`,
    `<td>${fmtUsd2(r.dollar_per_query_post)}</td>`,
    `<td>${fmtUsd(r.dollars_saved)}</td>`,
    `<td>${fmtUsd(r.annualized)}</td>`,
    "</tr>",
  ].join("");
}

function patternsTable(a: AuditOutput): string {
  if (a.top_patterns.length === 0) {
    return '<section class="block"><h2>Top patterns</h2><p class="muted">No patterns ranked.</p></section>';
  }
  return [
    '<section class="block">',
    "<h2>Top patterns</h2>",
    "<table>",
    "<thead><tr>",
    "<th>#</th><th>Freq</th><th>Avg ms</th><th>Table</th><th>Pattern (redacted)</th><th>$ in window</th>",
    "</tr></thead>",
    "<tbody>",
    ...a.top_patterns.map(
      (p) =>
        `<tr><td>${p.rank}</td><td>${fmtInt(p.freq)}</td><td>${fmtInt(p.avg_ms)}</td><td>${esc(p.table_fqn)}</td><td><code>${esc(p.pattern_redacted)}</code></td><td>${fmtUsd2(p.est_dollars_in_window)}</td></tr>`,
    ),
    "</tbody>",
    "</table>",
    "</section>",
  ].join("\n");
}

function methodologyBlock(a: AuditOutput): string {
  const pb = a.passthrough_reasons_breakdown;
  return [
    '<section class="block methodology">',
    "<h2>Methodology</h2>",
    "<ul>",
    `<li>Static analysis only — no DuckDB execution.</li>`,
    `<li>Cloud-services credits ignored.</li>`,
    `<li>Warehouse credit pricing assumed flat at the configured <code>--credit-price</code>.</li>`,
    `<li>Passthroughs by reason: writes ${fmtInt(pb.writes)}, snowflake_features ${fmtInt(pb.snowflake_features)}, parse_failed ${fmtInt(pb.parse_failed)}, no_tables ${fmtInt(pb.no_tables)}.</li>`,
    "</ul>",
    a.disclaimers.length > 0 ? `<p class="muted">${a.disclaimers.map(esc).join(" · ")}</p>` : "",
    "</section>",
  ]
    .filter(Boolean)
    .join("\n");
}

function footer(record: StoredAudit): string {
  return [
    '<footer class="foot">',
    `<p class="muted">Audit id <code>${esc(record.id)}</code> · uploaded ${esc(record.created_at)} · <a href="https://github.com/CITGuru/melt">github.com/CITGuru/melt</a></p>`,
    "</footer>",
  ].join("\n");
}

export function renderNotFound(id: string): string {
  return [
    "<!doctype html>",
    '<html lang="en"><head>',
    '<meta charset="utf-8" />',
    '<meta name="viewport" content="width=device-width, initial-scale=1" />',
    "<title>Audit not found — Melt</title>",
    "<style>",
    css(),
    "</style></head><body>",
    '<main class="wrap"><section class="block">',
    "<h1>Audit not found</h1>",
    `<p>No audit was found for id <code>${esc(id)}</code>. Audit URLs come from <code>melt audit share</code> — if you think this is wrong, re-run that command and share the new URL.</p>`,
    '<p><a href="/">Back to getmelt.com</a></p>',
    "</section></main></body></html>",
  ].join("\n");
}

function css(): string {
  // Inline so the page is one HTTP round-trip.
  return `
:root { --bg:#fff; --bg-soft:#f7f8fa; --fg:#0b0d10; --muted:#5b6470; --border:#e6e8ec; --accent:#ff6a3d; --code-bg:#f3f4f7; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#0a0c10; --bg-soft:#11151b; --fg:#eef1f5; --muted:#8b94a0; --border:#1c222a; --accent:#ff7a4d; --code-bg:#0f1318; }
}
* { box-sizing: border-box; }
body { margin:0; background: var(--bg); color: var(--fg); font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
.wrap { max-width: 1080px; margin: 0 auto; padding: 32px 24px 96px; }
.kicker { color: var(--accent); font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; font-size: 12px; }
h1 { margin: 8px 0 4px; font-size: 32px; letter-spacing: -0.01em; }
h2 { margin: 32px 0 12px; font-size: 20px; }
.window { color: var(--muted); margin-top: 0; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 24px; }
.card { border: 1px solid var(--border); background: var(--bg-soft); border-radius: 10px; padding: 16px 18px; }
.card .label { color: var(--muted); font-size: 13px; }
.card .value { font-size: 22px; font-weight: 700; margin-top: 4px; }
.block { margin-top: 32px; }
table { width: 100%; border-collapse: collapse; font-size: 14.5px; }
th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 600; font-size: 13px; }
code { background: var(--code-bg); padding: 1px 6px; border-radius: 4px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 13px; }
.muted { color: var(--muted); }
.band { color: var(--muted); font-size: 13.5px; margin-top: 8px; }
.foot { margin-top: 64px; border-top: 1px solid var(--border); padding-top: 16px; font-size: 13px; }
a { color: var(--accent); }
`.trim();
}

function esc(s: unknown): string {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
