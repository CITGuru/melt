export type Post = {
  slug: string;
  title: string;
  excerpt: string;
  category: string;
  readTime: string;
  author: string;
  authorRole: string;
  publishedAt: string;
  body: string;
};

export const posts: Post[] = [
  {
    slug: "why-warehouse-bills-explode-with-agents",
    title: "Why your warehouse bill explodes once agents start writing SQL",
    excerpt:
      "Human-driven workloads have natural throttles. Autonomous pipelines don't. We dig into the new query mix and what it does to credit consumption.",
    category: "Must read",
    readTime: "12 min read",
    author: "Toby Oyetoke",
    authorRole: "Founder, Melt",
    publishedAt: "April 18, 2026",
    body: `
<p>Snowflake's billing model assumes a particular shape of demand: a handful of analysts running dashboards a few times a day, dbt models materialising overnight, ad-hoc queries when someone asks a question. That shape worked for a decade.</p>

<p>It does not work for the agent era.</p>

<h2>The new query mix</h2>
<p>An autonomous research agent doesn't run on a schedule. It runs <em>per prompt</em>. A single prompt can fire dozens of small filters, joins, and aggregates as the agent iterates toward an answer. Multiply that by a few hundred agents in production, and the warehouse never gets to spin down.</p>
<p>We pulled three weeks of query history from a design partner running a vertical-AI product. Of <code>9.8M</code> queries, <code>83%</code> scanned less than 200&nbsp;MB. Most were single-table filters and counts. Most were issued by an agent that didn't actually need to use a Snowflake warehouse to answer them.</p>

<h2>Where the credits go</h2>
<ul>
  <li><strong>Cold-start tax.</strong> Each query that hits a warm warehouse is cheap. Each one that hits a cold one pays the full minute.</li>
  <li><strong>Concurrency walls.</strong> Once an agent fleet saturates a warehouse, the queue grows; teams scale up; the bill rises faster than the work.</li>
  <li><strong>Repeat work.</strong> Agents re-run nearly-identical queries dozens of times per session. Snowflake bills you each time.</li>
</ul>

<h2>The lakehouse alternative — for the right queries</h2>
<p>If your underlying Parquet is already on S3 via Iceberg or DuckLake, DuckDB can answer most of these queries locally for cents. No warehouse spin-up, no credit burn. The hard part isn't running them — it's deciding, per query, when the lake is allowed to.</p>

<blockquote>The bet behind Melt is that most of those reads don't actually need Snowflake compute. The routing decision is per-query and invisible to the agent or driver issuing it.</blockquote>

<p>Writes, Snowflake-only features, and genuinely large joins stay on Snowflake. Everything else gets a chance to run cheaper. You don't rewrite your dbt project, your BI tool, or your agent's prompt — you change one connection string.</p>

<h3>What changes for your team</h3>
<p>Practically: nothing in the application layer. Drivers connect to melt instead of <code>account.snowflakecomputing.com</code>. Every routing decision is logged, exported to <code>/metrics</code>, and visible in <code>melt route &quot;...&quot;</code>. If you don't like a route, you policy-mark the table or schema and melt stops touching it.</p>
<p>Operationally: you've added one stateless proxy and one stateful sync service. The sync pulls CDC out of Snowflake into your lake, single-writer. Both are open-source, both are written in Rust, both are run on hardware you already have.</p>

<p>If you've started to feel the agent-era warehouse bill, ping us. We're onboarding design partners weekly.</p>
`.trim(),
  },
  {
    slug: "per-query-routing-in-detail",
    title: "Per-query routing, in detail",
    excerpt:
      "How melt classifies each statement, decides on a route, and proves it didn't lie. A guided tour through the router, the parity sampler, and the policy gates.",
    category: "Architecture",
    readTime: "9 min read",
    author: "Toby Oyetoke",
    authorRole: "Founder, Melt",
    publishedAt: "April 4, 2026",
    body: `
<p>Routing is the heart of Melt. Every statement that reaches the proxy gets parsed, classified, and routed independently. This post walks through how that decision is made and how we keep it honest.</p>
<h2>Three routes, one invariant</h2>
<p>The router emits one of three routes for each statement: <strong>Lake</strong>, <strong>Snowflake passthrough</strong>, or <strong>Dual execution</strong>. The invariant across all three is correctness: whatever Melt returns is exactly what Snowflake would have returned for the same statement, with the same session state, at the same point in time.</p>
<h2>The classifier</h2>
<p>The classifier walks the parsed AST and answers a fixed set of questions: is this a write? Does it use a Snowflake-only feature? Are all referenced tables synced and active? Is the estimated scan under <code>[router].lake_max_scan_bytes</code>? Does any referenced table carry a policy marker? The answers feed a deterministic decision table.</p>
<h2>Translating to DuckDB</h2>
<p>For Lake routes, melt rewrites the SQL to DuckDB dialect. <code>IFF</code> becomes <code>CASE WHEN</code>. <code>QUALIFY</code> rides through. Time-zone arithmetic gets normalised. Anything we can't safely round-trip falls back to Snowflake — silently, by design.</p>
<h2>Dual execution</h2>
<p>Some queries touch a mix of synced and unsynced tables. Instead of falling all-or-nothing back to Snowflake, dual execution plan-splits: pushes some operators down to Snowflake, runs the rest locally on DuckDB, bridges results via Arrow IPC. Off by default; opt in per case under <code>[router].hybrid_</code>*.</p>
<h2>The parity sampler</h2>
<p>For each route class you can configure a sampling rate. Sampled queries dual-run against both backends and we diff the results. Drift alerts immediately. We refuse to sample anything touching policy-protected tables.</p>
<p>The whole router is open-source. Read the code if you don't believe us.</p>
`.trim(),
  },
  {
    slug: "policy-modes",
    title: "Passthrough, allowlist, enforce: pick your policy",
    excerpt:
      "Three operating modes, three different risk postures. How to choose, how to migrate between them, and what the auditors will ask for.",
    category: "Operations",
    readTime: "7 min read",
    author: "Toby Oyetoke",
    authorRole: "Founder, Melt",
    publishedAt: "March 22, 2026",
    body: `
<p>Melt ships with three policy modes. Each one trades convenience for control.</p>
<h2>Passthrough</h2>
<p>Easiest to roll out. Every statement passes through to Snowflake unless explicitly opted into routing. Use this to wire melt up alongside production traffic without changing behaviour.</p>
<h2>Allowlist</h2>
<p>The recommended starting point once you've validated parity. Routing is enabled per schema or table, off by default elsewhere. Lets you grow coverage workload by workload without surprises.</p>
<h2>Enforce</h2>
<p>The opposite of allowlist. Routing is on by default; specific tables are marked Snowflake-only. Used by teams that have driven coverage above 90% and want to flip the failure mode.</p>
<h3>Migrating</h3>
<p>You can change modes via hot-reload — no restarts. Bring routing up in passthrough, then allowlist a single schema, then expand. Drop into enforce only when you can articulate why every Snowflake-only table needs to stay there.</p>
`.trim(),
  },
  {
    slug: "hybrid-plans-for-declared-remote-tables",
    title: "Hybrid plans for declared-remote tables",
    excerpt:
      "When all-or-nothing passthrough leaves money on the table. How dual execution stitches DuckDB and Snowflake into one plan — and how we keep it honest.",
    category: "Guide",
    readTime: "10 min read",
    author: "Toby Oyetoke",
    authorRole: "Founder, Melt",
    publishedAt: "March 9, 2026",
    body: `
<p>Some of your tables won't sync. Compliance won't let you. The data's too volatile. The CDC stream is too lossy. Whatever the reason — declaring a table <em>remote-only</em> in melt is fine. The question is what to do when a query touches one of them <strong>and</strong> a synced table at the same time.</p>
<h2>The all-or-nothing cliff</h2>
<p>Without dual execution, any query that touches a declared-remote table goes Snowflake. Even when most of the work is on synced tables that DuckDB could handle. You pay full warehouse for the privilege of joining one remote dimension table.</p>
<h2>Plan-splitting</h2>
<p>With dual execution, the planner identifies operators that have to run upstream and pushes them down with a tight predicate. The rest run locally on DuckDB. Results bridge via Arrow IPC and finalise in DuckDB. The router prints the annotated plan in <code>melt route</code> output so you can see, operator by operator, where each piece executes.</p>
<h2>Keeping it honest</h2>
<p>Plan-split queries are sampler-eligible like any other. We refuse to federate anything touching policy-protected tables. Every dual plan is surfaced in metrics with its split ratio, so you can budget Snowflake spend per declared-remote table.</p>
`.trim(),
  },
  {
    slug: "what-are-warehouse-credits",
    title: "What actually is a warehouse credit?",
    excerpt:
      "An honest field guide to Snowflake's billing unit, why two queries that look the same can cost wildly different amounts, and what melt does about it.",
    category: "Management",
    readTime: "6 min read",
    author: "Toby Oyetoke",
    authorRole: "Founder, Melt",
    publishedAt: "February 24, 2026",
    body: `
<p>Snowflake bills you in <em>credits</em>, and credits are deceptively complex. This is a quick field guide.</p>
<h2>What a credit pays for</h2>
<p>A credit pays for a unit of compute time on a warehouse. Bigger warehouse = more credits per minute. Idle warehouses cost nothing. Warehouses you keep warm cost the keep-warm time.</p>
<h2>Why two identical queries can cost differently</h2>
<ul>
  <li>Cold start vs. warm warehouse.</li>
  <li>Cluster size and concurrency.</li>
  <li>Result-set caching.</li>
  <li>Auto-suspend timing.</li>
</ul>
<p>This is why a single dashboard refresh can range from "nothing" to "noticeable" to "alarming" depending on what else was happening at the time.</p>
<h2>What melt changes</h2>
<p>Credits you don't pay for are the cheapest credits. Melt routes eligible reads to DuckDB on hardware you already own. The reads it can't route still cost what they always cost. The savings come from <em>what doesn't go to the warehouse anymore</em>.</p>
`.trim(),
  },
];

export function getPost(slug: string): Post | undefined {
  return posts.find((p) => p.slug === slug);
}
