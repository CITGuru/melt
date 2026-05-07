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
  featuredImage?: string;
};

export const posts: Post[] = [
  {
    slug: "meet-melt",
    title: "Cut your Snowflake bill, change one connection string",
    excerpt:
      "Why we're building melt — an open-source proxy that sits in front of Snowflake and decides, per query, where it should run. The launch post.",
    category: "Launch",
    readTime: "4 min read",
    author: "Melt Team",
    authorRole: "Builders of Melt",
    publishedAt: "May 7, 2026",
    body: `
<p>There's a specific moment, a few weeks into any serious agent rollout, when somebody on the data team runs <code>SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) FROM QUERY_HISTORY</code> and screenshots the result into Slack with a panicked emoji. The first time we saw that screenshot, in late 2025, we thought it was a one-off. By February it was every week, at every design partner. The bill curve had bent.</p>

<p>Melt is what we built to fix it.</p>

<p>Snowflake's billing assumes humans drive the workload. Analysts refresh dashboards a few times a day. dbt models materialise overnight. Somebody asks a question, somebody runs a query. Compute spins up, runs, suspends. The bill, mostly, makes sense.</p>

<p>Agents don't run on that cadence. A single prompt fans out into dozens of small filters, joins, and aggregates as the model iterates toward an answer. Multiply that across a fleet of agents and the warehouse never gets to spin down. The cluster keeps autoscaling. The credits compound. The line goes up.</p>

<p>We've spent the last few months looking at that bill query by query, alongside design partners who got there first. The pattern is consistent: most of the queries agents generate don't actually need Snowflake compute. They scan less than 200&nbsp;MB of Parquet. They're single-table filters, counts, and aggregates that an embedded engine could answer against a local copy of the lake. But every one of them runs against a warehouse, because that's where the driver was pointed.</p>

<p>Melt is the routing layer that fixes that. It's an open-source proxy that sits in front of Snowflake — drop-in, self-hosted, Apache-2.0 — and decides per query where each statement should run. You change the host on your driver's connection string. That's the whole migration. The proxy speaks Snowflake's REST wire protocol, so the official driver, JDBC, ODBC, dbt, Looker, Sigma, Hex, and anything else you've already wired up keep working without modification.</p>

<p>There are two decisions melt makes on every query.</p>

<p>The first is <strong>query routing</strong>: should this statement run against the lake at all? If your data is already on S3 via Iceberg or DuckLake, DuckDB can answer most reads locally for cents. No warehouse spin-up, no credit burn, no minimum billing window. Writes, Snowflake-only features, and oversize scans pass through. Dual execution stitches plans that touch a mix of synced and remote tables, pushing some operators down to Snowflake and finalising the rest in DuckDB. Every decision is logged, exported to <code>/metrics</code>, and visible in <code>melt route</code> so a reviewer sees exactly where each query landed and why.</p>

<p>The second is <strong>warehouse routing</strong>, shipping next. Most accounts have a couple of warehouse sizes already provisioned and most queries land on the wrong one. Melt right-sizes per statement: <code>XSMALL</code> for the small filter, <code>LARGE</code> for the nightly aggregate. Per-statement, transparent to the driver. The customers who today only run lake routing tend to cut about half their bill. The ones who get both halves cut more.</p>

<p>We're being deliberate about what we're not doing. We don't rewrite SQL. We don't build a cost-prediction model trained on three years of <code>QUERY_HISTORY</code>. We don't auto-create or auto-resize warehouses. We never store query text. The data plane runs in your VPC. The router is open-source, and we'll keep arguing in pull requests about whether a particular optimisation is worth its parity risk.</p>

<p>Today, query routing is in the open repo: per-query routing, dual execution, the parity sampler, the three policy modes — passthrough, allowlist, enforce — and the metrics and audit surface. Warehouse routing — design doc shipped, Phase 1 in flight — is next.</p>

<p>If your warehouse bill has started feeling agent-shaped, talk to us. The repo is open. The contact form goes to a real human.</p>

<p><em>— Toby<br/>Founder, Melt</em></p>
`.trim(),
  },
];

export function getPost(slug: string): Post | undefined {
  return posts.find((p) => p.slug === slug);
}
