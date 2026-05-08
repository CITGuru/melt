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
      "An open-source proxy that sits in front of Snowflake and decides, per query, where each statement should run. Here's why we built it.",
    category: "Launch",
    readTime: "4 min read",
    author: "Melt Team",
    authorRole: "Builders of Melt",
    publishedAt: "May 7, 2026",
    body: `
<p>Every few weeks now, somebody on a data team runs <code>SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) FROM QUERY_HISTORY</code> and pastes the result into Slack with a panicked emoji. The first one we saw, late last year, looked like a one-off. By February we'd seen six. The bill curve had bent.</p>

<p>Melt is what we built to fix it.</p>

<p>Snowflake's billing assumes humans drive the workload. Analysts refresh dashboards in the morning, dbt models materialise overnight, somebody asks a question and somebody runs a query. Compute spins up, runs, suspends. The bill mostly makes sense.</p>

<p>Agents don't run on that cadence. One prompt fans out into dozens of small filters and aggregates while the model iterates toward an answer. Across a fleet of agents, the warehouse never spins down, the cluster keeps autoscaling, and the line on the bill chart goes up and stays up.</p>

<p>We've spent months going through those bills query by query, with design partners who got there first. Most of what agents generate doesn't actually need Snowflake compute. Scans under 200&nbsp;MB of Parquet. Single-table filters, counts, simple group-bys. The kind of thing DuckDB answers in milliseconds against the lake. But every one of them lands on a warehouse, because that's where the driver was pointed.</p>

<p>Melt is the routing layer that fixes that. It's a self-hosted open-source proxy you drop in front of Snowflake. You change the host on your driver's connection string and that's the whole migration. Because the proxy speaks Snowflake's REST wire protocol, the official driver, JDBC, ODBC, dbt, Looker, Sigma, Hex, and anything else already wired up to your warehouse keeps working without modification.</p>

<p>Two decisions get made on every query.</p>

<p>The first is <strong>query routing</strong>: should this statement run against the lake at all? If your data is already on S3 via Iceberg or DuckLake, DuckDB can answer most reads locally for cents. The warehouse never spins up. Writes, Snowflake-only features, and oversize scans pass through to Snowflake unchanged. For queries that touch a mix of synced and remote tables, dual execution plan-splits the work, pushing some operators down to Snowflake and finishing the rest in DuckDB. Every decision is logged, exported to <code>/metrics</code>, and printable from <code>melt route</code> so a reviewer can see where each query landed and why.</p>

<p>The second is <strong>warehouse routing</strong>, which is shipping soon. Most accounts have a couple of warehouse sizes already provisioned and most queries land on the wrong one. Melt picks the right size per statement: <code>XSMALL</code> for a filter, <code>LARGE</code> for the nightly aggregate, and your driver doesn't notice. Customers running just query routing today tend to cut about half their bill. The ones running both cut more.</p>

<p>Today, query and warehouse routing is in the open repo. That includes the router itself, dual execution, the parity sampler, three policy modes (passthrough, allowlist, enforce), and the full metrics and audit surface. Materialized and incremental views are next.</p>

<p>If your warehouse bill has started feeling agent-shaped, talk to us. The repo is open and the contact form goes to a real person.</p>

<p><em>Melt Team</em></p>
`.trim(),
  },
];

export function getPost(slug: string): Post | undefined {
  return posts.find((p) => p.slug === slug);
}
