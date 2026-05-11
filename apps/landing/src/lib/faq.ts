export type FaqEntry = {
  /** Stable slug appended after `faq-` for the DOM anchor and JSON-LD parity. */
  id: string;
  question: string;
  answer: string;
};

export type FaqBlock = {
  /** Per-page canonical FAQ JSON-LD @id. */
  schemaId: string;
  entries: FaqEntry[];
};

export const homeFaq: FaqBlock = {
  schemaId: "https://www.meltcomputing.com/#faq",
  entries: [
    {
      id: "what-is-melt",
      question: "What is Melt?",
      answer:
        "Melt is a self-hosted Rust proxy that sits in front of Snowflake. Drivers connect to Melt as if it were Snowflake, and for every incoming statement Melt makes a per-query routing decision: most reads run on a DuckDB-backed lakehouse on S3 (Iceberg or DuckLake), while writes and Snowflake-specific work pass through to Snowflake unchanged. The lake copy stays current via CDC streams pulled out of Snowflake, so the answer the lake returns is the answer Snowflake would have returned.",
    },
    {
      id: "vs-query-optimizer",
      question:
        "How is Melt different from a query optimizer like Keebo or Espresso?",
      answer:
        "Query optimizers rewrite or re-time your SQL so Snowflake runs it more efficiently — the work still runs on the warehouse. Melt does not touch your SQL; it routes each statement to the engine that returns the same result for the lowest cost. When the answer is the lake, the warehouse never spins up at all, so there are no credits to optimise. The two approaches are complementary, but only routing removes traffic from Snowflake entirely.",
    },
    {
      id: "vs-cost-dashboard",
      question: "How is Melt different from a cost dashboard like Select?",
      answer:
        "Select tells you what your warehouse cost; Melt changes what the warehouse runs. Dashboards are observability — they help you right-size, attribute, or chargeback, but every query still hits Snowflake. Melt is an active proxy that decides per query whether Snowflake compute is required at all, and exports the same routing decisions to Prometheus and structured logs so you keep the visibility you had.",
    },
    {
      id: "sql-or-driver-changes",
      question: "Do I need to change my SQL or driver?",
      answer:
        "No. Melt speaks Snowflake's REST wire protocol exactly, so JDBC, ODBC, the Python connector, Go, Looker, Sigma, Hex, and dbt connect unmodified. The only change is the connection-string host. Dialect deltas like IFF, QUALIFY, DATEADD, and PARSE_JSON are handled inside the proxy when a query routes to the lake, so your queries stay portable. Snowflake-only constructs (Snowpark, GENERATOR, INFORMATION_SCHEMA access, Time Travel, lateral FLATTEN) pass through to Snowflake unchanged rather than being rewritten.",
    },
    {
      id: "routing-decision",
      question: "How does the routing decision work?",
      answer:
        'For every statement, the router parses the SQL, classifies it (writes and Snowflake-only features always pass through), checks that all referenced tables are synced and active, applies your policy mode, and checks the estimated scan against a configurable byte cap. Eligible reads run on DuckDB against Iceberg or DuckLake on S3; everything else forwards to Snowflake verbatim. The decision is observable in /metrics, in structured logs, and offline via melt route "<sql>", which prints the route, the typed reason, and the translated DuckDB SQL.',
    },
    {
      id: "cost-reduction-assumptions",
      question: "What does the cost-reduction headline assume?",
      answer:
        "The biggest savings come from accounts dominated by small, high-frequency reads — BI dashboards refreshing on a tile cadence, agent fleets firing thousands of lookups per day — where each query bills a Snowflake minimum but scans well under a gigabyte. Workloads that are mostly large joins or Snowflake-only features see smaller savings because those keep passing through. The hero figure on this site is workload-dependent and assumes a routable read mix; a published method and an interactive calculator will sit alongside the claim.",
    },
    {
      id: "where-does-melt-run",
      question: "Where does Melt run?",
      answer:
        "Melt is deployed in your own infrastructure. It has two services — a stateless proxy that scales horizontally and a single-writer sync that pulls Snowflake CDC into the lakehouse — and runs against your own Postgres catalog and S3-compatible storage (AWS, MinIO, Cloudflare R2, Backblaze B2, or Wasabi). Snowflake credentials, the lakehouse, and the data plane all stay inside your network.",
    },
    {
      id: "sensitive-table-policy",
      question: "What happens when a query touches a sensitive table?",
      answer:
        "Policy is configurable in three modes. The default passthrough mode forces any table with a Snowflake policy marker straight to Snowflake. allowlist keeps anything outside an explicit list off the lake. enforce rewrites references to sync-maintained filtered views so row-access and masking policies are honoured on the lake side. Sync polls Snowflake's POLICY_REFERENCES on a configurable interval and refreshes markers without a restart.",
    },
    {
      id: "setup-time",
      question: "How long does setup take?",
      answer:
        'The Docker quickstart brings Melt up against a local Postgres and MinIO in one command, and routing decisions can be inspected immediately with melt route "<sql>" — no Snowflake credentials required to try the router offline. For a real Snowflake account, configuration is a single melt.toml file (account, sync allowlist, backend selection) and pointing your drivers at Melt\'s host. Production splits proxy and sync across pods; the shape is documented in the architecture guide.',
    },
    {
      id: "dbt-looker-agents",
      question: "Can I use Melt with dbt, Looker, and AI agents?",
      answer:
        "Yes. Anything that speaks Snowflake's REST shape works unmodified — the official Snowflake Python connector connects to Melt with only host and port changed. JDBC and ODBC clients (Looker, Sigma, Hex, Tableau), Go, and dbt behave the same way. AI agents that generate SQL via these drivers route through Melt automatically, and they are typically the workload that gets the largest cost win because their queries are individually small and frequent — exactly the shape where Snowflake's per-query minimums hurt most.",
    },
  ],
};

export const featureFaqBySlug: Record<string, FaqBlock> = {
  "query-routing": {
    schemaId: "https://www.meltcomputing.com/features/query-routing#faq",
    entries: [
      {
        id: "what-is-query-routing",
        question: "What is query routing in Melt?",
        answer:
          "Query routing is a per-statement decision the Melt proxy makes for every incoming SQL statement: route to the lake (DuckDB on Iceberg or DuckLake), pass through to Snowflake, or run as a dual-execution plan. Eligible reads — read-only, all referenced tables synced and active, scan under the configured byte cap, no policy violations — run on the lake; everything else forwards to Snowflake unchanged. The same drivers and the same SQL get the same answers; only the engine changes.",
      },
      {
        id: "rewrites-my-sql",
        question: "Does query routing rewrite my SQL?",
        answer:
          "The original statement is forwarded verbatim when the route is Snowflake. When the route is the lake, the proxy applies dialect rewrites only — IFF to CASE WHEN, DATEADD normalisation, QUALIFY lowering, PARSE_JSON and FLATTEN rewrites — so a Snowflake-dialect query runs against DuckDB. If the translator cannot produce a safe rewrite, the statement falls through to Snowflake rather than guessing.",
      },
      {
        id: "what-routes-where",
        question: "What kinds of queries route to the lake versus Snowflake?",
        answer:
          "Read-only queries against synced and active tables, scanning under [router].lake_max_scan_bytes, with no policy markers and no Snowflake-only features (Snowpark, GENERATOR, INFORMATION_SCHEMA access, Time Travel), route to the lake. Writes — DML, DDL, MERGE, TRUNCATE, GRANT, REVOKE — and Snowflake-only constructs, oversize scans, policy-protected tables, and parser bailouts all pass through to Snowflake. The router never silently downgrades a query to a different answer.",
      },
      {
        id: "why-was-it-routed",
        question:
          "How do I see why a specific query was routed where it was?",
        answer:
          'melt route "<sql>" runs the entire decision pipeline offline and prints the chosen route, the typed reason (for example UnderThreshold, WriteStatement, UsesSnowflakeFeature("FLATTEN"), AboveThreshold, TranslationFailed), and the translated DuckDB SQL. The same route, reason, and backend labels are exported on the melt_router_decisions_total Prometheus counter, so live traffic is observable in any Grafana board without scraping logs.',
      },
    ],
  },
  "warehouse-routing": {
    schemaId:
      "https://www.meltcomputing.com/features/warehouse-routing#faq",
    entries: [
      {
        id: "what-is-warehouse-routing",
        question: "What is warehouse routing?",
        answer:
          "When a query has to hit Snowflake, warehouse routing lands it on a warehouse that is already warm. Melt observes the warehouse fleet in real time off the wire (no INFORMATION_SCHEMA polling, no synthetic probes) and scores each candidate by warmth recency, in-flight count, concurrency headroom, queue depth, and size. The goal is to stop paying the 60-second cold-start minimum Snowflake bills every time a warehouse resumes.",
      },
      {
        id: "vs-multi-cluster",
        question: "Why not just use Snowflake multi-cluster warehouses?",
        answer:
          "Multi-cluster warehouses solve cross-cluster fan-out natively, but the feature is gated to Snowflake's Enterprise tier, which carries roughly a 50% per-credit premium. Warehouse routing gives Standard-tier accounts the same shape — a pool of warehouses with traffic distributed across them — without paying the Enterprise premium. You still own the warehouse list; Melt just picks the right one per statement.",
      },
      {
        id: "driver-warehouse-binding",
        question:
          "Does warehouse routing change which warehouse my driver connects to?",
        answer:
          "The driver still connects to the warehouse it is configured for. Warehouse routing rewrites the warehouse at the session-swap layer inside the proxy for a given statement; the driver does not see it. A leading /*+ MELT_WAREHOUSE('TRANSFORM_WH') */ SQL hint pins a query to a named warehouse and bypasses the router when you need explicit control — for example, on a tenant warehouse with isolation requirements.",
      },
      {
        id: "production-ready",
        question: "Is warehouse routing production-ready?",
        answer:
          "Warehouse routing is in alpha. Design and prototyping are active, with a flag-gated implementation planned in a near-term release and a public design RFC ahead of GA. The warmth-ledger plumbing piggybacks on the existing per-statement instrumentation, and the session-swap path is an extension of the existing passthrough rewrite, so the surface area is incremental rather than a new service.",
      },
    ],
  },
};
