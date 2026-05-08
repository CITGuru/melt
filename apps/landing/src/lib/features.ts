export type FeatureStatus = "live" | "alpha";
export type FeatureCategory = "Routing" | "Warehouse" | "Views";
export type FeatureIconName =
  | "routing"
  | "split"
  | "loadBalance"
  | "warehouse"
  | "stack"
  | "delta";

export type HowItWorksStep = {
  step: string;
  title: string;
  description: string;
};

export type CodeExample = {
  filename: string;
  language: string;
  code: string;
  caption?: string;
};

export type NamedItem = {
  title: string;
  description: string;
};

export type Comparison = {
  name: string;
  description: string;
};

export type FaqItem = {
  question: string;
  answer: string;
};

export type Architecture = {
  description?: string;
  diagram?: string;
  captions?: NamedItem[];
};

export type Feature = {
  slug: string;
  title: string;
  category: FeatureCategory;
  status: FeatureStatus;
  iconName: FeatureIconName;
  /** One-line descriptor for the nav dropdown. */
  shortDescription: string;
  /** Hero subtitle on the feature page. */
  tagline: string;
  /** SEO. */
  metaTitle: string;
  metaDescription: string;

  // Common to both live and alpha
  architecture?: Architecture;
  workloads?: NamedItem[];
  comparisons?: Comparison[];
  faq?: FaqItem[];

  // Live only
  howItWorks?: HowItWorksStep[];
  codeExamples?: CodeExample[];
  benefits?: NamedItem[];

  // Alpha only
  problemFraming?: string[];
  alphaPromise?: NamedItem[];
  bets?: NamedItem[];
  audience?: NamedItem[];
};

export const features: Feature[] = [
  // ────────────────────────────  ROUTING  ────────────────────────────
  {
    slug: "query-routing",
    title: "Query Routing",
    category: "Routing",
    status: "live",
    iconName: "routing",
    shortDescription:
      "Parse, classify, and route every statement on its own merit.",
    tagline:
      "A Rust proxy that parses every Snowflake statement, classifies it against your sync state and policy markers, and runs it on the cheapest engine that returns the same result. Same drivers, same SQL, same answers — most reads never touch a warehouse.",
    metaTitle: "Query Routing — Per-statement Snowflake cost routing",
    metaDescription:
      "Drop-in Rust proxy that parses every Snowflake query, classifies it, and routes eligible reads to a DuckDB lakehouse. No SQL changes, per-query audit.",
    howItWorks: [
      {
        step: "01",
        title: "Parse",
        description:
          "Every statement that arrives at the proxy is parsed with sqlparser-rs against the Snowflake dialect — including IFF, QUALIFY, FLATTEN, semi-structured access, time-zone arithmetic, and stored-procedure calls. The same AST drives both the routing decision and the rewrite, so per-statement overhead stays under a millisecond. If the parser can't produce a clean tree, the statement falls through to Snowflake passthrough rather than guessing.",
      },
      {
        step: "02",
        title: "Classify",
        description:
          "The router runs three checks against the AST: is_write (DML, DDL, GRANT/REVOKE, MERGE, TRUNCATE), uses_snowflake_features (Snowpark calls, GENERATOR, INFORMATION_SCHEMA, Time Travel), and extract_tables (resolves 1-, 2-, 3-part names against the session's database/schema). Writes and Snowflake-only constructs short-circuit to passthrough — the lake never sees them. Comment hints override the decision tree before classification runs.",
      },
      {
        step: "03",
        title: "Look up sync, policy, and scan stats",
        description:
          "For the resolved table set, the router fans out three concurrent catalog reads in a single Postgres round trip: tables_exist against the lakehouse catalog, policy_markers, and estimate_scan_bytes from the per-table statistics maintained by sync. All three sit behind LRU TTL caches (5-minute existence cache, 1-minute byte estimate by default), so a hot dashboard query is decided entirely from memory after the first hit.",
      },
      {
        step: "04",
        title: "Apply policy and threshold gates",
        description:
          "The policy gate enforces one of three modes: passthrough forces any policy-marked table to Snowflake, allowlist forces tables outside an explicit list, and enforce rewrites references to sync-maintained filtered views so masked columns are honored locally. After policy, lake_max_scan_bytes caps the total estimated scan. Anything failing produces a typed PassthroughReason that ends up on melt_router_decisions_total{reason=…}.",
      },
      {
        step: "05",
        title: "Translate and dispatch",
        description:
          "Eligible reads run through the translation passes in melt-core: IFF → CASE WHEN, DECODE → CASE chain, NVL → COALESCE, EQUAL_NULL → IS NOT DISTINCT FROM, DATEADD normalization, QUALIFY lowering, PARSE_JSON / FLATTEN rewrites. The rewritten SQL runs in DuckDB against the configured StorageBackend (DuckLake on Postgres + S3, or an Iceberg REST catalog). The original statement, the chosen route, and the translated SQL are emitted as a structured log line and as labels on every decision.",
      },
    ],
    architecture: {
      description:
        "A left-to-right flow with the proxy in the middle, split into a clear decision pipeline. Eight nodes, three exit lanes. Every decision emits melt_router_decisions_total{route, reason, backend} and a structured log line. melt route \"<sql>\" runs the same pipeline offline.",
      diagram: `client ──▶ parse ──▶ classify ──▶ catalog stats ──▶ policy gate ──▶ translate ──▶ route?
 (driver)   sqlparser    is_write?       fan-out (3x)      passthrough     IFF→CASE        ╱│╲
            Snowflake    snowflake_fn?   tables_exist      allowlist       QUALIFY        ╱ │ ╲
            dialect      extract_tables  policy_markers    enforce         DATEADD       ▼  ▼  ▼
                         (resolves        scan_bytes                       PARSE_JSON   LAKE SF DUAL
                          1/2/3-part)     (TTL LRU)                        FLATTEN`,
      captions: [
        {
          title: "Lake exit",
          description:
            "DuckDB executes against Iceberg / DuckLake on S3. No warehouse spin-up, sub-second on cached parquet stats.",
        },
        {
          title: "Snowflake exit",
          description:
            "Verbatim forward to upstream. Used for writes, Snowflake-only features, oversize estimates, policy-protected tables, parser bailouts.",
        },
        {
          title: "Dual exit",
          description:
            "Plan-split between DuckDB and Snowflake with an Arrow IPC bridge. See Dual Execution.",
        },
      ],
    },
    codeExamples: [
      {
        filename: "$ melt route",
        language: "bash",
        caption: "Lake-eligible read — translation, route, and reason",
        code: `$ melt route "SELECT day, count(*) FROM analytics.public.events
              WHERE day > current_date - 7 GROUP BY 1"

route:  lake
reason: UnderThreshold { estimated_bytes: 184_352_117 }

translated:
SELECT day, count(*) FROM public.events
  WHERE day > current_date - INTERVAL 7 DAY
  GROUP BY 1`,
      },
      {
        filename: "melt.toml",
        language: "toml",
        caption: "The full routing surface — every threshold and policy mode",
        code: `[router]
# Sum of per-table scan estimates above which a query falls back
# to Snowflake instead of running on the lake.
lake_max_scan_bytes      = "2GiB"
table_exists_cache_ttl   = "5m"
estimate_bytes_cache_ttl = "1m"

# Hybrid execution (see /features/dual-execution).
hybrid_execution             = true
hybrid_max_remote_scan_bytes = "5GiB"

[snowflake.policy]
# passthrough = any masked table forces Snowflake
# allowlist   = explicit table list; everything else passes through
# enforce     = rewrite refs to sync-maintained filtered views
mode             = "passthrough"
refresh_interval = "60s"

[sync]
auto_discover = true
include = ["ANALYTICS.MARTS.*", "ANALYTICS.PUBLIC.*"]
exclude = ["ANALYTICS.LEGACY.*"]
remote  = ["WAREHOUSE.HUGE_FACT_*"]   # never sync; federate at query time`,
      },
      {
        filename: "/metrics",
        language: "text",
        caption: "Every routing decision is observable in Prometheus",
        code: `melt_router_decisions_total{route="lake",      reason="under_threshold",   backend="ducklake"} 184_201
melt_router_decisions_total{route="snowflake", reason="write_statement",   backend="ducklake"}  12_044
melt_router_decisions_total{route="snowflake", reason="above_threshold",   backend="ducklake"}     617
melt_router_decisions_total{route="hybrid",    reason="remote_by_config",  backend="ducklake"}   1_932

melt_router_decision_duration_seconds_bucket{le="0.001"} 178_904
melt_router_decision_duration_seconds_bucket{le="0.005"} 196_892`,
      },
    ],
    benefits: [
      {
        title: "Cost lands where the work lands",
        description:
          "Most analytics queries scan well under a gigabyte. DuckDB answers them on the proxy host for cents — no warehouse spin-up, no minimum billing window, no idle credits. Workloads dominated by small reads drop from a steady warehouse line item to a fixed proxy host plus S3 reads.",
      },
      {
        title: "One connection-string change, no SQL rewrites",
        description:
          "Melt speaks the same Snowflake REST shape your drivers already speak. JDBC, ODBC, the Python connector, Go, dbt, Looker, Sigma, and Hex connect unmodified — only host changes. The translator handles dialect deltas inside the proxy, so your queries stay portable.",
      },
      {
        title: "Per-query auditability",
        description:
          "Every routing decision is exported to /metrics with route, reason, and backend labels, written as a structured log line, and reproducible offline via melt route \"<sql>\". The CLI prints the route, the typed PassthroughReason, the estimated scan bytes, and the rewritten DuckDB SQL.",
      },
      {
        title: "Correctness as a precondition, not a check",
        description:
          "A query is only eligible for the lake when every referenced table is in active sync state, no policy markers are attached (or the operator chose enforce mode), the translator finishes cleanly, and the scan estimate fits the cap. Anything else passes through unchanged.",
      },
      {
        title: "Sub-millisecond decisions on the hot path",
        description:
          "Parse, classify, and stats lookups all share one AST and three TTL LRU caches. Decision latency is dominated by the catalog round trip on a cache miss (single-digit milliseconds) and by parsing on a hit (sub-millisecond).",
      },
      {
        title: "Operational escape hatches",
        description:
          "Comment hints (/*+ melt_route(snowflake) */, /*+ melt_strategy(materialize) */) override routing per query. [sync].include/exclude/remote globs control mirroring and federation. POST /admin/reload validates and atomically swaps config into the running proxy via ArcSwap — no restart, no downtime.",
      },
    ],
    workloads: [
      {
        title: "BI dashboards refreshing every 60 seconds",
        description:
          "A Sigma board with twelve tiles, each a SELECT … GROUP BY day against a 30-day partition, scans roughly 150 MB per tile. On Snowflake that's a continuously-warm XS warehouse plus the per-second floor. On Melt, all twelve tiles route to the lake on the first refresh, sit hot in DuckDB's buffer pool, and re-execute on every tick without touching the warehouse.",
      },
      {
        title: "Agent fleets generating ten thousand small queries per day",
        description:
          "A research agent issues SELECT … WHERE customer_id = ? LIMIT N and SELECT count(*) FROM events thousands of times while iterating on a question. Each one is 1–50 MB on disk after pruning. Snowflake bills a minimum credit per query through the warehouse; DuckDB resolves them in tens of milliseconds with predicate pushdown to Parquet stats.",
      },
      {
        title: "dbt projects with 200 read-heavy models",
        description:
          "Most are incremental selects against synced tables — joins, window functions (QUALIFY), and aggregates that translate cleanly into DuckDB. Materializations stay on Snowflake because the router treats CREATE TABLE … AS as a write. Nightly dbt run reads cheap, writes land where you expect, and the dbt logs see the same Snowflake REST responses they always have.",
      },
      {
        title: "Reverse-ETL jobs pulling segment definitions",
        description:
          "SELECT user_id FROM analytics.public.user_segments WHERE segment IN (…) running 288 times a day, scanning under 80 MB each. The lake-routing TTL cache makes every run after the first essentially free on the proxy side, and the Census/Hightouch driver doesn't know the difference.",
      },
      {
        title: "Federated queries over Snowflake-only fact tables",
        description:
          "A dashboard joins synced users with a 4 TB transactions table the operator declared remote so it's never mirrored. With hybrid_execution = true, the planner pushes the predicate-filtered transaction scan to Snowflake, streams the result back over Arrow IPC, and finishes the join in DuckDB. The 4 TB table is never copied and the dashboard works.",
      },
    ],
    comparisons: [
      {
        name: "Doing nothing (Snowflake auto-suspend, query acceleration, multi-cluster)",
        description:
          "Snowflake's own knobs improve warehouse utilization but don't change which engine answers a query. Every read still bills against a virtual warehouse, still pays the per-second floor, still pays the spin-up tax. Melt removes the warehouse from the path entirely for queries that don't need it. The two are complementary — auto-suspend on the warehouse you keep is still useful — but they solve different problems.",
      },
      {
        name: "Greybeam",
        description:
          "Greybeam ships a similar architecture (proxy in front of Snowflake, Iceberg-backed cache, per-query routing) as a managed product. Melt is open-source under Apache-2.0, self-hosted, and brings the routing decision into your repo: melt.toml controls every threshold, [sync].remote declares federation patterns, comment hints override per-query, POST /admin/reload is a documented endpoint. Dual execution is a first-class plan IR with a printable plan tree, not a black-box fallback.",
      },
      {
        name: "SELECT and observability tools",
        description:
          "SELECT excels at warehouse right-sizing, anomaly detection, per-team budget attribution, and cost visibility — none of which Melt does today. SELECT does not route or rewrite queries; it makes the warehouse you already pay for cheaper. Melt removes traffic from the warehouse altogether. Many teams run both: SELECT to keep the warehouse Melt still uses honest, Melt to keep less traffic on the warehouse in the first place.",
      },
    ],
    faq: [
      {
        question: "What happens if a query can't run on DuckDB?",
        answer:
          "It falls through to Snowflake passthrough automatically with a typed reason — WriteStatement, UsesSnowflakeFeature(\"FLATTEN\"), TableMissing, AboveThreshold, TranslationFailed{detail}, etc. The reason is exported on melt_router_decisions_total{reason=…} and printed by melt route. The client sees the same Snowflake response it would have without Melt; nothing fails open.",
      },
      {
        question: "How does access control work?",
        answer:
          "Driver authentication is forwarded to Snowflake at login — Melt doesn't issue or store user credentials. Snowflake row-access and masking policies are honored through [snowflake.policy]: passthrough sends any policy-marked table straight to Snowflake; enforce rewrites references to filtered views that sync materializes from POLICY_REFERENCES. Service auth (PAT or RSA key-pair) is only used by the sync loop.",
      },
      {
        question: "What's the latency overhead?",
        answer:
          "Decision latency is bounded by parsing plus three catalog lookups behind LRU TTL caches — sub-millisecond on a cache hit, single-digit milliseconds on a miss. The proxy itself is a Tokio-based Rust binary with no per-request allocations beyond the AST. End-to-end, lake-routed queries usually finish faster than Snowflake (no warehouse round-trip).",
      },
      {
        question: "Can I disable routing for specific tables or queries?",
        answer:
          "Three knobs. [sync].exclude = [\"…\"] keeps a table off the lake entirely. [sync].remote = [\"…\"] keeps a table off the lake but lets dual execution federate it. Per-query, leading comment hints — /*+ melt_route(snowflake) */, /*+ melt_route(lake) */, /*+ melt_strategy(materialize) */ — override the decision tree without a config change.",
      },
      {
        question: "How do I roll this back if something breaks?",
        answer:
          "The proxy is a drop-in for the Snowflake REST endpoint. Rolling back is a connection-string flip back to your real Snowflake host — your drivers don't notice. For partial rollback, set [router].lake_max_scan_bytes = \"0B\" and reload via POST /admin/reload: every read falls through to Snowflake while the proxy keeps logging decisions.",
      },
      {
        question: "How fresh is the lake copy?",
        answer:
          "Sync pulls Snowflake CDC streams (incremental STREAM consumption every 60 seconds by default) and applies inserts/updates/deletes through the lakehouse writer. Tables advance through pending → bootstrapping → active; only active tables are eligible for lake routing. melt sync status and the table_stats metric expose per-table sync lag.",
      },
    ],
  },

  {
    slug: "dual-execution",
    title: "Dual Execution",
    category: "Routing",
    status: "live",
    iconName: "split",
    shortDescription:
      "Plan-split between DuckDB and Snowflake via Arrow IPC.",
    tagline:
      "When a query touches both lake-synced and Snowflake-only tables, melt's router splits the plan: Snowflake-resident operators ship to Snowflake, the rest run in DuckDB, and Arrow batches stitch the result. No whole-query fallback, no SQL changes.",
    metaTitle: "Dual Execution — Plan-split queries across DuckDB and Snowflake",
    metaDescription:
      "Hybrid plans for queries that touch both lake-synced and Snowflake-only tables. Pushdown collapse, Arrow IPC bridge, parity-sampled correctness. Opt-in.",
    howItWorks: [
      {
        step: "01",
        title: "Parse and classify placement",
        description:
          "The proxy parses incoming Snowflake-dialect SQL and walks every ObjectName. Each fully-qualified table is classified through TableSourceRegistry: tables matching a [sync].remote glob — or promoted by hybrid_allow_bootstrapping / hybrid_allow_oversize — get tagged Placement::Remote; lake-synced tables stay Placement::Local. If no remote tables are referenced, the router returns the existing Lake/Snowflake decision unchanged.",
      },
      {
        step: "02",
        title: "Build the hybrid plan",
        description:
          "The plan builder walks the AST and produces an Arc<HybridPlan> whose root: PlanNode tree carries Placement annotations and RemoteSql collapse nodes. Two transforms run in order: subtree collapse (any Query block whose every relation is Remote becomes a single RemoteFragment) and per-scan attach rewrite (every leftover Remote ObjectName is rewritten to sf_link.<...>, DuckDB's attached Snowflake catalog).",
      },
      {
        step: "03",
        title: "Choose a strategy per fragment",
        description:
          "BridgeStrategy::Attach for single-scan nodes — run as sf_link.<...> through DuckDB's snowflake_scan operator, vectorized with predicate pushdown. BridgeStrategy::Materialize for multi-scan nodes — ship the fragment SQL to Snowflake, drain Arrow RecordBatches into DuckDB, stage as CREATE TEMP TABLE __remote_N AS …. Snowflake executes joins inside the fragment natively; DuckDB scans the staged temp table for the outer join.",
      },
      {
        step: "04",
        title: "Bridge results through Arrow",
        description:
          "Both strategies move data over Arrow columnar buffers (zero-copy where the schema matches). For Materialize, the proxy opens a Snowflake fragment cursor and feeds RecordBatches through the Appender path against __remote_N. For Attach, DuckDB drives the round-trip itself: the community Snowflake extension wraps Arrow ADBC, so per-batch IPC happens inside DuckDB's pipelined executor.",
      },
      {
        step: "05",
        title: "Final SQL on DuckDB",
        description:
          "The mutated AST is run through translate_ast to convert any remaining Snowflake-dialect constructs to DuckDB dialect; the resulting local_sql references both bridge surfaces (__remote_0, __remote_1, … for Materialize fragments; sf_link.<...> for Attach scans) alongside lake-resident tables. DuckDB hash-joins, aggregates, and projects across all of them, then the proxy streams the result back over the original Snowflake wire protocol.",
      },
    ],
    architecture: {
      description:
        "Eight nodes, one happy path with two parallel legs that merge at DuckDB. The same query gets to choose between Attach (streaming, vectorized) and Materialize (one Snowflake roundtrip, native join), or mix both — strategy is decided per fragment.",
      diagram: `client ──▶ melt-proxy ──▶ hybrid plan builder
   ▲              │              ╱           ╲
   │              │      Materialize       Attach
   │              ▼          ╱                 ╲
   │      ┌──────────────┐         ┌──────────────────┐
   │      │ Snowflake    │         │ DuckDB           │
   │      │ HTTP + Arrow │         │ ATTACH 'sf_link' │
   │      │ chunks       │         │ snowflake_scan   │
   │      └──────┬───────┘         └────────┬─────────┘
   │             │ Arrow IPC                │ Arrow IPC
   │             │ RecordBatch stream       │ pipelined
   │             ▼                          │
   │     CREATE TEMP TABLE __remote_N       │
   │     AS … (DuckDB Appender)             │
   │             └────────────┬─────────────┘
   │                          ▼
   │             DuckDB executes local_sql
   │             (joins __remote_N + lake +
   └─────────────  sf_link.<...> aliases)`,
      captions: [
        {
          title: "Builder",
          description:
            "Produces HybridPlan with RemoteFragment[] (Materialize) and AttachRewrite[] (Attach) plus rewritten local_sql.",
        },
        {
          title: "Materialize leg",
          description:
            "Collapsed multi-table subtrees → one Snowflake roundtrip per fragment → __remote_N temp table. Used for joins on Snowflake-resident tables.",
        },
        {
          title: "Attach leg",
          description:
            "Single-table remote scans → DuckDB's snowflake_scan via ADBC → no materialization, predicate pushdown through the extension.",
        },
        {
          title: "Parity sampler",
          description:
            "Replays a configurable fraction of hybrid queries against pure Snowflake. Mismatches increment melt_hybrid_parity_mismatches_total and emit a structured WARN.",
        },
      ],
    },
    codeExamples: [
      {
        filename: "$ melt route",
        language: "bash",
        caption: "Mixed strategy — Attach for users, Materialize for the orders ⨝ products subquery",
        code: `$ melt route "SELECT u.region, COUNT(*)
       FROM   sf.warehouse.users u
       JOIN   ice.analytics.events e ON e.uid = u.id
       WHERE  e.ts > '2026-01-01'
         AND  u.id IN (
            SELECT buyer_id
              FROM sf.warehouse.orders o
              JOIN sf.warehouse.products p ON p.id = o.pid
             WHERE p.category = 'electronics'
         )
       GROUP BY u.region"

route: hybrid
reason: remote_by_config
strategy: mixed              chain_decided_by: heuristic
remote_fragments: 1   attach_rewrites: 1   est_remote_bytes: 22_210_000

[REMOTE,materialize]  __remote_0   (2 tables: WAREHOUSE.ORDERS, WAREHOUSE.PRODUCTS)
SELECT o.buyer_id
  FROM sf_link.WAREHOUSE.ORDERS  o
  JOIN sf_link.WAREHOUSE.PRODUCTS p ON p.id = o.pid
 WHERE p.category = 'electronics'

[REMOTE,attach]       WAREHOUSE.USERS  →  sf_link.WAREHOUSE.USERS

local SQL (DuckDB dialect):
SELECT u.region, COUNT(*)
  FROM sf_link.WAREHOUSE.USERS u
  JOIN ice.analytics.events    e ON e.uid = u.id
 WHERE e.ts > '2026-01-01'
   AND u.id IN (SELECT * FROM __remote_0)
 GROUP BY u.region`,
      },
      {
        filename: "melt.toml",
        language: "toml",
        caption: "The full hybrid surface — every threshold, strategy, and kill switch",
        code: `[sync]
remote = [
    "BIG_WAREHOUSE.*",
    "FOO_DB.PUBLIC.EVENTS",
]

[router]
hybrid_execution             = true            # master switch

# Materialize-strategy caps. Above the sum, the query collapses
# to passthrough with reason = AboveThreshold.
hybrid_max_remote_scan_bytes = "5GiB"
hybrid_max_fragment_bytes    = "2GiB"

# Attach-strategy cap; intentionally higher than Materialize because
# Attach streams without bulk materialization.
hybrid_attach_enabled        = true
hybrid_max_attach_scan_bytes = "10GiB"

hybrid_allow_bootstrapping   = false   # promote bootstrapping tables
hybrid_allow_oversize        = false   # promote one over-cap table

# Sampled correctness check: replay this fraction against pure
# Snowflake; mismatches → melt_hybrid_parity_mismatches_total.
hybrid_parity_sample_rate    = 0.01

[router.hybrid_strategy]
chain = ["heuristic"]                  # static heuristic (default)
# chain = ["cost", "heuristic"]        # cost-driven Attach-vs-Materialize`,
      },
    ],
    benefits: [
      {
        title: "No more all-or-nothing cliff",
        description:
          "Without dual exec, one Snowflake-only table forces the entire query back to Snowflake — even if 90% of the bytes are lake-resident. Melt's hybrid path keeps the lake side on DuckDB and only ships the operators that need Snowflake.",
      },
      {
        title: "Sub-query level cost reduction",
        description:
          "The router's pushdown collapse rule turns multi-table remote subtrees into a single Snowflake-side join, executed natively. No row-by-row marshalling, no DuckDB hash-join over Snowflake-resident dimensions. melt_hybrid_pushdown_collapsed_total exposes how often this fires.",
      },
      {
        title: "Parity-checked correctness",
        description:
          "Every hybrid query is eligible for replay against pure Snowflake at hybrid_parity_sample_rate. Mismatches surface immediately as a labelled counter and a structured WARN carrying plan shape, row counts, and (in Hash mode) a per-row XOR-of-SHA256 digest.",
      },
      {
        title: "Zero SQL changes",
        description:
          "Same JDBC/ODBC/Python/Go driver, same wire protocol, same dialect. All rewriting happens inside the proxy: sf_link.<...> aliases and __remote_N temp tables are invisible to the client. dbt models, BI tools, and ad-hoc analyst SQL keep working unchanged.",
      },
      {
        title: "Observable plans",
        description:
          "melt route <sql> prints the full plan tree — every [REMOTE,attach] and [REMOTE,materialize] node, the strategy that decided each, the fragment SQL Snowflake will execute, and the rewritten local_sql. Eleven Prometheus histograms and counters under melt_hybrid_* cover strategy mix, fragment bytes, materialize latency, fallbacks, and parity.",
      },
      {
        title: "Opt-in safety, with kill switches at every layer",
        description:
          "Off by default. hybrid_attach_enabled = false disables Attach globally. Per-query /*+ melt_route(snowflake) */ or /*+ melt_strategy(materialize) */ overrides the planner. Per-table policy markers refuse hybrid for compliance-sensitive tables. Above hybrid_max_*_bytes, the query collapses cleanly to passthrough with a reason trail.",
      },
    ],
    workloads: [
      {
        title: "Synced fact + declared-remote dimension (compliance carve-out)",
        description:
          "HR_DB.PEOPLE.EMPLOYEES carries PII you've decided not to sync; it stays in [sync].remote = [\"HR_DB.*\"]. Your daily report joins synced events to remote employees. Dual exec emits a single Attach rewrite for EMPLOYEES, lets DuckDB push down the WHERE region IN (...) predicate, and joins against the lake-resident events locally. PII never crosses the lake.",
      },
      {
        title: "90/10 lake-heavy dashboard joining a small Snowflake-only lookup",
        description:
          "Fact tables in the lake; one slowly-changing dimension (MARKETING.CAMPAIGNS) declared remote because a sibling team owns it. Dual exec ships a tight Snowflake fragment for CAMPAIGNS (~thousands of rows), DuckDB scans those once, and the broadcast hash join finishes locally against the multi-billion-row fact tables.",
      },
      {
        title: "Bootstrapping window — query while a freshly-discovered table is still syncing",
        description:
          "Auto-discovery picks up WAREHOUSE.NEW_TABLE at 10:00. Without dual exec, queries against it passthrough until state = active. With hybrid_allow_bootstrapping = true, the router promotes NEW_TABLE to Remote until sync catches up — Attach-rewrites it on-the-fly, keeps the rest of the query local. Reason flips from RemoteByConfig to RemoteBootstrapping in metrics.",
      },
      {
        title: "Remote-to-remote join (two tables you're choosing not to sync)",
        description:
          "Multi-tenant SaaS pattern: BIG.WAREHOUSE.ORDERS ⨝ BIG.WAREHOUSE.PRODUCTS for a Shopify-style report, both declared remote. The collapse rule fuses the join into one RemoteFragment whose snowflake_sql covers both tables; Snowflake executes the join natively in one pass.",
      },
      {
        title: "Single oversize table next to lake-sized peers",
        description:
          "A clickstream table is over lake_max_scan_bytes = 100 GB and you don't want to sync it; everything else in the same query fits comfortably. With hybrid_allow_oversize = true, the router promotes only the over-cap table to Remote (Attach via the extension's predicate pushdown) and keeps the rest local.",
      },
    ],
    comparisons: [
      {
        name: "Whole-query fallback (default melt without dual exec; Greybeam's published behavior)",
        description:
          "Any non-eligible table — unsynced, oversize, policy-protected — bounces the entire statement to Snowflake. Simple, safe, and the right behavior when the proxy can't reason about subplans. The cost: even queries where 95% of the bytes were lake-resident get warehoused. Dual execution is the strict generalization.",
      },
      {
        name: "Federation engines (Trino / Presto, Calcite-based adapters)",
        description:
          "Trino and Calcite federate across heterogeneous backends. Differences: (a) Trino is itself a query engine, so adopting it means moving compute off Snowflake; (b) the Snowflake side runs as a generic JDBC connector with no awareness of Snowflake-specific features (clustering hints, QUALIFY, FLATTEN, VARIANT/OBJECT); (c) no client-side migration — your dbt project, your driver, your SQL stays untouched with melt.",
      },
      {
        name: "Manually rewriting queries to keep them Snowflake-only",
        description:
          "Works for a frozen workload but doesn't survive table-by-table sync onboarding, multi-tenant carve-outs, or the bootstrapping window. It's also self-defeating for the lake-sized 90% of analytics queries where Snowflake compute is what dual exec is specifically saving you.",
      },
    ],
    faq: [
      {
        question: "When does hybrid fire vs. fall back to Snowflake passthrough?",
        answer:
          "Hybrid fires when the query references at least one Remote-classified table AND the build path doesn't bail. Bailouts: set ops, window functions over a Remote table, anything where placement isn't unambiguous, or — at execute time — any threshold above hybrid_max_remote_scan_bytes / hybrid_max_fragment_bytes / hybrid_max_attach_scan_bytes. Bailouts route to Snowflake passthrough with a reason label (set_op, window_over_remote, above_threshold). Policy-protected tables also refuse hybrid regardless of size.",
      },
      {
        question: "How is correctness verified?",
        answer:
          "Two layers. (1) Test fixtures exercise hundreds of variant SQL files with @expected.route / @expected.strategy annotations on every PR. (2) At runtime, hybrid_parity_sample_rate (default 1%) replays a fraction of live hybrid queries against pure Snowflake; row counts compared in RowCount mode, plus a per-row XOR-of-SHA256 digest in Hash mode. Mismatches emit a structured WARN with enough context to reproduce.",
      },
      {
        question: "What's the latency cost of the Arrow bridge?",
        answer:
          "Two regimes. Attach is pipelined: DuckDB calls into the community Snowflake extension per batch over Arrow Flight. Materialize pays a one-time staging cost (Snowflake fragment + Arrow drain into __remote_N) but the subsequent local scan runs at native DuckDB speed. The melt_hybrid_materialize_latency_seconds histogram surfaces the staging time per query; for typical small-dimension Materialize, it's tens of milliseconds.",
      },
      {
        question: "Can I disable dual exec for sensitive tables, or for one tenant?",
        answer:
          "Three layers, finest-grained first. Per-table: tag with a policy marker — short-circuits to passthrough. Per-query: /*+ melt_route(snowflake) */ forces passthrough; /*+ melt_strategy(materialize) */ keeps it hybrid but disables Attach. Per-tenant: hybrid_attach_enabled = false kills Attach globally, hybrid_execution = false kills hybrid entirely. All three kill paths are first-class — no recompile, no restart.",
      },
      {
        question: "How do I see which queries went dual?",
        answer:
          "Three surfaces. melt route <sql> prints the plan tree offline. /metrics exposes melt_router_hybrid_reasons_total{reason=…}, melt_hybrid_strategy_total{strategy=…}, melt_hybrid_pushdown_collapsed_total, melt_hybrid_remote_scan_bytes{strategy=…}, and melt_hybrid_fallbacks_total{reason=…}. Per-query traces in structured logs carry the full plan summary, joinable on query_id and query_hash.",
      },
    ],
  },

  // ─────────────────────────  WAREHOUSE  ─────────────────────────
  {
    slug: "warehouse-routing",
    title: "Warehouse Routing",
    category: "Warehouse",
    status: "alpha",
    iconName: "loadBalance",
    shortDescription:
      "Land queries on already-warm warehouses. Stop paying for cold starts.",
    tagline:
      "When a query has to hit Snowflake, send it to a warehouse that's already warm. Melt watches the fleet in real time and lands each statement on the cheapest warm cluster that can run it — no Enterprise-tier multi-cluster, no cold-start tax, no per-tenant pinning spreadsheet.",
    metaTitle: "Warehouse Routing (Alpha) — Melt",
    metaDescription:
      "An open-source proxy that routes Snowflake queries across your existing warehouses to land them on the warmest cluster with capacity. Reclaims the 60-second cold-start tax.",
    problemFraming: [
      "Snowflake bills warehouses with a hard 60-second minimum every time they resume. That's not a documentation footnote — it's the dominant cost line item on any account where compute pulses. A query that runs in 800 ms against a cold Large warehouse still costs the full minute. Spread across an agent fleet firing thousands of small statements a day, the cold-start bill alone is often a five-figure monthly line.",
      "The default behaviour makes this worse. AUTO_SUSPEND is usually set to 60–600 seconds because keeping warehouses warm-and-idle burns credits too. So warehouses cycle: wake up, run for 60 seconds even if the query took 800 ms, suspend, and wake up again the next time anything lands on them. The AUTO_RESUME UX is excellent — drivers don't see it — but every resume rings the cash register.",
      "This used to be tolerable because human-driven workloads bunch. Analysts open a dashboard at 9am, dbt runs at 3am — within those windows a single warehouse stays warm and the 60-second floor is amortised. Agent-driven SQL breaks that pattern. Copilots, research agents, and autonomous pipelines fire small queries at machine cadence, often across many warehouses, and each individual query is too cheap to amortise its own cold start. The cold-start tax becomes the workload.",
      "The escape hatch Snowflake offers is multi-cluster warehouses: a single warehouse logical object that fans out across N physical clusters under load. It's a real fix — but it's gated to the Enterprise tier, which carries roughly a 50% credit premium. You pay more per credit so that Snowflake can do something a proxy could do for free: route the next query to the cluster that's already warm.",
    ],
    alphaPromise: [
      {
        title: "Real-time warm-warehouse detection",
        description:
          "The proxy already terminates every Snowflake session and observes every passthrough query end-to-end. Warehouse Routing extends that to a per-warehouse warmth ledger: last-statement timestamp, in-flight count, recent queue depth, last-known size. State is updated synchronously off the wire — no INFORMATION_SCHEMA polling, no SHOW WAREHOUSES cron, no five-minute lag.",
      },
      {
        title: "Concurrency-aware placement, not just \"warmest\"",
        description:
          "The naive policy — always route to the most-recently-used warehouse — collapses a fleet into one warehouse that queues. The router scores each candidate against warmth recency, in-flight count vs. size-derived concurrency cap, queue depth, and hint. A warm warehouse already at its concurrency budget loses to a slightly older warm one with headroom.",
      },
      {
        title: "Per-statement override via SQL hint",
        description:
          "Routing is a heuristic; we'd rather give operators an out than ship a black box. A leading /*+ MELT_WAREHOUSE('TRANSFORM_WH') */ comment pins the query to a named warehouse and bypasses the router. Same syntax that exists for the existing Lake/Snowflake routing override, extended to warehouse selection.",
      },
      {
        title: "Multi-cluster fan-out on the Standard tier",
        description:
          "Define a warehouse pool in melt.toml (e.g. READS_POOL = [\"READS_WH_A\", \"READS_WH_B\", \"READS_WH_C\"]). Drivers connecting with warehouse=READS_POOL get their statements distributed across the underlying physical warehouses by the same routing logic — the same shape Enterprise multi-cluster gives you, on Standard pricing, with the warehouse list visible to the operator.",
      },
      {
        title: "Concurrency-budget enforcement",
        description:
          "When a warehouse hits its size-derived concurrency budget, new statements are routed elsewhere or queued in the proxy with a fast-fail timeout — not handed to Snowflake to sit in its queue while the meter runs. The budget tracks per-warehouse, per-pool, and globally.",
      },
      {
        title: "Honest probes, not synthetic load",
        description:
          "When a warehouse goes cold (or the proxy starts cold), warmth is rediscovered by piggybacking on real driver traffic, not by issuing SELECT 1 keep-alives. We pay the cold-start cost on the first real query that needs Snowflake, not on a synthetic probe that bills 60 seconds for nothing.",
      },
    ],
    architecture: {
      description:
        "Routing is a synchronous in-process decision in the proxy — no separate service, no Redis, no network hop on the hot path. State is per-process; multiple proxy replicas converge via shared session affinity plus a lightweight Postgres-backed ledger sync for cross-replica warmth signals.",
      diagram: `driver ──▶ melt-proxy ──▶ Query Router (Lake / Snowflake)
                                          │
                                Snowflake-bound query
                                          ▼
                          ┌──────────────────────────────┐
                          │  Warehouse Router            │
                          │   ├ Warmth ledger (per WH)   │
                          │   ├ Concurrency budgets      │
                          │   ├ Pool resolution          │
                          │   └ Hint parser              │
                          └──────────────┬───────────────┘
                                         │ chosen warehouse
                                         ▼
                          ┌──────────────────────────────┐
                          │  Session swap (rewrite       │
                          │  warehouse in passthrough)   │
                          └──────────────┬───────────────┘
                                         ▼
                                   Snowflake
                                         │ result
                                         ▼
                          Warmth ledger update
                          (rt + bytes_scanned + exit code)`,
    },
    bets: [
      {
        title: "Warmth tracking from the wire, not from INFORMATION_SCHEMA",
        description:
          "Snowflake's account-usage views lag five to forty-five minutes — useful for billing, useless for routing. The proxy already sees every query begin and end. We trust that signal as the source of truth; the only cost is per-process state.",
      },
      {
        title: "Routing decisions live in the proxy, not in a sidecar",
        description:
          "Yuki's published architecture splits the proxy from a separate decision engine over Redis. That's flexible, but it adds a network hop and a state-sync problem the open-source community will end up debugging. We co-locate the decision with the wire path. The routing function is a pure function of (warmth ledger, budgets, hint) — extractable later if needed.",
      },
      {
        title: "We won't touch warehouse lifecycle in this milestone",
        description:
          "Suspending warehouses, resizing them, creating them on demand — that's all Warehouse Management, which is its own surface area. Warehouse Routing is strictly: given the warehouses that exist, pick the right one. Doing both at once would force a deeper integration with Snowflake's admin API that's out of scope for the alpha.",
      },
      {
        title: "No machine learning for the v1 router",
        description:
          "A scoring function with a half-dozen knobs (warmth recency, concurrency headroom, queue depth, size match, hint, pool membership) is well-conditioned, observable, and easy to audit. If we end up needing learned weights, we'll add them; we won't ship them as the default and ask operators to trust a model they can't explain.",
      },
    ],
    workloads: [
      {
        title: "Mid-day BI traffic that pulses every minute",
        description:
          "Looker / Sigma / Hex dashboards refresh on a tile cadence; each refresh fans out to ten or twenty separate queries. Default routing today triggers a RESUME storm if the warehouse has just suspended — every refresh costs an extra 60 seconds. Warehouse Routing keeps successive refreshes on whichever warehouse stayed warmest in the pool.",
      },
      {
        title: "Agent fleets firing across many warehouses",
        description:
          "A multi-tenant SaaS gives each tenant its own warehouse for isolation. The agent fleet sends queries unevenly: tenant A is hot for an hour, then quiet; tenant B picks up. Warehouse Routing pools the eligible tenants' warehouses (where policy permits) and lands queries on whichever is currently serving traffic.",
      },
      {
        title: "Ad-hoc analyst queries from notebooks",
        description:
          "Jupyter / Hex / Cursor users fire queries with thinking pauses between them, exactly the pattern that fights AUTO_SUSPEND. The pool stays warm because ten analysts share three warehouses; without the router, each analyst's pinned warehouse cycles independently.",
      },
      {
        title: "dbt runs that overlap with BI traffic",
        description:
          "A dbt build kicked off mid-day used to either contend with BI on the same warehouse (queue) or trigger a fresh cold start on its own (tax). Routing across a pool with concurrency budgets keeps the build fast without lighting up a third warehouse.",
      },
      {
        title: "Multi-tenant SaaS with predictable pinning",
        description:
          "Some tenants need their own warehouse for compliance. Per-statement hint (/*+ MELT_WAREHOUSE('TENANT_42_WH') */) preserves pinning where required, and the router covers everyone else.",
      },
    ],
    comparisons: [
      {
        name: "Yuki Data",
        description:
          "Closest direct comp and has been at this longer. Closed-source SaaS, multi-platform (Snowflake + BigQuery), separate decision engine over Redis. Differences that matter for melt's audience: open source, decisions on the same proxy that already routes queries to DuckDB, and a routing function that's a single Rust call instead of a Redis round-trip.",
      },
      {
        name: "Snowflake Multi-Cluster Warehouses (Enterprise)",
        description:
          "Native solution to cross-cluster fan-out. Costs ~50% more per credit than the Standard tier — you pay the premium so Snowflake can do what a proxy could do for free. The bet: a Standard-tier account plus melt costs less than an Enterprise-tier account doing the same thing.",
      },
      {
        name: "Status quo (auto-suspend / auto-resume)",
        description:
          "Free and built-in. But reactive, not proactive — every cold start still pays the 60-second floor; suspend timing is per-warehouse with no cross-fleet awareness. Useful as a backstop on top of warehouse routing, not a replacement for it.",
      },
    ],
    audience: [
      {
        title: "Data platform team on Snowflake Standard with bursty ad-hoc traffic",
        description:
          "5–20 person team, $20K–$200K/month Snowflake bill, a handful of warehouses sized M to XL, AUTO_SUSPEND at 60–300 seconds because every credit hurts. They've looked at upgrading to Enterprise for multi-cluster and the math didn't work — the 50% premium swallows most of the queueing they'd avoid.",
      },
      {
        title: "Multi-tenant SaaS with a warehouse-per-tenant pattern",
        description:
          "Per-tenant isolation is contractual, but most tenants are quiet most of the time; the cold-start bill on the long tail is several times the bill on the active tenants. Warehouse Routing pools eligible tenants and preserves hard pinning for the ones that need it.",
      },
      {
        title: "Anyone running an agent fleet against Snowflake",
        description:
          "Coding agents, research agents, autonomous pipelines — workloads that fire small queries at machine cadence with no human-driven amortisation window. This is the workload that exposes the cold-start tax most starkly.",
      },
    ],
    faq: [
      {
        question: "When will this be available?",
        answer:
          "Alpha. Design and prototyping are active, no production rollout yet. The warmth-ledger plumbing piggybacks on the existing statement_complete instrumentation, and the session-swap path is an extension of the existing passthrough rewrite — both surface area we already touch. Expect a flag-gated implementation in a near-term release with a public design RFC ahead of GA.",
      },
      {
        question: "Will it work on Snowflake Standard tier?",
        answer:
          "Yes — that's the point. Cross-warehouse fan-out is the headline use case for Standard-tier accounts, because Snowflake's native answer (multi-cluster warehouses) is gated to Enterprise. Standard-tier customers get pool-based routing without paying the Enterprise per-credit premium.",
      },
      {
        question: "How do you detect warmth without polluting INFORMATION_SCHEMA?",
        answer:
          "The proxy already terminates every Snowflake session and observes every query end-to-end. Warmth is updated from that signal — last-statement-end timestamp per warehouse — not from SHOW WAREHOUSES or INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY (which lag and cost credits to query). No synthetic probes.",
      },
      {
        question: "What happens if all warehouses are cold?",
        answer:
          "The router pays the cold start exactly once: the chosen warehouse resumes, runs the query, and the warmth ledger captures the new state so subsequent queries pile onto it instead of cold-starting peers. The deterministic fallback when the warmth ledger is empty is \"use the warehouse the driver named at login\" — same behaviour as today.",
      },
      {
        question: "Will I lose Query Routing to DuckDB if I enable Warehouse Routing?",
        answer:
          "No. Query Routing decides whether a statement runs on the lake or on Snowflake; Warehouse Routing decides which Snowflake warehouse it lands on. They compose: a query the lake can't safely answer goes to Snowflake passthrough, and Warehouse Routing then picks the warmest eligible warehouse for it.",
      },
    ],
  },

  {
    slug: "warehouse-management",
    title: "Warehouse Management",
    category: "Warehouse",
    status: "alpha",
    iconName: "warehouse",
    shortDescription:
      "Right-size warehouses, manage suspends, and budget by team.",
    tagline:
      "The control plane for the Snowflake credits you do still spend. Melt right-sizes warehouses, schedules suspends and resumes around your team's actual hours, and enforces per-team budgets at admit-time — from the same proxy that already sees every statement.",
    metaTitle: "Warehouse Management (Alpha) — Melt",
    metaDescription:
      "Continuous warehouse right-sizing, schedule-aware suspends, per-team budgets enforced before queries run, and credit anomaly alerts. Open source, in your VPC.",
    problemFraming: [
      "Warehouse management on Snowflake is, for almost every team we talk to, a quarterly cleanup project disguised as a permanent process. Right-sizing happens when somebody on the data team carves out a Friday to spelunk in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY, sort by bytes_spilled_to_remote_storage, eyeball the top offenders, and ALTER three or four warehouses up or down. By the next quarter the workload mix has drifted, an ML team has started running training on the shared LARGE, and the spreadsheet is wrong again.",
      "Auto-suspend, the one knob Snowflake ships in the box, is reactive and blunt. You pick a single idle timeout per warehouse and live with the consequences either way. Set it short and your analysts pay cold-start tax all morning; set it long and the warehouse idles through lunch, the standup, and the all-hands. Nothing in the box says \"this warehouse should suspend at 6pm on weekdays and stay down until 8am, except when the on-call dbt job runs.\"",
      "Budgets and chargeback are worse. Snowflake's Resource Monitors are warehouse-scoped, can only suspend or notify (not throttle, not warn-then-allow, not split by user), don't see serverless or Cortex spend, and assign exactly one monitor per warehouse. Custom Budgets added tag-based tracking but only emit notifications — they don't block, throttle, or route. So FinOps reality on most data teams is a dbt model, a Looker dashboard nobody opens until invoice day, and a Slack thread where someone gets blamed for a 4× spike that already happened.",
      "The escape hatch most teams reach for is SELECT (now joining DoiT). SELECT is genuinely good at the observability half. But it's an observer: it ingests ACCOUNT_USAGE, surfaces what you should do, and leaves the doing to you. Pricing is 4% of Snowflake spend with a $1,499/mo minimum — which scales with the bill you're trying to cut.",
    ],
    alphaPromise: [
      {
        title: "Continuous right-sizing",
        description:
          "Melt watches a rolling window of statements through the proxy — bytes scanned, runtime, spill, queue depth — and sets each warehouse's size for the next hour against the workload mix it actually saw. Right-sizing is per-warehouse-per-hour, decision is logged, override is one line in melt.toml, resize is a single ALTER WAREHOUSE.",
      },
      {
        title: "Schedule-aware suspend and resume",
        description:
          "Auto-suspend stays on as a safety net, but Melt layers a calendar over it: pre-warm the BI warehouse at 7:55am Mon–Fri so the first dashboard isn't cold; suspend the dbt warehouse at 6pm and leave it down through the weekend. Schedules are TOML, hot-reloaded, and override-able by Slack command.",
      },
      {
        title: "Per-team and per-workload budgets, enforced at admit-time",
        description:
          "Melt classifies every statement and tags it by team / role / query-tag / warehouse. Budgets are TOML — team = \"growth\", monthly_credits = 5000, on_breach = \"warn\" or \"throttle\" or \"block\". Enforcement happens before the warehouse spins up. Soft caps slow the offender's queue; hard caps reject with a SQL-shaped error and a runbook link.",
      },
      {
        title: "Anomaly detection on credits, with Slack and PagerDuty",
        description:
          "A per-warehouse, per-team, per-workload forecast runs on the metrics the proxy is already exporting. Crosses the band, fires a webhook. Out of the box: Slack message with the offending team / pattern / warehouse, PagerDuty incident on high-severity, and a melt route --since=15m command in the Slack response.",
      },
      {
        title: "Cost attribution by user, role, query tag, and warehouse",
        description:
          "Because every statement crosses the proxy with its session context attached, attribution doesn't depend on QUERY_HISTORY lag (45 minutes, up to 3 hours) or on dbt-package conventions. Melt produces a near-real-time credits_used stream labeled by user, role, warehouse, query_tag, and any custom session label.",
      },
      {
        title: "Warehouse personalities",
        description:
          "Define warehouses by intent: cheap_and_slow (small, aggressive auto-suspend), interactive (right-sized for p95 latency, never suspends during work hours), burst (resizes up for bursts of remote-table joins, snaps back down). Personalities compose with Warehouse Routing — the per-statement router knows which personality a query qualifies for.",
      },
    ],
    architecture: {
      description:
        "The decision engine reuses the Postgres-backed control plane (crates/melt-control/) that today stores sync state and policy markers, the Snowflake HTTP client (crates/melt-snowflake/) that today carries sync's outbound calls, and the credit math that powers the local audit. None of it is \"let's build a new ingestion pipeline.\"",
      diagram: `driver / dbt / agent / BI ──▶ melt-proxy
                                  parses, classifies, tags every statement
                                          │
                       admit-time         │ statement metrics
                       budget gate        ▼
                                  ┌─────────────────────┐
                                  │ melt-metrics        │
                                  │ per-statement       │
                                  │ credits_used,       │
                                  │ user/role/tag/wh    │
                                  └──────────┬──────────┘
                                             ▼
                          ┌──────────────────────────────────┐
                          │  melt-control (control plane)    │
                          │   • right-sizing forecast        │
                          │   • schedule evaluator           │
                          │   • budget ledger                │
                          │   • anomaly detector             │
                          └──────┬─────────────┬─────────────┘
              ALTER WH            │             │  webhook fanout
              SUSPEND/RESUME      ▼             ▼
                        ┌────────────┐  ┌──────────────────┐
                        │ Snowflake  │  │ Slack /          │
                        │ REST API   │  │ PagerDuty / etc  │
                        └────────────┘  └──────────────────┘`,
    },
    bets: [
      {
        title: "Decisions live in the proxy, not in an observer that ingests QUERY_HISTORY",
        description:
          "Anyone who's tried to build this from QUERY_HISTORY has felt the latency (45 min to 3 hours) and the attribution gaps (session context is gone). Melt's bet: the proxy is the right control point. It sees every statement live with full session context, can label and bill credits in-flight, and can act on a decision the same second it makes it.",
      },
      {
        title: "Right-sizing is per-warehouse-per-hour, not per-query",
        description:
          "Per-query right-sizing is what Warehouse Routing does, and it's a different problem. Per-warehouse-per-hour is the right granularity for the control plane: workload mix is stable enough at the hour scale, the resize cost (a single ALTER) is negligible, and operators can reason about it. Going finer is a science project; going coarser is what spreadsheets already do.",
      },
      {
        title: "Budgets enforce at proxy admit-time, not after the credit has burned",
        description:
          "Snowflake Resource Monitors and Custom Budgets are notify-or-suspend-after-the-fact. By the time the alert fires, the credits are gone. Because Melt is in front of the warehouse, it can refuse, throttle, or warn the next statement from a team that's blown its budget — the same way an admission controller does on Kubernetes. That's the only enforcement model that closes the loop.",
      },
      {
        title: "Open source, so your FinOps team can audit the decision logic",
        description:
          "The auto-resize forecast, the budget ledger, and the anomaly detector are all in crates/melt-control/, Apache-2.0. When the decision engine resizes a warehouse, the reasoning is in source, not in a vendor dashboard. This matters for a discipline whose entire value depends on practitioners trusting the numbers.",
      },
    ],
    workloads: [
      {
        title: "12 warehouses, no full-time FinOps person",
        description:
          "A 4–15-person data platform team owning Snowflake spend, no dedicated FinOps practitioner, currently choosing between paying SELECT 4% of spend or maintaining their own dbt-snowflake-monitoring fork. Melt drops in as the proxy and gives them the enforcement loop without a second vendor.",
      },
      {
        title: "Multi-tenant SaaS doing per-customer cost attribution",
        description:
          "A platform that runs customer workloads against shared Snowflake compute and needs to chargeback by customer. Today: a query_tag convention nobody enforces and a dbt model that approximates attribution from QUERY_HISTORY. With Melt: every statement carries the customer's session label, and credits_used{customer=...} flows directly into the per-customer billing pipeline.",
      },
      {
        title: "Bursty ML training that needs LARGE for 2h then nothing",
        description:
          "A research team that lights up a warehouse for a tuning sweep, walks away, and forgets it for the weekend. A schedule entry says personality = \"burst\", auto_suspend_after_idle = \"30m\", hard_suspend_at = \"Friday 18:00\". The training run gets the size it needs; the warehouse is guaranteed off by Friday evening regardless of who started it.",
      },
      {
        title: "Resumed from a Slack-bot last night and forgot to suspend",
        description:
          "The most common failure mode in every account we've looked at. Someone hits /warehouse resume WH_ANALYTICS at 11pm to debug something, finds the answer, closes the laptop. Melt's idle claw-back recognizes \"no human-issued statement in 90 minutes\" — task-driven keep-alive doesn't reset the timer — and suspends.",
      },
    ],
    comparisons: [
      {
        name: "SELECT / DoiT PerfectScale",
        description:
          "4% of Snowflake spend, $1,499/mo min. Mature observability, automated savings recommendations, usage groups, monitors, AI copilot, dbt-package origin. Observer-only — surfaces what to do, doesn't enforce. Can't refuse a query at admit time. Pricing scales with the spend you're cutting.",
      },
      {
        name: "Snowflake Resource Monitors",
        description:
          "Free, native. Hard credit caps per warehouse, suspend/notify. One monitor per warehouse, no team attribution, no serverless/Cortex coverage, only acts after credits burn. Useful as a backstop, not a control plane.",
      },
      {
        name: "Snowflake Custom Budgets",
        description:
          "Free, native. Tag-based monthly limits, broader coverage than monitors. Notification-only — no enforcement, no proxy-level admit-time gating.",
      },
      {
        name: "dbt-snowflake-monitoring + custom models",
        description:
          "Free, MIT, open-sourced by SELECT. Daily-spend models, cost attribution, tagged dbt query metadata. DIY plumbing, observation-only, ≥45-min QUERY_HISTORY lag, no enforcement loop.",
      },
    ],
    audience: [
      {
        title: "The 4–15-person data platform team",
        description:
          "Owns Snowflake spend at $200k–$2M/yr scale. Has tried dbt-snowflake-monitoring, has a partial Looker dashboard, has been pitched SELECT and is wincing at the % pricing. Doesn't have a dedicated FinOps person and doesn't want to hire one. Wants enforcement, not another dashboard to ignore.",
      },
      {
        title: "The multi-tenant SaaS data team",
        description:
          "Bills customers based on Snowflake usage. Today's attribution leans on query_tag discipline customers half-honor and dbt models that lag the truth. Wants real-time per-tenant credits with hard cutoffs when a customer hits their tier limit, not an end-of-month surprise.",
      },
      {
        title: "The platform engineer adding FinOps to an agent rollout",
        description:
          "Just shipped agentic SQL into production, watched QUERY_HISTORY 5×, and got a Slack message from finance. Already evaluating Melt for query routing because that's where the bill curve bent. Adding warehouse management on top is the same proxy, the same melt.toml, the same VPC.",
      },
    ],
    faq: [
      {
        question: "How is this different from SELECT?",
        answer:
          "SELECT (now joining DoiT) is an observer — it ingests ACCOUNT_USAGE, surfaces recommendations, and leaves enforcement to your team. Melt is in the data path: it sees every statement live, can refuse or throttle a query at admit time, and attributes credits in-flight without a QUERY_HISTORY join. SELECT is more mature on the observability surface today; Melt closes the loop the other product can't, from the same proxy you're already running. We won't tell you Melt replaces SELECT in alpha — for many teams the pragmatic answer is \"use both\" until parity catches up.",
      },
      {
        question: "Will I save more vs. Snowflake's native Resource Monitors?",
        answer:
          "Resource Monitors only act after credits are already burned — they suspend or notify, but the spend has happened. They're warehouse-scoped only (one monitor per warehouse, no team attribution). Melt's budgets enforce at admit-time before the query runs, label spend by user/role/tag in real time, and cover workloads Resource Monitors can't see.",
      },
      {
        question: "Can I run this without giving up query routing?",
        answer:
          "Yes. Warehouse management is a separate [control] block in melt.toml and works whether routing is on, off, in passthrough mode, or running allowlist-only. If you only want the control plane and want every query to passthrough to Snowflake unchanged, set [router].mode = \"passthrough\" and you have a Snowflake control plane that doesn't make routing decisions.",
      },
      {
        question: "How does cost attribution work — by user, by tag, by warehouse?",
        answer:
          "All four. Every statement that crosses the proxy is labeled by user, role, warehouse, and query_tag (whatever the session set), and credits are computed in real-time from the same credits_per_hour × execution_time math the melt audit command uses today. Output flows into /metrics, the budget ledger, and webhook streams.",
      },
      {
        question: "What's the alpha scope — what works vs. what's planned?",
        answer:
          "Alpha will start with: schedule-driven suspend/resume (TOML-defined, hot-reloaded), per-team / per-warehouse budgets with soft (warn) and hard (block) caps enforced at proxy admit-time, real-time per-statement credit attribution into /metrics, and Slack + PagerDuty webhook fanout. Continuous right-sizing and warehouse personalities will follow; the design is documented but the auto-resize forecast needs more design-partner workload data before we ship it on by default.",
      },
      {
        question: "What grants does this need?",
        answer:
          "On top of the existing service-auth grants, the control plane needs OPERATE on the warehouses it manages (so it can ALTER WAREHOUSE, SUSPEND, RESUME) and READ on the relevant ACCOUNT_USAGE views for budget-ledger backfill on cold start. Everything Melt does live runs on session-scoped data the proxy already sees.",
      },
    ],
  },

  // ──────────────────────────  VIEWS  ──────────────────────────
  {
    slug: "materialized-views",
    title: "Materialized Views",
    category: "Views",
    status: "alpha",
    iconName: "stack",
    shortDescription:
      "Auto-detect hot query patterns and materialize them in your lake.",
    tagline:
      "Melt watches your routed traffic, finds the queries that fire over and over, and quietly materializes them as cached tables in your lake — so the next dashboard refresh, agent call, or embedded-analytics tile lands in milliseconds against a file you already own.",
    metaTitle: "Materialized Views (Alpha) — Melt",
    metaDescription:
      "Melt detects repeated query patterns from real Snowflake traffic and materializes them to Iceberg or DuckLake automatically. No CREATE MATERIALIZED VIEW. No Enterprise tier. No dbt model file.",
    problemFraming: [
      "Every analytics workload has a long tail of queries that look almost identical. The 9am dashboard refresh hits the same count(*) WHERE day > current_date - 7 GROUP BY 1 it ran yesterday. The customer-facing tile asks for top-N by month, parameterized only by tenant_id, ten thousand times a day. The agent fleet keeps rephrasing events in the last 24h grouped by source. Snowflake recomputes the lot, every time, with metered credits.",
      "Snowflake's result cache is supposed to save you — except it only matches exact query text and is invalidated by any mutation to the underlying tables. A WHERE created_at > $now filter, a session timezone shift, or a Looker dashboard appending LIMIT 5000 defeats it. For the parameterized BI and product-analytics workloads that dominate real warehouses, the result cache hit rate is close to zero.",
      "Snowflake's Materialized Views are the next answer up the stack — and they are conspicuously thin. They're an Enterprise-tier SKU, the SQL surface is heavily restricted (no joins, no UNION, no HAVING, no window functions, no QUALIFY, no UDFs), and on a large fact table the background maintenance can quietly cost more than the queries they were built to accelerate.",
      "Melt sits in the path of every query. We can already see the patterns. We can do something about them.",
    ],
    alphaPromise: [
      {
        title: "Pattern detection from observed traffic",
        description:
          "Every routed statement is parsed and reduced to a fingerprint: AST shape minus literals, with parameters and timezone-relative bounds normalized. A rolling pattern detector groups fingerprint hits across sessions, users, and connection strings, so the same dashboard counts as one pattern even when ten Looker users hit it.",
      },
      {
        title: "Recommend → preview → opt-in",
        description:
          "Melt never materializes silently. Patterns crossing a frequency × scan-bytes threshold show up as MV recommendations in the control plane, with the candidate view body, the projected storage cost, the projected refresh cost, and the estimated speedup against historical scans. You opt in per pattern.",
      },
      {
        title: "Materialize to Iceberg or DuckLake — your storage, your format",
        description:
          "MVs land in the same lake the rest of melt syncs to, in your S3 / R2 / GCS bucket, in the same catalog your other tools already read. No Snowflake Enterprise tier required and no proprietary cache layer to lift out later.",
      },
      {
        title: "Cost-aware, not schedule-driven, refresh",
        description:
          "Each MV gets a freshness SLO (e.g. \"≤ 60s stale\"). The refresh planner balances query_frequency × freshness_SLO against refresh_cost × storage_cost and only recomputes when a serve would otherwise miss SLO. Hot MVs refresh on every CDC tick; cold ones lazily refresh on demand.",
      },
      {
        title: "Transparent serve",
        description:
          "When a query whose fingerprint matches an active MV arrives, the router rewrites it against the materialized table and answers from the lake — same SQL went in, same result set comes out. Clients (psql, JDBC, dbt, Looker, Sigma, agents) never know they hit a cache.",
      },
      {
        title: "Per-pattern observability and eviction",
        description:
          "Each MV exports melt_mv_serves_total{pattern,freshness}, melt_mv_hit_rate, melt_mv_storage_bytes, melt_mv_refresh_cost_credits, and per-pattern p50/p95 served latency. An MV that hasn't been served in N hours, or whose storage cost has overtaken the warehouse credits it saves, is auto-marked for retirement.",
      },
    ],
    architecture: {
      description:
        "The fingerprinter, pattern detector, and recommender are the new pieces. Everything downstream (storage, sync, route, serve) is already in production for melt's normal lake-routing path — MVs reuse the catalog, the writer pools, and the router decision plane.",
      diagram: `Routed query stream
        │  AST + extracted TableRefs (already produced)
        ▼
  ┌──────────────┐
  │ Fingerprinter│ — strip literals, normalize parameters, hash AST shape
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ Pattern      │ — rolling counts, frequency × scan-bytes
  │ detector     │
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ MV           │ — candidate view body + projected cost
  │ recommender  │
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │ Operator     │ — melt mv accept <pattern_id>
  │ opt-in       │
  └──────┬───────┘
         ▼
  ┌──────────────┐    ┌──────────────┐
  │ Materializer │ ─▶ │ Iceberg /    │
  └──────────────┘    │ DuckLake     │
                      └──────┬───────┘
                             ▼
                      ┌──────────────┐
                      │ Refresh      │ — driven by CDC + SLO
                      │ planner      │
                      └──────┬───────┘
                             ▼
                      ┌──────────────┐
                      │ Matcher      │ — fingerprint → rewrite if active
                      └──────┬───────┘
                             ▼
                      Serve from cache (DuckDB over the MV)`,
    },
    bets: [
      {
        title: "Pattern detection beats user-declared",
        description:
          "The MVs that should exist in any non-trivial workload outnumber the ones a human will ever hand-write. The query log already knows which they are; we're going to surface them instead of waiting for someone to file a ticket.",
      },
      {
        title: "Materialize in your lake, not in Snowflake",
        description:
          "Iceberg / DuckLake on object storage is an order of magnitude cheaper per terabyte than Snowflake-resident MVs, doesn't need an Enterprise SKU, and is portable — when you eventually rip melt out, the MVs are still queryable tables you own.",
      },
      {
        title: "Recommend → opt-in, not auto-create",
        description:
          "Storage and refresh costs are bounded only when a human decides which patterns are worth materializing. Magical auto-creation is a great demo and a terrible production posture; we're explicitly rejecting it.",
      },
      {
        title: "Refresh is cost-aware, not schedule-driven",
        description:
          "Cron-based refresh is the wrong abstraction. A pattern queried 1000×/day at 60s SLO needs different refresh behavior than a pattern queried 5×/hour at 1h SLO. Refresh frequency is an output of query_frequency × freshness_SLO ÷ refresh_cost, not a config knob.",
      },
      {
        title: "Serve from cache happens at the proxy",
        description:
          "The same Rust hot path that already classifies and routes every query is where MV matching lives. Clients don't pick a cache, don't get a different driver, don't change their SQL — the cache is invisible by design.",
      },
    ],
    workloads: [
      {
        title: "The 60-second dashboard",
        description:
          "A BI tool refreshes a 7-day-trailing aggregate every 60 seconds for thirty open browser tabs. Today: thirty cold scans of the daily partitions every minute. With Materialized Views: one MV refreshed once per CDC tick, served from a 50 MB Iceberg snapshot. Bill drops by ~99% on that pattern alone.",
      },
      {
        title: "Embedded analytics tiles",
        description:
          "A SaaS product renders a top 10 customers by month tile inside the dashboard of every account — WHERE tenant_id = $1, hit ~1000×/day per tenant, parameterized so the result cache never hits. Melt fingerprints them as one pattern and materializes a tenant-partitioned MV.",
      },
      {
        title: "Agent fleets rephrasing the same question",
        description:
          "An LLM-driven analytics agent keeps phrasing variations of how many events in the last 24h, grouped by source — same question, never the same SQL. Fingerprinting collapses the variants into one pattern; the MV serves all of them.",
      },
      {
        title: "QUALIFY / ROLLUP / window-heavy SQL",
        description:
          "Snowflake's own MVs legally cannot materialize these. Because melt materializes by running the query and storing the result as a normal Iceberg table, there's no SQL-surface restriction — anything that returns rows can be cached.",
      },
      {
        title: "Schema-evolution-stable lookups",
        description:
          "Reference tables joined into hundreds of queries (countries, currencies, feature_flags) get materialized as denormalized lookup MVs and join-pushed into queries that touch them, eliminating a Snowflake roundtrip per lookup.",
      },
    ],
    comparisons: [
      {
        name: "Snowflake Materialized Views (Enterprise tier)",
        description:
          "User-declared, restricted SQL surface (no joins, UNION, window fns, QUALIFY, UDFs), expensive on big tables, locked into Snowflake compute. Enterprise tier required.",
      },
      {
        name: "Snowflake Dynamic Tables",
        description:
          "More flexible than MVs, declarative refresh with TARGET_LAG. Still user-declared, still Snowflake-only, still incur warehouse credits to maintain even when nobody reads the result.",
      },
      {
        name: "dbt materialized models",
        description:
          "Run on dbt's schedule, not query-driven. Requires manual model definition. Works well for known workloads, but the long tail of patterns that should be materialized stays invisible until someone writes a model file.",
      },
      {
        name: "Snowflake result cache",
        description:
          "Free, native. But matches only exact query text and is defeated by any parameter, timezone shift, or session difference. Hit rate on parameterized BI workloads is near-zero.",
      },
    ],
    audience: [
      {
        title: "Embedded analytics and product-facing dashboards",
        description:
          "Teams where the same shape of query repeats thousands of times a day, parameterized just enough that Snowflake's result cache never hits. The economics break down fastest here — every customer, every refresh, every minute.",
      },
      {
        title: "Snowflake Standard-tier accounts",
        description:
          "Can't access native MVs at all and don't want to upgrade an entire account to Enterprise just to accelerate three dashboards.",
      },
      {
        title: "Analytics-engineering teams drowning in dbt incremental models",
        description:
          "Models that exist only because someone noticed a slow query — and want the next ten of those to materialize themselves before anyone notices.",
      },
      {
        title: "Agent / LLM analytics platforms",
        description:
          "Where query text is non-deterministic but query intent isn't — fingerprinting catches the pattern even when the SQL differs.",
      },
    ],
    faq: [
      {
        question: "How does it match queries to MVs — exact text, or shape?",
        answer:
          "Shape. Each statement is parsed and reduced to a fingerprint that strips literals, normalizes parameters, and canonicalizes things like now() - interval '7 day' vs current_date - 7. Two queries that differ only in tenant_id or WHERE day > <today> are one pattern. That's the entire reason this beats Snowflake's result cache for real workloads.",
      },
      {
        question: "What freshness guarantees does it provide?",
        answer:
          "Per-MV. You set a freshness SLO when you accept a recommendation — \"serve up to 60 seconds stale,\" \"serve up to 5 minutes stale,\" etc. The refresh planner uses the same CDC stream that already keeps your synced lake tables fresh. If the planner can't meet the SLO, the matcher falls back to running the original query through Snowflake — never serves a stale answer past its declared SLO.",
      },
      {
        question: "Where are MVs stored?",
        answer:
          "In your lake — the same Iceberg or DuckLake catalog the rest of melt syncs to, in your S3 / R2 / GCS bucket. They're plain Iceberg tables. You can read them from Spark, Trino, DuckDB, anything that speaks the format. If you ever uninstall melt, the MVs stay queryable.",
      },
      {
        question: "What if my data changes faster than the MV can refresh?",
        answer:
          "Two outcomes, both safe. If the MV is inside SLO, you're served from it. If the MV would be outside SLO (the CDC stream is lagging or the refresh hasn't completed), the matcher routes that query through to Snowflake instead. We never silently serve data older than what you asked for.",
      },
      {
        question: "How is this different from Snowflake's native MVs and Dynamic Tables?",
        answer:
          "Three things. (1) Workload-driven, not user-declared — melt sees which patterns matter and recommends them. (2) Stored in your lake, not in Snowflake's storage layer — cheaper, portable, no Enterprise tier. (3) No SQL-surface restriction — Snowflake MVs forbid joins, UNION, QUALIFY, window functions, UDFs; melt materializes anything that returns rows because we materialize the output, not the plan.",
      },
      {
        question: "Will my dashboards see stale data?",
        answer:
          "Only if you opted in to a freshness SLO that allows it. The default for new MVs is conservative; the matcher won't serve outside-SLO results. The full audit trail lives next to every other routing decision in /metrics and the melt route CLI — you can see, per query, whether it was served from an MV, which MV, and how stale that MV was at serve time.",
      },
    ],
  },

  {
    slug: "incremental-views",
    title: "Incremental Views",
    category: "Views",
    status: "alpha",
    iconName: "delta",
    shortDescription:
      "Refresh-as-you-go views maintained from CDC streams.",
    tagline:
      "Define a view once in dbt-style SQL. Melt maintains it incrementally off the same Snowflake CDC stream that keeps your lake fresh — sub-second freshness, queryable from any engine, no warehouse credits to keep it warm.",
    metaTitle: "Incremental Views (Alpha) — Melt",
    metaDescription:
      "CDC-fed incremental view maintenance built on top of the proxy: dbt-compatible models, sub-second freshness, per-view SLOs, materialized into Iceberg or DuckLake.",
    problemFraming: [
      "Most \"real-time\" analytics is a fiction held together by cron. The dashboard tile that says \"live active sessions\" is usually a dbt incremental model running every 15 minutes on a Snowflake task. The anomaly alert that pages on-call is a Streams + Tasks pipeline whose SCHEDULE is set to 5 MINUTE because the team got tired of the cloud-services bill at 1 MINUTE. The freshness gap between what users see and what's true in the warehouse is measured in coffee breaks.",
      "Snowflake's native answer is two-and-a-half products. Streams + Tasks are operationally heavy — tasks fail silently, pin a warehouse for the duration of every run, and the minimum schedule for a non-triggered task is one minute. Dynamic Tables are the newer, declarative answer: set TARGET_LAG = '1 minute' and Snowflake decides when to refresh. The freshness floor is one minute, the compute is your warehouse, and every refresh of every dependent dynamic table charges credits whether anyone queried the result or not.",
      "Teams who actually need streaming-grade freshness — embedded analytics, anomaly detection, real-time ML features — have been quietly migrating that derived state out of the warehouse entirely. Materialize and RisingWave are excellent at this. The catch is that they're separate databases. You operate them alongside Snowflake, you split your data model across two systems, you re-implement RBAC, and your single source of truth is now two sources of truth that diverge whenever the pipeline lags.",
      "Melt already runs the CDC pipeline. We pull change streams out of Snowflake to keep the lake fresh — that's how Lake-routed queries see committed data within seconds. Incremental Views asks the obvious question: if the same change stream is already flowing through the proxy, why are you paying a warehouse to recompute a GROUP BY user_id every minute?",
    ],
    alphaPromise: [
      {
        title: "dbt-compatible model files",
        description:
          "Drop a .sql file into your existing dbt project with {{ config(materialized='incremental_view') }}. {{ ref() }} and {{ source() }} resolve through melt's catalog. The model body is the SQL melt incrementally maintains — no DSL, no second copy of the warehouse data model.",
      },
      {
        title: "Strategy auto-selection",
        description:
          "Melt classifies the view body and picks an incremental plan: append-only for monotonic event streams, key-based upsert for dimensions and aggregates, delete+insert for windowed views with retraction, full recompute on schema change. The user can override per view; the default is what the SQL implies.",
      },
      {
        title: "CDC-fed off the existing pipeline",
        description:
          "The same CREATE STREAM we already create per synced base table feeds the operator graph. No new sources, no new connectors, no new IAM. A view over five Snowflake tables is five existing change streams plus one operator graph in the proxy.",
      },
      {
        title: "Per-view freshness SLOs",
        description:
          "freshness_slo = '5s' in the model config makes the SLO first-class: melt sizes the operator graph's compute budget, watermarks lag against the SLO, and exports melt_incremental_view_lag_seconds per view so you can alert on it.",
      },
      {
        title: "Configurable back-pressure",
        description:
          "When upstream commits exceed a view's compute budget, the view degrades against its SLO instead of crashing the proxy. Three policies: degrade_freshness (lag grows, queries still return), pause_view (mark stale, fall back to upstream Snowflake on read), shed_load (drop the view from the graph until commits subside).",
      },
      {
        title: "Materialized into Iceberg / DuckLake",
        description:
          "The view's output lands as a regular lake table at its FQN. Spark, Trino, DuckDB, Snowflake-with-an-Iceberg-catalog — anything that reads the catalog reads the view. No melt lock-in for the derived state.",
      },
    ],
    architecture: {
      description:
        "An incremental view is one operator graph per view, fed by the existing CDC reader, applied as atomic snapshot commits to an Iceberg or DuckLake table.",
      diagram: `Snowflake (source of truth — base tables + change tracking)
        │  CHANGES(...) — committed CDC, drained per tick
        ▼
  ┌──────────────────┐
  │  melt-sync       │  shared CDC reader (already runs for the lake)
  └────────┬─────────┘
           │ Arrow batches, (__row_id, __action, post-image)
           ▼
  ┌──────────────────┐
  │  Operator graph  │  per-view: filter → join → agg, maintained incrementally
  │  (per view)      │  state lives in DuckDB; arrangements are differential
  └────────┬─────────┘
           │ row-level diffs against current view state
           ▼
  ┌──────────────────┐
  │  Apply           │  delete-only → delete pre-images → insert post-images
  │  (3-step atomic) │  one transaction per CDC tick
  └────────┬─────────┘
           ▼
  ┌──────────────────┐
  │  Iceberg /       │  view materialized at its FQN, snapshot per commit
  │  DuckLake        │
  └────────┬─────────┘
           │ table-exists cache invalidates → router routes to lake
           ▼
  ┌──────────────────┐
  │  melt-proxy      │  serves the view to any Snowflake-wire client
  └──────────────────┘`,
    },
    bets: [
      {
        title: "dbt-compatible model files, not a new DSL",
        description:
          "Streaming systems lose every tutorial-page-1 fight against dbt run. The fix is to not have the fight: a melt incremental view is a dbt model with a different materialized= value. ref(), source(), tests, docs all keep working. The only new thing the user learns is the freshness SLO.",
      },
      {
        title: "Reuse the CDC pipeline melt already runs",
        description:
          "Sync is already pulling change streams out of Snowflake to keep the lake fresh. Incremental Views is a second consumer of the same byte stream — zero new infra, zero new credentials, zero new failure modes that aren't already paged on.",
      },
      {
        title: "Incremental compute in the proxy, not the warehouse",
        description:
          "Dynamic Tables charge warehouse credits to keep derived state fresh. Incremental Views maintains state on the proxy's CPU, against state that lives in DuckDB and lands in S3. The warehouse never spins up to maintain a view, and a row that nobody reads costs nothing to keep current except a few microseconds of operator-graph CPU.",
      },
      {
        title: "Per-view freshness SLOs are first-class",
        description:
          "Materialize defaults to \"as fresh as possible,\" which is correct for a streaming database and wrong for a fleet of views with different criticality. The SLO is the contract — melt sizes the operator graph against it and tells you immediately when a view can't hold it. The user picks the trade-off, not the system.",
      },
      {
        title: "Materialize into Iceberg / DuckLake, not into melt",
        description:
          "Derived state belongs in the open table format. Spark, Trino, DuckDB, and Snowflake-with-Iceberg-catalogs all read the views without melt in the loop. If you turn melt off tomorrow, the views are still queryable; they just stop refreshing.",
      },
    ],
    workloads: [
      {
        title: "Embedded analytics tiles",
        description:
          "Active sessions in the last hour, revenue today, alerts firing right now — the kind of tile a SaaS product ships in its in-app dashboard. Today they're either backed by a 5-minute Snowflake task (visible staleness) or a hand-rolled cache. Incremental Views maintains them sub-second off CDC.",
      },
      {
        title: "Anomaly-detection alerting",
        description:
          "A 30-minute Streams + Tasks pipeline that scores transactions against a rolling baseline. The 30-minute interval was chosen because anything tighter blew the cloud-services bill, not because 30 minutes was acceptable. The view runs continuously off CDC; alerts fire within seconds of the offending commit.",
      },
      {
        title: "Frequent-but-small dbt incremental models",
        description:
          "Models marked materialized='incremental' running every 15 minutes via Airflow because the upstream only changes a few hundred rows per minute. Convert to materialized='incremental_view', change nothing else, get sub-second freshness without the orchestrator and without a warehouse spin-up per run.",
      },
      {
        title: "Real-time ML features",
        description:
          "A user-segment feature view (\"user has placed 3+ orders in the last 24h\") currently produced by a Flink job consuming Kafka and writing to a feature store. With melt, the same view is a dbt model maintained off Snowflake CDC and materialized into Iceberg — the feature store reads it directly, and the data team owns the SQL.",
      },
      {
        title: "Dashboards that should be live but aren't",
        description:
          "Internal exec dashboards that quote KPIs against last night's snapshot. The freshness expectation was never written down because the warehouse cost of keeping it current was prohibitive. Incremental Views removes the warehouse from the equation.",
      },
    ],
    comparisons: [
      {
        name: "Snowflake Streams + Tasks",
        description:
          "Native to Snowflake. 1 minute schedule floor (scheduled), seconds (triggered). Hand-written merge SQL per task; brittle DAGs; pins a warehouse for every run.",
      },
      {
        name: "Snowflake Dynamic Tables",
        description:
          "Declarative refresh with TARGET_LAG. 1 minute freshness floor, Snowflake-only, charges warehouse credits per refresh whether queried or not. Closer to incremental views in shape but locked into Snowflake compute.",
      },
      {
        name: "Materialize / RisingWave",
        description:
          "Sub-second freshness, full SQL semantics over CDC. Excellent at this — but separate databases to operate alongside the warehouse. You re-implement RBAC, observability, on-call, and your single source of truth is now two.",
      },
      {
        name: "dbt incremental models",
        description:
          "The status quo. Whatever your scheduler runs at — typically 15 min to 1 hour. Warehouse credits per run. Works, but not real-time.",
      },
    ],
    audience: [
      {
        title: "Teams who want streaming freshness without a streaming database",
        description:
          "You already run Snowflake. You already run dbt. You don't want a second database with its own RBAC, its own observability, and its own on-call rotation just to keep one dashboard tile current.",
      },
      {
        title: "dbt users with incremental models that need to be more frequent than 15 min",
        description:
          "The model is already correct. The issue is that the scheduler can't run it fast enough without the warehouse bill becoming the line item the CFO asks about. Convert the materialization, keep the SQL.",
      },
      {
        title: "Embedded analytics product teams",
        description:
          "You ship live tiles to customers. You've been pricing them against Snowflake credits per refresh per customer per minute, and the math is bad. Move the maintenance into the proxy; let the warehouse charge for genuinely new compute, not for keeping an aggregate warm.",
      },
      {
        title: "Real-time ML feature teams currently on Flink or Spark Structured Streaming",
        description:
          "You'd rather own SQL than Java. You'd rather your features land in Iceberg than in a feature-store-shaped vendor lock-in.",
      },
    ],
    faq: [
      {
        question: "Is this dbt-compatible?",
        answer:
          "Yes — that's the whole point. An incremental view is a dbt model with materialized='incremental_view'. ref() and source() resolve through melt's catalog. dbt run registers the view with melt; melt does the maintenance. dbt test runs against the materialized output like any other table.",
      },
      {
        question: "What's the freshness SLO floor?",
        answer:
          "The alpha targets 1–5 second freshness for hash-aggregates and equi-joins over CDC-fed sources, and we'll publish exact numbers per operator class as the implementation hardens. There's no schedule and no minimum interval; the view refreshes when CDC delivers a row.",
      },
      {
        question: "What happens when upstream throughput exceeds a view's compute budget?",
        answer:
          "You pick the policy. Default is degrade_freshness: the view keeps serving with growing lag. pause_view marks the view stale and falls back to executing the underlying SQL upstream. shed_load removes the view from the graph entirely until upstream commits subside. The system never silently corrupts the result.",
      },
      {
        question: "Where are the views stored?",
        answer:
          "In your lake. The view's output lands as a regular Iceberg or DuckLake table at the view's FQN, in the same S3 / R2 / GCS bucket as the rest of the synced data. Anything that reads your catalog can read your views.",
      },
      {
        question: "Can I have a view that joins across Snowflake-only and lake-synced tables?",
        answer:
          "Not in the alpha. An incremental view's sources have to be CDC-fed (synced base tables, or other incremental views). For a view that needs to join against a Snowflake-only table on every read, you can express it as a regular SQL view and let Dual Execution plan-split the query at read time.",
      },
      {
        question: "How is this different from Snowflake Dynamic Tables?",
        answer:
          "Three differences. (1) Freshness floor — Dynamic Tables' minimum TARGET_LAG is one minute; incremental views target seconds. (2) Compute model — Dynamic Tables charge warehouse credits to refresh whether anyone reads the view or not; incremental views maintain state on the proxy's CPU. (3) Portability — Dynamic Tables live inside Snowflake; incremental views materialize into Iceberg / DuckLake, queryable from any engine.",
      },
    ],
  },
];

export const featuresBySlug: Record<string, Feature> = Object.fromEntries(
  features.map((f) => [f.slug, f]),
);

export const featuresByCategory: Record<FeatureCategory, Feature[]> = {
  Routing: features.filter((f) => f.category === "Routing"),
  Warehouse: features.filter((f) => f.category === "Warehouse"),
  Views: features.filter((f) => f.category === "Views"),
};

export const featureCategoryOrder: FeatureCategory[] = [
  "Routing",
  "Warehouse",
  "Views",
];
