import { Pill, SectionHeader } from "../UI";

const queryChips = ["Lake", "Passthrough", "Dual exec", "Policy", "Allowlist"];
const warehouseChips = [
  "Right-sizing",
  "Warm-warehouse routing",
  "Cost attribution",
];

export function Strategies() {
  return (
    <section
      id="features"
      className="relative py-24 md:py-32"
    >
      <div className="mx-auto max-w-6xl px-6">
        <div className="flex flex-col items-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            Two routing strategies
          </span>
          <SectionHeader
            title={<>Where each query runs.</>}
            description="Melt makes two decisions per query — independently, transparently, with no driver changes. Together they form the unified Snowflake cost layer."
          />
        </div>

        <div className="mt-14 grid lg:grid-cols-2 gap-5">
          <StrategyCard
            number="01"
            title="Query routing"
            subtitle="Lake or Snowflake?"
            description="For each statement, melt decides whether DuckDB on your S3 lake can answer it. Eligible reads go local for cents. Writes and Snowflake-only features pass through. Dual execution stitches plans that touch both."
            chips={queryChips}
            tag={{ label: "Available today", tone: "live" }}
            visual={<QueryVisual />}
          />
          <StrategyCard
            number="02"
            title="Warehouse routing"
            subtitle="Which Snowflake warehouse?"
            description="For the queries that stay on Snowflake, melt picks the warehouse that actually fits — XSMALL for a tiny filter, LARGE for the nightly aggregate. Per-statement, transparent to your driver."
            chips={warehouseChips}
            tag={{ label: "Coming soon", tone: "soon" }}
            visual={<WarehouseVisual />}
          />
        </div>
      </div>
    </section>
  );
}

function StrategyCard({
  number,
  title,
  subtitle,
  description,
  chips,
  tag,
  visual,
}: {
  number: string;
  title: string;
  subtitle: string;
  description: string;
  chips: string[];
  tag: { label: string; tone: "live" | "soon" };
  visual: React.ReactNode;
}) {
  const tagCls =
    tag.tone === "live"
      ? "bg-emerald-100 text-emerald-700 border border-emerald-200"
      : "bg-orange-100 text-orange-700 border border-orange-200";
  return (
    <article className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <span className="text-xs font-mono text-muted-2 tracking-widest">
          STRATEGY {number}
        </span>
        <span
          className={`inline-flex items-center gap-1.5 text-[11px] font-medium uppercase tracking-[0.14em] rounded-full px-2.5 py-1 ${tagCls}`}
        >
          <span
            className={`h-1.5 w-1.5 rounded-full ${
              tag.tone === "live" ? "bg-emerald-500" : "bg-orange-500"
            }`}
          />
          {tag.label}
        </span>
      </div>

      <div className="flex flex-col gap-2">
        <h3 className="text-3xl md:text-4xl font-semibold tracking-tight text-ink">
          {title}
        </h3>
        <p className="text-base text-ink-2/80">{subtitle}</p>
      </div>

      <div>{visual}</div>

      <p className="text-muted leading-relaxed">{description}</p>

      <div className="flex flex-wrap gap-2 mt-auto">
        {chips.map((c) => (
          <Pill key={c}>{c}</Pill>
        ))}
      </div>
    </article>
  );
}

/* Visual: a query branching to Lake vs Snowflake */
function QueryVisual() {
  return (
    <div className="rounded-2xl border border-line bg-bg-2/60 p-4">
      <div className="grid grid-cols-[auto_1fr_auto] items-center gap-3 text-xs font-mono">
        <div className="px-2.5 py-1.5 rounded-lg bg-white border border-line text-ink">
          query
        </div>
        <div className="relative h-10">
          <Branch />
        </div>
        <div className="flex flex-col gap-1.5">
          <span className="px-2.5 py-1 rounded-lg bg-emerald-100 text-emerald-700 border border-emerald-200 text-center">
            DuckDB · lake
          </span>
          <span className="px-2.5 py-1 rounded-lg bg-ink/10 text-ink border border-line text-center">
            Snowflake
          </span>
        </div>
      </div>
    </div>
  );
}

function Branch() {
  return (
    <svg
      viewBox="0 0 100 40"
      className="w-full h-full text-muted"
      fill="none"
      preserveAspectRatio="none"
      aria-hidden
    >
      <path
        d="M0 20 Q 50 20 75 6"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
      <path
        d="M0 20 Q 50 20 75 34"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
    </svg>
  );
}

/* Visual: warehouse size selector */
function WarehouseVisual() {
  const sizes: { label: string; w: number; active?: boolean }[] = [
    { label: "XS", w: 18 },
    { label: "S", w: 32, active: true },
    { label: "M", w: 50 },
    { label: "L", w: 70 },
    { label: "XL", w: 92 },
  ];
  return (
    <div className="rounded-2xl border border-line bg-bg-2/60 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-[10px] uppercase tracking-widest text-muted">
          Estimated scan
        </span>
        <span className="text-[11px] font-mono text-ink-2">~ 184 MB</span>
      </div>
      <div className="flex items-end gap-2">
        {sizes.map((s) => (
          <div
            key={s.label}
            className="flex-1 flex flex-col items-center gap-1.5"
          >
            <div
              className={`w-full rounded-md border ${
                s.active
                  ? "bg-orange-200 border-orange-300"
                  : "bg-white border-line"
              }`}
              style={{ height: `${s.w}%`, minHeight: 14 }}
            />
            <span
              className={`text-[10px] font-mono ${
                s.active ? "text-orange-700 font-semibold" : "text-muted"
              }`}
            >
              {s.label}
            </span>
          </div>
        ))}
      </div>
      <div className="mt-3 flex items-center justify-between text-[10px] font-mono text-muted">
        <span>warehouse selected</span>
        <span className="text-orange-700 font-semibold">SMALL</span>
      </div>
    </div>
  );
}
