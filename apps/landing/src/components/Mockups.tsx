/* SVG-based UI mockups so the page has texture without external images. */

export function PhoneMockup() {
  return (
    <div className="relative mx-auto w-[260px] aspect-[9/19] rounded-[42px] bg-ink p-3 soft-shadow-lg">
      <div className="absolute inset-x-1/3 top-2 h-5 rounded-full bg-black/80 z-10" />
      <div className="relative h-full w-full rounded-[34px] overflow-hidden bg-gradient-to-b from-sky-2 to-white">
        <div className="absolute inset-x-0 top-0 h-12 flex items-end justify-between px-5 pb-1.5 text-[10px] text-ink-2/70">
          <span>9:41</span>
          <span className="flex gap-1 items-center">
            <span className="h-1.5 w-1.5 rounded-full bg-ink/60" />
            <span className="h-1.5 w-1.5 rounded-full bg-ink/60" />
            <span className="h-1.5 w-1.5 rounded-full bg-ink/60" />
          </span>
        </div>
        <div className="px-4 pt-12 flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <span className="text-[11px] uppercase tracking-widest text-muted">Today</span>
            <span className="h-7 w-7 rounded-full bg-ink text-white text-[10px] flex items-center justify-center">
              T
            </span>
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-widest text-muted">Saved</p>
            <p className="text-3xl font-semibold tracking-tight">$1,284</p>
            <p className="text-[10px] text-muted">vs. yesterday +12%</p>
          </div>

          <div className="bg-white rounded-2xl p-3 border border-line soft-shadow flex flex-col gap-2">
            <div className="flex items-center justify-between">
              <span className="text-[10px] uppercase tracking-widest text-muted">Routes</span>
              <span className="text-[10px] text-muted">last 1h</span>
            </div>
            <Bar label="Lake"        value={86} color="bg-emerald-400" />
            <Bar label="Snowflake"   value={9}  color="bg-ink" />
            <Bar label="Dual"        value={5}  color="bg-accent" />
          </div>

          <div className="bg-white rounded-2xl p-3 border border-line soft-shadow">
            <span className="text-[10px] uppercase tracking-widest text-muted">Recent query</span>
            <p className="text-[12px] mt-1 leading-snug font-mono">
              SELECT day, count(*) FROM events GROUP BY 1
            </p>
            <span className="mt-2 inline-flex items-center gap-1 text-[10px] text-emerald-700 bg-emerald-100 px-2 py-0.5 rounded-full">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" /> LAKE · 0.4s · $0.0003
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

function Bar({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-16 text-[10px] text-ink-2/80">{label}</span>
      <div className="flex-1 h-2 rounded-full bg-line overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${value}%` }} />
      </div>
      <span className="w-8 text-right text-[10px] tabular-nums text-muted">{value}%</span>
    </div>
  );
}

export function LaptopMockup() {
  return (
    <div className="relative mx-auto w-full max-w-[760px]">
      <div className="rounded-[20px] bg-ink p-2.5 soft-shadow-lg">
        <div className="rounded-[14px] bg-white overflow-hidden border border-line">
          <div className="h-9 flex items-center gap-2 px-4 border-b border-line bg-bg-2">
            <span className="h-2.5 w-2.5 rounded-full bg-[#ff5f56]" />
            <span className="h-2.5 w-2.5 rounded-full bg-[#ffbd2e]" />
            <span className="h-2.5 w-2.5 rounded-full bg-[#27c93f]" />
            <span className="ml-4 text-[11px] font-mono text-muted">app.meltcomputing.com / routes</span>
          </div>
          <div className="grid grid-cols-[180px_1fr] min-h-[360px]">
            <aside className="border-r border-line bg-bg-2 p-3 flex flex-col gap-1.5 text-[12px]">
              {[
                ["Routes", true],
                ["Tables", false],
                ["Sync state", false],
                ["Policies", false],
                ["Metrics", false],
                ["Settings", false],
              ].map(([label, active]) => (
                <span
                  key={label as string}
                  className={`px-2.5 py-1.5 rounded-lg ${
                    active
                      ? "bg-ink text-white"
                      : "text-ink-2 hover:bg-white"
                  }`}
                >
                  {label as string}
                </span>
              ))}
              <span className="mt-auto text-[10px] uppercase tracking-widest text-muted px-2 pt-3">
                v0.1.0
              </span>
            </aside>
            <div className="p-5 flex flex-col gap-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-[10px] uppercase tracking-widest text-muted">Live routing</p>
                  <h4 className="text-lg font-semibold">Last 5 minutes</h4>
                </div>
                <span className="text-[11px] text-muted">2,481 queries</span>
              </div>
              <div className="grid grid-cols-3 gap-3">
                <Stat label="Lake" value="86%" tint="emerald" />
                <Stat label="Snowflake" value="9%" tint="ink" />
                <Stat label="Dual" value="5%" tint="accent" />
              </div>
              <div className="rounded-2xl border border-line">
                <div className="grid grid-cols-[110px_1fr_70px_60px] text-[10px] uppercase tracking-widest text-muted px-4 py-2 border-b border-line bg-bg-2">
                  <span>Route</span>
                  <span>Query</span>
                  <span>Latency</span>
                  <span className="text-right">Cost</span>
                </div>
                {[
                  { tag: "LAKE", q: "SELECT day, count(*) FROM events GROUP BY 1", ms: "412ms", c: "$0.0003" },
                  { tag: "LAKE", q: "SELECT region, sum(total) FROM orders WHERE…", ms: "228ms", c: "$0.0001" },
                  { tag: "DUAL", q: "SELECT u.region, sum(o.total) FROM users u JOIN…", ms: "1.1s",  c: "$0.04" },
                  { tag: "SF",   q: "MERGE INTO orders USING staging.orders s ON…",     ms: "2.3s",  c: "$0.18" },
                  { tag: "LAKE", q: "SELECT count(*) FROM users WHERE created_at >…",  ms: "184ms", c: "$0.0002" },
                ].map((r, i, arr) => (
                  <div
                    key={i}
                    className={`grid grid-cols-[110px_1fr_70px_60px] items-center px-4 py-2.5 text-[12px] ${
                      i !== arr.length - 1 ? "border-b border-line" : ""
                    }`}
                  >
                    <span>
                      <RouteTag tag={r.tag as "LAKE" | "DUAL" | "SF"} />
                    </span>
                    <span className="font-mono text-ink-2 truncate">{r.q}</span>
                    <span className="font-mono text-muted">{r.ms}</span>
                    <span className="font-mono text-ink text-right">{r.c}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
      <div className="mx-auto h-1.5 w-1/2 rounded-b-2xl bg-ink/80" />
    </div>
  );
}

function Stat({
  label,
  value,
  tint,
}: {
  label: string;
  value: string;
  tint: "emerald" | "ink" | "accent";
}) {
  const tone =
    tint === "emerald"
      ? "from-emerald-50 to-white text-emerald-700 border-emerald-200"
      : tint === "accent"
        ? "from-orange-50 to-white text-orange-700 border-orange-200"
        : "from-bg-2 to-white text-ink border-line";
  return (
    <div className={`rounded-2xl p-3 border bg-gradient-to-b ${tone}`}>
      <p className="text-[10px] uppercase tracking-widest opacity-70">{label}</p>
      <p className="text-2xl font-semibold tracking-tight">{value}</p>
    </div>
  );
}

function RouteTag({ tag }: { tag: "LAKE" | "DUAL" | "SF" }) {
  const cls =
    tag === "LAKE"
      ? "bg-emerald-100 text-emerald-700"
      : tag === "DUAL"
        ? "bg-orange-100 text-orange-700"
        : "bg-ink/10 text-ink";
  return (
    <span className={`text-[10px] font-mono px-2 py-0.5 rounded-full ${cls}`}>
      {tag}
    </span>
  );
}

export function RoutingBoardMockup() {
  const cols = [
    { title: "Pending", color: "bg-sky-2", items: ["events.daily_rollup", "users.region_count"] },
    { title: "Lake", color: "bg-emerald-100", items: ["orders.summary", "events.last_7d", "users.signup_funnel"] },
    { title: "Snowflake", color: "bg-ink/10", items: ["orders.merge_ingest", "policy.audit_log"] },
    { title: "Dual", color: "bg-orange-100", items: ["users x orders_remote"] },
  ];
  return (
    <div className="rounded-3xl bg-white border border-line p-5 soft-shadow grid grid-cols-2 lg:grid-cols-4 gap-3 w-full">
      {cols.map((c) => (
        <div key={c.title} className="flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <span className="text-[10px] uppercase tracking-widest text-muted">{c.title}</span>
            <span className={`h-2 w-2 rounded-full ${c.color}`} />
          </div>
          {c.items.map((it) => (
            <div
              key={it}
              className="rounded-xl border border-line p-2.5 bg-bg-2/50 flex flex-col gap-1.5"
            >
              <span className="text-[12px] font-mono text-ink truncate">{it}</span>
              <div className="flex items-center justify-between">
                <span className="text-[10px] text-muted">~ {Math.round(Math.random() * 800 + 100)}ms</span>
                <span className="text-[10px] text-muted">${(Math.random() * 0.05).toFixed(4)}</span>
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

export function CostMockup() {
  const rows = [
    { l: "Agent dashboard refresh", b: "$184", a: "$3.10",  s: "98%" },
    { l: "dbt incremental models",  b: "$47",  a: "$11.20", s: "76%" },
    { l: "BI ad-hoc filters",       b: "$612", a: "$8.40",  s: "99%" },
    { l: "Heavy joins (oversize)",  b: "$96",  a: "$96",    s: "0%"  },
    { l: "Writes / MERGE",          b: "$58",  a: "$58",    s: "0%"  },
  ];
  return (
    <div className="rounded-3xl bg-white border border-line p-5 soft-shadow w-full">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-[10px] uppercase tracking-widest text-muted">May statement</p>
          <h4 className="text-xl font-semibold tracking-tight">Warehouse credits</h4>
        </div>
        <div className="text-right">
          <p className="text-[10px] uppercase tracking-widest text-muted">Saved</p>
          <p className="text-2xl font-semibold text-emerald-600">−82%</p>
        </div>
      </div>
      <div className="mt-4 rounded-2xl border border-line overflow-hidden">
        <div className="grid grid-cols-[1.6fr_0.7fr_0.7fr_0.5fr] text-[10px] uppercase tracking-widest text-muted px-4 py-2 border-b border-line bg-bg-2">
          <span>Workload</span>
          <span>Before</span>
          <span>With melt</span>
          <span className="text-right">Saved</span>
        </div>
        {rows.map((r, i) => (
          <div
            key={r.l}
            className={`grid grid-cols-[1.6fr_0.7fr_0.7fr_0.5fr] items-center px-4 py-2.5 text-[12px] ${
              i !== rows.length - 1 ? "border-b border-line" : ""
            }`}
          >
            <span className="text-ink truncate">{r.l}</span>
            <span className="font-mono text-muted line-through decoration-ink/20">{r.b}</span>
            <span className="font-mono text-ink">{r.a}</span>
            <span
              className={`text-right text-[11px] font-mono px-2 py-0.5 rounded-full justify-self-end ${
                r.s === "0%"
                  ? "bg-bg-2 text-muted border border-line"
                  : "bg-emerald-100 text-emerald-700"
              }`}
            >
              {r.s}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
