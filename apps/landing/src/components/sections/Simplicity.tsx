import { SectionHeader } from "../UI";

export function Simplicity() {
  return (
    <section className="relative py-24 md:py-32 bg-bg-2 border-y border-line">
      <div className="mx-auto max-w-6xl px-6 flex flex-col items-center gap-4">
        <span className="text-xs uppercase tracking-[0.18em] text-muted">features</span>
        <SectionHeader
          title={
            <>
              Built for data teams,
              <br />
              powered by simplicity.
            </>
          }
        />
      </div>

      <div className="mx-auto max-w-6xl px-6 mt-14 grid grid-cols-1 lg:grid-cols-12 gap-5">
        <Card className="lg:col-span-7" mockup={<CustomizeMockup />}>
          <h3 className="text-2xl md:text-3xl font-semibold tracking-tight">
            Smart, flexible, and built around your routing rules.
          </h3>
          <p className="text-muted leading-relaxed">
            Tune scan budgets, mark sensitive tables Snowflake-only, sample for
            parity, allowlist by schema. Make melt feel like an extension of
            your data platform.
          </p>
        </Card>

        <Card className="lg:col-span-5" mockup={<IntegrationsMockup />}>
          <h3 className="text-2xl md:text-3xl font-semibold tracking-tight">
            Plugs into the tools you already pay for.
          </h3>
          <p className="text-muted leading-relaxed">
            JDBC, ODBC, the Python connector, dbt, Looker, Sigma, Hex — all
            connect unmodified. Push routing decisions into Datadog, Slack, or
            your own warehouse.
          </p>
        </Card>

        <Card className="lg:col-span-4">
          <Icon>
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden>
              <path d="M3 12h6l3-9 3 18 3-9h3" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </Icon>
          <h4 className="text-lg font-semibold tracking-tight">Observe in realtime</h4>
          <p className="text-sm text-muted leading-relaxed">
            Every routing decision streams to /metrics, with structured logs
            and tracing. See, in real time, where each query landed and why.
          </p>
        </Card>

        <Card className="lg:col-span-4">
          <Icon>
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden>
              <path d="M4 6h16M4 12h10M4 18h16" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
            </svg>
          </Icon>
          <h4 className="text-lg font-semibold tracking-tight">Speaks your dialect</h4>
          <p className="text-sm text-muted leading-relaxed">
            We translate Snowflake SQL to DuckDB on the fly — IFF, QUALIFY,
            FLATTEN, time-zone arithmetic — and refuse routes that can’t round-trip.
          </p>
        </Card>

        <Card className="lg:col-span-4">
          <Icon>
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden>
              <rect x="3" y="4" width="18" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.6" />
              <rect x="3" y="14" width="18" height="6" rx="1.5" stroke="currentColor" strokeWidth="1.6" />
            </svg>
          </Icon>
          <h4 className="text-lg font-semibold tracking-tight">View things your way</h4>
          <p className="text-sm text-muted leading-relaxed">
            Toggle between dashboards, route tables, kanban-style sync state, or
            timeline views — pick the lens that fits your team’s shift.
          </p>
        </Card>
      </div>
    </section>
  );
}

function Card({
  children,
  mockup,
  className = "",
}: {
  children: React.ReactNode;
  mockup?: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-4 ${className}`}
    >
      {mockup ? <div className="mb-2">{mockup}</div> : null}
      {children}
    </div>
  );
}

function Icon({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex h-11 w-11 items-center justify-center rounded-2xl bg-ink text-white">
      {children}
    </span>
  );
}

function CustomizeMockup() {
  return (
    <div className="rounded-2xl border border-line bg-bg-2/60 p-4 grid grid-cols-2 gap-3">
      {[
        ["Scan budget", "2 GB"],
        ["Parity sampler", "1 / 100"],
        ["Allowlist", "analytics.*"],
        ["Sensitive", "pii.users"],
      ].map(([k, v]) => (
        <div key={k as string} className="bg-white rounded-xl border border-line p-3">
          <p className="text-[10px] uppercase tracking-widest text-muted">{k}</p>
          <p className="font-mono text-sm mt-1">{v}</p>
        </div>
      ))}
    </div>
  );
}

function IntegrationsMockup() {
  const items = ["JDBC", "ODBC", "dbt", "Looker", "Sigma", "Hex", "Datadog", "Slack"];
  return (
    <div className="rounded-2xl border border-line bg-bg-2/60 p-4 grid grid-cols-4 gap-2">
      {items.map((t) => (
        <div
          key={t}
          className="aspect-square rounded-xl bg-white border border-line flex items-center justify-center text-xs font-medium text-ink-2"
        >
          {t}
        </div>
      ))}
    </div>
  );
}
