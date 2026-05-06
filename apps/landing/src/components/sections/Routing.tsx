import { Pill, PrimaryCTA, SectionHeader } from "../UI";
import { RoutingBoardMockup } from "../Mockups";

const chips = ["Lake", "Passthrough", "Dual exec", "Policy", "Allowlist"];

export function Routing() {
  return (
    <section id="features" className="relative py-24 md:py-32 bg-bg-2 border-y border-line">
      <div className="mx-auto max-w-6xl px-6 grid lg:grid-cols-2 gap-12 lg:gap-16 items-center">
        <div className="flex flex-col gap-6">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            per-query routing
          </span>
          <SectionHeader
            align="left"
            title={<>Keep every query moving forward.</>}
            description="Parse, classify, and deliver each statement on its own merit. Reads the lake can answer go to DuckDB, writes pass through, and dual-execution stitches the rest. Every route is observable, reversible, and policy-gated."
          />
          <div className="flex flex-wrap gap-2">
            {chips.map((c) => (
              <Pill key={c}>{c}</Pill>
            ))}
          </div>
          <div className="mt-2">
            <PrimaryCTA href="/contact-us">Try Melt free</PrimaryCTA>
          </div>
        </div>
        <div>
          <RoutingBoardMockup />
        </div>
      </div>
    </section>
  );
}
