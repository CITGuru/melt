import { Pill, PrimaryCTA, SectionHeader } from "../UI";
import { CostMockup } from "../Mockups";

const chips = ["Cost attribution", "Budgets", "Forecasts", "Integrations"];

export function Cost() {
  return (
    <section id="benefits" className="relative py-24 md:py-32">
      <div className="mx-auto max-w-6xl px-6 grid lg:grid-cols-2 gap-12 lg:gap-16 items-center">
        <div className="order-2 lg:order-1">
          <CostMockup />
        </div>
        <div className="order-1 lg:order-2 flex flex-col gap-6">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            warehouse savings
          </span>
          <SectionHeader
            align="left"
            title={<>Track credits, save more, sleep better.</>}
            description="See exactly which workloads route off Snowflake, what they would have cost, and what they cost now. Whether you bill by warehouse, by team, or by agent — melt keeps the books straight."
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
      </div>
    </section>
  );
}
