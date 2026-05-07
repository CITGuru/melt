import { SectionHeader } from "../UI";
import { RoutingBoardMockup } from "../Mockups";

export function Routing() {
  return (
    <section
      id="live"
      className="relative py-24 md:py-32 bg-bg-2 border-y border-line"
    >
      <div className="mx-auto max-w-6xl px-6 grid lg:grid-cols-2 gap-12 lg:gap-16 items-center">
        <div className="flex flex-col gap-6">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            Live in production
          </span>
          <SectionHeader
            align="left"
            title={<>Every query, every decision — visible.</>}
            description="Tables flow through Pending → Lake / Snowflake / Dual as melt routes them, with latency and cost attached to each. Same view your engineers see at 3am, in the metrics tab, on a 32-inch operations dashboard."
          />
        </div>
        <div>
          <RoutingBoardMockup />
        </div>
      </div>
    </section>
  );
}
