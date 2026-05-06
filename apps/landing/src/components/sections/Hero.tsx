import { PrimaryCTA, GhostCTA } from "../UI";
import { Cloud } from "../Clouds";

export function Hero() {
  return (
    <section className="relative overflow-hidden pt-36 md:pt-44 pb-20 md:pb-28 bg-sky">
      <Cloud className="absolute -left-20 top-32 w-[360px] opacity-90 drift-slow" />
      <Cloud className="absolute right-[-60px] top-44 w-[420px] opacity-80 drift-slow-2" />
      <Cloud className="absolute left-[18%] bottom-10 w-[280px] opacity-70 drift-slow" />
      <Cloud className="absolute right-[14%] bottom-24 w-[220px] opacity-60 drift-slow-2" />

      <div className="relative mx-auto max-w-6xl px-6 flex flex-col items-center text-center gap-8">
        <h1 className="text-5xl sm:text-6xl md:text-[88px] font-semibold tracking-tight text-ink leading-[0.98] max-w-[15ch]">
          Run your data warehouse like a pro
        </h1>

        <p className="text-lg md:text-xl text-ink-2 leading-relaxed max-w-2xl">
          All-in-one routing layer for Snowflake, lakehouse, and the agents
          generating SQL faster than your warehouse can keep up. From the first
          driver connection to the final invoice, melt has your back.
        </p>

        <div className="flex flex-col sm:flex-row items-center gap-3 mt-1">
          <PrimaryCTA href="/contact-us">Try Melt free</PrimaryCTA>
          <GhostCTA href="/#features">See features</GhostCTA>
        </div>

        <p className="text-xs text-muted-2 mt-4 tracking-wide">
          Trusted by 7,000+ data teams, agents, and analytics studios
        </p>
      </div>
    </section>
  );
}
