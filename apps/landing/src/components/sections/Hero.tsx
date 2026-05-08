import Link from "next/link";
import { PrimaryCTA, GhostCTA, ArrowRight } from "../UI";
import { Cloud } from "../Clouds";

export function Hero() {
  return (
    <section className="relative overflow-hidden pt-36 md:pt-44 pb-20 md:pb-28 bg-sky">
      <Cloud className="absolute -left-20 top-32 w-[360px] opacity-90 drift-slow" />
      <Cloud className="absolute right-[-60px] top-44 w-[420px] opacity-80 drift-slow-2" />
      <Cloud className="absolute left-[18%] bottom-10 w-[280px] opacity-70 drift-slow" />
      <Cloud className="absolute right-[14%] bottom-24 w-[220px] opacity-60 drift-slow-2" />

      <div className="relative mx-auto max-w-6xl px-6 flex flex-col items-center text-center gap-7">
        <div className="spin-border-pill soft-shadow">
          <Link
            href="/blog/meet-melt"
            className="inline-flex items-center gap-2 rounded-full bg-white/85 backdrop-blur px-3 py-1.5 text-xs text-ink-2 hover:text-ink transition-colors"
          >
            <span className="rounded-full bg-ink text-white px-2 py-0.5 text-[10px] font-medium tracking-wide uppercase">
              New
            </span>
            Why we&apos;re building melt
            <ArrowRight />
          </Link>
        </div>

        <h1 className="text-5xl sm:text-6xl md:text-7xl lg:text-[80px] font-semibold tracking-tight text-ink leading-[1.02] max-w-[18ch]">
          Cut your Snowflake bill by 75%, without changing a query.
        </h1>

        <p className="text-lg md:text-xl text-ink-2 leading-relaxed max-w-2xl">
          Melt is a proxy that sits in front of Snowflake and routes each query
          to the cheapest engine, right-sized compute, in real time. No code changes, no warehouse downtime.
        </p>

        <div className="flex flex-col sm:flex-row items-center gap-3 mt-1">
          <PrimaryCTA href="/contact-us">Book a demo</PrimaryCTA>
          <GhostCTA href="/#features">See how it works</GhostCTA>
        </div>

        <p className="text-xs text-muted-2 mt-3 tracking-wide">
          5-minute setup · Drop-in for any Snowflake driver · Self-hosted
        </p>
      </div>
    </section>
  );
}
