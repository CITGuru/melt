import { PrimaryCTA } from "../UI";
import { Cloud } from "../Clouds";

export function CTA() {
  return (
    <section className="relative overflow-hidden py-24 md:py-32 bg-sky-soft">
      <Cloud className="absolute -left-16 top-10 w-[300px] opacity-80 drift-slow" />
      <Cloud className="absolute right-0 top-20 w-[260px] opacity-70 drift-slow-2" />
      <Cloud className="absolute left-1/4 bottom-0 w-[220px] opacity-70 drift-slow" />

      <div className="relative mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-6">
        <h2 className="text-5xl md:text-6xl font-semibold tracking-tight text-ink leading-[1.05]">
          Ready to get started?
        </h2>
        <p className="text-lg text-ink-2 max-w-xl">
          Download Melt for free and route your first query in under five
          minutes. No credit card required.
        </p>
        <PrimaryCTA href="/contact-us">Try Melt free</PrimaryCTA>
      </div>
    </section>
  );
}
