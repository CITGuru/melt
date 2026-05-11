import Link from "next/link";
import { Nav } from "./Nav";
import { FAQ } from "./sections/FAQ";
import { Footer } from "./sections/Footer";
import { ArrowRight, PrimaryCTA, GhostCTA } from "./UI";
import { FeatureIcon } from "./FeatureIcons";
import {
  type Feature,
  type NamedItem,
  featuresByCategory,
} from "@/lib/features";
import { featureFaqBySlug } from "@/lib/faq";

export function FeaturePageLayout({ feature }: { feature: Feature }) {
  const isAlpha = feature.status === "alpha";
  const siblings = featuresByCategory[feature.category].filter(
    (f) => f.slug !== feature.slug,
  );
  const faq = featureFaqBySlug[feature.slug];

  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <Hero feature={feature} isAlpha={isAlpha} />

        {isAlpha ? <AlphaBody feature={feature} /> : <LiveBody feature={feature} />}

        {siblings.length > 0 ? (
          <SiblingSection feature={feature} siblings={siblings} />
        ) : null}

        {faq ? <FAQ block={faq} /> : null}

        <FinalCTA feature={feature} isAlpha={isAlpha} />
      </main>
      <Footer />
    </>
  );
}

/* ──────────────────────────────  HERO  ────────────────────────────── */

function Hero({ feature, isAlpha }: { feature: Feature; isAlpha: boolean }) {
  return (
    <section className="relative pt-36 md:pt-44 pb-16 md:pb-24 bg-sky-soft">
      <div className="mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-6">
        <div className="flex items-center gap-2">
          <Link
            href="/#features"
            className="inline-flex items-center gap-1.5 text-xs uppercase tracking-[0.18em] text-muted hover:text-ink transition-colors"
          >
            {feature.category}
          </Link>
          {isAlpha ? <AlphaPill /> : null}
        </div>
        <h1 className="text-5xl md:text-6xl lg:text-[72px] font-semibold tracking-tight text-ink leading-[1.04]">
          {feature.title}
        </h1>
        <p className="text-lg md:text-xl text-ink-2 leading-relaxed max-w-2xl">
          {firstTwoSentences(feature.tagline)}
        </p>
        <div className="flex flex-col sm:flex-row items-center gap-3 mt-2">
          <PrimaryCTA href="/contact-us">
            {isAlpha ? "Get early access" : "Book a demo"}
          </PrimaryCTA>
          <GhostCTA href="/#features">All features</GhostCTA>
        </div>
      </div>
    </section>
  );
}

/* ─────────────────────────────  BODIES  ───────────────────────────── */

function LiveBody({ feature }: { feature: Feature }) {
  const steps = (feature.howItWorks ?? []).slice(0, 3).map((s, i) => ({
    label: `0${i + 1}`,
    title: s.title,
    description: firstSentence(s.description),
  }));
  const benefits = (feature.benefits ?? []).slice(0, 3).map((b) => ({
    title: b.title,
    description: firstSentence(b.description),
  }));

  return (
    <>
      {steps.length > 0 ? (
        <ThreeUp
          eyebrow="How it works"
          title={`${feature.title} in three steps.`}
          items={steps}
          numbered
          background="white"
        />
      ) : null}
      {benefits.length > 0 ? (
        <ThreeUp
          eyebrow="What you get"
          title="Drop in. Same drivers. Lower bill."
          items={benefits}
          background="bg-2"
        />
      ) : null}
    </>
  );
}

function AlphaBody({ feature }: { feature: Feature }) {
  const promises = (feature.alphaPromise ?? []).slice(0, 3).map((p) => ({
    title: p.title,
    description: firstSentence(p.description),
  }));
  const audience = (feature.audience ?? []).slice(0, 3).map((a) => ({
    title: a.title,
    description: firstSentence(a.description),
  }));

  return (
    <>
      {promises.length > 0 ? (
        <ThreeUp
          eyebrow="What it'll do"
          title="The capabilities, at a glance."
          items={promises}
          background="white"
        />
      ) : null}
      {audience.length > 0 ? (
        <ThreeUp
          eyebrow="Built for"
          title="Where this fits."
          items={audience}
          background="bg-2"
        />
      ) : null}
    </>
  );
}

/* ──────────────────────────  THREE-UP CARD ROW  ──────────────────── */

function ThreeUp({
  eyebrow,
  title,
  items,
  background,
  numbered = false,
}: {
  eyebrow: string;
  title: string;
  items: NamedItem[];
  background: "white" | "bg-2";
  numbered?: boolean;
}) {
  const sectionBg = background === "white" ? "" : "bg-bg-2 border-y border-line";
  return (
    <section className={`py-20 md:py-28 ${sectionBg}`}>
      <div className="mx-auto max-w-5xl px-6">
        <div className="flex flex-col items-center text-center gap-4 max-w-2xl mx-auto">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            {eyebrow}
          </span>
          <h2 className="text-3xl md:text-4xl lg:text-5xl font-semibold tracking-tight text-ink leading-[1.04]">
            {title}
          </h2>
        </div>
        <div className="mt-12 md:mt-16 grid md:grid-cols-3 gap-5">
          {items.map((item, i) => (
            <div
              key={item.title}
              className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-4"
            >
              {numbered ? (
                <span className="font-mono text-sm text-accent">
                  0{i + 1}
                </span>
              ) : (
                <span className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-accent/10 text-accent">
                  <CheckIcon />
                </span>
              )}
              <h3 className="text-lg md:text-xl font-semibold tracking-tight text-ink leading-snug">
                {item.title}
              </h3>
              <p className="text-sm md:text-base text-ink-2 leading-relaxed">
                {item.description}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ──────────────────────────  SIBLINGS + CTA  ─────────────────────── */

function SiblingSection({
  feature,
  siblings,
}: {
  feature: Feature;
  siblings: Feature[];
}) {
  return (
    <section className="py-16 md:py-20">
      <div className="mx-auto max-w-5xl px-6">
        <p className="text-xs uppercase tracking-[0.18em] text-muted mb-6">
          More in {feature.category}
        </p>
        <div className="grid md:grid-cols-2 gap-4">
          {siblings.map((s) => (
            <Link
              key={s.slug}
              href={`/features/${s.slug}`}
              className="bg-white rounded-3xl border border-line soft-shadow p-6 flex items-start gap-4 hover:soft-shadow-lg transition-shadow"
            >
              <span className="mt-0.5 inline-flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-bg-2 text-ink">
                <FeatureIcon name={s.iconName} />
              </span>
              <div className="flex flex-col gap-1.5 min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-base font-semibold text-ink">
                    {s.title}
                  </span>
                  {s.status === "alpha" ? <AlphaPill compact /> : null}
                </div>
                <span className="text-sm text-muted leading-snug">
                  {s.shortDescription}
                </span>
              </div>
              <ArrowRight />
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

function FinalCTA({
  feature,
  isAlpha,
}: {
  feature: Feature;
  isAlpha: boolean;
}) {
  return (
    <section className="py-20 md:py-28 bg-sky-soft border-t border-line">
      <div className="mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-5">
        <h2 className="text-3xl md:text-5xl font-semibold tracking-tight text-ink leading-[1.05]">
          {isAlpha
            ? `Want ${feature.title.toLowerCase()} in your stack early?`
            : "Get up and running in 5 minutes."}
        </h2>
        <p className="text-base md:text-lg text-muted max-w-xl">
          {isAlpha
            ? "We're shipping this with a small group of design partners. Tell us about your workload and we'll set you up."
            : "Connect your Snowflake account, swap your connection string, and watch your warehouse credits drop."}
        </p>
        <div className="flex flex-col sm:flex-row items-center gap-3">
          <PrimaryCTA href="/contact-us">
            {isAlpha ? "Get early access" : "Book a demo"}
          </PrimaryCTA>
          <GhostCTA href="/#features">All features</GhostCTA>
        </div>
      </div>
    </section>
  );
}

/* ──────────────────────────  PRIMITIVES  ─────────────────────────── */

function AlphaPill({ compact = false }: { compact?: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1 font-medium uppercase tracking-[0.16em] rounded-full bg-accent/10 text-accent border border-accent/30 ${
        compact ? "text-[9px] px-1.5 py-0.5" : "text-[10px] px-2 py-0.5"
      }`}
    >
      Alpha
    </span>
  );
}

function CheckIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path
        d="M5 12l5 5 9-11"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/* Helpers — keep card copy short by trimming descriptions. */

function firstSentence(text: string): string {
  const m = text.match(/^[^.!?]*[.!?]/);
  return (m ? m[0] : text).trim();
}

function firstTwoSentences(text: string): string {
  const m = text.match(/^([^.!?]*[.!?]\s*){1,2}/);
  return (m ? m[0] : text).trim();
}
