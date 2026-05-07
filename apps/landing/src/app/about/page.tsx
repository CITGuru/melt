import type { Metadata } from "next";
import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { Cloud } from "@/components/Clouds";
import { PrimaryCTA, GhostCTA, SectionHeader } from "@/components/UI";

export const metadata: Metadata = {
  title: "About — Melt",
  description:
    "Why melt exists, what we believe, and who's building it.",
};

export default function AboutPage() {
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        {/* Hero */}
        <section className="relative overflow-hidden pt-36 md:pt-44 pb-20 md:pb-28 bg-sky">
          <Cloud className="absolute -left-20 top-32 w-[300px] opacity-80 drift-slow" />
          <Cloud className="absolute right-[-40px] top-44 w-[260px] opacity-70 drift-slow-2" />

          <div className="relative mx-auto max-w-6xl px-6 flex flex-col items-center text-center gap-6">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">
              About melt
            </span>
            <h1 className="text-5xl sm:text-6xl md:text-7xl font-semibold tracking-tight text-ink leading-[1.04] max-w-3xl">
              The unified Snowflake cost layer.
            </h1>
            <p className="text-lg md:text-xl text-ink-2 leading-relaxed max-w-2xl">
              Melt is an open-source proxy that sits in front of Snowflake and
              decides, per query, where it should run — the cheap lakehouse, the
              right-sized warehouse, or both. Drop-in for any Snowflake driver.
              Transparent to dbt, BI tools, and agents.
            </p>
          </div>
        </section>

        {/* Why now */}
        <section className="relative py-24 md:py-32">
          <div className="mx-auto max-w-6xl px-6 grid lg:grid-cols-12 gap-12 lg:gap-16 items-start">
            <div className="lg:col-span-7 flex flex-col gap-6">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                Why now
              </span>
              <SectionHeader
                align="left"
                title={
                  <>
                    Agents broke the assumptions
                    <br />
                    Snowflake billing was built on.
                  </>
                }
              />
              <div className="prose-melt">
                <p>
                  For a decade, warehouse demand had natural throttles. Analysts
                  refreshed dashboards a few times a day. dbt models materialised
                  overnight. Ad-hoc queries fired when someone asked a question.
                </p>
                <p>
                  Autonomous agents don&apos;t run on that cadence — they run
                  per prompt. A single conversation can fan out into dozens of
                  small filters, joins, and aggregates. Across a fleet of
                  agents, the warehouse never gets to spin down.
                </p>
                <p>
                  Most of those queries don&apos;t actually need Snowflake
                  compute. The bet behind melt is that a routing layer between
                  the driver and Snowflake — invisible to the agent issuing the
                  query — can save the bill without changing a connection
                  string.
                </p>
              </div>
            </div>

            <aside className="lg:col-span-5 lg:sticky lg:top-32 flex flex-col gap-4">
              <div className="bg-white rounded-3xl border border-line soft-shadow p-7 flex flex-col gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-muted">
                  Median credit reduction
                </span>
                <span className="text-6xl font-semibold tracking-tight text-ink">
                  −82%
                </span>
                <p className="text-sm text-muted">
                  Across read-heavy workloads in the design partner cohort.
                </p>
              </div>
              <div className="bg-white rounded-3xl border border-line soft-shadow p-7 flex flex-col gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-muted">
                  Lines of code to migrate
                </span>
                <span className="text-6xl font-semibold tracking-tight text-ink">
                  1
                </span>
                <p className="text-sm text-muted">
                  The host on your driver&apos;s connection string. That&apos;s
                  the entire migration.
                </p>
              </div>
            </aside>
          </div>
        </section>

        {/* Principles */}
        <section className="relative py-24 md:py-32 bg-bg-2 border-y border-line">
          <div className="mx-auto max-w-6xl px-6">
            <div className="flex flex-col items-center gap-4">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                What we believe
              </span>
              <SectionHeader
                title={<>Three principles, no compromise.</>}
              />
            </div>

            <div className="mt-14 grid md:grid-cols-3 gap-5">
              <Principle
                title="Correctness over throughput."
                body="Whatever melt returns equals what Snowflake would have returned. A parity sampler dual-runs a fraction of routed queries and alerts on drift. We refuse to federate anything that touches policy-protected tables."
              />
              <Principle
                title="Open by default."
                body="The proxy, the sync, and the CLI are Apache-2.0. Self-hosted by default. No phone-home telemetry. The only data that leaves your environment is what you choose to send to the hosted control plane."
              />
              <Principle
                title="Transparent to your stack."
                body="Drop-in for the official Snowflake driver, JDBC, ODBC, dbt, Looker, Sigma, Hex. Your connection string changes; nothing else does. No SQL rewriting, no app-side routing logic, no warehouse downtime."
              />
            </div>
          </div>
        </section>

        {/* Team */}
        <section className="relative py-24 md:py-32">
          <div className="mx-auto max-w-6xl px-6 grid lg:grid-cols-12 gap-12 items-start">
            <div className="lg:col-span-5 flex flex-col gap-4">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                Who&apos;s building it
              </span>
              <SectionHeader
                align="left"
                title={<>A small team, an open repo.</>}
                description="We&apos;re a tiny crew of data-infra engineers shipping in public, with a growing community of contributors and design partners running melt in production."
              />
            </div>

            <div className="lg:col-span-7 flex flex-col gap-4">
              {/* <PersonCard
                initials="TO"
                name="Toby Oyetoke"
                role="Founder"
                bio="Building melt because every dollar of warehouse spend should be a deliberate choice. Previously shipped data infrastructure at scale across fintech and AI tooling."
                links={[
                  { label: "GitHub", href: "https://github.com/citguru" },
                  { label: "Writing", href: "/blog" },
                ]}
              /> */}
              <ContributorsCard />
            </div>
          </div>
        </section>

        {/* Closing CTA */}
        <section className="relative overflow-hidden py-24 md:py-32 bg-sky-soft">
          <Cloud className="absolute -left-16 top-10 w-[280px] opacity-80 drift-slow" />
          <Cloud className="absolute right-0 top-20 w-[240px] opacity-70 drift-slow-2" />

          <div className="relative mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-6">
            <h2 className="text-4xl md:text-5xl font-semibold tracking-tight text-ink leading-[1.05]">
              Build with us, or just use it.
            </h2>
            <p className="text-lg text-ink-2 max-w-xl">
              Melt is open-source and self-hosted. Pull the binary, point your
              driver at it, and watch the credits drop. Or talk to us if you
              want it run for you.
            </p>
            <div className="flex flex-col sm:flex-row items-center gap-3">
              <PrimaryCTA href="https://github.com/citguru/melt" external>
                Get melt on GitHub
              </PrimaryCTA>
              <GhostCTA href="/contact-us">Talk to the team</GhostCTA>
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}

function Principle({
  title,
  body,
}: {
  title: string;
  body: string;
}) {
  return (
    <article className="bg-white rounded-3xl border border-line soft-shadow p-7 flex flex-col gap-3">
      <h3 className="text-xl font-semibold tracking-tight text-ink leading-snug">
        {title}
      </h3>
      <p className="text-muted leading-relaxed">{body}</p>
    </article>
  );
}

function PersonCard({
  initials,
  name,
  role,
  bio,
  links,
}: {
  initials: string;
  name: string;
  role: string;
  bio: string;
  links: { label: string; href: string }[];
}) {
  return (
    <article className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-5">
      <div className="flex items-center gap-4">
        <span className="h-14 w-14 rounded-full bg-gradient-to-br from-orange-200 to-orange-400 text-ink font-semibold text-lg inline-flex items-center justify-center">
          {initials}
        </span>
        <div className="flex flex-col">
          <span className="text-base font-semibold text-ink">{name}</span>
          <span className="text-sm text-muted">{role}</span>
        </div>
      </div>
      <p className="text-ink-2 leading-relaxed">{bio}</p>
      <div className="flex items-center gap-4 mt-1">
        {links.map((l) => (
          <a
            key={l.label}
            href={l.href}
            target={l.href.startsWith("http") ? "_blank" : undefined}
            rel={l.href.startsWith("http") ? "noreferrer" : undefined}
            className="text-sm font-medium text-ink-2 hover:text-ink transition-colors"
          >
            {l.label} ↗
          </a>
        ))}
      </div>
    </article>
  );
}

function ContributorsCard() {
  return (
    <article className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <span className="text-xs uppercase tracking-[0.18em] text-muted">
          Contributors & design partners
        </span>
        <span className="text-sm text-muted">
          <span className="font-semibold text-ink">growing</span> · open repo
        </span>
      </div>
      <p className="text-ink-2 leading-relaxed">
        Melt ships in public. Engineers from data teams running it in
        production file issues, PRs, and field reports against the open repo
        every week — that&apos;s where most of melt&apos;s sharpest features
        come from.
      </p>
      <a
        href="https://github.com/citguru/melt"
        target="_blank"
        rel="noreferrer"
        className="inline-flex items-center gap-2 self-start rounded-full bg-ink text-white px-5 py-2.5 text-sm font-medium hover:bg-ink-2 transition-colors"
      >
        Contribute on GitHub
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden>
          <path
            d="M5 12h14M13 5l7 7-7 7"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </a>
    </article>
  );
}
