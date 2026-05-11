import type { Metadata } from "next";
import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { Cloud } from "@/components/Clouds";
import { PrimaryCTA, GhostCTA } from "@/components/UI";
import { CostCalculator } from "@/components/sections/CostCalculator";

export const metadata: Metadata = {
  title: "Methodology — Melt",
  description:
    "The math behind the 75% figure on the homepage, with an interactive calculator you can run on your own workload.",
};

export default function MethodologyPage() {
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        {/* Hero */}
        <section className="relative overflow-hidden pt-36 md:pt-44 pb-20 md:pb-28 bg-sky">
          <Cloud className="absolute -left-20 top-32 w-[320px] opacity-80 drift-slow" />
          <Cloud className="absolute right-[-40px] top-44 w-[260px] opacity-70 drift-slow-2" />
          <Cloud className="absolute left-[18%] bottom-12 w-[220px] opacity-60 drift-slow" />

          <div className="relative mx-auto max-w-4xl px-6 flex flex-col items-center text-center gap-6">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">
              Methodology
            </span>
            <h1 className="text-5xl sm:text-6xl md:text-7xl font-semibold tracking-tight text-ink leading-[1.04] max-w-3xl">
              How Melt cuts a Snowflake bill.
            </h1>
            <p className="text-lg md:text-xl text-ink-2 leading-relaxed max-w-2xl">
              The math behind the 75% figure on the homepage, plus a calculator
              you can run on your own workload.
            </p>
          </div>
        </section>

        {/* Document body */}
        <section className="relative py-16 md:py-20">
          <div className="mx-auto max-w-3xl px-6">
            <div className="prose-melt">
              <p>
                This document is the source of truth behind the{" "}
                <em>
                  &ldquo;Cut your Snowflake bill by 75%, without changing a
                  query&rdquo;
                </em>{" "}
                hero claim. It defines the canonical reference workload, the
                math model, and the sensitivity range. Every public number on
                the landing site, in <code>readme.md</code>, and in the
                interactive calculator should derive from the inputs and
                equations on this page.
              </p>

              {/* TL;DR */}
              <h2>TL;DR</h2>
              <p>
                For an agent-shaped Snowflake workload, Melt compresses spend
                along two independent routing dimensions:
              </p>
              <ol className="list-decimal pl-5 my-4 space-y-2">
                <li>
                  <strong>Query routing</strong> — for every statement, decide
                  whether the cheap lakehouse engine can answer it. Lake-routed
                  queries pay <strong>$0</strong> Snowflake credits.
                </li>
                <li>
                  <strong>Warehouse routing</strong> — for the statements that
                  do stay on Snowflake, pick the smallest warehouse that
                  satisfies the query instead of running everything on the
                  shared oversized one.
                </li>
              </ol>
              <p>
                On the canonical agent-shaped workload defined below, the
                combined savings land between <strong>75% and 97%</strong>{" "}
                depending on lake-eligible fraction and whether warehouse
                routing is enabled. The hero quotes the floor.
              </p>

              {/* Canonical workload */}
              <h2>Canonical reference workload</h2>
              <ProseTable
                head={["Input", "Value", "Source"]}
                rows={[
                  [
                    "Query volume",
                    "100,000 queries/month (~3,333/day)",
                    "Typical mid-sized agent fleet",
                  ],
                  [
                    "Workload shape",
                    "60% small filters, 25% selective joins, 12% daily aggregations, 3% top-N",
                    <code key="src">examples/bench/workload.toml</code>,
                  ],
                  [
                    "Average query latency on Snowflake",
                    "150 ms",
                    <>
                      Bench median, <code>fixtures/results-real.json</code>
                    </>,
                  ],
                  [
                    "Baseline warehouse",
                    <>
                      One shared LARGE, <code>AUTO_SUSPEND=60</code>
                    </>,
                    <code key="src">
                      docs/internal/WAREHOUSE_MANAGEMENT.md §16
                    </code>,
                  ],
                  [
                    "Credit rate",
                    "$3.00 / credit (Snowflake Standard list)",
                    "Snowflake published pricing",
                  ],
                  [
                    "Lake-eligible fraction",
                    "85%",
                    <>
                      <code>examples/bench/workload.toml</code>{" "}
                      <code>[synthetic].lake_route_fraction</code>
                    </>,
                  ],
                  [
                    "Warehouse credits/hour (XS→2XL)",
                    "1, 2, 4, 8, 16, 32",
                    <>
                      Snowflake published pricing; mirrored in bench{" "}
                      <code>cost.warehouse_credits_per_hour</code>
                    </>,
                  ],
                ]}
              />
              <p>
                Every input is a knob in the calculator. Defaults match this
                table.
              </p>

              {/* Cost model */}
              <h2>The cost model</h2>
              <p>
                Every query on Snowflake bills warehouse credits proportional
                to its active-warehouse seconds:
              </p>
              <ProsePre>{`cost(query) = (latency_seconds / 3600) × credits_per_hour(warehouse) × credit_rate`}</ProsePre>
              <p>
                This is the same model the bench harness uses (see{" "}
                <code>examples/bench/README.md §Cost model</code>), and it
                sanity-checks against the observed bench numbers: 100 queries ×
                142 ms × 8 credits/h × $3 / 3600 ≈ $0.095, vs $0.102 measured.
              </p>
              <p>
                Lake-routed queries run on DuckDB locally over Parquet on S3 —{" "}
                <strong>zero Snowflake credits</strong>. The Melt host&apos;s
                compute is real but small, and out of scope for v1.
              </p>

              <h3>Lake-routing-only savings</h3>
              <p>
                For a workload of N queries with lake-eligible fraction{" "}
                <code>f</code>:
              </p>
              <ProsePre>{`baseline_cost = N × avg_latency × credits_per_hour × credit_rate / 3600
melt_cost     = (1 − f) × baseline_cost
savings       = f × baseline_cost`}</ProsePre>
              <p>
                Plugging the canonical inputs (100k/mo, 150 ms, L, $3, f=0.85):
              </p>
              <ul>
                <li>
                  baseline = 100,000 × 0.150 × 8 × 3 / 3600 ={" "}
                  <strong>$100/month</strong>
                </li>
                <li>
                  Melt = 0.15 × $100 = <strong>$15/month</strong>
                </li>
                <li>
                  savings = <strong>$85/month, 85% reduction</strong>
                </li>
              </ul>

              <h3>Warehouse-routing savings on the Snowflake residual</h3>
              <p>
                Per <code>docs/internal/WAREHOUSE_MANAGEMENT.md §16</code>,
                right-sizing each statement to its smallest sufficient
                warehouse converts a shared-LARGE baseline into a mixed pool.
                Weighted-mean credits/hour for the canonical mix:
              </p>
              <ProsePre>{`mix_credits_per_hour = 0.60×1 + 0.25×2 + 0.12×4 + 0.03×8 = 1.82
ratio                = 1.82 / 8 = 22.75%
warehouse_savings    = 77.25% on the Snowflake-residual portion`}</ProsePre>

              <h3>Combined savings</h3>
              <p>Combined savings as a fraction of baseline spend:</p>
              <ProsePre>{`savings_fraction = f + (1 − f) × (1 − mix_ratio)
                 = f + (1 − f) × 0.7725`}</ProsePre>
              <p>On the canonical inputs (f = 0.85):</p>
              <ProsePre>{`savings_fraction = 0.85 + 0.15 × 0.7725 = 96.6%
combined_melt    = $3.41/month
combined_savings = $96.59/month`}</ProsePre>
            </div>
          </div>
        </section>

        {/* Calculator */}
        <CostCalculator />

        {/* Sensitivity + caveats + references */}
        <section className="relative py-16 md:py-20">
          <div className="mx-auto max-w-3xl px-6">
            <div className="prose-melt">
              <h2>Sensitivity table</h2>
              <ProseTable
                head={[
                  "Lake fraction f",
                  "Warehouse routing",
                  "Net savings",
                  "Monthly Melt cost (canonical)",
                ]}
                rows={[
                  ["0.50", "off", "50.0%", "$50.00"],
                  ["0.65", "off", "65.0%", "$35.00"],
                  ["0.85", "off", "85.0%", "$15.00"],
                  ["0.50", "on", "88.6%", "$11.40"],
                  ["0.65", "on", "92.0%", "$7.97"],
                  ["0.85", "on", "96.6%", "$3.41"],
                ]}
              />
              <p>
                <strong>The hero&apos;s 75% is the conservative floor.</strong>{" "}
                It holds:
              </p>
              <ul>
                <li>At lake fraction ≥ 0.75 with warehouse routing off; or</li>
                <li>At lake fraction ≥ 0.32 with warehouse routing on; or</li>
                <li>
                  At a Medium-baseline warehouse with lake fraction ≥ 0.75.
                </li>
              </ul>
              <p>
                It is not the upper bound — the upper bound for the canonical
                case is 96.6%.
              </p>

              <h2>
                What the figure does <em>not</em> include
              </h2>
              <ul>
                <li>
                  <strong>Melt host compute.</strong> DuckDB cycles on the Melt
                  host are real but trivial vs. Snowflake credits at the scale
                  Melt is built for. Not counted.
                </li>
                <li>
                  <strong>Sync infrastructure cost.</strong> Lake routing only
                  works if the underlying tables are synced to object storage.
                  S3 (or equivalent) storage + transfer is a real line item;
                  for the design-partner cohort it&apos;s ~1–3% of the
                  displaced Snowflake spend. Not counted in the headline.
                </li>
                <li>
                  <strong>Cold-start tax.</strong> A warehouse that has fully
                  suspended pays a 1-minute credit floor when it wakes. The
                  model assumes the warehouse is already warm for the
                  Snowflake-residual fraction; if the fall-through traffic is
                  sparse, real-world savings are 5–15% lower until
                  per-statement warehouse routing ships.
                </li>
                <li>
                  <strong>Edition multipliers and consumption commitments.</strong>{" "}
                  Real bills include Enterprise/Business-Critical multipliers,
                  serverless adjustments, and contractual rate discounts.
                  Override <code>credit_rate</code> in the calculator to match
                  a specific contract.
                </li>
              </ul>

              <h2>What changes the figure</h2>
              <ul>
                <li>
                  <strong>Workload shape.</strong> A workload that&apos;s
                  already 100% large aggregates can&apos;t be lake-routed and
                  won&apos;t move with Melt; conversely, an LLM-agent workload
                  with 95%+ small reads compresses harder.
                </li>
                <li>
                  <strong>Lake-eligible fraction.</strong> Tables not in the
                  sync allowlist, queries in <code>enforce</code> policy mode
                  that hit policy-protected tables, and queries with
                  Snowflake-specific features (semi-structured{" "}
                  <code>FLATTEN</code>, scripting, external functions) all fall
                  through to passthrough.
                </li>
                <li>
                  <strong>Baseline warehouse size.</strong> Customers who
                  already right-sized warehouses on their own see less from
                  warehouse routing. Customers running everything on a shared
                  LARGE or larger see the full §16 effect.
                </li>
                <li>
                  <strong>Workload concurrency / cold-start frequency.</strong>{" "}
                  A bursty agent fleet that wakes the warehouse repeatedly pays
                  the cold-start floor more often than the model assumes.
                </li>
              </ul>
              <p>
                Re-measure with the bench harness (<code>examples/bench/</code>
                ) on any workload you care about — same model, your numbers.
              </p>

              <h2>How the hero number stays honest</h2>
              <ul>
                <li>
                  The hero copy quotes the <strong>floor</strong>, not the
                  median or upper bound.
                </li>
                <li>
                  The hero copy links to this page from &ldquo;75%&rdquo;.
                </li>
                <li>
                  FAQ schema (homepage) is workload-dependent:{" "}
                  <em>
                    &ldquo;Savings depend on workload shape, lake-eligible
                    fraction, and baseline warehouse size. See the methodology
                    page for the math and a calculator.&rdquo;
                  </em>
                </li>
                <li>
                  Every blog or comparison number that quotes a{" "}
                  <code>%</code> reduction either:
                  <ol className="list-decimal pl-5 mt-1 space-y-1">
                    <li>
                      cites this page and uses one of the rows in the
                      sensitivity table, or
                    </li>
                    <li>
                      publishes its own workload shape + inputs in the same
                      paragraph.
                    </li>
                  </ol>
                </li>
              </ul>

              <h2>References</h2>
              <ul>
                <li>
                  Bench harness:{" "}
                  <a
                    href="https://github.com/citguru/melt/tree/main/examples/bench"
                    target="_blank"
                    rel="noreferrer"
                  >
                    <code>examples/bench/README.md</code>
                  </a>
                  , <code>examples/bench/workload.toml</code>,{" "}
                  <code>examples/bench/fixtures/results-real.json</code>
                </li>
                <li>
                  Warehouse routing design + cost arithmetic:{" "}
                  <code>docs/internal/WAREHOUSE_MANAGEMENT.md §16</code>
                </li>
                <li>
                  Repo readme:{" "}
                  <a
                    href="https://github.com/citguru/melt#readme"
                    target="_blank"
                    rel="noreferrer"
                  >
                    <code>readme.md</code>
                  </a>
                </li>
              </ul>
            </div>
          </div>
        </section>

        {/* Closing CTA */}
        <section className="relative overflow-hidden py-24 md:py-32 bg-sky-soft">
          <Cloud className="absolute -left-16 top-10 w-[280px] opacity-80 drift-slow" />
          <Cloud className="absolute right-0 top-20 w-[240px] opacity-70 drift-slow-2" />

          <div className="relative mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-6">
            <h2 className="text-4xl md:text-5xl font-semibold tracking-tight text-ink leading-[1.05]">
              Run the numbers against your account.
            </h2>
            <p className="text-lg text-ink-2 max-w-xl">
              Bring your workload shape and credit rate. We&apos;ll walk
              through the bench harness with you and share where the savings
              land before you commit to a deployment.
            </p>
            <div className="flex flex-col sm:flex-row items-center gap-3">
              <PrimaryCTA href="/contact-us">Book a cost review</PrimaryCTA>
              <GhostCTA href="/blog">Read the engineering blog</GhostCTA>
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}

function ProseTable({
  head,
  rows,
}: {
  head: React.ReactNode[];
  rows: React.ReactNode[][];
}) {
  return (
    <div className="not-prose my-6 overflow-x-auto rounded-2xl border border-line bg-white soft-shadow">
      <table className="w-full text-sm">
        <thead className="bg-bg-2">
          <tr>
            {head.map((h, i) => (
              <th
                key={i}
                className="text-left px-4 py-3 font-medium text-muted whitespace-nowrap"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} className="border-t border-line align-top">
              {row.map((cell, ci) => (
                <td key={ci} className="px-4 py-3 text-ink-2">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ProsePre({ children }: { children: string }) {
  return (
    <pre className="not-prose my-5 overflow-x-auto rounded-2xl border border-line bg-bg-2 px-4 py-3 text-sm text-ink leading-relaxed font-mono">
      <code>{children}</code>
    </pre>
  );
}
