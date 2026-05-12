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

        {/* Document body, part 1: intro through combined-savings math */}
        <section className="relative py-16 md:py-20">
          <div className="mx-auto max-w-3xl px-6">
            <div className="prose-melt">
              <p>
                Melt cuts a typical agent-shaped Snowflake bill by 75–97% on
                the same query workload. This page shows the canonical
                reference workload, the math model, and the inputs that move
                the number, plus a calculator you can run on your own usage.
              </p>

              {/* TL;DR */}
              <h2>TL;DR</h2>
              <p>
                For an agent-shaped Snowflake workload, Melt reduces spend two
                ways:
              </p>
              <ol className="list-decimal pl-5 my-4 space-y-2">
                <li>
                  <strong>Query routing.</strong> For every statement, decide
                  whether the cheap lakehouse engine can answer it.
                  Lake-routed queries pay <strong>$0</strong> Snowflake
                  credits.
                </li>
                <li>
                  <strong>Warehouse routing.</strong> For the statements that
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
                    "Our public bench harness (linked below)",
                  ],
                  [
                    "Average query latency on Snowflake",
                    "150 ms",
                    "Median observed in our public bench harness",
                  ],
                  [
                    "Baseline warehouse",
                    <>
                      One shared LARGE, <code>AUTO_SUSPEND=60</code>
                    </>,
                    "Snowflake warehouse-sizing analysis",
                  ],
                  [
                    "Credit rate",
                    "$3.00 / credit (Snowflake Standard list)",
                    "Snowflake published pricing",
                  ],
                  [
                    "Lake-eligible fraction",
                    "85%",
                    "Calibrated against our agent-shaped reference workload",
                  ],
                  [
                    "Warehouse credits/hour (XS→2XL)",
                    "1, 2, 4, 8, 16, 32",
                    "Snowflake published pricing",
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
                This is the same model our open-source bench harness uses, and
                it sanity-checks against the observed bench numbers: 100
                queries × 142 ms × 8 credits/h × $3 / 3600 ≈ $0.095, vs $0.102
                measured.
              </p>
              <p>
                Lake-routed queries run on DuckDB locally over Parquet on S3,
                with <strong>zero Snowflake credits</strong>. The Melt host’s
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
                Right-sizing each statement to its smallest sufficient
                warehouse converts a shared-LARGE baseline into a mixed pool.
                Snowflake publishes per-size credit rates (XS=1, S=2, M=4, L=8,
                2XL=16, …); the weighted-mean credits/hour for the canonical
                mix is:
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

        {/* Document body, part 2: floor paragraph onward */}
        <section className="relative py-16 md:py-20">
          <div className="mx-auto max-w-3xl px-6">
            <div className="prose-melt">
              <p>
                <strong>Why the hero quotes 75% as the floor.</strong> With
                warehouse routing off, you’d need a lake-eligible fraction
                below 0.75 to fall under 75% savings. With warehouse routing
                on, you’d need to drop below 0.32. On a Medium-baseline
                warehouse you’d need lake fraction below 0.75. Slide the
                calculator above to test your own workload — the upper bound
                on the canonical inputs is 96.6%.
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
                  for the design-partner cohort it’s ~1–3% of the displaced
                  Snowflake spend. Not counted in the headline.
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
                  <strong>
                    Edition multipliers and consumption commitments.
                  </strong>{" "}
                  Real bills include Enterprise/Business-Critical multipliers,
                  serverless adjustments, and contractual rate discounts.
                  Override <code>credit_rate</code> in the calculator to match
                  a specific contract.
                </li>
              </ul>

              <h2>What changes the figure</h2>
              <ul>
                <li>
                  <strong>Workload shape.</strong> A workload that’s already
                  100% large aggregates can’t be lake-routed and won’t move
                  with Melt; conversely, an LLM-agent workload with 95%+ small
                  reads compresses harder.
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
                  LARGE or larger see the full effect.
                </li>
                <li>
                  <strong>
                    Workload concurrency / cold-start frequency.
                  </strong>{" "}
                  A bursty agent fleet that wakes the warehouse repeatedly
                  pays the cold-start floor more often than the model assumes.
                </li>
              </ul>
              <p>
                Re-measure with our public bench harness on any workload you
                care about. Same model, your numbers.
              </p>

              <h2>How the hero number stays honest</h2>
              <ul>
                <li>
                  The hero copy quotes the <strong>floor</strong>, not the
                  median or upper bound.
                </li>
                <li>
                  The hero copy links to this page from “75%”.
                </li>
                <li>
                  FAQ schema (homepage) is workload-dependent:{" "}
                  <em>
                    “Savings depend on workload shape, lake-eligible fraction,
                    and baseline warehouse size. See the methodology page for
                    the math and a calculator.”
                  </em>
                </li>
                <li>
                  Every blog or comparison number that quotes a{" "}
                  <code>%</code> reduction either:
                  <ol className="list-decimal pl-5 mt-1 space-y-1">
                    <li>
                      cites this page and uses the canonical reference inputs
                      above, or
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
                  Public bench harness:{" "}
                  <a
                    href="https://github.com/CITGuru/melt/tree/main/examples/bench"
                    target="_blank"
                    rel="noreferrer"
                  >
                    github.com/CITGuru/melt/tree/main/examples/bench
                  </a>
                </li>
                <li>
                  Snowflake warehouse pricing:{" "}
                  <a
                    href="https://www.snowflake.com/pricing/"
                    target="_blank"
                    rel="noreferrer"
                  >
                    snowflake.com/pricing
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
              Bring your workload shape and credit rate. We’ll walk through the
              bench harness with you and share where the savings land before
              you commit to a deployment.
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
