"use client";

import { useMemo, useState } from "react";
import { Pill, PrimaryCTA, SectionHeader } from "@/components/UI";

type Size = "XS" | "S" | "M" | "L" | "XL" | "2XL";

const SIZES: Size[] = ["XS", "S", "M", "L", "XL", "2XL"];

const CREDITS_PER_HOUR: Record<Size, number> = {
  XS: 1,
  S: 2,
  M: 4,
  L: 8,
  XL: 16,
  "2XL": 32,
};

const DEFAULTS = {
  queriesPerDay: 3333,
  warehouse: "L" as Size,
  lakeFractionPct: 85,
  creditRate: 3.0,
  warehouseRoutingOn: false,
  avgLatencyMs: 150,
  mix: { XS: 60, S: 25, M: 12, L: 3 },
};

function formatMonthly(n: number) {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 100) {
    return `$${Math.round(n).toLocaleString("en-US")}`;
  }
  return `$${n.toFixed(2)}`;
}

function formatAnnual(n: number) {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1000) {
    return `$${Math.round(n).toLocaleString("en-US")}`;
  }
  return `$${n.toFixed(2)}`;
}

function formatPct(n: number) {
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function clampNumber(value: number, min: number, max: number) {
  if (Number.isNaN(value)) return min;
  return Math.min(max, Math.max(min, value));
}

export function CostCalculator() {
  const [queriesPerDay, setQueriesPerDay] = useState<number>(
    DEFAULTS.queriesPerDay,
  );
  const [warehouse, setWarehouse] = useState<Size>(DEFAULTS.warehouse);
  const [lakeFractionPct, setLakeFractionPct] = useState<number>(
    DEFAULTS.lakeFractionPct,
  );
  const [creditRate, setCreditRate] = useState<number>(DEFAULTS.creditRate);
  const [warehouseRoutingOn, setWarehouseRoutingOn] = useState<boolean>(
    DEFAULTS.warehouseRoutingOn,
  );

  const [showAdvanced, setShowAdvanced] = useState<boolean>(false);
  const [avgLatencyMs, setAvgLatencyMs] = useState<number>(
    DEFAULTS.avgLatencyMs,
  );
  const [mixXS, setMixXS] = useState<number>(DEFAULTS.mix.XS);
  const [mixS, setMixS] = useState<number>(DEFAULTS.mix.S);
  const [mixM, setMixM] = useState<number>(DEFAULTS.mix.M);
  const [mixL, setMixL] = useState<number>(DEFAULTS.mix.L);

  const mixSum = mixXS + mixS + mixM + mixL;
  const mixValid = mixSum === 100;

  const result = useMemo(() => {
    if (!Number.isFinite(queriesPerDay) || queriesPerDay <= 0) return null;

    const queriesPerMonth = queriesPerDay * 30;
    const avgLatencySec = avgLatencyMs / 1000;
    const baselineCreditsPH = CREDITS_PER_HOUR[warehouse];
    const f = lakeFractionPct / 100;

    const baseline =
      (queriesPerMonth * avgLatencySec * baselineCreditsPH * creditRate) / 3600;

    const mixCreditsPH =
      (mixXS / 100) * CREDITS_PER_HOUR.XS +
      (mixS / 100) * CREDITS_PER_HOUR.S +
      (mixM / 100) * CREDITS_PER_HOUR.M +
      (mixL / 100) * CREDITS_PER_HOUR.L;

    const routingActive = warehouseRoutingOn && mixValid;
    const mixRatio = routingActive ? mixCreditsPH / baselineCreditsPH : 1;

    const meltCost = baseline * (1 - f) * mixRatio;
    const savings = baseline - meltCost;
    const savingsPct = baseline > 0 ? savings / baseline : 0;
    const annualSavings = savings * 12;

    return {
      baseline,
      meltCost,
      savings,
      savingsPct,
      annualSavings,
      routingActive,
    };
  }, [
    queriesPerDay,
    warehouse,
    lakeFractionPct,
    creditRate,
    warehouseRoutingOn,
    avgLatencyMs,
    mixXS,
    mixS,
    mixM,
    mixL,
    mixValid,
  ]);

  const isCanonical =
    queriesPerDay === DEFAULTS.queriesPerDay &&
    warehouse === DEFAULTS.warehouse &&
    lakeFractionPct === DEFAULTS.lakeFractionPct &&
    creditRate === DEFAULTS.creditRate &&
    avgLatencyMs === DEFAULTS.avgLatencyMs &&
    mixXS === DEFAULTS.mix.XS &&
    mixS === DEFAULTS.mix.S &&
    mixM === DEFAULTS.mix.M &&
    mixL === DEFAULTS.mix.L;

  return (
    <section className="relative py-20 md:py-28 bg-bg-2 border-y border-line">
      <div className="mx-auto max-w-6xl px-6 flex flex-col gap-12">
        <div className="flex flex-col items-center gap-2">
          <SectionHeader
            eyebrow="Melt savings calculator"
            title={<>Plug in your workload. Get your number.</>}
            description="The math is straightforward — latency, credits per hour, and the fraction of queries that can land on the lake. Defaults reproduce the canonical workload in the methodology above. Change them to match your account."
          />
          {isCanonical ? (
            <div className="mt-2">
              <Pill>Canonical workload</Pill>
            </div>
          ) : null}
        </div>

        <div className="grid gap-8 lg:grid-cols-12">
          {/* Inputs column */}
          <div className="lg:col-span-5 bg-white rounded-3xl border border-line soft-shadow p-6 md:p-7 flex flex-col gap-5">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">
              Inputs
            </span>

            <Field
              label="Queries per day"
              hint="Total Snowflake statements your workload issues per day."
            >
              <input
                type="number"
                inputMode="numeric"
                min={1}
                max={1_000_000}
                step={1}
                value={queriesPerDay}
                onChange={(e) =>
                  setQueriesPerDay(
                    clampNumber(Number(e.target.value), 1, 1_000_000),
                  )
                }
                className="w-full rounded-xl border border-line bg-white px-3 py-2 text-base text-ink focus:outline-none focus:ring-2 focus:ring-ink/15"
              />
            </Field>

            <Field
              label="Baseline warehouse"
              hint="Size of the shared warehouse you run today."
            >
              <select
                value={warehouse}
                onChange={(e) => setWarehouse(e.target.value as Size)}
                className="w-full rounded-xl border border-line bg-white px-3 py-2 text-base text-ink focus:outline-none focus:ring-2 focus:ring-ink/15"
              >
                {SIZES.map((s) => (
                  <option key={s} value={s}>
                    {s} ({CREDITS_PER_HOUR[s]} credits/h)
                  </option>
                ))}
              </select>
            </Field>

            <Field
              label={`Lake-routed fraction · ${lakeFractionPct}%`}
              hint="Share of queries the lakehouse engine can answer on its own."
            >
              <input
                type="range"
                min={0}
                max={100}
                step={1}
                value={lakeFractionPct}
                onChange={(e) =>
                  setLakeFractionPct(
                    clampNumber(Number(e.target.value), 0, 100),
                  )
                }
                className="w-full accent-ink"
              />
            </Field>

            <Field
              label="Credit rate ($/credit)"
              hint="Override to match your contract; defaults to Snowflake Standard list."
            >
              <input
                type="number"
                inputMode="decimal"
                min={0.5}
                max={10}
                step={0.1}
                value={creditRate}
                onChange={(e) =>
                  setCreditRate(clampNumber(Number(e.target.value), 0.5, 10))
                }
                className="w-full rounded-xl border border-line bg-white px-3 py-2 text-base text-ink focus:outline-none focus:ring-2 focus:ring-ink/15"
              />
            </Field>

            <div className="flex items-start justify-between gap-4 rounded-2xl border border-line bg-bg-2 px-4 py-3">
              <div className="flex flex-col">
                <span className="text-sm font-medium text-ink">
                  Warehouse routing
                </span>
                <span className="text-xs text-muted">
                  Right-size each Snowflake-residual query to its smallest
                  sufficient warehouse.
                </span>
              </div>
              <button
                type="button"
                role="switch"
                aria-checked={warehouseRoutingOn}
                disabled={!mixValid}
                onClick={() => setWarehouseRoutingOn((v) => !v)}
                className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
                  warehouseRoutingOn && mixValid
                    ? "bg-ink"
                    : "bg-line-2"
                } ${!mixValid ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}`}
              >
                <span
                  className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                    warehouseRoutingOn && mixValid
                      ? "translate-x-6"
                      : "translate-x-1"
                  }`}
                />
              </button>
            </div>

            <button
              type="button"
              onClick={() => setShowAdvanced((v) => !v)}
              className="self-start text-sm font-medium text-ink-2 hover:text-ink transition-colors"
            >
              {showAdvanced ? "Hide" : "Show"} advanced inputs
            </button>

            {showAdvanced ? (
              <div className="flex flex-col gap-5 border-t border-line pt-5">
                <Field
                  label="Avg query latency (ms)"
                  hint="Median Snowflake execution time across the workload."
                >
                  <input
                    type="number"
                    inputMode="numeric"
                    min={10}
                    max={30_000}
                    step={1}
                    value={avgLatencyMs}
                    onChange={(e) =>
                      setAvgLatencyMs(
                        clampNumber(Number(e.target.value), 10, 30_000),
                      )
                    }
                    className="w-full rounded-xl border border-line bg-white px-3 py-2 text-base text-ink focus:outline-none focus:ring-2 focus:ring-ink/15"
                  />
                </Field>

                <div className="flex flex-col gap-2">
                  <span className="text-sm font-medium text-ink">
                    Workload mix (XS / S / M / L %)
                  </span>
                  <div className="grid grid-cols-4 gap-2">
                    {[
                      { label: "XS", value: mixXS, set: setMixXS },
                      { label: "S", value: mixS, set: setMixS },
                      { label: "M", value: mixM, set: setMixM },
                      { label: "L", value: mixL, set: setMixL },
                    ].map((m) => (
                      <label key={m.label} className="flex flex-col gap-1">
                        <span className="text-xs text-muted">{m.label}</span>
                        <input
                          type="number"
                          inputMode="numeric"
                          min={0}
                          max={100}
                          step={1}
                          value={m.value}
                          onChange={(e) =>
                            m.set(clampNumber(Number(e.target.value), 0, 100))
                          }
                          className="w-full rounded-xl border border-line bg-white px-3 py-2 text-base text-ink focus:outline-none focus:ring-2 focus:ring-ink/15"
                        />
                      </label>
                    ))}
                  </div>
                  {!mixValid ? (
                    <span className="text-xs text-accent">
                      Mix must sum to 100 (currently {mixSum}). Warehouse
                      routing disabled until corrected.
                    </span>
                  ) : (
                    <span className="text-xs text-muted">
                      Sum: {mixSum}%
                    </span>
                  )}
                </div>
              </div>
            ) : null}
          </div>

          {/* Outputs column */}
          <div className="lg:col-span-7 flex flex-col gap-5">
            <div className="flex items-center justify-between">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                Estimated monthly impact
              </span>
              {result?.routingActive ? (
                <Pill>Warehouse routing on</Pill>
              ) : null}
            </div>

            <div className="grid gap-4 md:grid-cols-3">
              <article className="bg-sky-soft rounded-3xl border border-line soft-shadow p-6 flex flex-col gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-muted">
                  $ / month saved
                </span>
                <span className="text-5xl md:text-6xl font-semibold tracking-tight text-ink leading-none">
                  {result ? formatMonthly(result.savings) : "—"}
                </span>
                <span className="text-xs text-muted">
                  vs Snowflake passthrough
                </span>
              </article>

              <article className="bg-white rounded-3xl border border-line soft-shadow p-6 flex flex-col gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-muted">
                  % reduction
                </span>
                <span className="text-5xl md:text-6xl font-semibold tracking-tight text-ink leading-none">
                  {result ? formatPct(result.savingsPct) : "—"}
                </span>
                <span className="text-xs text-muted">
                  of credit spend on this workload
                </span>
              </article>

              <article className="bg-white rounded-3xl border border-line soft-shadow p-6 flex flex-col gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-muted">
                  Annualized savings
                </span>
                <span className="text-3xl md:text-4xl font-semibold tracking-tight text-ink leading-none">
                  {result ? formatAnnual(result.annualSavings) : "—"}
                </span>
                <span className="text-xs text-muted">
                  if the workload shape holds
                </span>
              </article>
            </div>

            <div className="bg-white rounded-3xl border border-line soft-shadow overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-bg-2">
                    <tr>
                      <th className="text-left px-5 py-3 font-medium text-muted">
                        Path
                      </th>
                      <th className="text-right px-5 py-3 font-medium text-muted">
                        $ / month
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-t border-line">
                      <td className="px-5 py-3 text-ink-2">
                        Snowflake passthrough (baseline)
                      </td>
                      <td className="px-5 py-3 text-right font-medium text-ink">
                        {result ? formatMonthly(result.baseline) : "—"}
                      </td>
                    </tr>
                    <tr className="border-t border-line">
                      <td className="px-5 py-3 text-ink-2">Melt-routed</td>
                      <td className="px-5 py-3 text-right font-medium text-ink">
                        {result ? formatMonthly(result.meltCost) : "—"}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <p className="text-xs text-muted leading-relaxed">
              Numbers assume a flat $/credit and an already-warm warehouse.
              Cold-start tax and edition multipliers can shift the result by
              5–15% in either direction — see the methodology for the full
              caveats.
            </p>

            <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3 pt-2">
              <PrimaryCTA href="/contact-us">Book a 30-min cost review</PrimaryCTA>
              <a
                href="https://github.com/citguru/melt/tree/main/examples/bench"
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-2 rounded-full bg-white/70 backdrop-blur border border-line px-5 py-3 text-sm font-medium text-ink hover:bg-white transition-colors"
              >
                Run the bench on your own workload
              </a>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="flex flex-col gap-1.5">
      <span className="text-sm font-medium text-ink">{label}</span>
      {children}
      {hint ? <span className="text-xs text-muted">{hint}</span> : null}
    </label>
  );
}
