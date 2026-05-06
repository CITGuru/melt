"use client";

import { useState } from "react";
import { ArrowRight, SectionHeader } from "../UI";
import Link from "next/link";

type Tier = {
  name: string;
  tagline: string;
  monthly: number | null;
  annually: number | null;
  priceLabel?: string;
  features: string[];
  cta: string;
  ctaHref: string;
  highlighted?: boolean;
};

const tiers: Tier[] = [
  {
    name: "Melt Basic",
    tagline: "For solo data work and side projects.",
    monthly: 0,
    annually: 0,
    priceLabel: "Free",
    features: [
      "Self-hosted proxy + sync",
      "Unlimited tables",
      "Per-query routing",
      "DuckLake & Iceberg backends",
      "Community support",
    ],
    cta: "Try Melt free",
    ctaHref: "/contact-us",
  },
  {
    name: "Melt Premium",
    tagline: "For pro teams and growing data orgs.",
    monthly: 189,
    annually: 87,
    features: [
      "Everything in Basic",
      "Hosted control plane",
      "Single-tenant data plane",
      "Cost attribution + budgets",
      "Email + Slack support",
    ],
    cta: "Get started",
    ctaHref: "/contact-us",
    highlighted: true,
  },
  {
    name: "Melt Enterprise",
    tagline: "For regulated and high-scale teams.",
    monthly: null,
    annually: null,
    priceLabel: "Flexible",
    features: [
      "Everything in Premium",
      "SOC 2, BAA, custom DPA",
      "Dedicated solutions engineer",
      "SSO/SAML + custom backends",
      "24×7 on-call SLA",
    ],
    cta: "Contact sales",
    ctaHref: "/contact-us",
  },
];

export function Pricing() {
  const [annual, setAnnual] = useState(true);

  return (
    <section id="pricing" className="relative py-24 md:py-32">
      <div className="mx-auto max-w-6xl px-6">
        <div className="flex flex-col items-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">pricing</span>
          <SectionHeader
            title={
              <>
                Simple plans
                <br />
                for serious data work.
              </>
            }
          />
          <div
            role="tablist"
            aria-label="Billing cadence"
            className="mt-4 inline-flex items-center gap-1 rounded-full bg-white border border-line p-1 text-sm soft-shadow"
          >
            <button
              role="tab"
              aria-selected={annual}
              onClick={() => setAnnual(true)}
              className={`px-5 py-1.5 rounded-full transition-colors ${
                annual ? "bg-ink text-white" : "text-ink-2"
              }`}
            >
              Annually
            </button>
            <button
              role="tab"
              aria-selected={!annual}
              onClick={() => setAnnual(false)}
              className={`px-5 py-1.5 rounded-full transition-colors ${
                !annual ? "bg-ink text-white" : "text-ink-2"
              }`}
            >
              Monthly
            </button>
            <span className="ml-1 mr-2 text-[10px] font-medium uppercase tracking-[0.16em] text-accent">
              Save 20%
            </span>
          </div>
        </div>

        <div className="mt-14 grid md:grid-cols-3 gap-5 items-stretch">
          {tiers.map((t) => {
            const price =
              t.priceLabel ?? (annual ? `$${t.annually}/mo` : `$${t.monthly}/mo`);
            return (
              <div
                key={t.name}
                className={`relative rounded-3xl p-7 md:p-8 flex flex-col gap-6 ${
                  t.highlighted
                    ? "bg-ink text-white border border-ink soft-shadow-lg"
                    : "bg-white text-ink border border-line soft-shadow"
                }`}
              >
                {t.highlighted ? (
                  <span className="absolute -top-3 left-7 text-[10px] uppercase tracking-[0.18em] bg-accent text-white px-2.5 py-1 rounded-full">
                    Most popular
                  </span>
                ) : null}
                <div className="flex flex-col gap-1">
                  <span className={`text-sm font-medium ${t.highlighted ? "text-white" : "text-ink"}`}>
                    {t.name}
                  </span>
                  <span className={`text-sm ${t.highlighted ? "text-white/70" : "text-muted"}`}>
                    {t.tagline}
                  </span>
                </div>
                <div className="flex flex-col gap-1">
                  <span className="text-5xl font-semibold tracking-tight leading-none">
                    {price}
                  </span>
                  {!t.priceLabel ? (
                    <span
                      className={`text-xs ${t.highlighted ? "text-white/60" : "text-muted"}`}
                    >
                      {annual ? "billed yearly" : "billed monthly"}
                    </span>
                  ) : null}
                </div>
                <ul className="flex flex-col gap-3 text-sm">
                  {t.features.map((f) => (
                    <li key={f} className="flex items-start gap-3">
                      <Check tinted={t.highlighted} />
                      <span className={t.highlighted ? "text-white/90" : "text-ink-2"}>{f}</span>
                    </li>
                  ))}
                </ul>
                <Link
                  href={t.ctaHref}
                  className={`mt-auto inline-flex items-center justify-center gap-2 rounded-full px-5 py-3 text-sm font-medium transition-colors ${
                    t.highlighted
                      ? "bg-white text-ink hover:bg-bg-2"
                      : "bg-ink text-white hover:bg-ink-2"
                  }`}
                >
                  {t.cta}
                  <ArrowRight />
                </Link>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}

function Check({ tinted }: { tinted?: boolean }) {
  return (
    <span
      className={`mt-0.5 inline-flex h-5 w-5 items-center justify-center rounded-full ${
        tinted ? "bg-white/15 text-white" : "bg-emerald-100 text-emerald-700"
      }`}
    >
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" aria-hidden>
        <path
          d="M5 12l5 5 9-11"
          stroke="currentColor"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </span>
  );
}
