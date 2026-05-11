"use client";

import { useState, type FormEvent } from "react";
import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { Cloud } from "@/components/Clouds";
import { ArrowRight } from "@/components/UI";

type Status = "idle" | "submitting" | "success";

export default function ContactPage() {
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (status === "submitting") return;
    setError(null);
    setStatus("submitting");

    const form = e.currentTarget;
    const data = new FormData(form);
    const payload = {
      name: String(data.get("name") ?? ""),
      email: String(data.get("email") ?? ""),
      company: String(data.get("company") ?? ""),
      size: String(data.get("size") ?? ""),
      message: String(data.get("message") ?? ""),
    };

    try {
      const res = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        setStatus("success");
        return;
      }
      const body = (await res.json().catch(() => ({}))) as { error?: string };
      setError(body.error || "Something went wrong. Please try again or email hello@meltcomputing.com.");
      setStatus("idle");
    } catch {
      setError("Network error. Please try again or email hello@meltcomputing.com.");
      setStatus("idle");
    }
  }

  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <section className="relative pt-36 md:pt-44 pb-20 md:pb-28 bg-sky overflow-hidden">
          <Cloud className="absolute -left-16 top-32 w-[300px] opacity-80 drift-slow" />
          <Cloud className="absolute right-0 top-44 w-[260px] opacity-70 drift-slow-2" />
          <div className="relative mx-auto max-w-6xl px-6 grid lg:grid-cols-12 gap-12 items-start">
            <div className="lg:col-span-5 flex flex-col gap-5">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                contact
              </span>
              <h1 className="text-5xl md:text-6xl font-semibold tracking-tight text-ink leading-[1.04]">
                Let’s melt
                <br />
                your bill.
              </h1>
              <p className="text-lg text-ink-2 leading-relaxed max-w-md">
                Tell us about your warehouse setup and we’ll get back within
                one business day. Onboarding for design partners is free.
              </p>

              <div className="mt-4 flex flex-col gap-3 text-sm">
                <ContactRow
                  label="Email"
                  value="hello@meltcomputing.com"
                  href="mailto:hello@meltcomputing.com"
                />
                <ContactRow
                  label="GitHub"
                  value="github.com/citguru/melt"
                  href="https://github.com/citguru/melt"
                />
                {/* Discord row hidden until we have a real invite link */}
              </div>
            </div>

            <div className="lg:col-span-7">
              <div className="bg-white rounded-3xl border border-line soft-shadow p-6 md:p-8">
                {status === "success" ? (
                  <div className="flex flex-col items-center gap-4 py-12 text-center">
                    <span className="inline-flex h-14 w-14 items-center justify-center rounded-full bg-emerald-100 text-emerald-700">
                      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden>
                        <path d="M5 12l5 5 9-11" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </span>
                    <h2 className="text-2xl font-semibold tracking-tight">
                      Got it &mdash; thanks.
                    </h2>
                    <p className="text-muted max-w-sm">
                      We’ve emailed you a confirmation. We’ll be in touch
                      within one business day.
                    </p>
                  </div>
                ) : (
                  <form onSubmit={onSubmit} className="flex flex-col gap-4" noValidate>
                    <h2 className="text-2xl font-semibold tracking-tight">
                      Tell us a bit about your setup
                    </h2>
                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="Name" id="name" name="name" required />
                      <Field
                        label="Work email"
                        id="email"
                        name="email"
                        type="email"
                        required
                        placeholder="you@company.com"
                      />
                    </div>
                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="Company" id="company" name="company" />
                      <Select
                        label="Warehouse size"
                        id="size"
                        name="size"
                        options={[
                          "< $10k / month",
                          "$10–$50k / month",
                          "$50–$250k / month",
                          "$250k+ / month",
                        ]}
                      />
                    </div>
                    <Field
                      label="What are you trying to route?"
                      id="message"
                      name="message"
                      textarea
                      placeholder="A few lines about your workload, agents, dbt setup, BI tools…"
                    />
                    {error ? (
                      <div
                        role="alert"
                        className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800"
                      >
                        {error}
                      </div>
                    ) : null}
                    <button
                      type="submit"
                      disabled={status === "submitting"}
                      className="mt-2 inline-flex items-center justify-center gap-2 self-start rounded-full bg-ink text-white px-6 py-3 text-sm font-medium hover:bg-ink-2 transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
                    >
                      {status === "submitting" ? "Sending…" : "Send message"}
                      <ArrowRight />
                    </button>
                    <p className="text-xs text-muted">
                      Please use a work email. By submitting you agree to the{" "}
                      <a href="/privacy-policy" className="underline">
                        privacy policy
                      </a>
                      .
                    </p>
                  </form>
                )}
              </div>
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}

function ContactRow({
  label,
  value,
  href,
}: {
  label: string;
  value: string;
  href: string;
}) {
  return (
    <a
      href={href}
      className="bg-white rounded-2xl border border-line soft-shadow px-4 py-3 flex items-center justify-between hover:soft-shadow-lg transition-shadow"
    >
      <span className="flex flex-col">
        <span className="text-[11px] uppercase tracking-[0.18em] text-muted">
          {label}
        </span>
        <span className="font-medium">{value}</span>
      </span>
      <ArrowRight />
    </a>
  );
}

function Field({
  label,
  id,
  name,
  type = "text",
  required,
  textarea,
  placeholder,
}: {
  label: string;
  id: string;
  name: string;
  type?: string;
  required?: boolean;
  textarea?: boolean;
  placeholder?: string;
}) {
  const cls =
    "w-full rounded-2xl border border-line bg-bg-2/40 px-4 py-3 text-sm text-ink placeholder:text-muted-2 focus:outline-none focus:bg-white focus:border-ink transition-colors";
  return (
    <label htmlFor={id} className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-ink-2">{label}</span>
      {textarea ? (
        <textarea id={id} name={name} rows={5} placeholder={placeholder} className={cls} />
      ) : (
        <input id={id} name={name} type={type} required={required} placeholder={placeholder} className={cls} />
      )}
    </label>
  );
}

function Select({
  label,
  id,
  name,
  options,
}: {
  label: string;
  id: string;
  name: string;
  options: string[];
}) {
  return (
    <label htmlFor={id} className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-ink-2">{label}</span>
      <select
        id={id}
        name={name}
        className="w-full rounded-2xl border border-line bg-bg-2/40 px-4 py-3 text-sm text-ink focus:outline-none focus:bg-white focus:border-ink transition-colors"
        defaultValue=""
      >
        <option value="" disabled>
          Pick one
        </option>
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </label>
  );
}
