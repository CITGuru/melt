"use client";

import { useState } from "react";
import { SectionHeader } from "../UI";
import { LaptopMockup, PhoneMockup } from "../Mockups";

export function DeviceSync() {
  const [tab, setTab] = useState<"web" | "mobile">("web");
  return (
    <section className="relative py-24 md:py-32">
      <div className="mx-auto max-w-6xl px-6">
        <div className="flex flex-col items-center text-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            Seamless across surfaces
          </span>
          <SectionHeader
            title={
              <>
                Run melt anywhere,
                <br />
                stay in sync.
              </>
            }
            description="Drop the proxy in your VPC, watch the dashboard from your laptop, and pull live routing stats on the go. One control plane, every surface."
          />
          <div
            role="tablist"
            aria-label="Surface"
            className="mt-4 inline-flex items-center gap-1 rounded-full bg-white border border-line p-1 text-sm soft-shadow"
          >
            <button
              role="tab"
              aria-selected={tab === "web"}
              onClick={() => setTab("web")}
              className={`px-5 py-1.5 rounded-full transition-colors ${
                tab === "web" ? "bg-ink text-white" : "text-ink-2"
              }`}
            >
              Web App
            </button>
            <button
              role="tab"
              aria-selected={tab === "mobile"}
              onClick={() => setTab("mobile")}
              className={`px-5 py-1.5 rounded-full transition-colors ${
                tab === "mobile" ? "bg-ink text-white" : "text-ink-2"
              }`}
            >
              Mobile App
            </button>
          </div>
        </div>

        <div className="mt-14 flex justify-center">
          {tab === "web" ? <LaptopMockup /> : <PhoneMockup />}
        </div>
      </div>
    </section>
  );
}
