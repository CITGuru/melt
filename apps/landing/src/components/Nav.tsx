"use client";

import Link from "next/link";
import { Logo } from "./Logo";
import { useEffect, useRef, useState } from "react";
import { FeatureIcon } from "./FeatureIcons";
import {
  featureCategoryOrder,
  featuresByCategory,
} from "@/lib/features";

type LinkItem = {
  label: string;
  href?: string;
  menu?: "features";
};

const links: LinkItem[] = [
  // { label: "Features", menu: "features" },  // hidden for now — un-comment to restore the dropdown
  { label: "Benefits", href: "/#benefits" },
  { label: "Pricing", href: "/#pricing" },
  { label: "Blog", href: "/blog" },
  { label: "Contact", href: "/contact-us" },
];

export function Nav() {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [mobileFeaturesOpen, setMobileFeaturesOpen] = useState(false);
  const [menu, setMenu] = useState<"features" | null>(null);
  const navRef = useRef<HTMLDivElement>(null);
  const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  function clearCloseTimer() {
    if (closeTimerRef.current) {
      clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }

  function openMenu(name: "features") {
    clearCloseTimer();
    setMenu(name);
  }

  function scheduleClose() {
    clearCloseTimer();
    closeTimerRef.current = setTimeout(() => setMenu(null), 140);
  }

  function closeAll() {
    clearCloseTimer();
    setMenu(null);
    setMobileOpen(false);
    setMobileFeaturesOpen(false);
  }

  useEffect(() => () => clearCloseTimer(), []);

  // Only lock body scroll for the mobile full-screen menu.
  useEffect(() => {
    if (!mobileOpen) {
      document.body.style.overflow = "";
      document.body.style.paddingRight = "";
      return;
    }
    const scrollbarWidth =
      window.innerWidth - document.documentElement.clientWidth;
    document.body.style.overflow = "hidden";
    if (scrollbarWidth > 0) {
      document.body.style.paddingRight = `${scrollbarWidth}px`;
    }
    return () => {
      document.body.style.overflow = "";
      document.body.style.paddingRight = "";
    };
  }, [mobileOpen]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") closeAll();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (!menu) return;
    function onClick(e: MouseEvent) {
      if (!navRef.current) return;
      if (!navRef.current.contains(e.target as Node)) setMenu(null);
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [menu]);

  return (
    <header className="fixed top-4 inset-x-0 z-50 flex justify-center px-4">
      {(menu || mobileOpen) ? (
        <div
          className="fixed inset-0 -z-10 bg-ink/30 backdrop-blur-[2px] animate-fade-in"
          onClick={closeAll}
          aria-hidden
        />
      ) : null}

      <div ref={navRef} className="relative w-full max-w-5xl">
        <nav
          aria-label="Primary"
          className="flex items-center gap-2 rounded-full bg-white/70 backdrop-blur-xl border border-line ring-1 ring-white/40 px-3 py-2 soft-shadow"
        >
          <Link
            href="/"
            className="flex items-center gap-2 px-2 py-1"
            onClick={() => setMenu(null)}
          >
            <Logo />
          </Link>

          <ul className="hidden md:flex items-center gap-0.5 ml-3">
            {links.map((l) =>
              l.menu ? (
                <li key={l.label}>
                  <button
                    type="button"
                    aria-haspopup="true"
                    aria-expanded={menu === l.menu}
                    onClick={() =>
                      setMenu((cur) => (cur === l.menu ? null : l.menu!))
                    }
                    onMouseEnter={() => openMenu(l.menu!)}
                    onMouseLeave={scheduleClose}
                    onFocus={() => openMenu(l.menu!)}
                    className={`inline-flex items-center gap-1 px-3 py-2 text-sm rounded-full transition-colors ${
                      menu === l.menu
                        ? "bg-bg-2 text-ink"
                        : "text-ink-2 hover:text-ink"
                    }`}
                  >
                    {l.label}
                    <Chevron open={menu === l.menu} />
                  </button>
                </li>
              ) : (
                <li key={l.label}>
                  <Link
                    href={l.href!}
                    onClick={() => setMenu(null)}
                    className="px-3 py-2 text-sm text-ink-2 hover:text-ink transition-colors rounded-full"
                  >
                    {l.label}
                  </Link>
                </li>
              )
            )}
          </ul>

          <div className="ml-auto flex items-center gap-2">
            <a
              href="https://github.com/citguru/melt"
              target="_blank"
              rel="noreferrer"
              onClick={() => setMenu(null)}
              className="hidden sm:inline-flex items-center gap-2 rounded-full bg-ink text-white px-4 py-2 text-sm font-medium hover:bg-ink-2 transition-colors"
            >
              Try Melt free
              <Arrow />
            </a>
            <button
              type="button"
              aria-label="Open menu"
              aria-expanded={mobileOpen}
              onClick={() => setMobileOpen((v) => !v)}
              className="md:hidden inline-flex h-10 w-10 items-center justify-center rounded-full border border-line text-ink"
            >
              <svg width="16" height="12" viewBox="0 0 16 12" fill="none">
                <path d="M1 1h14M1 6h14M1 11h14" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
              </svg>
            </button>
          </div>
        </nav>

        {/* Desktop Features dropdown — 3 columns × 2 items */}
        {menu === "features" ? (
          <div
            role="menu"
            aria-label="Features"
            onMouseEnter={() => openMenu("features")}
            onMouseLeave={scheduleClose}
            className="hidden md:block absolute inset-x-0 top-full pt-2.5 animate-fade-up"
          >
            <div className="rounded-3xl bg-white border border-line soft-shadow-lg p-3">
              <div className="grid grid-cols-3 gap-1">
                {featureCategoryOrder.map((category) => (
                  <div key={category} className="flex flex-col">
                    <span className="px-3 pt-2 pb-1 text-[10px] font-medium uppercase tracking-[0.18em] text-ink-2/60">
                      {category}
                    </span>
                    <div className="flex flex-col gap-0.5">
                      {featuresByCategory[category].map((item) => (
                        <Link
                          key={item.slug}
                          href={`/features/${item.slug}`}
                          role="menuitem"
                          onClick={() => setMenu(null)}
                          className="group flex items-start gap-3 p-3 rounded-2xl hover:bg-bg-2 transition-colors"
                        >
                          <span className="mt-0.5 inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-bg-2 text-ink group-hover:bg-ink group-hover:text-white transition-colors">
                            <FeatureIcon name={item.iconName} />
                          </span>
                          <span className="flex flex-col gap-0.5 min-w-0">
                            <span className="flex items-center gap-2">
                              <span className="text-sm font-medium text-ink">
                                {item.title}
                              </span>
                              {item.status === "alpha" ? <AlphaPill /> : null}
                            </span>
                            <span className="text-xs text-muted leading-snug">
                              {item.shortDescription}
                            </span>
                          </span>
                        </Link>
                      ))}
                    </div>
                  </div>
                ))}
              </div>

              <div className="mt-2 flex items-center justify-between rounded-2xl bg-bg-2 px-4 py-3">
                <span className="text-xs text-muted">
                  Looking for the full feature breakdown?
                </span>
                <Link
                  href="/#features"
                  onClick={() => setMenu(null)}
                  className="inline-flex items-center gap-1.5 text-sm font-medium text-ink hover:text-ink-2"
                >
                  See all features
                  <Arrow />
                </Link>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      {/* Mobile menu */}
      {mobileOpen ? (
        <div
          className="md:hidden fixed inset-x-0 top-20 px-4 z-40 max-h-[calc(100dvh-6rem)] overflow-y-auto animate-fade-up"
          onClick={(e) => {
            if (e.target === e.currentTarget) closeAll();
          }}
        >
          <div className="rounded-3xl bg-white border border-line soft-shadow-lg p-3 max-w-5xl mx-auto">
            <ul className="flex flex-col gap-0.5">
              {links.map((l) =>
                l.menu === "features" ? (
                  <li key={l.label} className="flex flex-col">
                    <button
                      type="button"
                      aria-expanded={mobileFeaturesOpen}
                      onClick={() => setMobileFeaturesOpen((v) => !v)}
                      className="flex items-center justify-between w-full px-4 py-3 text-base text-ink rounded-2xl hover:bg-bg-2"
                    >
                      <span>{l.label}</span>
                      <Chevron open={mobileFeaturesOpen} />
                    </button>
                    {mobileFeaturesOpen ? (
                      <div className="flex flex-col pt-1 pb-2 pl-2">
                        {featureCategoryOrder.map((category) => (
                          <div key={category} className="flex flex-col">
                            <span className="px-3 pt-3 pb-1 text-[10px] font-medium uppercase tracking-[0.18em] text-ink-2/60">
                              {category}
                            </span>
                            <ul className="flex flex-col gap-0.5">
                              {featuresByCategory[category].map((item) => (
                                <li key={item.slug}>
                                  <Link
                                    href={`/features/${item.slug}`}
                                    onClick={closeAll}
                                    className="flex items-start gap-3 px-3 py-2.5 rounded-xl hover:bg-bg-2"
                                  >
                                    <span className="mt-0.5 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-bg-2 text-ink">
                                      <FeatureIcon name={item.iconName} />
                                    </span>
                                    <span className="flex flex-col gap-0.5 min-w-0">
                                      <span className="flex items-center gap-2">
                                        <span className="text-sm font-medium text-ink">
                                          {item.title}
                                        </span>
                                        {item.status === "alpha" ? (
                                          <AlphaPill />
                                        ) : null}
                                      </span>
                                      <span className="text-xs text-muted leading-snug">
                                        {item.shortDescription}
                                      </span>
                                    </span>
                                  </Link>
                                </li>
                              ))}
                            </ul>
                          </div>
                        ))}
                        <Link
                          href="/#features"
                          onClick={closeAll}
                          className="flex items-center justify-between mx-1 mt-3 px-3 py-2.5 rounded-xl bg-bg-2 text-sm font-medium text-ink"
                        >
                          See all features
                          <Arrow />
                        </Link>
                      </div>
                    ) : null}
                  </li>
                ) : (
                  <li key={l.label}>
                    <Link
                      href={l.href!}
                      onClick={closeAll}
                      className="block px-4 py-3 text-base text-ink rounded-2xl hover:bg-bg-2"
                    >
                      {l.label}
                    </Link>
                  </li>
                )
              )}
              <li className="mt-2 px-2">
                <a
                  href="https://github.com/citguru/melt"
                  target="_blank"
                  rel="noreferrer"
                  onClick={closeAll}
                  className="flex items-center justify-center gap-2 rounded-full bg-ink text-white px-4 py-3 text-sm font-medium"
                >
                  Try Melt free
                  <Arrow />
                </a>
              </li>
            </ul>
          </div>
        </div>
      ) : null}
    </header>
  );
}

function AlphaPill() {
  return (
    <span className="inline-flex items-center text-[9px] font-medium uppercase tracking-[0.16em] rounded-full bg-accent/10 text-accent border border-accent/30 px-1.5 py-0.5">
      Alpha
    </span>
  );
}

function Arrow() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path
        d="M5 12h14M13 5l7 7-7 7"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      width="11"
      height="11"
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden
      className={`transition-transform duration-200 ${open ? "rotate-180" : ""}`}
    >
      <path
        d="M6 9l6 6 6-6"
        stroke="currentColor"
        strokeWidth="2.2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
