"use client";

import Link from "next/link";
import { Logo } from "./Logo";
import { useEffect, useRef, useState } from "react";

type LinkItem = {
  label: string;
  href?: string;
  menu?: "features";
};

const links: LinkItem[] = [
  { label: "Features", menu: "features" },
  { label: "Benefits", href: "/#benefits" },
  { label: "Pricing", href: "/#pricing" },
  { label: "Blog", href: "/blog" },
  { label: "Contact", href: "/contact-us" },
];

type FeatureItem = {
  title: string;
  description: string;
  href: string;
  icon: React.ReactNode;
  comingSoon?: boolean;
};

type FeatureGroup = {
  name: string;
  comingSoon?: boolean;
  items: FeatureItem[];
};

const featureGroups: FeatureGroup[] = [
  {
    name: "Query routing",
    items: [
      {
        title: "Per-query routing",
        description: "Parse, classify, and route every statement on its own merit.",
        href: "/#strategies",
        icon: <IconRouting />,
      },
      {
        title: "Dual execution",
        description: "Plan-split between DuckDB and Snowflake via Arrow IPC.",
        href: "/blog/hybrid-plans-for-declared-remote-tables",
        icon: <IconSplit />,
      },
      {
        title: "Parity sampler",
        description: "Dual-run a fraction of routed queries; alert on drift.",
        href: "/blog/per-query-routing-in-detail",
        icon: <IconShield />,
      },
      {
        title: "Policy modes",
        description: "Passthrough, allowlist, enforce — with hot-reload.",
        href: "/blog/policy-modes",
        icon: <IconLock />,
      },
    ],
  },
  {
    name: "Warehouse routing",
    comingSoon: true,
    items: [
      {
        title: "Right-sizing",
        description: "XSMALL for tiny filters, LARGE for nightly aggregates.",
        href: "/#benefits",
        icon: <IconChart />,
        comingSoon: true,
      },
      {
        title: "Warm-warehouse routing",
        description: "Land on warehouses that are already running.",
        href: "/#benefits",
        icon: <IconRouting />,
        comingSoon: true,
      },
      {
        title: "Per-statement override",
        description: "Each statement gets the warehouse it actually needs.",
        href: "/#benefits",
        icon: <IconSplit />,
        comingSoon: true,
      },
      {
        title: "Cost attribution",
        description: "See which workloads route off, and what they would have cost.",
        href: "/#benefits",
        icon: <IconCoins />,
      },
    ],
  },
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
  // The desktop dropdown lives inside the fixed nav, so the page can stay scrollable.
  // When locking, compensate for the now-hidden scrollbar so nothing shifts.
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

  // Escape closes everything
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") closeAll();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  // Click outside the nav closes the desktop dropdown
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
      {/* Backdrop overlay — shared by desktop dropdown and mobile menu */}
      {(menu || mobileOpen) ? (
        <div
          className="fixed inset-0 -z-10 bg-ink/30 backdrop-blur-[2px] animate-fade-in"
          onClick={closeAll}
          aria-hidden
        />
      ) : null}

      <div
        ref={navRef}
        className="relative w-full max-w-5xl"
      >
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

        {/* Desktop Features dropdown */}
        {menu === "features" ? (
          <div
            role="menu"
            aria-label="Features"
            onMouseEnter={() => openMenu("features")}
            onMouseLeave={scheduleClose}
            className="hidden md:block absolute inset-x-0 top-full pt-2.5 animate-fade-up"
          >
            <div className="rounded-3xl bg-white border border-line soft-shadow-lg p-3">
              <div className="grid grid-cols-2 gap-1">
                {featureGroups.map((group) => (
                  <div key={group.name} className="flex flex-col">
                    <div className="flex items-center justify-between px-3 pt-2 pb-1.5">
                      <span className="text-[11px] font-medium uppercase tracking-[0.18em] text-ink-2/70">
                        {group.name}
                      </span>
                      {group.comingSoon ? (
                        <span className="inline-flex items-center gap-1 text-[10px] font-medium uppercase tracking-[0.14em] rounded-full px-2 py-0.5 bg-orange-100 text-orange-700 border border-orange-200">
                          Coming soon
                        </span>
                      ) : null}
                    </div>
                    <div className="flex flex-col gap-1">
                      {group.items.map((item) => (
                        <Link
                          key={item.title}
                          href={item.href}
                          role="menuitem"
                          onClick={() => setMenu(null)}
                          className={`group flex items-start gap-3 p-3 rounded-2xl hover:bg-bg-2 transition-colors ${
                            item.comingSoon ? "opacity-60" : ""
                          }`}
                        >
                          <span className="mt-0.5 inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-bg-2 text-ink group-hover:bg-ink group-hover:text-white transition-colors">
                            {item.icon}
                          </span>
                          <span className="flex flex-col gap-0.5 min-w-0">
                            <span className="text-sm font-medium text-ink">
                              {item.title}
                            </span>
                            <span className="text-xs text-muted leading-snug">
                              {item.description}
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
                  href="/#strategies"
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
                        {featureGroups.map((group) => (
                          <div key={group.name} className="flex flex-col">
                            <div className="flex items-center justify-between px-3 pt-2 pb-1">
                              <span className="text-[10px] font-medium uppercase tracking-[0.18em] text-ink-2/70">
                                {group.name}
                              </span>
                              {group.comingSoon ? (
                                <span className="inline-flex items-center gap-1 text-[10px] font-medium uppercase tracking-[0.14em] rounded-full px-2 py-0.5 bg-orange-100 text-orange-700 border border-orange-200">
                                  Coming soon
                                </span>
                              ) : null}
                            </div>
                            <ul className="flex flex-col gap-0.5">
                              {group.items.map((item) => (
                                <li key={item.title}>
                                  <Link
                                    href={item.href}
                                    onClick={closeAll}
                                    className={`flex items-start gap-3 px-3 py-2.5 rounded-xl hover:bg-bg-2 ${
                                      item.comingSoon ? "opacity-60" : ""
                                    }`}
                                  >
                                    <span className="mt-0.5 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-bg-2 text-ink">
                                      {item.icon}
                                    </span>
                                    <span className="flex flex-col gap-0.5 min-w-0">
                                      <span className="text-sm font-medium text-ink">
                                        {item.title}
                                      </span>
                                      <span className="text-xs text-muted leading-snug">
                                        {item.description}
                                      </span>
                                    </span>
                                  </Link>
                                </li>
                              ))}
                            </ul>
                          </div>
                        ))}
                        <Link
                          href="/#strategies"
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

function IconRouting() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path d="M4 6h6l4 12h6M4 18h6" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="4" cy="6" r="1.6" fill="currentColor" />
      <circle cx="4" cy="18" r="1.6" fill="currentColor" />
      <circle cx="20" cy="18" r="1.6" fill="currentColor" />
    </svg>
  );
}
function IconSplit() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path d="M6 4v6a4 4 0 0 0 4 4h4a4 4 0 0 1 4 4v2M18 4v6a4 4 0 0 1-4 4h-4a4 4 0 0 0-4 4v2" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
function IconCoins() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <ellipse cx="9" cy="7" rx="5" ry="2.5" stroke="currentColor" strokeWidth="1.7" />
      <path d="M4 7v5c0 1.4 2.2 2.5 5 2.5s5-1.1 5-2.5V7" stroke="currentColor" strokeWidth="1.7" />
      <path d="M10 13v4c0 1.4 2.2 2.5 5 2.5s5-1.1 5-2.5v-5" stroke="currentColor" strokeWidth="1.7" />
      <ellipse cx="15" cy="12" rx="5" ry="2.5" stroke="currentColor" strokeWidth="1.7" />
    </svg>
  );
}
function IconShield() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path d="M12 3 4 6v6c0 4.5 3.4 7.8 8 9 4.6-1.2 8-4.5 8-9V6l-8-3Z" stroke="currentColor" strokeWidth="1.7" strokeLinejoin="round" />
      <path d="m9 12 2 2 4-4" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
function IconLock() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <rect x="4" y="10" width="16" height="10" rx="2" stroke="currentColor" strokeWidth="1.7" />
      <path d="M8 10V7a4 4 0 0 1 8 0v3" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
    </svg>
  );
}
function IconChart() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden>
      <path d="M4 19h16" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      <path d="M7 16V9M12 16V5M17 16v-5" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
    </svg>
  );
}
