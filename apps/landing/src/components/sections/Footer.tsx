import Link from "next/link";
import { Logo } from "../Logo";

const cols: { heading: string; links: { label: string; href: string }[] }[] = [
  {
    heading: "Pages",
    links: [
      { label: "Home", href: "/" },
      { label: "Features", href: "/#features" },
      { label: "Pricing", href: "/#pricing" },
      { label: "Blog", href: "/blog" },
    ],
  },
  {
    heading: "Information",
    links: [
      { label: "Contact", href: "/contact-us" },
      { label: "Privacy", href: "/privacy-policy" },
      { label: "Terms of use", href: "/terms-of-use" },
      { label: "404", href: "/404" },
    ],
  },
];

export function Footer() {
  return (
    <footer className="relative bg-bg-2 border-t border-line">
      <div className="mx-auto max-w-6xl px-6 py-14 md:py-16 grid lg:grid-cols-12 gap-10">
        <div className="lg:col-span-5 flex flex-col gap-4">
          <Logo className="text-lg" />
          <p className="text-sm text-muted leading-relaxed max-w-xs">
            Your favorite data warehouse routing layer. Built for the era of
            agent-driven SQL.
          </p>
          <div className="flex items-center gap-2 mt-2">
            <Social href="https://github.com/" label="GitHub">
              <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 .2a8 8 0 0 0-2.5 15.6c.4.1.5-.2.5-.4v-1.4c-2.2.5-2.7-1-2.7-1-.4-1-.9-1.2-.9-1.2-.7-.5.1-.5.1-.5.8.1 1.2.8 1.2.8.7 1.2 1.9.9 2.4.7.1-.5.3-.9.5-1.1-1.8-.2-3.6-.9-3.6-3.9 0-.9.3-1.6.8-2.2 0-.2-.4-1 .1-2.1 0 0 .7-.2 2.2.8a7.6 7.6 0 0 1 4 0c1.5-1 2.2-.8 2.2-.8.4 1.1 0 1.9.1 2.1.5.6.8 1.3.8 2.2 0 3-1.8 3.7-3.6 3.9.3.3.6.8.6 1.6v2.4c0 .2.1.5.5.4A8 8 0 0 0 8 .2Z" />
              </svg>
            </Social>
            <Social href="https://x.com/" label="X">
              <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor">
                <path d="M18.244 2H21l-6.55 7.49L22.5 22h-6.812l-5.34-7.013L4.16 22H1.4l7.02-8.027L1.5 2h6.97l4.83 6.39L18.244 2Zm-1.19 18h1.876L7.05 4H5.05l12.005 16Z" />
              </svg>
            </Social>
            <Social href="https://youtube.com/" label="YouTube">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                <path d="M21.6 7.2a2.6 2.6 0 0 0-1.8-1.8C18.2 5 12 5 12 5s-6.2 0-7.8.4A2.6 2.6 0 0 0 2.4 7.2C2 8.8 2 12 2 12s0 3.2.4 4.8a2.6 2.6 0 0 0 1.8 1.8C5.8 19 12 19 12 19s6.2 0 7.8-.4a2.6 2.6 0 0 0 1.8-1.8c.4-1.6.4-4.8.4-4.8s0-3.2-.4-4.8ZM10 15V9l5.2 3L10 15Z" />
              </svg>
            </Social>
          </div>
        </div>

        <div className="lg:col-span-7 grid grid-cols-2 gap-8">
          {cols.map((c) => (
            <div key={c.heading} className="flex flex-col gap-3">
              <span className="text-xs uppercase tracking-[0.18em] text-muted">
                {c.heading}
              </span>
              <ul className="flex flex-col gap-2">
                {c.links.map((l) => (
                  <li key={l.label}>
                    <Link
                      href={l.href}
                      className="text-sm text-ink-2 hover:text-ink transition-colors"
                    >
                      {l.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>

      <div className="border-t border-line">
        <div className="mx-auto max-w-6xl px-6 py-5 flex flex-col md:flex-row items-center justify-between gap-3 text-xs text-muted">
          <span>© {new Date().getFullYear()} Melt. Apache-2.0 licensed.</span>
          <span>built for the era of agent-driven SQL</span>
        </div>
      </div>
    </footer>
  );
}

function Social({
  href,
  label,
  children,
}: {
  href: string;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <a
      href={href}
      aria-label={label}
      target="_blank"
      rel="noreferrer"
      className="h-9 w-9 inline-flex items-center justify-center rounded-full bg-white border border-line text-ink-2 hover:text-ink transition-colors"
    >
      {children}
    </a>
  );
}
