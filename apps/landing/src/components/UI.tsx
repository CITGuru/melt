import Link from "next/link";
import { ReactNode } from "react";

export function Eyebrow({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <span
      className={`inline-flex items-center gap-2 text-xs font-medium uppercase tracking-[0.18em] text-ink-2/70 ${className}`}
    >
      {children}
    </span>
  );
}

export function Pill({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full bg-white border border-line px-3 py-1 text-xs font-medium text-ink-2 soft-shadow ${className}`}
    >
      {children}
    </span>
  );
}

export function PrimaryCTA({
  href,
  children,
  className = "",
  external,
}: {
  href: string;
  children: ReactNode;
  className?: string;
  external?: boolean;
}) {
  const Tag = external ? "a" : Link;
  const props = external ? { target: "_blank", rel: "noreferrer" } : {};
  return (
    <Tag
      href={href}
      {...props}
      className={`inline-flex items-center gap-2 rounded-full bg-ink text-white px-5 py-3 text-sm font-medium hover:bg-ink-2 transition-colors ${className}`}
    >
      {children}
      <ArrowRight />
    </Tag>
  );
}

export function GhostCTA({
  href,
  children,
  className = "",
}: {
  href: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <Link
      href={href}
      className={`inline-flex items-center gap-2 rounded-full bg-white/70 backdrop-blur border border-line px-5 py-3 text-sm font-medium text-ink hover:bg-white transition-colors ${className}`}
    >
      {children}
    </Link>
  );
}

export function ArrowRight({ size = 14 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" aria-hidden>
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

export function SectionHeader({
  eyebrow,
  title,
  description,
  align = "center",
}: {
  eyebrow?: string;
  title: ReactNode;
  description?: ReactNode;
  align?: "center" | "left";
}) {
  const alignCls =
    align === "center"
      ? "text-center mx-auto items-center"
      : "text-left items-start";
  return (
    <div className={`flex flex-col gap-4 max-w-2xl ${alignCls}`}>
      {eyebrow ? <Eyebrow>{eyebrow}</Eyebrow> : null}
      <h2 className="text-4xl md:text-5xl lg:text-6xl font-semibold tracking-tight text-ink leading-[1.04]">
        {title}
      </h2>
      {description ? (
        <p className="text-base md:text-lg text-muted leading-relaxed max-w-xl">
          {description}
        </p>
      ) : null}
    </div>
  );
}
