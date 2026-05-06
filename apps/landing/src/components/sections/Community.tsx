import { ArrowRight, SectionHeader } from "../UI";

export function Community() {
  return (
    <section className="relative py-24 md:py-32">
      <div className="mx-auto max-w-6xl px-6">
        <div className="flex flex-col items-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">community</span>
          <SectionHeader title={<>Stay in the loop.</>} />
        </div>

        <div className="mt-14 grid md:grid-cols-2 gap-5">
          <Card
            count="15.2K"
            countLabel="followers"
            title="X / Twitter"
            description="Stay updated on new releases and discover how teams are routing queries with melt."
            cta="Follow us"
            href="https://x.com/"
            icon={
              <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                <path d="M18.244 2H21l-6.55 7.49L22.5 22h-6.812l-5.34-7.013L4.16 22H1.4l7.02-8.027L1.5 2h6.97l4.83 6.39L18.244 2Zm-1.19 18h1.876L7.05 4H5.05l12.005 16Z" />
              </svg>
            }
          />
          <Card
            count="32K"
            countLabel="subscribers"
            title="YouTube"
            description="Routing deep-dives, tutorials, and architecture walkthroughs to inspire and enhance your melt setup."
            cta="Subscribe"
            href="https://youtube.com/"
            icon={
              <svg width="22" height="22" viewBox="0 0 24 24" fill="currentColor">
                <path d="M21.6 7.2a2.6 2.6 0 0 0-1.8-1.8C18.2 5 12 5 12 5s-6.2 0-7.8.4A2.6 2.6 0 0 0 2.4 7.2C2 8.8 2 12 2 12s0 3.2.4 4.8a2.6 2.6 0 0 0 1.8 1.8C5.8 19 12 19 12 19s6.2 0 7.8-.4a2.6 2.6 0 0 0 1.8-1.8c.4-1.6.4-4.8.4-4.8s0-3.2-.4-4.8ZM10 15V9l5.2 3L10 15Z" />
              </svg>
            }
          />
        </div>
      </div>
    </section>
  );
}

function Card({
  count,
  countLabel,
  title,
  description,
  cta,
  href,
  icon,
}: {
  count: string;
  countLabel: string;
  title: string;
  description: string;
  cta: string;
  href: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-8 flex flex-col gap-5">
      <div className="flex items-center justify-between">
        <span className="inline-flex h-11 w-11 items-center justify-center rounded-2xl bg-ink text-white">
          {icon}
        </span>
        <span className="text-sm text-muted">
          <span className="text-ink font-semibold">{count}</span> {countLabel}
        </span>
      </div>
      <h3 className="text-2xl font-semibold tracking-tight">{title}</h3>
      <p className="text-muted leading-relaxed">{description}</p>
      <a
        href={href}
        target="_blank"
        rel="noreferrer"
        className="mt-2 inline-flex items-center gap-2 self-start rounded-full bg-ink text-white px-5 py-2.5 text-sm font-medium hover:bg-ink-2 transition-colors"
      >
        {cta}
        <ArrowRight />
      </a>
    </div>
  );
}
