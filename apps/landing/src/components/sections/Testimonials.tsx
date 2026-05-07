const featured = {
  quote:
    "Melt is by far the cleanest piece of data infrastructure we&apos;ve dropped into production this year.",
  name: "Marta P.",
  role: "VP Data, Series-C SaaS",
};

const more = [
  {
    quote:
      "Our agents were generating 4k queries a day against the warehouse. Pointing them at melt cut credits 81% in week one — and we changed nothing in dbt.",
    name: "Daniel R.",
    role: "Staff data engineer, marketplace",
  },
  {
    quote:
      "The route output is what got it through review. Compliance sees, query by query, where things landed and why.",
    name: "Lea D.",
    role: "Data platform lead, healthtech",
  },
  {
    quote:
      "Dual execution lifted the all-or-nothing cliff for our oversize tables. The parity sampler keeps us honest. Genuinely fun to operate.",
    name: "Sergio W.",
    role: "Analytics engineering, fintech",
  },
  {
    quote:
      "Self-hosted, no egress, every routing decision in metrics. It looked too good to be real. Then we shipped it. Then we forgot it was there.",
    name: "Jane J.",
    role: "Director of data infra, top-50 SaaS",
  },
];

export function Testimonials() {
  return (
    <section className="relative py-24 md:py-32">
      <div className="mx-auto max-w-6xl px-6">
        <div className="bg-white rounded-3xl border border-line soft-shadow p-8 md:p-12 flex flex-col items-center text-center gap-6">
          <Stars />
          <blockquote
            className="text-3xl md:text-5xl font-semibold tracking-tight leading-[1.08] max-w-3xl"
            dangerouslySetInnerHTML={{ __html: `&ldquo;${featured.quote}&rdquo;` }}
          />
          <figcaption className="flex items-center gap-3 mt-2">
            <Avatar name={featured.name} />
            <div className="flex flex-col text-left">
              <span className="text-sm font-medium">{featured.name}</span>
              <span className="text-xs text-muted">{featured.role}</span>
            </div>
          </figcaption>
        </div>

        <div className="mt-5 grid md:grid-cols-2 gap-5">
          {more.map((q) => (
            <figure
              key={q.name}
              className="bg-white rounded-3xl border border-line soft-shadow p-7 flex flex-col gap-4"
            >
              <Stars small />
              <blockquote className="text-base md:text-lg leading-relaxed text-ink-2">
                &ldquo;{q.quote}&rdquo;
              </blockquote>
              <figcaption className="flex items-center gap-3 mt-1">
                <Avatar name={q.name} />
                <div className="flex flex-col">
                  <span className="text-sm font-medium">{q.name}</span>
                  <span className="text-xs text-muted">{q.role}</span>
                </div>
              </figcaption>
            </figure>
          ))}
        </div>

        <p className="mt-6 text-center text-[11px] text-muted">
          Quotes paraphrased from design-partner conversations; full names withheld until GA.
        </p>
      </div>
    </section>
  );
}

function Stars({ small }: { small?: boolean }) {
  const size = small ? 14 : 18;
  return (
    <span className="inline-flex items-center gap-1">
      {Array.from({ length: 5 }).map((_, i) => (
        <svg key={i} width={size} height={size} viewBox="0 0 24 24" fill="#f6a45b">
          <path d="M12 2 14.6 8.6 22 9.3l-5.5 4.8L18.2 22 12 18 5.8 22l1.7-7.9L2 9.3l7.4-.7Z" />
        </svg>
      ))}
    </span>
  );
}

function Avatar({ name }: { name: string }) {
  const initials = name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();
  return (
    <span className="h-9 w-9 rounded-full bg-gradient-to-br from-orange-200 to-orange-400 text-ink font-semibold text-sm inline-flex items-center justify-center">
      {initials}
    </span>
  );
}
