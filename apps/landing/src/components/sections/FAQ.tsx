import { SectionHeader } from "../UI";
import type { FaqBlock } from "@/lib/faq";

export function FAQ({ block }: { block: FaqBlock }) {
  const jsonLd = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "@id": block.schemaId,
    mainEntity: block.entries.map((e) => ({
      "@type": "Question",
      name: e.question,
      acceptedAnswer: {
        "@type": "Answer",
        text: e.answer,
      },
    })),
  };

  return (
    <section
      id="faq"
      aria-labelledby="faq-heading"
      className="relative py-24 md:py-32 bg-bg-2 border-y border-line"
    >
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <div className="mx-auto max-w-3xl px-6">
        <div className="flex flex-col items-center text-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">
            faq
          </span>
          <SectionHeader title={<>Frequently asked questions.</>} />
        </div>
        <ul className="mt-12 md:mt-16 flex flex-col gap-3 list-none p-0">
          {block.entries.map((e) => (
            <li key={e.id} className="list-none">
              <details
                id={`faq-${e.id}`}
                className="group rounded-2xl border border-line bg-white px-6 py-5 open:soft-shadow transition-shadow"
              >
                <summary className="faq-summary cursor-pointer flex items-start justify-between gap-4 text-base md:text-lg font-semibold text-ink leading-snug">
                  <span>{e.question}</span>
                  <FaqChevron />
                </summary>
                <p className="mt-3 text-sm md:text-base text-ink-2 leading-relaxed">
                  {e.answer}
                </p>
              </details>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}

function FaqChevron() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden
      className="mt-1 shrink-0 text-muted transition-transform duration-200 group-open:rotate-180"
    >
      <path
        d="M6 9l6 6 6-6"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
