import { ReactNode } from "react";
import { Nav } from "./Nav";
import { Footer } from "./sections/Footer";

export function LegalLayout({
  eyebrow,
  title,
  updated,
  children,
}: {
  eyebrow: string;
  title: string;
  updated: string;
  children: ReactNode;
}) {
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <section className="relative pt-36 md:pt-44 pb-12 bg-sky-soft">
          <div className="mx-auto max-w-3xl px-6 flex flex-col gap-4">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">{eyebrow}</span>
            <h1 className="text-5xl md:text-6xl font-semibold tracking-tight text-ink leading-[1.04]">
              {title}
            </h1>
            <p className="text-sm text-muted">Last updated {updated}</p>
          </div>
        </section>

        <section className="py-12 md:py-16">
          <div className="mx-auto max-w-3xl px-6">
            <div className="bg-white rounded-3xl border border-line soft-shadow p-7 md:p-10">
              <div className="prose-melt">{children}</div>
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}
