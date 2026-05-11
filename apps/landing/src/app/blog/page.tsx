import Link from "next/link";
import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { posts } from "@/lib/posts";
import { BlogCover } from "@/components/sections/BlogTeasers";
import { ArrowRight } from "@/components/UI";
import type { Metadata } from "next";
import { pageSeo } from "@/lib/seo";

const BLOG_DESCRIPTION =
  "Routing deep-dives, runbooks, and field notes from the team building Melt.";

export const metadata: Metadata = {
  title: "Blog — Melt",
  description: BLOG_DESCRIPTION,
  ...pageSeo({
    path: "/blog",
    description: BLOG_DESCRIPTION,
    socialTitle: "Melt blog",
  }),
};

export default function BlogIndex() {
  const [featured, ...rest] = posts;
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <section className="relative pt-36 md:pt-44 pb-12 md:pb-16 bg-sky-soft">
          <div className="mx-auto max-w-6xl px-6 flex flex-col items-center text-center gap-4">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">blog</span>
            <h1 className="text-5xl md:text-6xl lg:text-7xl font-semibold tracking-tight text-ink leading-[1.04] max-w-3xl">
              Ideas to level up
              <br />
              your data routing.
            </h1>
            <p className="text-lg text-ink-2 max-w-xl">
              Routing deep-dives, runbooks, and field notes from the team
              building Melt.
            </p>
          </div>
        </section>

        <section className="py-12 md:py-16">
          <div className="mx-auto max-w-6xl px-6">
            <Link
              href={`/blog/${featured.slug}`}
              className="bg-white rounded-3xl border border-line soft-shadow p-6 md:p-8 grid lg:grid-cols-12 gap-8 group hover:soft-shadow-lg transition-shadow"
            >
              <div className="lg:col-span-7">
                <BlogCover slug={featured.slug} variant="featured" />
              </div>
              <div className="lg:col-span-5 flex flex-col gap-4 justify-center">
                <div className="flex items-center gap-2">
                  <span className="text-xs uppercase tracking-[0.18em] text-accent">
                    {featured.category}
                  </span>
                  <span className="text-xs text-muted">·</span>
                  <span className="text-xs text-muted">{featured.readTime}</span>
                </div>
                <h2 className="text-2xl md:text-3xl font-semibold tracking-tight leading-tight">
                  {featured.title}
                </h2>
                <p className="text-muted leading-relaxed">{featured.excerpt}</p>
                <div className="flex items-center gap-3 mt-2">
                  <Avatar name={featured.author} />
                  <div className="flex flex-col">
                    <span className="text-sm font-medium">{featured.author}</span>
                    <span className="text-xs text-muted">{featured.publishedAt}</span>
                  </div>
                </div>
              </div>
            </Link>

            <div className="mt-8 grid md:grid-cols-2 lg:grid-cols-3 gap-5">
              {rest.map((p) => (
                <Link
                  key={p.slug}
                  href={`/blog/${p.slug}`}
                  className="bg-white rounded-3xl border border-line soft-shadow p-5 flex flex-col gap-4 hover:soft-shadow-lg transition-shadow"
                >
                  <BlogCover slug={p.slug} variant="featured" />
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-[10px] uppercase tracking-[0.18em] text-accent">
                      {p.category}
                    </span>
                    <span className="text-xs text-muted">·</span>
                    <span className="text-xs text-muted">{p.readTime}</span>
                  </div>
                  <h3 className="text-lg font-semibold tracking-tight leading-snug">
                    {p.title}
                  </h3>
                  <p className="text-sm text-muted leading-relaxed line-clamp-2">
                    {p.excerpt}
                  </p>
                  <div className="flex items-center justify-between mt-auto pt-2">
                    <span className="text-xs text-muted">{p.publishedAt}</span>
                    <ArrowRight />
                  </div>
                </Link>
              ))}
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
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
    <span className="h-9 w-9 rounded-full bg-gradient-to-br from-sky-1 to-sky-2 text-ink font-semibold text-sm inline-flex items-center justify-center">
      {initials}
    </span>
  );
}
