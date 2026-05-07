import Link from "next/link";
import { ArrowRight, SectionHeader } from "../UI";
import { posts } from "@/lib/posts";

export function BlogTeasers() {
  const [featured, ...rest] = posts;
  const more = rest.slice(0, 3);
  return (
    <section className="relative py-24 md:py-32 bg-bg-2 border-y border-line">
      <div className="mx-auto max-w-6xl px-6">
        <div className="flex flex-col items-center gap-4">
          <span className="text-xs uppercase tracking-[0.18em] text-muted">blog</span>
          <SectionHeader title={<>Ideas to level up your data routing.</>} />
        </div>

        <div className="mt-14 grid lg:grid-cols-12 gap-5">
          <Link
            href={`/blog/${featured.slug}`}
            className={`${
              more.length > 0 ? "lg:col-span-7" : "lg:col-span-12"
            } bg-white rounded-3xl border border-line soft-shadow p-6 md:p-8 flex flex-col gap-6 group hover:soft-shadow-lg transition-shadow`}
          >
            <BlogCover slug={featured.slug} variant="featured" />
            <div className="flex flex-col gap-3">
              <div className="flex items-center gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-accent">
                  {featured.category}
                </span>
                <span className="text-xs text-muted">·</span>
                <span className="text-xs text-muted">{featured.readTime}</span>
              </div>
              <h3 className="text-2xl md:text-3xl font-semibold tracking-tight leading-tight">
                {featured.title}
              </h3>
              <p className="text-muted leading-relaxed">{featured.excerpt}</p>
              <div className="flex items-center gap-3 mt-2">
                <Avatar name={featured.author} />
                <div className="flex flex-col">
                  <span className="text-sm font-medium">{featured.author}</span>
                  <span className="text-xs text-muted">{featured.authorRole}</span>
                </div>
                <span className="ml-auto text-xs text-muted">Featured</span>
              </div>
            </div>
          </Link>

          {more.length > 0 ? (
            <div className="lg:col-span-5 flex flex-col gap-5">
              {more.map((p) => (
                <Link
                  key={p.slug}
                  href={`/blog/${p.slug}`}
                  className="bg-white rounded-3xl border border-line soft-shadow p-5 flex items-center gap-4 group hover:soft-shadow-lg transition-shadow"
                >
                  <BlogCover slug={p.slug} variant="thumb" />
                  <div className="flex flex-col gap-1.5 flex-1 min-w-0">
                    <span className="text-[10px] uppercase tracking-[0.18em] text-accent">
                      {p.category}
                    </span>
                    <h4 className="text-base font-semibold tracking-tight leading-snug truncate">
                      {p.title}
                    </h4>
                    <span className="text-xs text-muted">{p.readTime}</span>
                  </div>
                  <ArrowRight />
                </Link>
              ))}
              <Link
                href="/blog"
                className="text-sm font-medium text-ink-2 hover:text-ink mt-2 inline-flex items-center gap-2 self-end"
              >
                See all articles <ArrowRight />
              </Link>
            </div>
          ) : null}
        </div>
      </div>
    </section>
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
    <span className="h-8 w-8 rounded-full bg-gradient-to-br from-sky-1 to-sky-2 text-ink font-semibold text-xs inline-flex items-center justify-center">
      {initials}
    </span>
  );
}

export function BlogCover({
  slug,
  variant = "featured",
}: {
  slug: string;
  variant?: "featured" | "thumb";
}) {
  const palette = paletteForSlug(slug);
  const isThumb = variant === "thumb";
  return (
    <div
      className={`relative overflow-hidden rounded-2xl ${
        isThumb ? "h-16 w-16 shrink-0" : "aspect-[16/8]"
      }`}
      style={{ background: palette.bg }}
      aria-hidden
    >
      <div className="absolute inset-0 bg-dots opacity-40" />
      <div
        className="absolute -right-6 -bottom-10 h-40 w-40 rounded-full"
        style={{ background: palette.glow, filter: "blur(24px)", opacity: 0.7 }}
      />
      {!isThumb ? (
        <span
          className="absolute bottom-4 left-4 text-white/90 font-mono text-[11px] uppercase tracking-[0.18em]"
        >
          melt · routing
        </span>
      ) : null}
    </div>
  );
}

function paletteForSlug(slug: string): { bg: string; glow: string } {
  const seed =
    Array.from(slug).reduce((a, c) => a + c.charCodeAt(0), 0) % 4;
  const palettes = [
    { bg: "linear-gradient(135deg, #c7d6ee, #97b2d8)", glow: "#fbbf99" },
    { bg: "linear-gradient(135deg, #f3d9c5, #e9a87a)", glow: "#fde7d4" },
    { bg: "linear-gradient(135deg, #cce5e0, #88b9b0)", glow: "#e8c8a3" },
    { bg: "linear-gradient(135deg, #d8d2ee, #9f8fd0)", glow: "#fbd2a8" },
  ];
  return palettes[seed];
}
