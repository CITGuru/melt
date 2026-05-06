import Link from "next/link";
import { notFound } from "next/navigation";
import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { posts, getPost } from "@/lib/posts";
import { BlogCover } from "@/components/sections/BlogTeasers";
import { ArrowRight } from "@/components/UI";
import type { Metadata } from "next";

export function generateStaticParams() {
  return posts.map((p) => ({ slug: p.slug }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}): Promise<Metadata> {
  const { slug } = await params;
  const post = getPost(slug);
  if (!post) return { title: "Post not found" };
  return { title: `${post.title} — Melt`, description: post.excerpt };
}

export default async function BlogPostPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const post = getPost(slug);
  if (!post) notFound();

  const others = posts.filter((p) => p.slug !== post.slug).slice(0, 3);

  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <article>
          <header className="relative pt-36 md:pt-44 pb-12 bg-sky-soft">
            <div className="mx-auto max-w-3xl px-6 flex flex-col gap-5">
              <Link
                href="/blog"
                className="inline-flex items-center gap-2 self-start text-sm text-ink-2 hover:text-ink"
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden>
                  <path d="M19 12H5M12 5l-7 7 7 7" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                All articles
              </Link>
              <div className="flex items-center gap-2">
                <span className="text-xs uppercase tracking-[0.18em] text-accent">{post.category}</span>
                <span className="text-xs text-muted">·</span>
                <span className="text-xs text-muted">{post.readTime}</span>
              </div>
              <h1 className="text-4xl md:text-6xl font-semibold tracking-tight text-ink leading-[1.05]">
                {post.title}
              </h1>
              <p className="text-lg text-ink-2 leading-relaxed">{post.excerpt}</p>
              <div className="flex items-center gap-3 mt-2">
                <span className="h-10 w-10 rounded-full bg-gradient-to-br from-orange-200 to-orange-400 text-ink font-semibold text-sm inline-flex items-center justify-center">
                  {initials(post.author)}
                </span>
                <div className="flex flex-col">
                  <span className="text-sm font-medium">{post.author}</span>
                  <span className="text-xs text-muted">
                    {post.authorRole} · {post.publishedAt}
                  </span>
                </div>
              </div>
            </div>
          </header>

          <div className="mx-auto max-w-4xl px-6 -mt-2">
            <BlogCover slug={post.slug} variant="featured" />
          </div>

          <div className="mx-auto max-w-3xl px-6 py-12 md:py-16">
            <div
              className="prose-melt"
              dangerouslySetInnerHTML={{ __html: post.body }}
            />
          </div>
        </article>

        {others.length > 0 ? (
          <section className="border-t border-line bg-bg-2 py-16">
            <div className="mx-auto max-w-6xl px-6">
              <h2 className="text-2xl md:text-3xl font-semibold tracking-tight mb-8">
                Keep reading
              </h2>
              <div className="grid md:grid-cols-3 gap-5">
                {others.map((p) => (
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
                    <span className="mt-auto inline-flex items-center gap-2 text-sm font-medium text-ink-2">
                      Read article <ArrowRight />
                    </span>
                  </Link>
                ))}
              </div>
            </div>
          </section>
        ) : null}
      </main>
      <Footer />
    </>
  );
}

function initials(name: string) {
  return name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();
}
