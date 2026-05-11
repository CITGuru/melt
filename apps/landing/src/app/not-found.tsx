import { Nav } from "@/components/Nav";
import { Footer } from "@/components/sections/Footer";
import { Cloud } from "@/components/Clouds";
import { PrimaryCTA, GhostCTA } from "@/components/UI";

export default function NotFound() {
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <section className="relative overflow-hidden pt-36 md:pt-44 pb-24 md:pb-32 bg-sky">
          <Cloud className="absolute -left-16 top-32 w-[300px] opacity-80 drift-slow" />
          <Cloud className="absolute right-0 top-44 w-[260px] opacity-70 drift-slow-2" />
          <Cloud className="absolute left-1/4 bottom-0 w-[220px] opacity-70 drift-slow" />

          <div className="relative mx-auto max-w-3xl px-6 flex flex-col items-center text-center gap-6">
            <span className="text-xs uppercase tracking-[0.18em] text-muted">
              error 404
            </span>
            <h1 className="text-7xl sm:text-8xl md:text-[160px] font-semibold tracking-tight text-ink leading-none">
              404
            </h1>
            <h2 className="text-2xl md:text-3xl font-semibold tracking-tight text-ink">
              This route didn’t classify.
            </h2>
            <p className="text-lg text-ink-2 max-w-md">
              We couldn’t find that page. The router fell through to a
              friendly fallback. Try one of these instead.
            </p>
            <div className="flex flex-col sm:flex-row items-center gap-3 mt-2">
              <PrimaryCTA href="/">Back to home</PrimaryCTA>
              <GhostCTA href="/blog">Read the blog</GhostCTA>
            </div>
          </div>
        </section>
      </main>
      <Footer />
    </>
  );
}
