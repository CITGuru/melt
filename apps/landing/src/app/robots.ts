import type { MetadataRoute } from "next";

const SITE = "https://meltcomputing.com";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: "/",
        // Avoid wasting crawl budget on framework / asset routes that
        // shouldn't show up in search results.
        disallow: ["/404", "/_next/", "/api/"],
      },
    ],
    sitemap: `${SITE}/sitemap.xml`,
    host: SITE,
  };
}
