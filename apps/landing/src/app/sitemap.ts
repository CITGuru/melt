import type { MetadataRoute } from "next";
import { posts } from "@/lib/posts";
import { features } from "@/lib/features";

const SITE = "https://meltcomputing.com";

export default function sitemap(): MetadataRoute.Sitemap {
  const now = new Date();

  const staticRoutes: MetadataRoute.Sitemap = [
    {
      url: `${SITE}/`,
      lastModified: now,
      changeFrequency: "weekly",
      priority: 1.0,
    },
    {
      url: `${SITE}/about`,
      lastModified: now,
      changeFrequency: "monthly",
      priority: 0.7,
    },
    {
      url: `${SITE}/blog`,
      lastModified: now,
      changeFrequency: "always",
      priority: 0.8,
    },
    {
      url: `${SITE}/methodology`,
      lastModified: now,
      changeFrequency: "monthly",
      priority: 0.8,
    },
    {
      url: `${SITE}/contact-us`,
      lastModified: now,
      changeFrequency: "monthly",
      priority: 0.6,
    },
    {
      url: `${SITE}/privacy-policy`,
      lastModified: now,
      changeFrequency: "yearly",
      priority: 0.3,
    },
    {
      url: `${SITE}/terms-of-use`,
      lastModified: now,
      changeFrequency: "yearly",
      priority: 0.3,
    },
  ];

  const blogRoutes: MetadataRoute.Sitemap = posts.map((p) => {
    const parsed = new Date(p.publishedAt);
    return {
      url: `${SITE}/blog/${p.slug}`,
      lastModified: Number.isNaN(parsed.getTime()) ? now : parsed,
      changeFrequency: "always",
      priority: 0.8,
    };
  });

  const featureRoutes: MetadataRoute.Sitemap = features.map((f) => ({
    url: `${SITE}/features/${f.slug}`,
    lastModified: now,
    changeFrequency: "always",
    priority: 0.8,
  }));

  return [...staticRoutes, ...blogRoutes, ...featureRoutes];
}
