import type { Metadata } from "next";

export const SITE_NAME = "Melt";
export const SITE_URL = "https://www.meltcomputing.com";

// Next.js auto-detects opengraph-image.tsx only when a page doesn't set its
// own openGraph block. We override openGraph per page, so we need to thread
// the root brand OG image through explicitly or social cards lose the image.
const DEFAULT_OG_IMAGE = {
  url: "/opengraph-image",
  width: 1200,
  height: 630,
  alt: "Melt — cut your Snowflake bill, change one connection string",
};

type SeoImage = { url: string; width?: number; height?: number; alt?: string };

type SeoInput = {
  path: string;
  description: string;
  // OG/Twitter card title. The brand is supplied via openGraph.siteName,
  // so omit any "— Melt" suffix here.
  socialTitle: string;
  ogType?: "website" | "article";
  image?: SeoImage;
};

export function pageSeo(
  input: SeoInput,
): Pick<Metadata, "alternates" | "openGraph" | "twitter"> {
  const image = input.image ?? DEFAULT_OG_IMAGE;
  return {
    alternates: { canonical: input.path },
    openGraph: {
      type: input.ogType ?? "website",
      siteName: SITE_NAME,
      locale: "en_US",
      title: input.socialTitle,
      description: input.description,
      url: input.path,
      images: [image],
    },
    twitter: {
      card: "summary_large_image",
      title: input.socialTitle,
      description: input.description,
      images: [image.url],
    },
  };
}
