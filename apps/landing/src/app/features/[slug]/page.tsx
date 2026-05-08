import { notFound } from "next/navigation";
import { FeaturePageLayout } from "@/components/FeaturePageLayout";
import { features, featuresBySlug } from "@/lib/features";
import type { Metadata } from "next";

export function generateStaticParams() {
  return features.map((f) => ({ slug: f.slug }));
}

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}): Promise<Metadata> {
  const { slug } = await params;
  const feature = featuresBySlug[slug];
  if (!feature) return { title: "Feature not found" };
  return {
    title: feature.metaTitle,
    description: feature.metaDescription,
  };
}

export default async function FeaturePage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const feature = featuresBySlug[slug];
  if (!feature) notFound();
  return <FeaturePageLayout feature={feature} />;
}
