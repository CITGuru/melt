import type { Metadata } from "next";
import { pageSeo } from "@/lib/seo";

const CONTACT_DESCRIPTION =
  "Tell us about your warehouse setup and we'll get back within one business day. Onboarding for design partners is free.";

export const metadata: Metadata = {
  title: "Contact — Melt",
  description: CONTACT_DESCRIPTION,
  ...pageSeo({
    path: "/contact-us",
    description: CONTACT_DESCRIPTION,
    socialTitle: "Talk to the Melt team",
  }),
};

export default function ContactLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return children;
}
