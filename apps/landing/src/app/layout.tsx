import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

// SERP-friendly title used in <title> + browser tab. Kept under
// 60 chars so Google doesn't truncate it in search results.
const DOCUMENT_TITLE =
  "Melt — Cut your Snowflake bill, change one connection string";
// Used in OG / Twitter cards. No "Melt — " prefix because the
// siteName field on the OG card already shows the brand separately.
const SOCIAL_TITLE =
  "Cut your Snowflake bill, change one connection string";
const SHARED_DESCRIPTION =
  "All-in-one routing layer for your data warehouse. Drop melt in front of Snowflake and route eligible reads to a DuckDB-backed lakehouse without touching a query.";

export const metadata: Metadata = {
  metadataBase: new URL("https://www.meltcomputing.com"),
  title: {
    default: DOCUMENT_TITLE,
    template: "%s — Melt",
  },
  description: SHARED_DESCRIPTION,
  applicationName: "Melt",
  keywords: [
    "Snowflake",
    "DuckDB",
    "data warehouse",
    "query routing",
    "warehouse routing",
    "lakehouse",
    "Iceberg",
    "DuckLake",
    "agent SQL",
    "Snowflake cost",
  ],
  authors: [{ name: "Toby Oyetoke" }],
  alternates: {
    canonical: "/",
  },
  robots: {
    index: true,
    follow: true,
  },
  openGraph: {
    type: "website",
    siteName: "Melt",
    title: SOCIAL_TITLE,
    description: SHARED_DESCRIPTION,
    url: "/",
    locale: "en_US",
  },
  twitter: {
    card: "summary_large_image",
    title: SOCIAL_TITLE,
    description: SHARED_DESCRIPTION,
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col">{children}</body>
    </html>
  );
}
