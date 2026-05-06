import { Nav } from "@/components/Nav";
import { Hero } from "@/components/sections/Hero";
import { TrustStrip } from "@/components/sections/TrustStrip";
import { DeviceSync } from "@/components/sections/DeviceSync";
import { Routing } from "@/components/sections/Routing";
import { Cost } from "@/components/sections/Cost";
import { Simplicity } from "@/components/sections/Simplicity";
import { Testimonials } from "@/components/sections/Testimonials";
import { Pricing } from "@/components/sections/Pricing";
import { BlogTeasers } from "@/components/sections/BlogTeasers";
import { Community } from "@/components/sections/Community";
import { CTA } from "@/components/sections/CTA";
import { Footer } from "@/components/sections/Footer";

export default function Home() {
  return (
    <>
      <Nav />
      <main className="flex flex-col w-full">
        <Hero />
        <TrustStrip label="Trusted by 7,000+ data teams, agents, and analytics studios" />
        <DeviceSync />
        <Routing />
        <Cost />
        <Simplicity />
        <Testimonials />
        <Pricing />
        <TrustStrip label="Trusted by 7,000+ data teams, agents, and analytics studios" />
        <BlogTeasers />
        <Community />
        <CTA />
      </main>
      <Footer />
    </>
  );
}
