import { Nav } from "@/components/Nav";
import { Hero } from "@/components/sections/Hero";
import { TrustStrip } from "@/components/sections/TrustStrip";
import { Strategies } from "@/components/sections/Strategies";
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
        <TrustStrip label="Built by data engineers behind" items={["Finic", "OpenDuck", "Pawrly"]} />
        {/* <DeviceSync /> */}
        <Strategies />
        <Routing />
        <Cost />
        <Simplicity />
        {/* <Testimonials /> — hidden until we have real partner quotes */}
        <Pricing />
        <TrustStrip label="Supported integrations and drivers" />
        <BlogTeasers />
        {/* <Community /> — hidden until we have real social handles + counts */}
        <CTA />
      </main>
      <Footer />
    </>
  );
}
