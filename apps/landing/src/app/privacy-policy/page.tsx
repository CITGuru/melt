import { LegalLayout } from "@/components/LegalLayout";
import type { Metadata } from "next";
import { pageSeo } from "@/lib/seo";

const PRIVACY_DESCRIPTION =
  "How Melt handles personal information and customer data.";

export const metadata: Metadata = {
  title: "Privacy Policy — Melt",
  description: PRIVACY_DESCRIPTION,
  ...pageSeo({
    path: "/privacy-policy",
    description: PRIVACY_DESCRIPTION,
    socialTitle: "Privacy policy",
  }),
};

export default function PrivacyPage() {
  return (
    <LegalLayout
      eyebrow="legal"
      title="Privacy policy"
      updated="May 1, 2026"
    >
      <p>
        This policy explains what information Melt collects, why, and how we
        handle it. Melt is built around the idea that your data should not
        leave your environment. The proxy and sync are self-hosted by default,
        and our cloud product runs in single-tenant data planes deployed in
        your VPC.
      </p>

      <h2>Information we collect</h2>
      <p>
        We collect the minimum information needed to provide the service and
        run the company:
      </p>
      <ul>
        <li>
          <strong>Account information.</strong> Name, work email, company
          name, and role you provide when you sign up or contact us.
        </li>
        <li>
          <strong>Operational telemetry.</strong> When you use the hosted
          control plane, we collect routing decision metadata (which route
          fired, latency, cost estimate). We never collect query text, query
          parameters, or query results.
        </li>
        <li>
          <strong>Website analytics.</strong> Aggregate, privacy-respecting
          page-level analytics on meltcomputing.com. No third-party tracking pixels.
        </li>
      </ul>

      <h2>What we do not collect</h2>
      <p>
        We do not collect query text, result sets, schema contents,
        authentication credentials, or any data that flows through the proxy.
        The data plane is deployed in your VPC and the metadata pipeline is
        opt-in and configurable per route class.
      </p>

      <h2>How we use information</h2>
      <ul>
        <li>To run, maintain, and improve the Melt service.</li>
        <li>To respond when you contact us.</li>
        <li>To meet legal and compliance obligations.</li>
      </ul>

      <h2>Sharing and sub-processors</h2>
      <p>
        We share information only with sub-processors strictly necessary to
        deliver the service (cloud hosting, transactional email, error
        tracking). The current list is published in our trust center and
        updated when sub-processors change.
      </p>

      <h2>Retention</h2>
      <p>
        Account information is retained for the lifetime of your account.
        Operational telemetry is retained for 30 days by default and
        configurable down to 24 hours. We delete data on request.
      </p>

      <h2>Your rights</h2>
      <p>
        Depending on your jurisdiction, you may have rights to access,
        correct, export, or delete your personal information. Email{" "}
        <a href="mailto:privacy@meltcomputing.com">privacy@meltcomputing.com</a> and we’ll
        respond within 30 days.
      </p>

      <h2>Contact</h2>
      <p>
        Questions about this policy:{" "}
        <a href="mailto:privacy@meltcomputing.com">privacy@meltcomputing.com</a>.
      </p>
    </LegalLayout>
  );
}
