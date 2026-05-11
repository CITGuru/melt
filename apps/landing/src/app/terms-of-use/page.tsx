import { LegalLayout } from "@/components/LegalLayout";
import type { Metadata } from "next";
import { pageSeo } from "@/lib/seo";

const TERMS_DESCRIPTION =
  "The terms governing your use of the Melt website and services.";

export const metadata: Metadata = {
  title: "Terms of Use — Melt",
  description: TERMS_DESCRIPTION,
  ...pageSeo({
    path: "/terms-of-use",
    description: TERMS_DESCRIPTION,
    socialTitle: "Terms of use",
  }),
};

export default function TermsPage() {
  return (
    <LegalLayout eyebrow="legal" title="Terms of use" updated="May 1, 2026">
      <p>
        These terms govern your use of meltcomputing.com and the Melt cloud service.
        The Melt open-source proxy and sync are licensed separately under the
        Apache License 2.0; nothing in this document overrides that license.
      </p>

      <h2>Use of the service</h2>
      <p>
        You may use the Melt cloud service to route SQL between Snowflake and
        DuckDB-backed lakehouses for workloads under your control. You agree
        not to use the service in violation of applicable law, to send us
        malware, to attempt unauthorised access, or to resell access without
        a written agreement.
      </p>

      <h2>Accounts</h2>
      <p>
        You are responsible for keeping your account credentials secure and
        for all activity under your account. Notify us immediately at{" "}
        <a href="mailto:security@meltcomputing.com">security@meltcomputing.com</a> if you
        suspect unauthorised access.
      </p>

      <h2>Customer data</h2>
      <p>
        You own your data. We process it only as needed to provide the
        service and as described in our{" "}
        <a href="/privacy-policy">privacy policy</a>. The data plane runs in
        your VPC. We do not collect query text or result sets.
      </p>

      <h2>Open-source software</h2>
      <p>
        The proxy, sync, and CLI are open source under Apache-2.0. You can
        run them anywhere, fork them, and modify them subject to the licence.
        Cloud-only features are governed by these terms.
      </p>

      <h2>Service availability</h2>
      <p>
        We aim for high availability of the hosted control plane but do not
        guarantee uninterrupted service except where promised in a written
        SLA. The data plane runs on infrastructure you control; its
        availability is your responsibility.
      </p>

      <h2>Termination</h2>
      <p>
        You can stop using the service at any time. We can suspend or
        terminate accounts that violate these terms or applicable law, with
        reasonable notice where practical.
      </p>

      <h2>Disclaimers</h2>
      <p>
        The service is provided &quot;as is&quot; to the maximum extent
        permitted by law. We disclaim implied warranties of merchantability,
        fitness for a particular purpose, and non-infringement. Our total
        liability is limited to fees you paid in the 12 months preceding the
        claim.
      </p>

      <h2>Changes</h2>
      <p>
        We may update these terms; we&apos;ll post the new version and update
        the &quot;Last updated&quot; date. Continued use after changes means
        you accept the updated terms.
      </p>

      <h2>Contact</h2>
      <p>
        Questions:{" "}
        <a href="mailto:legal@meltcomputing.com">legal@meltcomputing.com</a>.
      </p>
    </LegalLayout>
  );
}
