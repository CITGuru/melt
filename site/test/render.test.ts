// Render the projection page from the canonical fixture and assert
// the visible numbers match. This is the GET-side acceptance check —
// the page must render `total_queries`, both routable rows, and every
// top-pattern row using *only* the uploaded JSON.

import { describe, expect, it } from "vitest";

import { renderAuditPage, renderNotFound } from "../lib/render.js";
import { fixtureRedacted } from "./_fixture.js";

function record(payload = fixtureRedacted()) {
  return { id: "abc1234567", payload, created_at: "2026-05-05T00:00:00.000Z" };
}

describe("renderAuditPage", () => {
  it("renders the account, window, and totals", () => {
    const html = renderAuditPage(record());
    expect(html).toContain("ACME-DEMO");
    expect(html).toContain("2026-04-04T00:00:00Z");
    expect(html).toContain("2026-05-04T00:00:00Z");
    expect(html).toContain("12,345"); // total_queries formatted
    expect(html).toContain("$9,876.54"); // total_spend_usd
  });

  it("renders both routable rows", () => {
    const html = renderAuditPage(record());
    // static
    expect(html).toContain("$3,000");
    expect(html).toContain("$36,500");
    // conservative
    expect(html).toContain("$2,000");
    expect(html).toContain("$24,000");
  });

  it("renders every top_patterns row, including the table FQN", () => {
    const html = renderAuditPage(record());
    expect(html).toContain("ANALYTICS.PUBLIC.&lt;redacted&gt;");
    expect(html).toContain("PUBLIC.&lt;redacted&gt;");
    expect(html).toContain("$1,500.00");
    expect(html).toContain("$600.00");
    expect(html).toContain("$300.00");
  });

  it("includes confidence band and methodology disclaimers", () => {
    const html = renderAuditPage(record());
    expect(html).toContain("±20%");
    expect(html).toContain("static analysis only");
    expect(html).toContain("cloud-services credits ignored");
  });

  it("does not include noindex by default", () => {
    const html = renderAuditPage(record());
    expect(html).not.toContain("noindex");
  });

  it("includes noindex when opt-in", () => {
    const html = renderAuditPage(record(), { noindex: true });
    expect(html).toContain('content="noindex"');
  });

  it("escapes HTML in account/table_fqn", () => {
    const v = fixtureRedacted();
    v.account = "<script>alert(1)</script>";
    const html = renderAuditPage(record(v));
    expect(html).not.toContain("<script>alert(1)</script>");
    expect(html).toContain("&lt;script&gt;alert(1)&lt;/script&gt;");
  });
});

describe("renderNotFound", () => {
  it("returns a friendly 404 page mentioning the id", () => {
    const html = renderNotFound("abc1234567");
    expect(html).toContain("Audit not found");
    expect(html).toContain("abc1234567");
    expect(html).toContain("melt audit share");
  });
});
