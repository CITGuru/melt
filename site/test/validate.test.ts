// Validator behavior — schema fidelity (HTTP 400 class) and the
// defense-in-depth redaction-shape checks (HTTP 422 class).

import { describe, expect, it } from "vitest";

import { statusFor, validate } from "../lib/validate.js";
import { fixtureRedacted } from "./_fixture.js";

describe("validate (schema)", () => {
  it("accepts the redacted fixture as-is", () => {
    const r = validate(fixtureRedacted());
    expect(r.ok).toBe(true);
  });

  it("rejects non-object body with schema error", () => {
    const r = validate(42);
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe("schema");
      expect(statusFor(r.error)).toBe(400);
    }
  });

  it("rejects wrong schema_version", () => {
    const v = fixtureRedacted();
    (v as unknown as Record<string, unknown>).schema_version = 2;
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe("schema");
      expect(r.error.message).toMatch(/schema_version/);
    }
  });

  it("rejects missing account", () => {
    const v = fixtureRedacted();
    (v as unknown as Record<string, unknown>).account = "";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("schema");
  });

  it("rejects malformed window", () => {
    const v = fixtureRedacted();
    (v as unknown as Record<string, unknown>).window = { start: "x", end: "y", days: "30" };
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("schema");
  });
});

describe("validate (redaction defense-in-depth)", () => {
  it("rejects pattern that still has a predicate column", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "SELECT user_id FROM ANALYTICS.PUBLIC.<redacted>";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.error.kind).toBe("redaction");
      expect(statusFor(r.error)).toBe(422);
    }
  });

  it("rejects pattern with a numeric literal", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "SELECT … FROM ANALYTICS.PUBLIC.<redacted> 42";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("redaction");
  });

  it("rejects pattern with a quoted string literal", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "SELECT … FROM ANALYTICS.PUBLIC.<redacted> 'leak'";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("redaction");
  });

  it("rejects pattern with a parameter placeholder", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "SELECT … FROM ANALYTICS.PUBLIC.<redacted> WHERE x = ?";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("redaction");
  });

  it("rejects pattern with a comparison operator", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "SELECT … FROM ANALYTICS.PUBLIC.<redacted> >";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("redaction");
  });

  it("rejects unrecognized leading verb", () => {
    const v = fixtureRedacted();
    v.top_patterns[0]!.pattern_redacted = "EXPLAIN … FROM ANALYTICS.PUBLIC.<redacted>";
    const r = validate(v);
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.error.kind).toBe("redaction");
  });

  it("accepts the three canonical shapes", () => {
    const v = fixtureRedacted();
    v.top_patterns = [
      {
        rank: 1, freq: 1, avg_ms: 1, table_fqn: "DB.S.<redacted>",
        pattern_redacted: "SELECT … FROM DB.S.<redacted>", est_dollars_in_window: 1,
      },
      {
        rank: 2, freq: 1, avg_ms: 1, table_fqn: "DB.S.<redacted>",
        pattern_redacted: "SELECT … FROM DB.S.<redacted> …", est_dollars_in_window: 1,
      },
      {
        rank: 3, freq: 1, avg_ms: 1, table_fqn: "DB.S.<redacted>",
        pattern_redacted: "SELECT FROM DB.S.<redacted>", est_dollars_in_window: 1,
      },
    ];
    const r = validate(v);
    expect(r.ok).toBe(true);
  });
});
