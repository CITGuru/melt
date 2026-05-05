// Test-only loader for the audit-share fixture.
//
// We read the canonical JSON from the melt-audit crate so the wire
// shape is the single source of truth across the CLI tests and the
// site tests.

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { AuditOutput } from "../lib/types.js";

const here = dirname(fileURLToPath(import.meta.url));
const fixturePath = resolve(here, "../../crates/melt-audit/tests/fixtures/audit-share-redacted.json");

export function fixtureRedacted(): AuditOutput {
  const raw = readFileSync(fixturePath, "utf-8");
  return JSON.parse(raw) as AuditOutput;
}
