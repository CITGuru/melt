// Strict, hand-rolled validator for the audit JSON envelope plus
// defense-in-depth redaction-shape checks. No zod/ajv — keeps the
// Vercel function cold-start small and the supply-chain surface
// minimal for an anonymous public POST endpoint.

import { AuditOutput, SCHEMA_VERSION } from "./types.js";

export type ValidationError =
  | { kind: "schema"; message: string }
  | { kind: "redaction"; message: string };

export type ValidationResult =
  | { ok: true; value: AuditOutput }
  | { ok: false; error: ValidationError };

// HTTP status mapping for callers. Schema = 400, redaction = 422.
export function statusFor(err: ValidationError): 400 | 422 {
  return err.kind === "schema" ? 400 : 422;
}

export function validate(raw: unknown): ValidationResult {
  const schemaErr = validateSchema(raw);
  if (schemaErr) return { ok: false, error: { kind: "schema", message: schemaErr } };

  const value = raw as AuditOutput;

  const redactionErr = validateRedaction(value);
  if (redactionErr) return { ok: false, error: { kind: "redaction", message: redactionErr } };

  return { ok: true, value };
}

function validateSchema(raw: unknown): string | null {
  if (!isPlainObject(raw)) return "body must be a JSON object";
  if (raw.schema_version !== SCHEMA_VERSION) {
    return `unsupported schema_version (got ${JSON.stringify(raw.schema_version)}, expected ${SCHEMA_VERSION})`;
  }
  if (typeof raw.account !== "string" || raw.account.length === 0) {
    return "account must be a non-empty string";
  }

  const win = raw.window;
  if (!isPlainObject(win)) return "window must be an object";
  if (typeof win.start !== "string" || typeof win.end !== "string") {
    return "window.start and window.end must be ISO-8601 strings";
  }
  if (!Number.isFinite(win.days as number) || (win.days as number) <= 0) {
    return "window.days must be a positive number";
  }

  for (const k of [
    "total_queries",
    "total_spend_usd",
    "dollar_per_query_baseline",
    "confidence_band_pct",
  ] as const) {
    if (!Number.isFinite(raw[k])) return `${k} must be a finite number`;
  }

  for (const k of ["routable_static", "routable_conservative"] as const) {
    const r = raw[k];
    if (!isPlainObject(r)) return `${k} must be an object`;
    for (const f of ["count", "pct", "dollar_per_query_post", "dollars_saved", "annualized"]) {
      if (!Number.isFinite((r as Record<string, unknown>)[f])) {
        return `${k}.${f} must be a finite number`;
      }
    }
  }

  if (!Array.isArray(raw.top_patterns)) return "top_patterns must be an array";
  for (let i = 0; i < raw.top_patterns.length; i++) {
    const p = raw.top_patterns[i];
    if (!isPlainObject(p)) return `top_patterns[${i}] must be an object`;
    for (const f of ["rank", "freq", "avg_ms", "est_dollars_in_window"]) {
      if (!Number.isFinite((p as Record<string, unknown>)[f])) {
        return `top_patterns[${i}].${f} must be a finite number`;
      }
    }
    if (typeof (p as Record<string, unknown>).table_fqn !== "string") {
      return `top_patterns[${i}].table_fqn must be a string`;
    }
    if (typeof (p as Record<string, unknown>).pattern_redacted !== "string") {
      return `top_patterns[${i}].pattern_redacted must be a string`;
    }
  }

  const pb = raw.passthrough_reasons_breakdown;
  if (!isPlainObject(pb)) return "passthrough_reasons_breakdown must be an object";
  for (const f of ["writes", "snowflake_features", "parse_failed", "no_tables"]) {
    const v = (pb as Record<string, unknown>)[f];
    if (v !== undefined && !Number.isFinite(v)) {
      return `passthrough_reasons_breakdown.${f} must be a finite number when present`;
    }
  }

  if (!Array.isArray(raw.disclaimers)) return "disclaimers must be an array of strings";
  for (let i = 0; i < raw.disclaimers.length; i++) {
    if (typeof raw.disclaimers[i] !== "string") {
      return `disclaimers[${i}] must be a string`;
    }
  }

  return null;
}

// Defense-in-depth: confirm the CLI's verb-only redaction was actually
// applied and no literal values slipped through `redact_literals`. Two
// independent checks per row.
//
// Shape: `pattern_redacted` must look like `<VERB> … FROM <fqn>` with
// an optional trailing ` …`. Anything richer (predicate columns, joins,
// projections beyond the verb) means the CLI bypassed its trim.
//
// Literals: even if the shape passes, reject any pattern that still
// contains numeric tokens, quoted strings, or parameter placeholders
// (`?`, `$1`, `:name`). Those are the exact things `redact_literals`
// is supposed to strip; their presence here means a bug we want to
// catch before persisting.
export function validateRedaction(audit: AuditOutput): string | null {
  for (let i = 0; i < audit.top_patterns.length; i++) {
    const row = audit.top_patterns[i]!;
    const shapeErr = checkPatternShape(row.pattern_redacted, row.table_fqn);
    if (shapeErr) {
      return `top_patterns[${i}].pattern_redacted bypassed CLI verb-only redaction: ${shapeErr}`;
    }
    const literalErr = checkPatternLiterals(row.pattern_redacted, row.table_fqn);
    if (literalErr) {
      return `top_patterns[${i}].pattern_redacted still contains a literal value: ${literalErr}`;
    }
  }
  return null;
}

// Allowed shape: `^<VERB> (… )?FROM <fqn>( …)?$`. The verb is one of a
// fixed allowlist so adversarial payloads can't pass arbitrary tokens
// as a "verb".
const ALLOWED_VERBS = new Set([
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "MERGE",
  "WITH",
  "COPY",
  "CREATE",
  "ALTER",
  "DROP",
  "TRUNCATE",
  "CALL",
]);

function checkPatternShape(pattern: string, fqn: string): string | null {
  const trimmed = pattern.trim();
  if (trimmed.length === 0) return "empty pattern_redacted";

  const tokens = trimmed.split(/\s+/);
  const verb = tokens[0]?.toUpperCase() ?? "";
  if (!ALLOWED_VERBS.has(verb)) return `unrecognized leading verb ${JSON.stringify(verb)}`;

  // Pattern must be exactly: `VERB`, `VERB FROM fqn`, `VERB … FROM fqn`,
  // `VERB FROM fqn …`, or `VERB … FROM fqn …`. Any other token mix
  // means the CLI didn't trim the predicate.
  const allowedShapes = [
    new RegExp(`^${verb}$`),
    new RegExp(`^${verb} … FROM ${escapeRegex(fqn)}( …)?$`),
    new RegExp(`^${verb} FROM ${escapeRegex(fqn)}( …)?$`),
  ];
  for (const re of allowedShapes) {
    if (re.test(trimmed)) return null;
  }
  return `expected "${verb} [… ]FROM ${fqn}[ …]", got ${JSON.stringify(trimmed)}`;
}

// Detect leftover literals that should never appear in a CLI-redacted
// pattern: numeric tokens (other than the rank-0 placeholder), quoted
// strings, parameter placeholders, comparison operators with values.
const LITERAL_PATTERNS: Array<{ re: RegExp; name: string }> = [
  { re: /'[^']*'/, name: "single-quoted string literal" },
  { re: /"[^"]*"/, name: "double-quoted identifier/string" },
  { re: /\?/, name: "positional parameter placeholder (?)" },
  { re: /\$\d+/, name: "numbered parameter placeholder ($N)" },
  { re: /:[A-Za-z_][A-Za-z0-9_]*/, name: "named parameter (:name)" },
  { re: /\b\d+(\.\d+)?\b/, name: "numeric literal" },
  { re: /=/, name: "= comparison" },
  { re: /<>|!=|<=|>=|</, name: "comparison operator" },
];

function checkPatternLiterals(pattern: string, fqn: string): string | null {
  // The FQN itself is permitted to contain `<redacted>` and `.`-style
  // tokens that look like comparison operators or numerics. Scrub it
  // out before scanning so the literal check only fires on tokens
  // that came from outside the table reference.
  const sanitized = fqn.length > 0 ? pattern.split(fqn).join(" ") : pattern;
  for (const { re, name } of LITERAL_PATTERNS) {
    if (re.test(sanitized)) return name;
  }
  return null;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
