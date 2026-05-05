// TypeScript shape of the `melt audit` JSON envelope.
// Mirror of `crates/melt-audit/src/model.rs` (`AuditOutput`).
// Schema version 1.

export const SCHEMA_VERSION = 1 as const;

export interface AuditWindow {
  start: string;
  end: string;
  days: number;
}

export interface RoutableSummary {
  count: number;
  pct: number;
  dollar_per_query_post: number;
  dollars_saved: number;
  annualized: number;
}

export interface PatternRow {
  rank: number;
  freq: number;
  avg_ms: number;
  table_fqn: string;
  pattern_redacted: string;
  est_dollars_in_window: number;
}

export interface PassthroughBreakdown {
  writes: number;
  snowflake_features: number;
  parse_failed: number;
  no_tables: number;
}

export interface AuditOutput {
  schema_version: 1;
  account: string;
  window: AuditWindow;
  total_queries: number;
  total_spend_usd: number;
  dollar_per_query_baseline: number;
  routable_static: RoutableSummary;
  routable_conservative: RoutableSummary;
  top_patterns: PatternRow[];
  passthrough_reasons_breakdown: PassthroughBreakdown;
  confidence_band_pct: number;
  disclaimers: string[];
}

export interface StoredAudit {
  id: string;
  payload: AuditOutput;
  created_at: string;
}
