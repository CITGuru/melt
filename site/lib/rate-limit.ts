// Per-IP rate limit. KV TTL bucket: 20 submissions / IP / hour.
//
// `incrementWithTtl` is the backing primitive — atomic INCR with a
// one-shot EXPIRE on the first hit, so the bucket cleans itself up
// without an explicit cron. The choice of 20/h is from the issue
// spec; surface it via the constant so it's tunable in one place.

import type { AuditStore } from "./store.js";

export const RATE_LIMIT_PER_HOUR = 20;
export const RATE_LIMIT_TTL_SECONDS = 3600;

export interface RateLimitDecision {
  allowed: boolean;
  count: number;
  limit: number;
  retryAfterSeconds: number;
}

export async function checkRateLimit(
  store: AuditStore,
  ip: string,
): Promise<RateLimitDecision> {
  const key = `audit:rl:${ip}`;
  const count = await store.incrementWithTtl(key, RATE_LIMIT_TTL_SECONDS);
  return {
    allowed: count <= RATE_LIMIT_PER_HOUR,
    count,
    limit: RATE_LIMIT_PER_HOUR,
    retryAfterSeconds: RATE_LIMIT_TTL_SECONDS,
  };
}

// Vercel forwards client IPs in `x-forwarded-for`; the first entry is
// the originating client. Fall back to the request socket address
// only as a last resort (covers `vercel dev`).
export function clientIp(headers: Record<string, string | string[] | undefined>, fallback?: string): string {
  const xff = headers["x-forwarded-for"];
  if (typeof xff === "string" && xff.length > 0) {
    return xff.split(",")[0]!.trim();
  }
  if (Array.isArray(xff) && xff.length > 0) {
    return String(xff[0]).split(",")[0]!.trim();
  }
  const real = headers["x-real-ip"];
  if (typeof real === "string" && real.length > 0) return real;
  return fallback ?? "unknown";
}
