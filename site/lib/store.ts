// Storage adapter for `/audit/<short-id>` payloads.
//
// Production: Upstash Redis via `@upstash/redis` (Vercel marketplace
// integration; replaces the deprecated `@vercel/kv`). The
// `UPSTASH_REDIS_REST_URL` env var is the canonical signal that Redis
// has been provisioned for the project. Dev/test (`vercel dev`
// without Redis provisioned, or unit tests): process-local Map. The
// Map is intentionally not durable across cold starts — that's only
// acceptable for development. Anything that needs real persistence
// must run with Upstash Redis bound.

import type { StoredAudit } from "./types.js";

export interface AuditStore {
  put(record: StoredAudit): Promise<void>;
  get(id: string): Promise<StoredAudit | null>;
  // Atomic rate-limit increment with TTL on first hit. Returns the
  // post-increment count.
  incrementWithTtl(key: string, ttlSeconds: number): Promise<number>;
}

class InMemoryStore implements AuditStore {
  private audits = new Map<string, StoredAudit>();
  private counters = new Map<string, { count: number; expiresAt: number }>();

  async put(record: StoredAudit): Promise<void> {
    this.audits.set(record.id, record);
  }

  async get(id: string): Promise<StoredAudit | null> {
    return this.audits.get(id) ?? null;
  }

  async incrementWithTtl(key: string, ttlSeconds: number): Promise<number> {
    const now = Date.now();
    const existing = this.counters.get(key);
    if (!existing || existing.expiresAt <= now) {
      this.counters.set(key, { count: 1, expiresAt: now + ttlSeconds * 1000 });
      return 1;
    }
    existing.count += 1;
    return existing.count;
  }
}

class RedisStore implements AuditStore {
  private redis: RedisClient;
  private auditPrefix = "audit:doc:";

  constructor(redis: RedisClient) {
    this.redis = redis;
  }

  async put(record: StoredAudit): Promise<void> {
    await this.redis.set(this.auditPrefix + record.id, JSON.stringify(record));
  }

  async get(id: string): Promise<StoredAudit | null> {
    const v = await this.redis.get<string>(this.auditPrefix + id);
    if (v == null) return null;
    if (typeof v === "string") return JSON.parse(v) as StoredAudit;
    // The Upstash Redis SDK may already deserialize JSON when the
    // value was stored as an object — handle both shapes.
    return v as unknown as StoredAudit;
  }

  async incrementWithTtl(key: string, ttlSeconds: number): Promise<number> {
    const count = await this.redis.incr(key);
    if (count === 1) {
      await this.redis.expire(key, ttlSeconds);
    }
    return count;
  }
}

interface RedisClient {
  set(key: string, value: string): Promise<unknown>;
  get<T = unknown>(key: string): Promise<T | null>;
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<unknown>;
}

let cached: AuditStore | null = null;

export function resetStoreForTests(): void {
  cached = null;
}

export async function getStore(): Promise<AuditStore> {
  if (cached) return cached;
  if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
    const mod = await import("@upstash/redis");
    const redis = new mod.Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    });
    cached = new RedisStore(redis as unknown as RedisClient);
  } else {
    cached = new InMemoryStore();
  }
  return cached;
}

// Test helper — inject a custom store (e.g. the in-memory one shared
// between POST and GET handlers in `roundtrip.test.ts`).
export function setStoreForTests(store: AuditStore): void {
  cached = store;
}

export function makeInMemoryStore(): AuditStore {
  return new InMemoryStore();
}
