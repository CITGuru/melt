// POST /api/audit/share — receive an opt-in `melt audit share` upload,
// validate the envelope + redaction shape, persist with a short-id,
// and return the share URL.
//
// All limits and reasons in this file mirror the spec on POWA-189:
//   - 1 MiB max body  (HTTP 413)
//   - schema_version: 1 strict  (HTTP 400)
//   - redaction shape + literal scrub  (HTTP 422)
//   - 20 req / IP / hour rate limit  (HTTP 429)
//
// Success returns `{ "url": "<base>/audit/<id>" }` so the CLI doesn't
// need to know the runtime hostname.

import type { VercelRequest, VercelResponse } from "@vercel/node";

import { statusFor, validate } from "../../lib/validate.js";
import { getStore } from "../../lib/store.js";
import { newShortId } from "../../lib/short-id.js";
import { checkRateLimit, clientIp } from "../../lib/rate-limit.js";
import type { StoredAudit } from "../../lib/types.js";

export const MAX_BODY_BYTES = 1 << 20; // 1 MiB

export default async function handler(
  req: VercelRequest,
  res: VercelResponse,
): Promise<void> {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return jsonError(res, 405, "method_not_allowed", "POST only");
  }

  // Read the body bounded by 1 MiB. We do this before parsing to give
  // a clean 413 even if the upstream proxy already accepted the bytes.
  let raw: Buffer;
  try {
    raw = await readBoundedBody(req, MAX_BODY_BYTES);
  } catch (e) {
    if (e instanceof BodyTooLarge) {
      return jsonError(res, 413, "payload_too_large", `body exceeds ${MAX_BODY_BYTES} bytes`);
    }
    return jsonError(res, 400, "bad_body", `could not read request body: ${(e as Error).message}`);
  }

  const ct = (req.headers["content-type"] ?? "").toString().toLowerCase();
  if (!ct.includes("application/json")) {
    return jsonError(res, 415, "unsupported_media_type", "Content-Type must be application/json");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.toString("utf-8"));
  } catch (e) {
    return jsonError(res, 400, "bad_json", `invalid JSON: ${(e as Error).message}`);
  }

  // Rate-limit AFTER cheap parse failures so a flapping client doesn't
  // burn its bucket on its own bad payloads, but BEFORE storage IO so
  // a flooder can't fill the KV namespace. Use the same store instance
  // for both to keep test setup uniform.
  const store = await getStore();
  const ip = clientIp(req.headers, req.socket?.remoteAddress);
  const rl = await checkRateLimit(store, ip);
  if (!rl.allowed) {
    res.setHeader("Retry-After", String(rl.retryAfterSeconds));
    return jsonError(
      res,
      429,
      "rate_limited",
      `rate limit ${rl.limit}/h exceeded (count=${rl.count}); retry after ${rl.retryAfterSeconds}s`,
    );
  }

  const result = validate(parsed);
  if (!result.ok) {
    return jsonError(res, statusFor(result.error), result.error.kind, result.error.message);
  }

  const id = newShortId();
  const record: StoredAudit = {
    id,
    payload: result.value,
    created_at: new Date().toISOString(),
  };
  await store.put(record);

  const base = baseUrl(req);
  res.status(200).json({ url: `${base}/audit/${id}` });
}

class BodyTooLarge extends Error {
  constructor() {
    super("body too large");
  }
}

function readBoundedBody(req: VercelRequest, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    // If a previous middleware already parsed the body, prefer the
    // raw form when available, else re-serialize the parsed object.
    if ((req as unknown as { rawBody?: Buffer }).rawBody) {
      const buf = (req as unknown as { rawBody: Buffer }).rawBody;
      if (buf.byteLength > limit) return reject(new BodyTooLarge());
      return resolve(buf);
    }
    if (req.body !== undefined && req.body !== null) {
      const buf = Buffer.from(typeof req.body === "string" ? req.body : JSON.stringify(req.body));
      if (buf.byteLength > limit) return reject(new BodyTooLarge());
      return resolve(buf);
    }
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (chunk: Buffer | string) => {
      const c = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      total += c.byteLength;
      if (total > limit) {
        req.removeAllListeners("data");
        req.removeAllListeners("end");
        reject(new BodyTooLarge());
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function baseUrl(req: VercelRequest): string {
  // Honor the x-forwarded headers Vercel sets so the returned URL
  // matches the request's public origin (production or preview).
  const proto = (req.headers["x-forwarded-proto"] as string) || "https";
  const host = (req.headers["x-forwarded-host"] as string) || (req.headers.host as string) || "getmelt.com";
  return `${proto}://${host}`;
}

function jsonError(
  res: VercelResponse,
  status: number,
  code: string,
  message: string,
): void {
  res.status(status).json({ error: { code, message } });
}
