// GET /api/audit/[id] — render the savings projection from a stored
// audit JSON as an HTML page. Public, shareable, edge-cached. Uses
// only the uploaded JSON — no DB enrichment.

import type { VercelRequest, VercelResponse } from "@vercel/node";

import { getStore } from "../../lib/store.js";
import { isValidShortId } from "../../lib/short-id.js";
import { renderAuditPage, renderNotFound } from "../../lib/render.js";

export default async function handler(
  req: VercelRequest,
  res: VercelResponse,
): Promise<void> {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.setHeader("Allow", "GET, HEAD");
    res.status(405).json({ error: { code: "method_not_allowed", message: "GET only" } });
    return;
  }

  const id = pickId(req.query.id);
  if (id === null || !isValidShortId(id)) {
    res.status(404).setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(renderNotFound(id ?? ""));
    return;
  }

  const store = await getStore();
  const record = await store.get(id);
  if (!record) {
    res.status(404).setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(renderNotFound(id));
    return;
  }

  // No `noindex` by default — these URLs are designed to be shared.
  // Per-payload override is deliberately deferred until the CLI side
  // surfaces it (POWA-187 / POWA-89 §8 follow-up).
  const html = renderAuditPage(record);
  res.status(200);
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.setHeader("Cache-Control", "public, max-age=300, s-maxage=3600");
  res.send(html);
}

function pickId(q: string | string[] | undefined): string | null {
  if (typeof q === "string") return q;
  if (Array.isArray(q) && q.length > 0) return String(q[0]);
  return null;
}
