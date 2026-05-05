// End-to-end test of POST → GET that exercises the actual handler
// modules with a fake request/response pair. This is the
// "round-trip integration test" called out in POWA-189: the CLI's
// fixture JSON POSTs successfully and the returned short-URL renders
// the same numbers back.

import { describe, expect, it, beforeEach } from "vitest";
import { EventEmitter } from "node:events";

import postHandler, { MAX_BODY_BYTES } from "../api/audit/share.js";
import getHandler from "../api/audit/[id].js";
import { fixtureRedacted } from "./_fixture.js";
import { makeInMemoryStore, setStoreForTests, resetStoreForTests } from "../lib/store.js";

beforeEach(() => {
  resetStoreForTests();
  setStoreForTests(makeInMemoryStore());
});

describe("POST /api/audit/share", () => {
  it("accepts the canonical fixture and returns a 10-char share URL", async () => {
    const { req, res } = mockReqRes("POST", { "content-type": "application/json", "x-forwarded-for": "10.0.0.1" }, fixtureRedacted());
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(200);
    const body = res.bodyJson();
    expect(body).toHaveProperty("url");
    const url = String(body.url);
    const id = url.split("/audit/")[1]!;
    expect(id).toMatch(/^[0-9A-Za-z]{10}$/);
  });

  it("returns 422 when a pattern still contains a literal", async () => {
    const v = fixtureRedacted();
    (v.top_patterns[0] as { pattern_redacted: string }).pattern_redacted = "SELECT user_id FROM ANALYTICS.PUBLIC.<redacted> WHERE x = ?";
    const { req, res } = mockReqRes("POST", { "content-type": "application/json", "x-forwarded-for": "10.0.0.2" }, v);
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(422);
    expect(res.bodyJson().error.code).toBe("redaction");
  });

  it("returns 400 on malformed schema_version", async () => {
    const v = fixtureRedacted();
    (v as unknown as Record<string, unknown>).schema_version = 99;
    const { req, res } = mockReqRes("POST", { "content-type": "application/json", "x-forwarded-for": "10.0.0.3" }, v);
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(400);
    expect(res.bodyJson().error.code).toBe("schema");
  });

  it("returns 413 when the body exceeds 1 MiB", async () => {
    // Build a > 1 MiB body via the streaming path so the size guard
    // sees it. Use a long disclaimer string of garbage data.
    const v = fixtureRedacted();
    v.disclaimers = [
      "confidence band ±20%",
      "x".repeat(MAX_BODY_BYTES + 8),
    ];
    const { req, res } = mockReqRes(
      "POST",
      { "content-type": "application/json", "x-forwarded-for": "10.0.0.4" },
      v,
      { stream: true },
    );
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(413);
    expect(res.bodyJson().error.code).toBe("payload_too_large");
  });

  it("returns 429 on the 21st submission from the same IP", async () => {
    const ip = "10.0.0.21";
    const fixture = fixtureRedacted();
    for (let i = 0; i < 20; i++) {
      const { req, res } = mockReqRes("POST", { "content-type": "application/json", "x-forwarded-for": ip }, fixture);
      await postHandler(req as never, res as never);
      expect(res.statusCode).toBe(200);
    }
    const { req, res } = mockReqRes("POST", { "content-type": "application/json", "x-forwarded-for": ip }, fixture);
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(429);
    expect(res.bodyJson().error.code).toBe("rate_limited");
    expect(res.headers["Retry-After"]).toBe("3600");
  });

  it("returns 405 on GET", async () => {
    const { req, res } = mockReqRes("GET", { "x-forwarded-for": "10.0.0.5" });
    await postHandler(req as never, res as never);
    expect(res.statusCode).toBe(405);
  });
});

describe("GET → POST round-trip", () => {
  it("renders the same numbers from the stored fixture", async () => {
    const fx = fixtureRedacted();
    const { req: postReq, res: postRes } = mockReqRes(
      "POST",
      { "content-type": "application/json", "x-forwarded-for": "10.0.0.99", host: "getmelt.com", "x-forwarded-proto": "https" },
      fx,
    );
    await postHandler(postReq as never, postRes as never);
    expect(postRes.statusCode).toBe(200);
    const url = String(postRes.bodyJson().url);
    expect(url.startsWith("https://getmelt.com/audit/")).toBe(true);
    const id = url.split("/audit/")[1]!;

    const { req: getReq, res: getRes } = mockReqRes("GET", { "x-forwarded-for": "10.0.0.99" }, undefined, { query: { id } });
    await getHandler(getReq as never, getRes as never);
    expect(getRes.statusCode).toBe(200);
    expect(getRes.headers["Content-Type"]).toContain("text/html");
    expect(getRes.headers["Cache-Control"]).toBe("public, max-age=300, s-maxage=3600");
    const html = getRes.bodyText();
    // The same headline numbers from the source JSON must appear.
    expect(html).toContain(fx.account);
    expect(html).toContain("12,345");
    expect(html).toContain("$3,000");
    expect(html).toContain("$2,000");
    expect(html).toContain("ANALYTICS.PUBLIC.&lt;redacted&gt;");
  });

  it("returns 404 for an unknown id", async () => {
    const { req, res } = mockReqRes("GET", {}, undefined, { query: { id: "deadbeef99" } });
    await getHandler(req as never, res as never);
    expect(res.statusCode).toBe(404);
    expect(res.bodyText()).toContain("Audit not found");
  });

  it("returns 404 for an obviously invalid id", async () => {
    const { req, res } = mockReqRes("GET", {}, undefined, { query: { id: "../etc/passwd" } });
    await getHandler(req as never, res as never);
    expect(res.statusCode).toBe(404);
  });
});

interface MockOpts {
  stream?: boolean;
  query?: Record<string, string>;
}

function mockReqRes(
  method: string,
  headers: Record<string, string>,
  body?: unknown,
  opts: MockOpts = {},
): { req: MockRequest; res: MockResponse } {
  const req = new MockRequest(method, headers, body, opts);
  const res = new MockResponse();
  return { req, res };
}

class MockRequest extends EventEmitter {
  method: string;
  headers: Record<string, string>;
  body?: unknown;
  query: Record<string, string>;
  socket = { remoteAddress: "127.0.0.1" } as { remoteAddress?: string };

  constructor(method: string, headers: Record<string, string>, body: unknown, opts: MockOpts) {
    super();
    this.method = method;
    this.headers = headers;
    this.query = opts.query ?? {};
    if (opts.stream && body !== undefined) {
      // Emit body bytes asynchronously so the stream-reading code
      // path in the handler runs (used for the 413 guard test).
      const buf = Buffer.from(JSON.stringify(body));
      setImmediate(() => {
        this.emit("data", buf);
        this.emit("end");
      });
    } else if (body !== undefined) {
      this.body = body;
    }
  }
}

class MockResponse {
  statusCode = 200;
  headers: Record<string, string> = {};
  private bodyBuf: Buffer | string | object | undefined;

  status(code: number): this {
    this.statusCode = code;
    return this;
  }
  setHeader(k: string, v: string | number): this {
    this.headers[k] = String(v);
    return this;
  }
  json(obj: unknown): void {
    this.headers["Content-Type"] = this.headers["Content-Type"] ?? "application/json";
    this.bodyBuf = obj as object;
  }
  send(b: string | Buffer): void {
    this.bodyBuf = b;
  }
  bodyJson(): { error: { code: string; message: string }; url: string } & Record<string, unknown> {
    if (typeof this.bodyBuf === "object" && this.bodyBuf !== null) {
      return this.bodyBuf as { error: { code: string; message: string }; url: string } & Record<string, unknown>;
    }
    return JSON.parse(String(this.bodyBuf)) as { error: { code: string; message: string }; url: string } & Record<string, unknown>;
  }
  bodyText(): string {
    if (this.bodyBuf instanceof Buffer) return this.bodyBuf.toString("utf-8");
    if (typeof this.bodyBuf === "string") return this.bodyBuf;
    return JSON.stringify(this.bodyBuf);
  }
}
