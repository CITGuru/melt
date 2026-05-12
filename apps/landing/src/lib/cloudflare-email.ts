const CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4";

export type CloudflareEmailAddress = string | { email: string; name?: string };

export type CloudflareEmailPayload = {
  to: CloudflareEmailAddress | CloudflareEmailAddress[];
  from: CloudflareEmailAddress;
  subject: string;
  text?: string;
  html?: string;
  reply_to?: CloudflareEmailAddress;
  cc?: CloudflareEmailAddress[];
  bcc?: CloudflareEmailAddress[];
  headers?: Record<string, string>;
};

export type CloudflareEmailConfig = {
  accountId: string;
  apiToken: string;
};

export function loadCloudflareEmailConfig(): CloudflareEmailConfig | null {
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = process.env.CLOUDFLARE_EMAIL_API_TOKEN;
  if (!accountId || !apiToken) return null;
  return { accountId, apiToken };
}

export async function sendCloudflareEmail(
  config: CloudflareEmailConfig,
  payload: CloudflareEmailPayload,
): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
  const url = `${CLOUDFLARE_API_BASE}/accounts/${config.accountId}/email/sending/send`;
  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.apiToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    return { ok: false, status: 0, error: e instanceof Error ? e.message : "network error" };
  }
  if (response.ok) return { ok: true };
  const body = await response.text();
  return { ok: false, status: response.status, error: body.slice(0, 500) };
}
