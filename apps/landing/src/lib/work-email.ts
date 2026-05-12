const FREE_EMAIL_DOMAINS = new Set([
  "gmail.com",
  "googlemail.com",
  "yahoo.com",
  "yahoo.co.uk",
  "yahoo.co.in",
  "ymail.com",
  "rocketmail.com",
  "hotmail.com",
  "hotmail.co.uk",
  "outlook.com",
  "outlook.co.uk",
  "live.com",
  "msn.com",
  "aol.com",
  "icloud.com",
  "me.com",
  "mac.com",
  "proton.me",
  "protonmail.com",
  "pm.me",
  "gmx.com",
  "gmx.us",
  "gmx.de",
  "zoho.com",
  "yandex.com",
  "yandex.ru",
  "mail.com",
  "mail.ru",
  "fastmail.com",
  "tutanota.com",
  "tuta.io",
  "hey.com",
  "qq.com",
  "163.com",
  "126.com",
  "sina.com",
  "duck.com",
  "rediffmail.com",
]);

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export type WorkEmailResult =
  | { ok: true; email: string; domain: string }
  | { ok: false; reason: "invalid" | "free_provider" };

export function validateWorkEmail(input: string): WorkEmailResult {
  const trimmed = input.trim().toLowerCase();
  if (!EMAIL_RE.test(trimmed)) {
    return { ok: false, reason: "invalid" };
  }
  const domain = trimmed.split("@")[1];
  if (FREE_EMAIL_DOMAINS.has(domain)) {
    return { ok: false, reason: "free_provider" };
  }
  return { ok: true, email: trimmed, domain };
}
