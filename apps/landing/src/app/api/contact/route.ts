import { NextResponse } from "next/server";

import {
  loadCloudflareEmailConfig,
  sendCloudflareEmail,
  type CloudflareEmailPayload,
} from "@/lib/cloudflare-email";
import { validateWorkEmail } from "@/lib/work-email";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const HELLO_ADDRESS = "hello@meltcomputing.com";
const DEFAULT_FROM_NAME = "Melt";

type ContactInput = {
  name: string;
  email: string;
  company: string;
  size: string;
  message: string;
};

function sanitize(value: unknown, max = 4000): string {
  if (typeof value !== "string") return "";
  return value.replace(/\s+$/u, "").trim().slice(0, max);
}

function htmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#x27;");
}

function buildNotificationEmail(input: ContactInput, fromAddress: string): CloudflareEmailPayload {
  const lines = [
    `Name: ${input.name}`,
    `Email: ${input.email}`,
    input.company ? `Company: ${input.company}` : null,
    input.size ? `Warehouse size: ${input.size}` : null,
    "",
    "Message:",
    input.message || "(no message)",
  ].filter((l): l is string => l !== null);
  const text = lines.join("\n");

  const html = `
    <div style="font-family: -apple-system, system-ui, sans-serif; line-height: 1.5; color: #111;">
      <h2 style="margin: 0 0 12px;">New contact form submission</h2>
      <p><strong>Name:</strong> ${htmlEscape(input.name)}</p>
      <p><strong>Email:</strong> <a href="mailto:${htmlEscape(input.email)}">${htmlEscape(input.email)}</a></p>
      ${input.company ? `<p><strong>Company:</strong> ${htmlEscape(input.company)}</p>` : ""}
      ${input.size ? `<p><strong>Warehouse size:</strong> ${htmlEscape(input.size)}</p>` : ""}
      <p><strong>Message:</strong></p>
      <pre style="white-space: pre-wrap; background: #f6f7f9; padding: 12px; border-radius: 8px;">${htmlEscape(input.message || "(no message)")}</pre>
    </div>
  `;

  return {
    to: HELLO_ADDRESS,
    from: { email: fromAddress, name: `${DEFAULT_FROM_NAME} Contact Form` },
    reply_to: { email: input.email, name: input.name || undefined },
    subject: `New contact: ${input.name}${input.company ? ` (${input.company})` : ""}`,
    text,
    html,
  };
}

function buildConfirmationEmail(input: ContactInput, fromAddress: string): CloudflareEmailPayload {
  const firstName = input.name.split(/\s+/)[0] || "there";
  const text = `Hi ${firstName},

Thanks for reaching out to Melt — we've received your message and will get back to you within one business day.

For reference, here's what you sent:

${input.message || "(no message)"}

In the meantime, feel free to browse the docs at https://www.meltcomputing.com or reach us directly at ${HELLO_ADDRESS}.

— The Melt team`;

  const html = `
    <div style="font-family: -apple-system, system-ui, sans-serif; line-height: 1.55; color: #111; max-width: 560px;">
      <p>Hi ${htmlEscape(firstName)},</p>
      <p>Thanks for reaching out to Melt. We've received your message and will get back to you within one business day.</p>
      <p>For reference, here's what you sent:</p>
      <pre style="white-space: pre-wrap; background: #f6f7f9; padding: 12px; border-radius: 8px;">${htmlEscape(input.message || "(no message)")}</pre>
      <p>In the meantime, feel free to browse the docs at <a href="https://www.meltcomputing.com">meltcomputing.com</a> or reply to this email directly.</p>
      <p>— The Melt team</p>
    </div>
  `;

  return {
    to: { email: input.email, name: input.name || undefined },
    from: { email: fromAddress, name: DEFAULT_FROM_NAME },
    reply_to: HELLO_ADDRESS,
    subject: "We received your message — Melt",
    text,
    html,
  };
}

export async function POST(request: Request) {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 });
  }

  const raw = (body ?? {}) as Record<string, unknown>;
  const input: ContactInput = {
    name: sanitize(raw.name, 200),
    email: sanitize(raw.email, 320),
    company: sanitize(raw.company, 200),
    size: sanitize(raw.size, 80),
    message: sanitize(raw.message, 4000),
  };

  if (!input.name) {
    return NextResponse.json({ error: "Name is required." }, { status: 400 });
  }
  if (!input.email) {
    return NextResponse.json({ error: "Email is required." }, { status: 400 });
  }

  const emailCheck = validateWorkEmail(input.email);
  if (!emailCheck.ok) {
    const error =
      emailCheck.reason === "free_provider"
        ? "Please use a work email address — we can't accept free providers (gmail, yahoo, outlook, etc.)."
        : "That email address doesn't look right.";
    return NextResponse.json({ error, reason: emailCheck.reason }, { status: 400 });
  }
  input.email = emailCheck.email;

  const config = loadCloudflareEmailConfig();
  if (!config) {
    console.error("[contact] Cloudflare email config missing");
    return NextResponse.json(
      { error: "Email service is not configured. Please email hello@meltcomputing.com directly." },
      { status: 503 },
    );
  }

  const fromAddress = process.env.CONTACT_FROM_EMAIL || "noreply@meltcomputing.com";

  const notification = await sendCloudflareEmail(config, buildNotificationEmail(input, fromAddress));
  if (!notification.ok) {
    console.error("[contact] notification send failed", notification.status, notification.error);
    return NextResponse.json(
      { error: "We couldn't send your message right now. Please email hello@meltcomputing.com directly." },
      { status: 502 },
    );
  }

  const confirmation = await sendCloudflareEmail(config, buildConfirmationEmail(input, fromAddress));
  if (!confirmation.ok) {
    console.error("[contact] confirmation send failed", confirmation.status, confirmation.error);
  }

  return NextResponse.json({ ok: true });
}
