# Melt landing page

Single-file static site for [getmelt.dev](https://getmelt.dev) (or wherever you point it). No build step, no dependencies.

## Local preview

Open `index.html` directly in a browser, or serve it with anything:

```bash
# Python
python3 -m http.server -d site 4000

# Node (no install)
npx --yes serve site -p 4000
```

## Deploy to Vercel

The whole site lives in this folder. Two ways to ship it:

### Option A — Vercel dashboard (recommended)

1. Import the `CITGuru/melt` repo into Vercel.
2. In project settings → **General** → set **Root Directory** to `site`.
3. Framework preset: **Other** (no build step). Build command and output directory can be left blank.
4. Deploy.

That's it — every push to `main` redeploys. `vercel.json` in this folder configures clean URLs and a small set of security headers.

### Option B — Vercel CLI

```bash
npm i -g vercel
cd site
vercel        # first run links the project
vercel --prod # deploy
```

## Editing copy

All copy lives inline in `index.html`. The structure mirrors the readme and `docs/overview.md`, so when you change the routing model or add a new feature pillar, update both.

Notable spots:

- **Topbar announcement** — top of `<body>`, the `.topbar` div. Swap the message and the link target.
- **Hero headline** — search for `Cut your Snowflake bill`.
- **Compatibility strip** — the `.compat` block lists drivers/tools. Keep it honest — only add things that actually work.
- **Routes tab content** — each `.tabs .panel` block is one route. Code samples are illustrative, not literal CLI captures.
- **Footer doc links** — point at `github.com/CITGuru/melt/blob/main/...` so they work from a deployed Vercel site (markdown isn't rendered by the static host).

## Custom domain

Once deployed, add your domain in Vercel → **Settings** → **Domains**. The `og:url` meta tag in `index.html` is set to `https://getmelt.dev`; update it if you use a different domain.

## Analytics, waitlist, and UTM attribution

The landing page captures pageviews, named CTA events, and waitlist signups, and forwards UTM attribution into both. All instrumentation is **site-only** — none of it lives in the OSS Melt CLI/proxy code paths.

### Configuration (one place)

The IDs that providers care about live in `<meta>` tags at the top of `index.html`:

```html
<meta name="melt:plausible-domain"   content="getmelt.dev" />
<meta name="melt:waitlist-endpoint"  content="https://submit-form.com/__FORMSPARK_ID__" />
<meta name="melt:outbound-ref"       content="melt-site" />
```

Update those three values once accounts are provisioned. The Plausible script tag is hardcoded with `data-domain="getmelt.dev"` for now — change it on the `<script defer data-domain=…>` line in `<head>` if you move domains.

### Analytics — Plausible

- **Provider:** [Plausible Analytics](https://plausible.io) (hosted, EU-based).
- **Why:** privacy-first, no cookie banner needed, OSS-friendly narrative for an OSS project, custom events with props are first-class.
- **Cost:** $9/mo (Growth) covers up to 10k pageviews — sufficient through a Show HN spike. Auto-upgrade kicks in if we exceed it.
- **Dashboard:** https://plausible.io/getmelt.dev (after provisioning).
- **What we track:**
  - `pageview` — automatic (Plausible script).
  - `landing_view` — once per session, with full UTM payload as props.
  - `hero_cta_click` — primary/secondary CTAs in the hero, nav, and closing section.
  - `git_clone_copy` — clicks on the copy button next to the `git clone …` snippet.
  - `snippet_copy` — copy clicks on other quickstart snippets.
  - `docs_click` — clicks on any link to `docs/…` on GitHub.
  - `waitlist_signup` — form submission success.
  - `routes_tab_view` — switching the routes tab strip.
  - Outbound links to `github.com/*` are auto-tracked by the `outbound-links` extension.
- Every event includes any captured UTM values as props — see UTM section below.
- Email addresses are **never** sent to Plausible.

To provision: create a Plausible site for `getmelt.dev`, confirm the domain in Plausible's settings, and the existing `<script>` tag will start reporting immediately. No code change needed past the hardcoded domain match.

### Waitlist — Formspark

- **Provider:** [Formspark](https://formspark.io).
- **Why:** flat unlimited submissions, no per-submit pricing, plain HTTP POST endpoint, supports arbitrary hidden fields (we use those for UTMs), survives an HN spike (~10k submits / 24h burst) without rate-limit-induced data loss.
- **Cost:** $25/mo (Plus) for unlimited submissions, custom email notifications, and CSV export. Cheapest tier that survives the spike requirement.
- **Dashboard:** https://formspark.io/forms (after provisioning).
- **PII handling:** the submitted email is sent to Formspark only. It is **not** persisted in `localStorage`/`sessionStorage` and is **not** included in any analytics event. The only metadata forwarded with each submission is: `source=melt-site`, `referrer`, and any captured `utm_*` values.
- **No double opt-in** at v1 — the form returns success immediately and renders an inline success message. Keep this in mind when exporting to a newsletter tool: pipe through Buttondown's import flow and let it do the confirmation step there if/when we start sending.

To provision: create a form in the Formspark dashboard, copy the form ID, replace `__FORMSPARK_ID__` in the `melt:waitlist-endpoint` meta tag. Submissions land in the Formspark inbox + any configured email forwarding.

Until the placeholder is replaced, the form will render an inline error and log a warning to the browser console — submissions are **not silently dropped**.

### UTM attribution

On every landing, `?utm_source` / `utm_medium` / `utm_campaign` / `utm_content` / `utm_term` are read from the URL and stored:

- **`sessionStorage`** — current-session attribution. Always wins for events fired in the same tab.
- **`localStorage`** — first-touch attribution with a **30-day TTL**. Sticks across sessions until expiry, then is cleared on next visit.
- A fresh URL UTM **always overrides** the session value, but only writes the sticky value if the sticky slot is empty (first-touch wins for sticky).

UTM values are forwarded to:

1. **Every analytics event** — included as props alongside the event-specific payload.
2. **Waitlist submissions** — populated into the JSON body sent to Formspark (`utm_source`, `utm_medium`, etc.).
3. **Outbound `github.com/*` links** — appended as `?ref=melt-site` (configurable via `melt:outbound-ref`). The full UTM payload is not appended to outbound URLs to keep them shareable; we rely on Plausible's outbound-link tracking + the form metadata for attribution.

A single `landing_view` event fires once per session with the full UTM payload, so you can see clean source breakdowns in Plausible without double-counting per-CTA events.

### Verifying instrumentation locally

```bash
# Serve the site
python3 -m http.server -d site 4000

# Open with a UTM-tagged URL and watch the network tab
open "http://localhost:4000/?utm_source=hn&utm_campaign=showhn"
```

You should see:

- A `POST` to `https://plausible.io/api/event` for the pageview, then a second one for `landing_view` with `utm_source=hn` in the payload.
- A `POST` to `https://plausible.io/api/event` named `hero_cta_click` when you click "Get started", with the same UTM props.
- A `POST` to the configured Formspark endpoint when you submit a test email, with `utm_source` and `utm_campaign` in the JSON body.

In dev tools, blocking ad-trackers/extensions may suppress the Plausible script — disable them or use an incognito window before claiming the analytics is broken.
