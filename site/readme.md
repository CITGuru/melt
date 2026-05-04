# Melt landing page

Single-file static site for [melt.dev](https://melt.dev) (or wherever you point it). No build step, no dependencies.

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

Once deployed, add your domain in Vercel → **Settings** → **Domains**. The `og:url` meta tag in `index.html` is set to `https://melt.dev`; update it if you use a different domain.
