# melt-audit

`melt-audit` is a read-only CLI that runs against
`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` and produces a `$/savings`
projection per Snowflake account: how much of a prospect's last
30/60/90 days of spend would have been routable to a Melt-backed
DuckDB lake. Nothing leaves the operator's machine; sharing is a
separate opt-in subcommand.

Spec on [POWA-89#document-spec](/POWA/issues/POWA-89#document-spec).
Static classifier piggybacks on `melt-router`'s `is_write`,
`uses_snowflake_features`, and `extract_tables`, so the audit's
routable count is whatever the live router would decide on the same
SQL — no second source of truth.

## What you get

Every run writes three artifacts and prints the headline table to
stdout:

- a one-screen summary with `$/query baseline → projected`, total
  `$ saved` for the window, annualized projection, top routable
  table patterns;
- `melt-audit-<account>-<date>.json` (stable schema, `schema_version: 1`);
- `melt-audit-<account>-<date>.talkingpoints.md` — a
  Slack/Notion-ready markdown bullet list.

A confidence band is printed on every output. The audit does **not**
execute queries against DuckDB; the projection is a static
classification of historical SQL, not a measurement.

## Install

`melt-audit` ships two ways. They share `AuditArgs` and the same
exit-code surface, so flags work identically:

```bash
# A. Standalone binary
cargo install --path crates/melt-audit
melt-audit --help

# B. As a subcommand of the workspace `melt` CLI
cargo run -p melt-cli -- audit --help
```

The standalone binary is the recommended way to ship the audit to
prospects (no proxy / sync runtime baggage).

## Quickstart

### 1. Print the role-creation snippet

No Snowflake connection is opened — this is paste-and-go for a DBA:

```bash
melt-audit --print-grants
```

Output is the exact SQL from
[POWA-89#document-spec](/POWA/issues/POWA-89#document-spec) §2:
`CREATE ROLE MELT_AUDIT`, `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`,
`GRANT USAGE ON WAREHOUSE <wh>`, `GRANT ROLE MELT_AUDIT TO USER <u>`.

### 2. Try it offline against the bundled fixture

A ~10k-row synthetic agent-driven dbt fixture lives in
`examples/audit/` and exercises the full local-processing pipeline
(classify → aggregate → render) without a Snowflake connection:

```bash
melt-audit \
  --account ACME-DEMO \
  --window 30d \
  --fixture examples/audit/query-history-fixture.csv \
  --out-dir /tmp/melt-audit-demo
```

You should see a stdout summary, plus
`/tmp/melt-audit-demo/melt-audit-ACME-DEMO-*.json` and
`*.talkingpoints.md`. The same fixture is asserted within ±2
percentage points of `examples/audit/ground-truth.json` by
`cargo test -p melt-audit --test fixture`.

### 3. Run live against a Snowflake account

After your DBA has run the snippet from step 1, run a real audit:

```bash
# Programmatic Access Token (PAT)
melt-audit \
  --account ACME-PROD123 \
  --token "$SNOWFLAKE_PAT" \
  --window 30d

# OR key-pair auth
melt-audit \
  --account ACME-PROD123 \
  --user MELT_AUDIT_SVC \
  --private-key /path/to/rsa_key.p8 \
  --window 30d
```

Auth notes:

- `--token` is a Snowflake Programmatic Access Token (PAT) /
  OAuth bearer.
- `--private-key` plus `--user` does key-pair auth via Snowflake's
  REST API (the same path `melt-snowflake` uses for the proxy and
  sync).
- `--password` is rejected — the Snowflake REST API has no password
  flow. Use a PAT or key-pair instead.
- The audit issues exactly one statement against
  `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` under the `MELT_AUDIT`
  role and prints any auth/grant failure with the role + grant hint.

## Flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--account <locator>` | required (live), used as the JSON `account` field in fixture mode | same identifier the Snowflake driver uses |
| `--window <30d\|60d\|90d>` | `30d` | other values rejected to keep annualization math honest |
| `--token <pat>` | — | PAT / OAuth bearer; mutually exclusive with `--private-key` |
| `--private-key <pem>` | — | requires `--user`; PEM-encoded RSA |
| `--user <login>` | — | service user for key-pair auth |
| `--warehouse <name>` | `XSMALL` | warehouse used for the audit query itself |
| `--credit-price <usd>` | `3.00` | USD per Snowflake credit; flat rate for cost math |
| `--top-n <int>` | `20` | conservative routable rate restricts to the top-N hottest tables |
| `--out-dir <path>` | `.` | destination for JSON + talking-points |
| `--no-color` | off | strip ANSI for CI captures and `> file` redirects |
| `--print-grants` | off | print role snippet and exit 0 (no connection) |
| `--fixture <csv>` | off | offline run against a `QUERY_HISTORY` CSV; no connection |

Exit codes: `0` success, `1` runtime failure (auth, grants, network,
parse), `2` usage error (bad flag combo).

## Privacy

Default is local-only: one Snowflake statement, processed on the
operator's machine, written to local files. No telemetry, no HTTP
egress to any Melt-owned endpoint.

`QUERY_TEXT` from `ACCOUNT_USAGE.QUERY_HISTORY` can contain literal
values pasted into ad-hoc queries (PII, secrets). Every literal is
redacted to `?` before it hits any output artifact — JSON,
talking-points, or the stdout `top_patterns` block.

### Sharing audit results

`melt audit share` is the **opt-in** path that uploads a redacted
copy of the audit JSON to `getmelt.com/audit/share` and returns a
short URL the operator can include in a POC ask. It is never
automatic, and never runs without an explicit `share` invocation.

```bash
# Confirm-and-upload — prints the exact bytes about to leave the
# box and prompts `Upload? [y/N]`:
melt-audit share melt-audit-ACME-DEMO-2026-05-04.json

# CI / scripted: skip the prompt with --yes
melt-audit share melt-audit-ACME-DEMO-2026-05-04.json --yes

# Auto-pick the newest melt-audit-*.json in --out-dir:
melt-audit share --out-dir /tmp/melt-audit-demo --yes
```

Privacy guarantees on top of the literal redaction the audit
pipeline already applied:

- The talking-points markdown file is **never** read by the share
  path. Operators sometimes paste real numbers into it; the share
  path opens only the JSON artifact you point it at.
- `top_patterns[].pattern_redacted` is trimmed to a verb + table
  shape (`SELECT … FROM A.B.C …`) — predicate column lists, join
  structure, `GROUP BY` / `ORDER BY` clauses, etc. are dropped.
- With `--anonymize` (default for `share`), the table identifier
  below the schema level is stripped:
  `ANALYTICS.PUBLIC.EVENTS` → `ANALYTICS.PUBLIC.<redacted>`.
- A `melt-audit-…-shared.json` receipt is written next to the source
  JSON so the operator has a verifiable record of exactly what was
  uploaded.

Pass `--anonymize=false` only when sharing internally where the
table-level identifiers are safe to disclose. Pass `--endpoint
<url>` to point the client at a non-default endpoint (used by the
integration tests that don't rely on the live share endpoint).

## Accuracy bar

- The classifier is a strict subset of `melt-router`'s decision
  pipeline. A query `melt-router` would pass through is counted as
  pass-through here.
- Two routable rates are surfaced: `static` (every routable
  candidate, upper bound) and `conservative` (candidates restricted
  to the top-N hottest tables, the realistic "what would the
  prospect actually sync first" projection). The conservative
  number is the one to lead with in a prospect conversation.
- ±20% confidence band is printed on every output. Stated rationale:
  flat `--credit-price`, contracts vary, cloud-services credits
  ignored, warehouse start-up amortization ignored.

What the projection is **not**:

- a benchmark — no DuckDB execution; real `$/savings` is measured by
  the bench harness on real proxy traffic;
- a SQL linter — three buckets (`passthrough_forced`,
  `routable_candidate`, `unknown`) and stop;
- a routing simulator — no plan-split, no Iceberg/DuckLake reads.

## Tests

```bash
cargo test -p melt-audit            # unit + fixture acceptance
cargo build -p melt-audit --release # ship binary
```

The fixture acceptance test (`tests/fixture.rs`) re-runs the local
pipeline end-to-end against the bundled CSV and pins
`routable_static.pct` and `routable_conservative.pct` to the values
in `examples/audit/ground-truth.json` within ±2 percentage points.
