# melt-audit

Local-only `$/savings` projection from `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. A
prospect runs one command, gets an account-specific projection in ~60 seconds,
no data leaves their machine.

Spec: [POWA-89](/POWA/issues/POWA-89). Crate is the Rust implementation half;
the binary ships both standalone (`melt-audit`) and as the `melt audit`
subcommand on `melt-cli`.

## Quickstart — fixture mode (no Snowflake required)

The fixture path drives the integration test, the README demo, and CI. It runs
on a ~10k-row synthetic `QUERY_HISTORY` export bundled in `examples/audit/`.

```bash
cargo build -p melt-audit
target/debug/melt-audit \
  --fixture examples/audit/query-history-fixture.csv \
  --account ACME-DEMO \
  --window 30d
```

Sample output (truncated):

```
Melt audit — savings projection (30d window)
─────────────────────────────────────────────────────────────────────
  Total queries                    10,000
  Total Snowflake spend            $364  (30d)  · $/query  $0.0364
  Routable to lake (static)        7,025   (70.2%)
  Routable to lake (conservative)  4,307   (43.1%)   ← top-2 tables
─────────────────────────────────────────────────────────────────────
  Projected $/query post-Melt           $0.0108  (static)
                                        $0.0207  (conservative)
  Projected $ saved (30d)             $157 – $256
  Projected $ saved (annualized)     $1,908 – $3,113
─────────────────────────────────────────────────────────────────────
```

A `melt-audit-ACME-DEMO-<date>.json` and matching `*.talkingpoints.md` land
next to the run.

## Quickstart — live Snowflake

```bash
# 1. Have your DBA paste the read-only grants:
melt-audit --print-grants

# 2. Run the audit (key-pair shown; --password and --token also supported):
melt-audit \
  --account ACME-PROD123 \
  --user MELT_AUDIT \
  --private-key ~/.snowflake/melt-audit.pem \
  --window 30d
```

The CLI exits non-zero with a remediation message naming the missing role or
view if `MELT_AUDIT` lacks `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`.

## Required Snowflake grants

Paste-and-go for a prospect's DBA. Same snippet `--print-grants` emits.

```sql
CREATE ROLE MELT_AUDIT;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE MELT_AUDIT;
-- Reads ONLY:
--   SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
--   SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
GRANT USAGE ON WAREHOUSE <wh> TO ROLE MELT_AUDIT;
GRANT ROLE MELT_AUDIT TO USER <user>;
```

`ACCOUNT_USAGE` views have a documented up-to-3-hour latency; the audit reports
the actual `MIN/MAX(START_TIME)` of the rows it pulled, so the printed window
matches what Snowflake materialized.

## Outputs

Three artifacts per run, all written to `--out-dir` (default `.`):

| Artifact | Use |
| --- | --- |
| stdout table | The single screenful above. ANSI-colored. Single-screen by design. |
| `melt-audit-<account>-<date>.json` | Stable schema (`schema_version: 1`). Top-line totals, two routable rates (`routable_static`, `routable_conservative`), `top_patterns[]`, `passthrough_reasons_breakdown`, `confidence_band_pct`, `disclaimers[]`. |
| `*.talkingpoints.md` | Pre-formatted markdown for sales / Slack. |

Two routable rates are emitted by design:

- `routable_static` — every read-only candidate the engine's classifier accepts.
  Upper bound. Useful as context, not as a defended number.
- `routable_conservative` — the static set restricted to the top-N hottest tables
  (`--top-n`, default `20`). The "what would the prospect actually sync first"
  projection. **This is the number to lead a sales conversation with.**

## Privacy

Default mode never opens an outbound HTTP connection to a Melt endpoint. The
audit pulls one query from Snowflake (as the prospect's `MELT_AUDIT` role) and
writes everything to local disk.

`QUERY_TEXT` literals are redacted to `?` before they enter `top_patterns`,
JSON, or talking-points — the same redaction the proxy's logging path uses for
hint-stripped SQL. PII / secrets pasted into ad-hoc queries do not survive the
local stage.

`melt audit share` (separate subcommand, never automatic) uploads the JSON only,
strips identifiers below schema level when `--anonymize` is set, and prints the
exact bytes about to leave before asking for confirmation.

## Accuracy bar

- **Static analysis only.** No DuckDB process spawns. No translate passes run.
  Routable queries are assumed to cost `$0` post-Melt; non-routable queries are
  unchanged from baseline. The post-Melt `$/query` is a projection, not a
  measurement.
- **±20% confidence band** printed on every output. Cloud-services credits
  ignored; warehouse-startup amortization ignored; credit price flat at
  `--credit-price` (default `$3.00`).
- **Fixture acceptance:** `routable_conservative.pct` matches a hand-graded
  `examples/audit/ground-truth.json` within ±2 percentage points. Test:
  `cargo test -p melt-audit --test fixture`.

`melt-audit` is not a benchmark, a SQL linter, or a routing simulator with full
DuckDB execution. See spec §6 for the full anti-scope list.

## Layout

```
crates/melt-audit/
├── src/
│   ├── lib.rs          # public surface (re-exports of classify/aggregate/output/...)
│   ├── main.rs         # `melt-audit` binary entrypoint
│   ├── cli.rs          # shared clap arg surface (binary + `melt audit` subcommand)
│   ├── classify.rs     # wraps melt-router::classify::{is_write,uses_snowflake_features,extract_tables}
│   ├── aggregate.rs    # local pipeline: classify → bucket → top_patterns → JSON
│   ├── credit.rs       # warehouse-size → credits-per-hour table; cost math
│   ├── output.rs       # stdout / JSON / talking-points renderers
│   ├── snowflake.rs    # live ACCOUNT_USAGE pull through SnowflakeClient
│   ├── redact.rs       # literal redaction (the only outbound-safe SQL form)
│   ├── grants.rs       # the SQL snippet --print-grants emits
│   ├── fixture.rs      # CSV loader for the bundled ~10k synthetic corpus
│   └── model.rs        # versioned JSON schema types
└── tests/
    └── fixture.rs      # spec §8 acceptance test against examples/audit/
```
