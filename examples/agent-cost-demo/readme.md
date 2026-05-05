# Melt agent-cost demo — `make demo`

A single command that runs a 200-query agent-shaped workload twice — once
forced through Snowflake passthrough, once through Melt's router — and prints
a clean before/after `$/query` table. This is the asset the launch hero
asciinema and the Show HN demo are cut from (anchor metric per
[POWA-85](../../docs/internal/) GTM positioning).

```
cd examples/agent-cost-demo
make demo
```

No Snowflake account, no proxy build, no `pip install`. Stdlib only.
Reproducible across runs (seed=7). Finishes in <2s on a developer laptop.

## What it does

Two modes against the same 200-query bucket:

| Mode          | Execution                                                                | Cost model                                            |
| ------------- | ------------------------------------------------------------------------ | ----------------------------------------------------- |
| `passthrough` | Every query forced to Snowflake (the world without Melt).                | All queries bill the configured warehouse.            |
| `melt`        | Each query gets the route Melt would choose (lake / Snowflake / dual).   | Lake-routed reads cost $0; the rest bill normally.    |

Output: total queries, p50/p95 latency per mode, total cost, $/1k queries,
absolute savings, % savings, routable %, and the active cost-model
assumptions inline so anyone reading the asciinema can verify the math.

## Workload shape

Fixed mix per [POWA-94](../../) — realistic, not flattering:

| Shape                       | Count | % of total |
| --------------------------- | -----:| ----------:|
| Small filtered SELECTs      |   140 |        70% |
| Small aggregations          |    40 |        20% |
| Multi-table JOINs           |    20 |        10% |

Templates and parameter ranges live in [`agent_workload.toml`](agent_workload.toml).
Templates are deterministic per `seed`, so a given commit always prints the
same numbers — important for the asciinema not to drift between takes.

## Why Python

The other end-to-end harness in this repo (`examples/bench/`) is Python and
already defines the cost model. Three reasons to stay in Python here:

1. **One source of truth for cost numbers.** Both the bench and this demo
   point at the same Snowflake list-price assumption and the same warehouse
   credit table. Any change there has to land in one schema, not two.
2. **No new toolchain on a fresh laptop.** `make demo` only requires
   `python3` (stdlib `tomllib`, `random`, `statistics`, `subprocess`). No
   `pip install`, no `cargo build`, no Docker.
3. **Asciinema-friendly.** Output is text-only with ANSI gated on TTY, so
   recordings don't break on spinners or carriage returns.

If you'd rather drive Melt from Rust, [`examples/rust/`](../rust/) shows the
unmodified Snowflake driver path against a live proxy. That's the
production-shape integration; this directory is the launch demo.

## Cost model

All numbers are pulled from `[cost]` in `agent_workload.toml`. They're carried
over verbatim from `examples/bench/workload.toml` so claims stay consistent:

- `credit_rate_usd = 3.00` — Snowflake **Standard** edition list price.
  Enterprise is ~$4 and would show proportionally larger savings; Standard is
  the more conservative number.
  Source: <https://www.snowflake.com/pricing/>
- `default_warehouse_size = "L"` — single shared LARGE warehouse, the framing
  in melt's [readme.md](../../readme.md#why-melt-exists).
- `warehouse_credits_per_hour = { XS=1, S=2, M=4, L=8, XL=16 }` — Snowflake's
  documented doubling per size step; see
  [`docs/internal/WAREHOUSE_MANAGEMENT.md`](../../docs/internal/WAREHOUSE_MANAGEMENT.md).
- `lake_snowflake_credits_per_query = 0.0` — Lake-routed reads run on the
  local DuckDB engine; they consume zero Snowflake credits. Compute on the
  Melt host is real but small, and out of scope for v1 cost claims.

Latency model (see `[latency]` in the toml) is sampled from a piecewise
distribution carried over from the bench's `[synthetic]` block. Replace with
measured values once seed mode is wired (below).

**We never claim more savings than `routable% × passthrough_cost`.** If the
router decides to keep a query on Snowflake, the demo prices it the same way
the passthrough run does.

## Modes

- `make demo` (default `MODE=synthetic`) — runs entirely offline. Routing
  decisions come from each template's `expected_route` and latencies are
  sampled from `[latency]`. Numbers are reproducible across machines.
- `python3 demo.py --with-melt-route` — same workload, but cross-check each
  query against the local `melt route` CLI. Slower and machine-dependent (a
  router upgrade will change the numbers); useful as a regression guard, not
  for the launch asciinema.
- `make demo MODE=seed` — once [POWA-92](../../) (`melt sessions seed`) is
  merged, this mode opens a Snowflake-driver-compatible session against the
  local seed-mode proxy and times real round-trips. Cost numbers don't change
  shape (same model); only the latencies become measured rather than sampled.
  Until POWA-92 lands the demo prints a notice on stderr and falls back to
  synthetic so it still runs end-to-end.

## Recording the asciinema

```bash
asciinema rec demo.cast --rows 24 --cols 80 \
    --command "cd examples/agent-cost-demo && make demo"
```

Notes:

- ANSI is gated on `isatty()` so re-running into a pipe stays plain-text.
- Output fits in 80 columns; rendered table is ~16 lines.
- Deterministic seed → identical recording across takes.
- For the launch hero we'll want to record the seed-mode flavour once
  POWA-92 lands, so the cast shows real lake-side latencies (~100ms) rather
  than sampled ones.

## Acceptance gates ([POWA-94](../../))

- [x] 200 queries with the documented 70/20/10 shape.
- [x] Two modes: passthrough and melt; both compute $/query from the same
      cost model.
- [x] Output table fits 80 cols; ANSI gated on TTY.
- [x] Single-command run (`make demo`).
- [x] Deterministic seed.
- [ ] Top-level `readme.md` hero block points here. (See parent PR.)
- [ ] Asciinema cast attached. (Recorded after POWA-92 merges, so the cast
      shows real seed-mode latencies.)
- [ ] Seed-mode wiring → blocked on [POWA-92](../../).
