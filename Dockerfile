# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the `melt` binary.
#
# Stage 1 ("builder") compiles the workspace against rust:1.93-bookworm
# with cmake / gcc available — duckdb-rs links its bundled C code, the
# optional kafka feature compiles librdkafka inline, and the Iceberg
# crate pulls aws-sdk-glue + aws-sdk-s3 (rustls only, no openssl).
#
# Stage 2 ("adbc-fetch") pulls the ADBC Snowflake native driver out
# of the official Apache Arrow pip wheel and stages just the .so for
# the runtime image. Required by the dual-execution router's Attach
# strategy (community DuckDB Snowflake extension is a thin ADBC
# wrapper). See `docs/internal/DUAL_EXECUTION.md` §12.6 for the
# design; without this, hybrid Attach falls back to passthrough
# silently (see §8 Fallback and failure modes).
#
# Stage 3 ("runtime") is debian:bookworm-slim with the certs the
# binary needs and the ADBC driver staged into /usr/local/lib/.
#
# Build:
#   docker build -t melt:dev .
#
# Build with kafka CDC support enabled (heavier image, ~+30 MB):
#   docker build -t melt:dev --build-arg MELT_FEATURES="ducklake,iceberg,melt-ducklake/kafka" .
#
# Build a slim, single-backend image:
#   docker build -t melt-ducklake:dev --build-arg MELT_FEATURES=ducklake .
#
# Skip ADBC (smaller image, hybrid Attach falls back to passthrough):
#   docker build -t melt:dev --build-arg HYBRID_ADBC=false .

ARG RUST_VERSION=1.93
ARG MELT_FEATURES=""
# Pin the ADBC Snowflake driver. Keep this aligned with the version
# the community DuckDB Snowflake extension was built against — the
# extension dlopens this .so at runtime, mismatched ABIs will fail
# at first ATTACH. Bump deliberately and test against staging.
ARG ADBC_SNOWFLAKE_VERSION=1.6.0
ARG HYBRID_ADBC=true

# ─── Stage 1: builder ──────────────────────────────────────────────
FROM rust:${RUST_VERSION}-bookworm AS builder

# duckdb-rs needs cmake + a C++ compiler for its bundled build; the
# kafka feature additionally needs libsasl2/libssl headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake pkg-config \
        libsasl2-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /melt

# Cache dependency builds: copy manifests + lockfile first, prime
# cargo's index, then drop in the source. This keeps the slow
# duckdb-sys / aws-sdk compile out of the iterative rebuild path.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

ARG MELT_FEATURES
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/melt/target \
    if [ -z "$MELT_FEATURES" ]; then \
        cargo build --release --bin melt; \
    else \
        cargo build --release --bin melt \
            --no-default-features --features "$MELT_FEATURES"; \
    fi \
 && cp target/release/melt /melt/melt-bin

# ─── Stage 2: adbc-fetch ───────────────────────────────────────────
#
# Pulls the ADBC Snowflake native driver from the official Apache
# Arrow pip wheel and stages it for the runtime image. Using the
# wheel is the most reliable cross-platform path — Apache publishes
# pre-built binaries for linux x86_64/arm64 and macOS arm64, and the
# wheel includes ABI-compatibility metadata pip resolves correctly.
#
# We don't ship Python in the runtime image; this stage is purely a
# one-shot "extract the .so" step. The adbc_driver_snowflake package
# is ~30 MB; the resulting runtime addition is the .so itself.
#
# Skipping: build with `--build-arg HYBRID_ADBC=false` to omit
# entirely. Hybrid queries will then fall back to Snowflake
# passthrough at runtime (graceful — see §12.6 in the design doc).
FROM python:3.12-slim AS adbc-fetch
ARG ADBC_SNOWFLAKE_VERSION
ARG HYBRID_ADBC
RUN if [ "$HYBRID_ADBC" = "true" ]; then \
        pip install --no-cache-dir \
            "adbc-driver-snowflake==${ADBC_SNOWFLAKE_VERSION}" \
            "adbc-driver-manager==${ADBC_SNOWFLAKE_VERSION}" \
        && find /usr/local/lib/python3*/site-packages/adbc_driver_snowflake \
            -name 'libadbc_driver_snowflake*.so*' \
            -exec cp {} /libadbc_driver_snowflake.so \; \
        && test -f /libadbc_driver_snowflake.so \
            || (echo "FATAL: ADBC Snowflake .so not found in wheel" \
                && find /usr/local/lib/python3*/site-packages/adbc_driver_snowflake -ls \
                && exit 1); \
    else \
        echo "HYBRID_ADBC=false; staging empty placeholder" \
        && touch /libadbc_driver_snowflake.so; \
    fi

# ─── Stage 3: runtime ──────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
ARG HYBRID_ADBC

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 melt \
    && useradd  --system --uid 1000 --gid melt --home /melt --shell /usr/sbin/nologin melt \
    && mkdir -p /melt /etc/melt \
    && chown -R melt:melt /melt /etc/melt

COPY --from=builder /melt/melt-bin /usr/local/bin/melt

# ADBC Snowflake driver. DuckDB's community Snowflake extension
# dlopens this at first `ATTACH ... AS sf_link` — without it, the
# pool's startup probe logs a WARN and hybrid Attach silently falls
# back to Materialize / passthrough (see §12.6 in the design doc).
# Empty file when `HYBRID_ADBC=false` (small noise tax on disk;
# saves the trip through the ADBC build stage).
COPY --from=adbc-fetch /libadbc_driver_snowflake.so /usr/local/lib/libadbc_driver_snowflake.so
RUN chmod 0644 /usr/local/lib/libadbc_driver_snowflake.so \
 && ldconfig 2>/dev/null || true

USER melt
WORKDIR /melt

# Container expects:
#   /etc/melt/melt.toml               — runtime config (mount your own)
#   /etc/melt/cert.pem, /etc/melt/key.pem — TLS material (optional; see §8)
#
# Ports:
#   8443  → proxy listener (matches the example config)
#   9090  → metrics admin (/metrics, /healthz, /readyz)
EXPOSE 8443 9090

ENV RUST_LOG=info \
    MELT_CONFIG=/etc/melt/melt.toml

# tini reaps zombies and forwards SIGTERM cleanly to the proxy's
# graceful shutdown path (see crates/melt-proxy/src/shutdown.rs).
ENTRYPOINT ["/usr/bin/tini", "--", "melt"]
CMD ["--config", "/etc/melt/melt.toml", "all"]
