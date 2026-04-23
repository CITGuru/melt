# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the `melt` binary.
#
# Stage 1 ("builder") compiles the workspace against rust:1.93-bookworm
# with cmake / gcc available — duckdb-rs links its bundled C code, the
# optional kafka feature compiles librdkafka inline, and the Iceberg
# crate pulls aws-sdk-glue + aws-sdk-s3 (rustls only, no openssl).
#
# Stage 2 ("runtime") is debian:bookworm-slim with just the certs the
# binary needs to talk to Snowflake / S3 / catalog REST endpoints.
#
# Build:
#   docker build -t melt:dev .
#
# Build with kafka CDC support enabled (heavier image, ~+30 MB):
#   docker build -t melt:dev --build-arg MELT_FEATURES="ducklake,iceberg,melt-ducklake/kafka" .
#
# Build a slim, single-backend image:
#   docker build -t melt-ducklake:dev --build-arg MELT_FEATURES=ducklake .

ARG RUST_VERSION=1.93
ARG MELT_FEATURES=""

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

# ─── Stage 2: runtime ──────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 melt \
    && useradd  --system --uid 1000 --gid melt --home /melt --shell /usr/sbin/nologin melt \
    && mkdir -p /melt /etc/melt \
    && chown -R melt:melt /melt /etc/melt

COPY --from=builder /melt/melt-bin /usr/local/bin/melt

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
