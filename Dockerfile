# syntax=docker/dockerfile:1.7

# ── Stage 1: base image with cargo-chef + native toolchain for aws-lc-sys ───
FROM rust:1.88-bookworm AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install --locked cargo-chef --version 0.1.71
WORKDIR /build

# ── Stage 2: planner — emit a recipe of dependencies for caching ───────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: builder — cook deps (cached layer), then build our source ─────
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin buddy3d-proxy
COPY . .
RUN cargo build --release --bin buddy3d-proxy

# ── Stage 4: runtime — minimal Debian + ca-certs + the binary ──────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data
RUN groupadd -r app && useradd -r -g app -u 10001 -d /data -s /usr/sbin/nologin app \
    && chown -R app:app /data
USER app
COPY --from=builder /build/target/release/buddy3d-proxy /usr/local/bin/buddy3d-proxy
EXPOSE 8554 8080
VOLUME ["/data"]
ENV TOKEN_STORE_PATH=/data/tokens.json
ENTRYPOINT ["/usr/local/bin/buddy3d-proxy"]
CMD ["serve"]
