# syntax=docker/dockerfile:1.6
# weft-broker: tenant-scoped Postgres frontend. Lives in `weft-db`
# alongside Postgres; tenant pods talk to it instead of touching
# Postgres directly. Validates each request's projected SA token
# via TokenReview, runs a per-endpoint scope check, then delegates.
#
# The broker does NOT read the node catalog at runtime, so the
# build context never stages `catalog/` and a catalog edit doesn't
# invalidate this image. Cargo cache mounts keep the cargo build
# incremental across edits.

# Builder uses a plain base + rustup so the toolchain is read from
# `rust-toolchain.toml` (the single source of truth for the whole
# system), NOT baked into a `rust:X` image. `--default-toolchain none`
# means the first cargo invocation auto-installs + selects the pinned
# channel. Bump rust-toolchain.toml in one place; every image follows.
FROM debian:bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# sharing=locked: see listener.Dockerfile (parallel system builds vs
# cargo's registry lock living outside the mount).
RUN --mount=type=cache,id=weft-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-cargo-target-broker,target=/build/target,sharing=locked \
    cargo build --release -p weft-broker --bin weft-broker \
    && cp /build/target/release/weft-broker /usr/local/bin/weft-broker

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/weft-broker /usr/local/bin/weft-broker

EXPOSE 9090

CMD ["weft-broker"]
