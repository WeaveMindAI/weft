# syntax=docker/dockerfile:1.6
# weft-listener: per-tenant event-source daemon. One image serves
# every tenant; the dispatcher spawns a Deployment per tenant and
# feeds config (tenant id, dispatcher URL, tokens) via env vars.
#
# The listener does NOT read the node catalog at runtime, so the
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

# sharing=locked on the registry: the four system images build in
# parallel and cargo's own registry lock file lives OUTSIDE the
# mounted dir, so concurrent unsynchronized writes would corrupt it.
RUN --mount=type=cache,id=weft-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-cargo-target-listener,target=/build/target,sharing=locked \
    cargo build --release -p weft-listener --bin weft-listener \
    && cp /build/target/release/weft-listener /usr/local/bin/weft-listener

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/weft-listener /usr/local/bin/weft-listener

EXPOSE 8080

CMD ["weft-listener"]
