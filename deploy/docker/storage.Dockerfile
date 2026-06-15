# syntax=docker/dockerfile:1.6
# weft-storage: the per-tenant storage box. One Pod per tenant
# (lazy, scale-to-zero), placing file chunks across several plain-PVC
# backing disks mounted under /disks. Walls access by verified caller
# identity (relayed to the broker's /storage/authorize) and is the
# single authority for download capabilities.
#
# Does NOT read the node catalog: the build context never stages
# `catalog/`, so a catalog edit doesn't invalidate this image. Cargo
# cache mounts keep the build incremental.

# Builder uses a plain base + rustup so the toolchain is read from
# `rust-toolchain.toml` (the single source of truth), NOT baked into a
# `rust:X` image. Bump rust-toolchain.toml in one place; every image
# follows.
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
    --mount=type=cache,id=weft-cargo-target-storage,target=/build/target,sharing=locked \
    cargo build --release -p weft-storage --bin weft-storage \
    && cp /build/target/release/weft-storage /usr/local/bin/weft-storage

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/weft-storage /usr/local/bin/weft-storage

EXPOSE 8080

CMD ["weft-storage"]
