# syntax=docker/dockerfile:1.6
# Multi-stage build for weft-dispatcher.
#
# Stage 1 compiles the binary against glibc (debian:bookworm-slim).
# Stage 2 is a slim runtime with just the binary + ca-certificates +
# the runtime catalog (read by the dispatcher's describe / compile
# endpoints).
# No alpine/musl to avoid the TLS/DNS issues we'd hit later with
# reqwest's rustls vs system roots.
#
# Cargo build is cached two ways:
#   1. The cargo registry + the workspace target dir are mounted as
#      buildkit caches (`--mount=type=cache`), so an unchanged crate
#      set reuses prior fingerprints and rebuilds only the deltas.
#   2. The catalog directory is staged into the RUNTIME image only.
#      The builder doesn't read `catalog/`, so a catalog edit no
#      longer invalidates the builder's cargo layer.

# Builder uses a plain base + rustup so the toolchain is read from
# `rust-toolchain.toml` (the single source of truth for the whole
# system), NOT baked into a `rust:X` image. `--default-toolchain none`
# means the first cargo invocation auto-installs + selects exactly the
# pinned channel. Bump the toolchain in one place (rust-toolchain.toml)
# and every image follows.
FROM debian:bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

# Pin file first: rustup reads it on the next cargo call to select the
# toolchain. Then manifests + sources.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# sharing=locked: see listener.Dockerfile (parallel system builds vs
# cargo's registry lock living outside the mount).
RUN --mount=type=cache,id=weft-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-cargo-target-dispatcher,target=/build/target,sharing=locked \
    cargo build --release -p weft-dispatcher --bin weft-dispatcher \
    && cp /build/target/release/weft-dispatcher /usr/local/bin/weft-dispatcher

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -sLo /usr/local/bin/kubectl \
        https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/weft-dispatcher /usr/local/bin/weft-dispatcher
# Catalog (metadata + form specs + shared sources) is read at
# runtime by the dispatcher for describe + compile endpoints. Copied
# straight from the build context: the builder never needs it, so
# a catalog edit doesn't invalidate the cargo layer above.
COPY catalog /catalog
ENV WEFT_CATALOG_ROOT=/catalog

# Dispatcher listens on 9999 by default; map via WEFT_HTTP_PORT.
ENV WEFT_HTTP_PORT=9999
EXPOSE 9999

CMD ["weft-dispatcher"]
