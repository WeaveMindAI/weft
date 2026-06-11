# syntax=docker/dockerfile:1.6
# Shared builder base for every per-project worker image.
#
# Per-project worker images today start from `debian:bookworm-slim`,
# install rustup + build-essential, then compile. On a clean machine
# the first build is minutes; this base image moves the
# always-present steps (apt install, rustup, toolchain
# materialization) out of the per-project build into one shared
# layer. The engine workspace is staged in at `/weft/` (the same
# path the per-project build context expects) so per-project
# Dockerfiles do NOT need to re-COPY it.
#
# The cargo REGISTRY is deliberately NOT baked here: per-project
# builds mount a BuildKit cache at /root/.cargo/registry, which
# shadows anything baked into the image at that path. The shared
# cache mount is the registry warm.
#
# Per-project Dockerfile resolves `weft-engine = { path = "../weft/crates/weft-engine" }`
# against `/weft` from this base image, then compiles only the
# generated worker crate + per-node deps.
#
# Cargo's incremental target dir (`/work/target`) is intentionally
# NOT pre-warmed here. Per-project builds mount a BuildKit cache at
# that path, which would overlay anything baked in. The first
# per-project build pays the engine-compile cost (warming the cache
# mount); subsequent builds reuse it. Pre-warming would force a
# choice between baked layers and cache-mount incremental, and we
# pick incremental.

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# rustup with `--default-toolchain none`: the pinned toolchain comes
# from `rust-toolchain.toml`, materialized on the next `cargo`
# invocation below.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# Stage the workspace at `/weft/`, the same path the per-project
# Dockerfile's `COPY weft/ /weft/` was using. Per-project builds
# omit that COPY now and rely on this image's layer instead.
WORKDIR /weft
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Materialize the pinned toolchain (rustup installs it on the first
# cargo invocation; toolchains live in /root/.rustup, which no cache
# mount shadows). NOT `cargo fetch`: the registry it would populate
# sits at /root/.cargo/registry, which per-project builds shadow
# with their BuildKit cache mount, so baked registry bytes are dead.
RUN cargo --version
