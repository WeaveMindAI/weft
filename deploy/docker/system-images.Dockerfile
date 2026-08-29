# syntax=docker/dockerfile:1.6
# The four weft system images (dispatcher / listener / broker /
# infra-supervisor), built from ONE shared builder stage. Each image is
# a named runtime stage; the CLI selects it with
# `docker build --target <dispatcher|listener|broker|supervisor>`
# (see `weft-cli::images::ensure_system_image`).
#
# Why one file: the four binaries compile from the same workspace, so
# four separate Dockerfiles ran four full cargo passes over identical
# sources into four separate target caches. Here ONE `cargo build`
# compiles all four binaries into one shared target cache; the four
# runtime stages just copy their binary out. BuildKit deduplicates the
# builder stage across concurrent `--target` builds, so `weft daemon
# start` building all four in parallel still compiles once.
#
# Builder uses a plain base + rustup so the toolchain is read from
# `rust-toolchain.toml` (the single source of truth for the whole
# system), NOT baked into a `rust:X` image. `--default-toolchain none`
# means the first cargo invocation auto-installs + selects exactly the
# pinned channel. Bump rust-toolchain.toml in one place; every image
# follows.
#
# No alpine/musl to avoid the TLS/DNS issues we'd hit later with
# reqwest's rustls vs system roots.
#
# The catalog (metadata + form specs + shared sources) is staged into
# the DISPATCHER runtime stage only (its describe / compile endpoints
# read it). The builder never touches `catalog/`, so a catalog edit
# invalidates neither the cargo layer nor the other three images.
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

# sharing=locked on both mounts: BuildKit deduplicates this builder
# stage across the four concurrent `--target` builds, but two SEPARATE
# invocations (e.g. a second `weft daemon start`) can still mount
# concurrently, and cargo's registry lock
# file lives OUTSIDE the mounted dir while the shared target dir must
# not see two cargo invocations at once.
# SYNC: the `-p ... --bin ...` package list <-> the `ensure_system_image`
#       crate names in `provision_images`,
#       crates/weft-cli/src/commands/daemon.rs
RUN --mount=type=cache,id=weft-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-cargo-target-system,target=/build/target,sharing=locked \
    cargo build --release \
       -p weft-dispatcher -p weft-listener -p weft-broker -p weft-infra-supervisor \
       --bin weft-dispatcher --bin weft-listener --bin weft-broker --bin weft-infra-supervisor \
    && cp /build/target/release/weft-dispatcher \
          /build/target/release/weft-listener \
          /build/target/release/weft-broker \
          /build/target/release/weft-infra-supervisor \
          /usr/local/bin/

# ---
# Shared runtime bases. `runtime-kubectl` for the two images whose only
# k8s interaction surface is the kubectl binary (dispatcher +
# supervisor), pinned to one version so the two can't drift;
# `runtime-plain` for the rest.

FROM debian:bookworm-slim AS runtime-kubectl
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -sLo /usr/local/bin/kubectl \
        https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

FROM debian:bookworm-slim AS runtime-plain
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---
# weft-dispatcher: pure routing + lifecycle + placement.

FROM runtime-kubectl AS dispatcher
COPY --from=builder /usr/local/bin/weft-dispatcher /usr/local/bin/weft-dispatcher
# Catalog is read at runtime for describe + compile endpoints. Copied
# straight from the build context: the builder never needs it, so a
# catalog edit doesn't invalidate the cargo layer above.
COPY catalog /catalog
ENV WEFT_CATALOG_ROOT=/catalog
# Dispatcher listens on 9999 by default; map via WEFT_HTTP_PORT.
ENV WEFT_HTTP_PORT=9999
EXPOSE 9999
CMD ["weft-dispatcher"]

# ---
# weft-listener: per-tenant event-source daemon. One image serves every
# tenant; the dispatcher spawns a Deployment per tenant and feeds
# config (tenant id, dispatcher URL, tokens) via env vars.

FROM runtime-plain AS listener
COPY --from=builder /usr/local/bin/weft-listener /usr/local/bin/weft-listener
EXPOSE 8080
CMD ["weft-listener"]

# ---
# weft-broker: tenant-scoped Postgres frontend. Lives in `weft-db`
# alongside Postgres; tenant pods talk to it instead of touching
# Postgres directly. Validates each request's projected SA token via
# TokenReview, runs a per-endpoint scope check, then delegates.

FROM runtime-plain AS broker
COPY --from=builder /usr/local/bin/weft-broker /usr/local/bin/weft-broker
EXPOSE 9090
CMD ["weft-broker"]

# ---
# weft-infra-supervisor: per-tenant pod that owns runtime infra
# lifecycle: claims infra_lifecycle_command rows from the broker,
# executes them via kubectl, polls k8s for replica state, evaluates
# HealthProtocols, emits infra_event rows.

FROM runtime-kubectl AS supervisor
COPY --from=builder /usr/local/bin/weft-infra-supervisor /usr/local/bin/weft-infra-supervisor
CMD ["weft-infra-supervisor"]
