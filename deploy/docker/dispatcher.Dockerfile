# Multi-stage build for weft-dispatcher.
#
# Stage 1 compiles the binary against glibc (debian:bookworm-slim).
# Stage 2 is a slim runtime with just the binary + ca-certificates.
# No alpine/musl to avoid the TLS/DNS issues we'd hit later with
# reqwest's rustls vs system roots.

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
# toolchain. Then manifests (so cargo caches the dep layer), then src.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY catalog ./catalog

RUN cargo build --release -p weft-dispatcher --bin weft-dispatcher

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -sLo /usr/local/bin/kubectl \
        https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/weft-dispatcher /usr/local/bin/weft-dispatcher
# Catalog (metadata + form specs + shared sources) is read at
# runtime by the dispatcher for describe + compile endpoints.
COPY --from=builder /build/catalog /catalog
ENV WEFT_CATALOG_ROOT=/catalog

# Dispatcher listens on 9999 by default; map via WEFT_HTTP_PORT.
ENV WEFT_HTTP_PORT=9999
EXPOSE 9999

CMD ["weft-dispatcher"]
