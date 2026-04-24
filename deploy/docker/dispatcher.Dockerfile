# Multi-stage build for weft-dispatcher.
#
# Stage 1 compiles the binary against glibc (debian:bookworm-slim).
# Stage 2 is a slim runtime with just the binary + ca-certificates.
# No alpine/musl to avoid the TLS/DNS issues we'd hit later with
# reqwest's rustls vs system roots.

FROM rust:1.85-bookworm AS builder

WORKDIR /build

# Copy only the manifests first so cargo fetches deps once and
# caches the layer across source changes.
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

# Journal + project store land here.
ENV WEFT_DATA_DIR=/var/lib/weft

CMD ["weft-dispatcher"]
