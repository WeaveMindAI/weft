# weft-listener: the per-project event-source daemon. One image
# serves every project; the dispatcher spawns a Deployment per
# project and feeds config via env vars.

FROM rust:1.85-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY catalog ./catalog

RUN cargo build --release -p weft-listener --bin weft-listener

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/weft-listener /usr/local/bin/weft-listener

EXPOSE 8080

CMD ["weft-listener"]
