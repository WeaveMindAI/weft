# weft-broker: tenant-scoped Postgres frontend. Lives in `weft-db`
# alongside Postgres; tenant pods talk to it instead of touching
# Postgres directly. Validates each request's projected SA token
# via TokenReview, runs a per-endpoint scope check, then delegates.

FROM rust:1.94-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY catalog ./catalog

RUN cargo build --release -p weft-broker --bin weft-broker

# ---

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/weft-broker /usr/local/bin/weft-broker

EXPOSE 9090

CMD ["weft-broker"]
