# weft-broker: tenant-scoped Postgres frontend. Lives in `weft-db`
# alongside Postgres; tenant pods talk to it instead of touching
# Postgres directly. Validates each request's projected SA token
# via TokenReview, runs a per-endpoint scope check, then delegates.

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
