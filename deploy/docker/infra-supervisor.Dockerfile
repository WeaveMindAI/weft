# Multi-stage build for weft-infra-supervisor.
#
# Per-tenant pod that owns runtime infra lifecycle: claims
# infra_lifecycle_command rows from the broker, executes them via
# kubectl, polls k8s for replica state, evaluates HealthProtocols,
# emits infra_event rows.

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

RUN cargo build --release -p weft-infra-supervisor --bin weft-infra-supervisor

# ---

FROM debian:bookworm-slim AS runtime

# kubectl is the supervisor's only k8s interaction surface. We pin
# the same version the dispatcher uses (v1.31.0).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -sLo /usr/local/bin/kubectl \
        https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/weft-infra-supervisor /usr/local/bin/weft-infra-supervisor

CMD ["weft-infra-supervisor"]
