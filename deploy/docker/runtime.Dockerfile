# syntax=docker/dockerfile:1.6
# The weft runtime image: the one binary every install runs
# (`weft-runtime`), every role in it, plus the agent beside every infra
# unit (`weft-runtime unit-agent`). Built by `weft daemon start` for a
# local install (where it runs the unit agents) and by the release
# workflow for a cloud one (where it runs the machine, the serverless
# roles and the agents).
#
# The builder uses a plain base + rustup so the toolchain is read from
# `rust-toolchain.toml` (the single source of truth for the whole
# system), NOT baked into a `rust:X` image.
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

# sharing=locked: two builds of this image at once (a second `weft daemon
# start`) must not run cargo in one target dir together.
RUN --mount=type=cache,id=weft-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-cargo-target-runtime,target=/build/target,sharing=locked \
    cargo build --release -p weft-runtime --bin weft-runtime \
    && cp /build/target/release/weft-runtime /usr/local/bin/

# The Docker command line, for the host agent on a cloud machine: it runs
# the machine's unit on the machine's own Docker through its socket.
FROM debian:bookworm-slim AS docker-cli
ARG TARGETARCH
ARG DOCKER_VERSION=27.3.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN case "${TARGETARCH:-amd64}" in \
      amd64) arch=x86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "no Docker build for ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${arch}/docker-${DOCKER_VERSION}.tgz" \
       | tar -xz -C /tmp \
    && cp /tmp/docker/docker /usr/local/bin/docker

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/weft-runtime /usr/local/bin/weft-runtime
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
# The weft source a project version is compiled against: the dispatcher
# compiles every version itself and stages its worker build, so this
# image carries exactly the tree the CLI of the same version hashes.
# `WEFT_REPO_ROOT` is how the compiler finds it.
# SYNC: what the runtime image carries <-> crates/weft-cli/src/images.rs
#       (runtime_image_ref's carried inputs), .dockerignore (catalog/)
COPY rust-toolchain.toml Cargo.toml Cargo.lock /opt/weft/
COPY crates /opt/weft/crates
COPY catalog /opt/weft/catalog
COPY deploy/docker/worker-builder-base.Dockerfile deploy/docker/worker-builder-base-split.sh /opt/weft/deploy/docker/
ENV WEFT_REPO_ROOT=/opt/weft
ENTRYPOINT ["weft-runtime"]
CMD ["serve"]
