# syntax=docker/dockerfile:1.6
# Shared builder base for every per-project worker image.
#
# This image moves the project-INDEPENDENT cost out of every per-project
# worker build and pays it ONCE:
#   1. apt build packages + rustup + the pinned toolchain, and
#   2. the COMPILED stock worker: the full-library worker crate an
#      untouched project builds (the weft engine workspace, every
#      third-party crate it and the stdlib packages pull in, and one
#      `pkg_<package>` crate per stdlib package), built `--release` into
#      `/weft/target`.
#
# Why precompile the whole stock worker and not just the engine: the
# package crates pull in dependencies of their own (`sqlx`, `pyo3`,
# `tungstenite`, ...) and change how cargo unifies the features of the
# shared ones, so a base holding only the engine's dependency tree left
# every first build on a host recompiling the engine, most of that tree
# and every package crate (measured at 1m07 on a warm machine). The
# stock worker is emitted by the same codegen a project build uses, at
# the same paths (`/work` for the crate, `/weft/project-nodes` for the
# node sources, `pkg_<package>-<content slot>` for each package crate),
# so a per-project build that seeds its compile cache from this baked
# `/weft/target` finds every package crate it did not edit or add
# fingerprint-fresh and compiles only its own packages and the thin top
# crate.
#
# Sharing is safe by construction: a package crate's directory is named
# by the digest of its sources, so an edited stock package is a
# different unit, never a stale hit on this one; the engine and the
# dependencies are byte-identical across projects (same workspace
# source, same lock, same toolchain).
#
# Rebuild trigger: the base image tag is content-addressed on the
# WORKER CRATE CLOSURE (`codegen::worker_workspace_crates`: the only
# workspace crates a worker links) plus `Cargo.toml`, `Cargo.lock`,
# `rust-toolchain.toml`, the stdlib packages' `deps.toml` files (they
# decide the dependency tree and its feature unification) and this
# Dockerfile (see `hash::compute_builder_base_hash`). Edit the engine
# or a package's dependencies and the tag changes, so the setup script
# rebuilds this base and per-project worker Dockerfiles automatically
# `FROM` the new tag; edit a node's body, a non-worker crate (CLI,
# dispatcher, tests) and nothing here moves (a node body edit only
# changes that package's slot, which the next build compiles on its own).
#
# Build context: NOT the repo root. The CLI stages
# `.weft-base-context/` (`build::stage_builder_base_context`) holding
# exactly the closure crates, a workspace manifest scoped to them, the
# lock, the toolchain pin, the stock worker crate and the stdlib
# sources. The COPY paths below read from that staged layout.

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

# What the stdlib packages declare they compile with (`[system.build]`
# in their deps.toml; rendered by `build::stage_builder_base_context`
# from the same tables a project's builder stage reads). A project
# FROMing this image installs only the packages its own nodes add.
{{install_build_system_packages}}

# rustup with `--default-toolchain none`: the pinned toolchain comes
# from `rust-toolchain.toml`, materialized on the first `cargo`
# invocation below.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

# Bake the workspace at `/weft/`, the same path the per-project build
# context expects (the per-project Dockerfile omits the `COPY weft/`
# and relies on this image's layer instead).
WORKDIR /weft
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Precompile the stock worker into the shared `/weft/target`. Its crate
# (`.weft-warmup`, emitted by `codegen::emit` for the stock project) has
# the engine path deps (`../weft/crates/*`, resolving to `/weft/crates/*`),
# the fixed crates.io deps and one path dep per stdlib package crate.
# Building it at `/work` with the workspace lock and the stdlib at
# `/weft/project-nodes` pre-cooks every rlib with the SAME crate identity
# and feature unification a real worker triggers, so a per-project build
# (same layout, same lock, same target dir) reuses them.
#
# The compile runs against a PERSISTENT cache mount and the result is
# then copied into `/weft/target` as a real image layer. Two reasons
# for that split:
#   - the layer: per-project builds seed their compile cache from
#     `/weft/target`, so the precompiled rlibs must live in the image
#     itself (a cache mount would be invisible to them);
#   - the cache: successive base builds (every engine edit mints a new
#     content-addressed tag) reuse the previous build's artifacts, so
#     an engine edit recompiles only the crates it touched instead of
#     the whole dependency tree from cold. Fingerprints stay valid
#     across builds because the staging preserves source mtimes and
#     the in-container paths (/weft, /work) never change.
#
# `{{target_cache_key}}` is substituted by the staging step
# (`build::stage_builder_base_context`) with a hash of Cargo.lock +
# rust-toolchain.toml: a lock or toolchain change starts a FRESH cache
# instead of inheriting the old one. Without the key, the cache (and
# therefore the baked layer, since the whole cache is copied in)
# accumulates every dependency version and toolchain ever built:
# cargo never removes superseded artifacts, so the image would grow
# without bound across months of iteration. Inside one key, the package
# crates are the part that churns (every node edit is a new slot), so
# the same sweep a project build runs (`weft-cache-gc.sh`, emitted into
# the crate) drops the slots no base build has linked for 30 days
# before the cache is copied into the layer; `{{worker_binary}}` is the
# top crate's binary name it keeps.
#
# The registry cache id is the base's own, NOT shared with per-project
# worker builds: both sides mount `sharing=locked`, so a shared id
# would serialize a minutes-long base rebuild against every concurrent
# worker build. The one-time cost is re-fetching the crates.io
# artifacts this build needs into its own cache.
# The toolchain materializes implicitly on this first `cargo`
# invocation (rustup reads `rust-toolchain.toml`).
#
# `/work` and `/weft/project-nodes` are removed once compiled: a
# per-project build COPYs its own crate and node sources to the same
# paths, and leftovers from the stock worker (a package the project
# removed, a slot it no longer has) would sit beside them. Cargo judges
# freshness by path and mtime, and the staging mirrors the sources'
# mtimes, so the rlibs stay fresh for the files the project puts back.
COPY .weft-warmup /work
COPY project-nodes /weft/project-nodes
{{build_env_lines}}
RUN --mount=type=cache,id=weft-builder-base-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=weft-builder-base-target-{{target_cache_key}},target=/cache/target,sharing=locked \
    cp /weft/Cargo.lock /work/Cargo.lock \
    && cd /work \
    && CARGO_TARGET_DIR=/cache/target cargo build --release \
    && ( sh /work/weft-cache-gc.sh /cache/target/release 30 /work {{worker_binary}} \
         || echo 'weft: the compile cache sweep failed; the build is unaffected' >&2 ) \
    && mkdir -p /weft/target \
    && cp -a /cache/target/. /weft/target/ \
    && rm -rf /work /weft/project-nodes
