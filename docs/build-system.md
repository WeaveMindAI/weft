# Build system: per-node deps, system packages, build env

This doc covers how `weft build` turns a project into a worker
container image, and the per-node declarations that feed into
both the compiled Rust binary and the generated Dockerfile.

## What runs where

The host runs: docker, kind, kubectl, the `weft` CLI. That's it.
No Rust, no Python, no distro-specific libraries. Everything the
worker binary needs to compile happens inside a docker build.

The CLI's `weft build` pipeline:

1. Parse the weft source on the host.
2. Enrich + validate against the catalog.
3. Codegen a cargo crate at `.weft/target/build/` (Cargo.toml, src/,
   one `pkg_<name>.rs` shim per referenced catalog package).
4. Emit a multi-stage Dockerfile at `.weft/target/Dockerfile.worker`.
5. Stage the docker build context at `.weft/target/worker-image/`
   with the generated crate, the weft workspace, and the Dockerfile.
6. Run `docker build`. Inside docker:
   - Stage 1 (builder) installs build-time system packages, fetches
     rust via rustup, mounts `/work/` (the generated crate) and
     `/weft/` (the weft workspace), runs `cargo build --release`,
     produces the binary at `/worker`.
   - Stage 2 (runtime) installs runtime-only packages, copies the
     binary from the builder. No Rust in the final image.
7. If a kind cluster is available, `kind load` the image.

## Per-node `deps.toml`

Single-node packages keep a `deps.toml` next to the node's
`metadata.json` and `mod.rs`. Multi-node packages may declare
shared cargo deps in the package's `package.toml`
(`[dependencies]`); per-node `deps.toml` still applies on top.

### `[dependencies]`

Standard cargo syntax. Keys are crate names, values are whatever
cargo accepts.

```toml
[dependencies]
reqwest = { version = "0.12", features = ["json", "stream"] }
```

### `[system.build]` and `[system.runtime]`

OS-level packages, split by build stage. Each stage has four
manager-keyed tables (`apt`/`apk`/`yum`/`brew`). Each manager
table is keyed by `<distro>_<major>` or `default`.

- `[system.build]`: packages the BUILDER needs to compile the
  worker binary. Development headers, `pkg-config`, `openssl-dev`,
  `libpython3-dev`, and so on. Installed only in the builder
  stage, discarded before the final image is sealed.
- `[system.runtime]`: packages the RUNTIME needs at execution
  time. Shared libraries like `libpython3.X`, `ca-certificates`,
  `libssl3`. Installed in the final image.

For each stage and the chosen base image, codegen picks:

1. the `<distro>_<major>` entry matching the base image if present,
2. else the `default` entry,
3. else the node contributes nothing on that manager.

If a node declares entries for this manager but none match the
base AND no `default` exists, the build fails with a clear error.

Example (ExecPython):

```toml
[system.build.apt]
default = ["libpython3-dev", "pkg-config"]

[system.build.apk]
default = ["python3-dev", "pkgconfig"]

[system.runtime.apt]
default = ["python3", "python3-minimal"]

[system.runtime.apk]
default = ["python3"]
```

**Why split build vs runtime?** Docker multi-stage lets us
install dev headers and the Rust toolchain in the builder, then
discard them. The final image stays slim. This mirrors how
distro package maintainers already split `libfoo` (runtime) from
`libfoo-dev` (headers, tooling).

### `[build.env]`

Environment variables the builder container sets before `cargo
build`. Merged (union) across every referenced node, conflicts
abort the build with a clear error.

Values support one substitution: `{{catalog_path}}` expands to
the node's directory inside the builder container's
`/weft/catalog` mount.

Rare; most nodes don't need anything here. If your crate has a
`build.rs` that reads an env var (protoc location, feature
gates, etc.), this is where you declare it.

## Project-level build config

Optional `[build]` block in `weft.toml`. When absent, defaults
apply (Debian slim base + built-in Dockerfile template).

```toml
[build.worker]
# Override the default Debian slim base.
base_image = "ubuntu:24.04"

# Or ship a fully custom Dockerfile template.
# Path is relative to the project root.
dockerfile_template = "deploy/worker.Dockerfile.tmpl"
```

### Supported distros

The codegen infers `<distro>_<major>` from the base image:

| Base image                    | Manager | Distro key     |
|-------------------------------|---------|----------------|
| `debian:bookworm-slim`        | apt     | `debian_12`    |
| `debian:trixie-slim`          | apt     | `debian_13`    |
| `ubuntu:24.04`                | apt     | `ubuntu_24_04` |
| `alpine:3.19`                 | apk     | `alpine_3_19`  |
| `rockylinux:9-minimal`        | yum     | `rocky_9`      |
| `fedora:40`                   | yum     | `fedora_40`    |
| `python:3.13-slim-bookworm`   | apt     | `debian_12`    |

Unknown base images fall back to apt with an empty distro key;
codegen uses each node's `default` entries only. A warning is
logged.

### Custom Dockerfile templates

`dockerfile_template` makes codegen read a user-supplied file
instead of the built-in template. Five substitution tokens are
rendered:

| Token                              | Replaced with |
|------------------------------------|---------------|
| `{{base_image}}`                   | The `base_image` string (or the default). |
| `{{install_build_system_packages}}`| A `RUN ... install ...` line for build-stage packages, or empty. |
| `{{install_runtime_system_packages}}`| Same for runtime-stage packages. |
| `{{build_env_lines}}`              | One `ENV K=V` line per entry in the merged `[build.env]`. |
| `{{binary_name}}`                  | The sanitized crate name (tells the builder what binary to copy). |

The built-in template (see `worker_image.rs::default_template`)
is minimal:

```Dockerfile
# syntax=docker/dockerfile:1.6

FROM {{base_image}} AS builder
{{install_build_system_packages}}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /work
COPY build/ /work/
COPY weft/ /weft/

{{build_env_lines}}

RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/work/target,sharing=locked \
    cargo build --release \
    && cp target/release/{{binary_name}} /worker

FROM {{base_image}}
{{install_runtime_system_packages}}
COPY --from=builder /worker /usr/local/bin/worker
ENTRYPOINT ["/usr/local/bin/worker"]
```

A custom template can put arbitrary `RUN`, `ENV`, `USER` lines
around those tokens.

## Embedding Python (the PyO3 pattern)

ExecPython demonstrates a crate that links against a native
system library (libpython). The pattern generalizes to any
PyO3-using node, or any crate pulling `openssl-sys`,
`zstd-sys`, etc.

1. `[dependencies]` adds the cargo crate normally.
2. `[system.build.<manager>]` declares the `-dev` package that
   provides headers + the link-time library.
3. `[system.runtime.<manager>]` declares the matching runtime
   shared library.

Because the builder and runtime stages run on the SAME base
image, whatever libpython version the builder linked against is
available at runtime by construction. No abi3 shims, no
PYO3_CONFIG_FILE tricks, no manual version pinning.

## Caching

The staged build context is content-deterministic for a given
(project id, node set, base image, source) tuple. Docker's
layer cache reuses:

- The `FROM` layer across all projects using the same base.
- The `install_build_system_packages` layer across projects with
  the same build-stage packages.
- The cargo registry via `--mount=type=cache`, so
  `cargo fetch` only runs once per host, not per project.
- The `target/` directory via `--mount=type=cache`, so
  incremental compilation works across rebuilds.

On kind clusters, `kind load` short-circuits when the image
hash already matches what's on the node.

## Adding a new system-deps-carrying node

1. Add the cargo dep to your node's `deps.toml` `[dependencies]`.
2. For each supported package manager, declare a `default` entry
   AND (if the package name varies across distro versions)
   per-version entries under `[system.build.<manager>]` and
   `[system.runtime.<manager>]`.
3. If your crate's build script needs environment variables,
   add them under `[build.env]`.
4. `weft build` picks everything up automatically.

The CLI logs the chosen base image, distro key, manager, and
final package lists when it emits the Dockerfile, so you can
spot-check the result.
