# Packaging

Put nodes in a package when they share code or dependencies. A package
gives them one home, while each node keeps its own metadata and implementation.

## A bare node

A standalone node is a directory containing `metadata.json` and `mod.rs`:

```text
nodes/reply/
  metadata.json
  mod.rs
  deps.toml
  tests.rs
```

`deps.toml` and `tests.rs` are optional. For the node files themselves,
follow [Your first node](your-first-node.md).

## A package

A package has a `package.toml`. Its immediate subdirectories containing
`metadata.json` become member nodes:

```text
nodes/my_service/
  package.toml
  api.rs
  metadata.json
  send_message/
    metadata.json
    mod.rs
    tests.rs
  receive_message/
    metadata.json
    mod.rs
```

For example:

```toml
[package]
name = "my_service"

[dependencies]
uuid = { version = "1", features = ["v4"] }
```

The dependency is shared by the members. A member can use `super::api`
to reach `api.rs` at the package root. This is also where you can place a
[provider meter](../connections/meters.md) shared by the service's nodes.

The root `metadata.json` is optional and contains defaults inherited by
members. For how fields combine, read
[Package defaults](metadata.md#package-defaults).

## Nesting and discovery

The catalog searches recursively until it reaches a directory containing
`package.toml` or `metadata.json`. That directory is a package or a
standalone node; discovery then follows that unit's layout.
You can organize packages under category directories, but cannot hide
another package inside a package member.

Symlinks are followed. Broken links and cycles report errors.
Directories named `target`, `node_modules`, `.git`, and `.weft` are
excluded. If different nodes declare the same type name, catalog loading
reports a collision.

## What gets compiled

The compiler reads node metadata without compiling the node implementations.
That lets it check a graph before building its worker.

For the build, it includes the nodes the program references, plus their
packages' shared Rust files and dependencies. An unused sibling node is
left out; a shared root module is still included. The generated worker
registers the selected implementations explicitly.

## Dependencies

Put node-specific dependencies in `deps.toml` beside `mod.rs`:

```toml
[dependencies]
uuid = { version = "1", features = ["v4"] }
```

The generated package already provides `weft`, `weft-providers`,
`tokio`, `serde`, `serde_json`, `async-trait`, `anyhow`, and
`tracing`. Declare other crates your code uses, and comment dependencies
whose purpose would be unclear to the next reader.

If the code needs native libraries, declare the build and runtime
requirements separately. For example, these are dependency-file fragments
for an image using `apt`:

```toml
[build-dependencies]
cc = "1"

[system.build.apt]
default = ["pkg-config", "libssl-dev"]

[system.runtime.apt]
default = ["ca-certificates"]

[build.env]
SOME_PATH = "{{catalog_path}}/vendor"
```

For a standalone node, place `build.rs` beside `mod.rs` and declare its
crates under `[build-dependencies]` in `deps.toml`. Its entry point must
be `pub fn main()`: weft calls it from the generated build script.

Named packages currently also compile their root `build.rs` as a shared
runtime module, where build-only dependencies are unavailable. A script
using a crate declared only in `[build-dependencies]` therefore fails in
that layout.

The `system.build` packages are installed in the builder image;
`system.runtime` packages go in the worker image.

A system-package table can use a distro key such as `debian_12` in place
of `default`. weft selects the matching distro entry, then falls back to
`default` if one exists. In build environment values,
`{{catalog_path}}` expands to the node's staged directory.

## Sharing a package

Copy the package folder into another project's `nodes/` directory.
Include its shared files and any assets its code or build script needs.
If you used symlinks, their targets must remain available or be copied too.

The build also needs weft and the declared Rust and system dependencies.
