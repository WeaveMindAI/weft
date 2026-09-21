# Packaging

Several nodes that belong together, sharing code and dependencies.

```text
slack/
  package.toml
  api.rs                shared code
  metadata.json         optional defaults for every node here
  access/
    metadata.json
    mod.rs
  send_message/
    metadata.json
    mod.rs
    deps.toml
```

`package.toml` is short:

```toml
[package]
name = "slack"

[dependencies]
# cargo deps every node here gets
```

Its presence is what makes the folder a package, and the members are found by
looking rather than listed.

## Shared code

Any `.rs` at the package root becomes a module, and a node reaches it as
`super`:

```rust
use super::api;
```

That is where the request-building and the error-reading for one service goes,
written once instead of in every node that talks to it.

Each package compiles as its own crate, so a node sees its own package's shared
files and nothing else. Another package's code is never on your path.

It also means editing one node recompiles that package and relinks, rather than
rebuilding the world.

A shared file whose name is not a valid Rust identifier fails at build time
naming the file.

## Defaults

A `metadata.json` at the package root is a partial document every node
inherits, key by key, with the node's own value winning.

Put `types`, `tags`, an `icon`, a `color` or a `service` recipe there once.

`type`, `label` and `description` can never be defaults. They are one node's
identity, and the compiler refuses a package file carrying any of them:

```text
package-level metadata.json must not set `type`: it is one node's identity,
not a package default
```

The merge is shallow: a node's `types` replaces the package's rather than
adding to it.

## deps.toml

Per node, and every section is optional.

```toml
[dependencies]
reqwest = { workspace = true }

[system.runtime.apt]
default = ["ffmpeg"]

[system.build.apt]
debian_12 = ["pkg-config", "libssl-dev"]

[build.env]
SOME_PATH = "{{catalog_path}}/vendor"
```

| Section | What it is for |
|---|---|
| `[dependencies]` | Cargo crates this node needs |
| `[build-dependencies]` | Cargo build-deps, for the package's own `build.rs` |
| `[system.build.<manager>]` | OS packages needed to **compile**, thrown away before the runtime image is sealed |
| `[system.runtime.<manager>]` | OS packages needed to **run** |
| `[build.env]` | Environment during `cargo build`. `{{catalog_path}}` expands to your node's folder inside the builder |

Managers are `apt`, `apk`, `yum` and `brew`. Under each, a key per distribution
like `debian_12` or `alpine_3_19`, or `default` for all of them. weft looks for
the exact key, falls back to `default`, and fails only when neither is there.

`[build.env]` is deliberately narrow. Real build logic goes in a `build.rs`.

## What you get without asking

`weft`, `tokio`, `serde`, `serde_json`, `async-trait`, `anyhow` and `tracing`
are always there. So is `weft-providers`.

`weft` also re-exports `serde_json`, `reqwest`, `reqwest_middleware`,
`async_trait` and `inventory`, so you can name those types without declaring a
dependency on them.

## Local images

An infra node's containers are built from Dockerfiles in its own folder:

```text
postgres/database/
  metadata.json       "images": ["images/credential"]
  mod.rs
  images/credential/
    Dockerfile
    bootstrap.py
```

The path is relative to **the node's own directory**, not the package root, and
the last segment becomes the name you reference in the spec.
