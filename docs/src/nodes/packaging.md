# Packaging

A node is either a folder on its own or a member of a package. You want a
package when several nodes share code.

## A bare node

A directory with a `metadata.json` at its root. It stands alone.

```
nodes/reply/
  metadata.json
  mod.rs
  deps.toml        optional
  tests.rs         optional
```

## A package

A directory with a `package.toml` at its root. Its members are **auto-detected**:
every immediate subdirectory holding a `metadata.json`. Adding a node is adding
a folder.

```
nodes/slack/
  package.toml
  api.rs                      shared code, reached as `super::api`
  metadata.json               optional PARTIAL: defaults every member inherits
  send_message/
    metadata.json
    mod.rs
    tests.rs
  receive_message/
    metadata.json
    mod.rs
```

`package.toml` carries the package name and the cargo dependencies its members
share:

```toml
[package]
name = "slack"

[dependencies]
async-trait = "0.1"
serde_json = "1"
uuid = { version = "1", features = ["v4"] }
```

Any `.rs` file at the package root is shared code, reached from a member as
`use super::<filename>;`. That is where the API wrapper goes, and where a
package defines its own [provider meter](../connections/meters.md) when its
nodes call a paid service weft does not ship.

## Nesting and discovery

The catalog walk recurses until it hits a **unit**, meaning a directory with
either a `metadata.json` or a `package.toml`, and then stops descending. So
units may sit at any depth (`catalog/ai/llm/anthropic/`), and a unit never
nests inside a unit.

Symlinks are never followed, and `target`, `node_modules`, `.git` and `.weft`
are skipped. Two units declaring the same node type is a loud collision rather
than a last-one-wins.

## What gets compiled

Only what your program actually uses.

The compiler reads every node's `metadata.json` **without compiling any node
Rust**, which is what makes the editor's live feedback fast. Codegen then emits
one cargo crate per **referenced** package, containing only the referenced
nodes, plus a registry mapping node type names to implementations.

So a project using three nodes out of the whole catalog compiles three nodes.
Nothing scans the filesystem at run time; the generated code names exactly what
it needs.

## Dependencies

`deps.toml` next to a `mod.rs`, for that one node:

```toml
[dependencies]
reqwest = { version = "0.12", features = ["json"] }

[build-dependencies]
cc = "1"

[system.build.apt]
default = ["pkg-config", "libssl-dev"]

[system.runtime.apt]
default = ["ca-certificates"]

[build.env]
SOME_PATH = "{{catalog_path}}/vendor"
```

Always available without declaring anything: `weft`, `tokio`, `serde`,
`serde_json`, `async-trait`, `anyhow`, `tracing`, `uuid`.

`[system.*]` entries declare OS packages the node needs, keyed by package
manager and optionally by distro version. That is what lets a node carry a
native dependency without every user hand-installing it.

Comment each dependency with why it is there.

## Package-level metadata

A package root may hold a **partial** `metadata.json` of defaults every member
inherits, which is how a package's nodes share a `types` block or a provider
name: [Package defaults](metadata.md#package-defaults).

## Sharing a package

Copy the folder. A package is self-contained on disk, so putting one in your
project's `nodes/` is the whole install.

Nothing pulls a package from git for you yet. That command is an open
contribution slot, and the shape it should take is in
[CONTRIBUTING](https://github.com/WeaveMindAI/weft/blob/main/CONTRIBUTING.md#pulling-somebodys-nodes-from-git-up-for-grabs).

Whatever gets built has to hold one property. A project's `nodes/` is the
complete list of what its programs can do, and nothing outside the project
folder is reached during a build. That is what makes a project directory
portable, and what stops an upgrade changing what an existing program does.
