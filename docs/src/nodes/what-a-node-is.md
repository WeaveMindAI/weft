# What a node is

A folder with two files in it.

```text
word_count/
  metadata.json    what it takes and what it gives back
  mod.rs           what it does
```

The `metadata.json` is what makes the folder a node. Put one anywhere under
your project's `nodes/` directory and weft finds it.

| File | Needed | What it is |
|---|---|---|
| `metadata.json` | Yes | The declared surface: ports, types, settings. Go and read [metadata.json](metadata.md) |
| `mod.rs` | Yes, to run | The Rust: one `impl Node` |
| `deps.toml` | No | Extra crates, system packages, build environment |
| `tests.rs` | No | Its own tests. Go and read [testing a node](testing.md) |
| `images/<name>/Dockerfile` | Infra nodes only | A container this node needs built |

## A folder with metadata and no code

That is not an error. weft finds it, reports it as pending, and leaves it out
of the build. A program that never names it builds fine, and a program that
does gets told which folder is waiting for its Rust.

So writing the interface first and the body second is a supported way to work.
It is also worth knowing when a node you are sure exists comes back as an
unknown type: check whether its `mod.rs` is there.

## Packages

Drop a `package.toml` in a folder and it becomes a package. Every subfolder
holding a `metadata.json` is then one of its nodes, found automatically. There
is no list to maintain.

```text
slack/
  package.toml        [package] name = "slack", plus shared dependencies
  api.rs              shared code, reached from a node as `super::api`
  metadata.json       optional defaults every node here inherits
  access/
    metadata.json
    mod.rs
  send_message/
    metadata.json
    mod.rs
```

Package defaults merge key by key, and a node's own value wins. `type`, `label`
and `description` can never be defaults, because they are one node's identity,
and the compiler refuses a package file carrying any of them.

Two nodes with the same `type` fail loudly rather than one shadowing the other.

## The trait

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>;
    // everything below has a default
    fn node_type(&self) -> &'static str;
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec>;
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()>;
    fn tests(&self) -> Vec<NodeTest>;
}
```

`run` is the only one you have to write.

`provision_infra` and `setup_trigger` have defaults that fail loudly, naming
your node, if your metadata said you implement them and you did not. So
`requires_infra: true` without a `provision_infra` is an error you get told
about, not a silent nothing.

You never check which phase you are in. The engine reads your metadata and
calls the right body. For a plain node that means `run` is called in every
phase, including while a trigger is being set up, because a value feeding a
trigger's settings has to be produced then too. For a trigger, `run` only fires
on a real event, with the payload on `ctx.wake`.

## How it gets into the binary

There is no registration file. The compiler walks your `nodes/` folder, finds
the metadata, and generates the lookup table. Adding a node is adding a folder.

It only generates what your program references, and it emits one cargo crate
per package, so editing one node recompiles that package and relinks rather
than rebuilding everything.

Your program's definition is not baked into the binary. The worker fetches it
by hash at run time, which is why rewiring your graph without touching any Rust
is a cache hit on the image.

## The one macro

```rust
#[derive(NodeManifest)]
pub struct TextNode;
```

That reads the `metadata.json` sitting beside your file and attaches it to your
type. It reads the package defaults too, so what your code sees at run time is
the same merged document the compiler saw.

It checks at compile time that the file is there, is valid JSON, is an object,
and has a string `type`. Editing the JSON rebuilds the crate.

The struct's name is how it finds the file, so `TextNode` has to live in the
same folder as its metadata. By convention the struct is the node's `type` with
`Node` on the end.

Next, [write one](your-first-node.md).
