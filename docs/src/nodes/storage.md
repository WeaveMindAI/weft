# Storage

A running node reads and writes files through `ctx.storage`. Every write takes
a **scope**, and the scope decides where the file lives and **how long it
lives**. Pick it by how long you need the file to survive.

## The scopes

| Scope | Lives under | Lifetime |
|---|---|---|
| `StorageScope::Execution` | `exec/<run>/` | one run, unless flagged kept |
| `StorageScope::Project` | `project/<project_id>/` | as long as the project |
| `StorageScope::Shared { name }` | `shared/<name>/` | as long as the owner |
| `StorageScope::Asset` | the project's `@asset` files | read-only; a worker write to it is refused |

**`Execution`** is the default, and it is for scratch: intermediate files,
temporary downloads, anything nothing cares about once the run ends. It sweeps
itself up.

**`Project`** is for a project's own persistent state: a cache, an index,
accumulated outputs. It outlives individual runs and is shared across the
project's executions, and it is **deleted when the project is deleted**.

**`Shared { name }`** is tied to the owner rather than any project. It
survives runs and project deletion both. Projects naming the same `name` meet
in the same space, and first use auto-grants it.

So if you want a file to survive deleting the project, that is the scope
argument and nothing else: `Project` means no, `Shared` means yes. Changing
that one argument is the whole knob.

Reach for `Shared` when it is a dataset the owner reuses across projects, a
model they paid to build, or anything they would be upset to lose while
tidying up.

## The verbs

```rust
let storage = ctx.storage(StorageScope::Project);

storage.put(bytes, mime, filename, keep).await?;
storage.put_stream(stream, mime, filename, keep).await?;
storage.put_response(resp, what, mime, filename, keep).await?;  // straight from an HTTP response
storage.put_from_url(url, filename, keep).await?;               // the runtime fetches it

storage.get(&handle).await?;
storage.get_bytes(&handle).await?;
storage.get_range(&handle, range).await?;

storage.delete(&handle).await?;
storage.list().await?;

storage.keep(&handle, KeepTtl::Default).await?;
storage.presign(&handle, ttl_secs).await?;      // a signed URL, for handing a provider bytes
storage.public_link(&handle, ttl_secs).await?;  // a token-protected link, Option<String>
```

Scope governs writes and lists. Key-addressed verbs act on the key's own
scope, so reading a handle works regardless of which scope you asked for.

## The keep rule

**An Execution-scoped file your node emits must be kept.**

Every Execution write takes `keep: Option<KeepTtl>`. `None` means the file is
swept shortly after the run ends.

That is right for scratch and wrong for anything you pulse downstream, because
an emitted reference lands in the journal and renders in the editor long after
the run, where a swept file shows up as "media expired". So:

- A node producing a user-facing artifact (a generated image, synthesized
  speech, received media) passes `Some(KeepTtl::Default)`. That is 30 days,
  and **every access bumps the clock**, so artifacts still in use never expire
  while abandoned ones age out.
- A node whose file is cheaply re-fetchable, such as a plain download, may
  expose a `keep` boolean config input defaulting to off, and pass
  `keep.then_some(KeepTtl::Default)`, letting the user decide.

The `KeepFile` node extends or pins any stored file's lifetime after the fact.

## Files from the graph

A user-supplied file arrives as an ordinary typed input.

```json
"inputs": [
  { "name": "image", "type": "Image", "required": true, "exposure": "all" }
]
```

The type drives the editor's file filter and is what gets written into source;
a `"widget": { "kind": "file_drop", "accept": "image/png" }` narrows it
further. Your node reads it with `ctx.inputs.get::<FileHandle>("image")?`.

What lands in source is one clean line:

```weft
send = TelegramSendMedia {
  file: @asset("assets/photo.png", Image)
}
```

The [asset sync](../language/files-and-reuse.md#the-asset-sync) runs before
every build and makes storage mirror what the code references. Your node never
sees any of it: at run time the value is a normal media value, and `get` and
`get_bytes` read its bytes whichever handle it carries.

## Media in typed values

For converting whole typed values at a provider boundary, see
[Custom types](custom-types.md#media-inside-a-custom-type).

## Reaching files outside the editor

The stored files are addressable independently of the editor:

```bash
weft files ls
weft files inspect <key>
weft files download <key>
weft files rm <key>
weft files usage
```

So data a project wrote in `Shared` stays reachable after the project is gone.
