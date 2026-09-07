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
storage.identified("whatsapp:m1").put_from_url(url, None, None).await?; // once per identity, see below

storage.get(&handle).await?;
storage.get_bytes(&handle).await?;
storage.get_range(&handle, range).await?;

storage.delete(&handle).await?;
storage.list().await?;

storage.keep(&handle, KeepTtl::Default).await?;
storage.presign(&handle, ttl_secs).await?;      // a temporary link, always fetchable from your body
storage.public_link(&handle, ttl_secs).await?;  // an internet-reachable link, or None
```

A stored file arriving on one of your inputs already carries a `url` inside
its marker, minted for this firing (an hour): the runtime links every file
input before your body runs, so a body that hands the value to something
that only fetches URLs needs no call of its own. The link is the
internet-reachable one when the install serves one (a public address, or a
bucket declared public), so a provider can fetch it too. Otherwise it is
signed for the cluster's own address: your body can fetch it, nothing
outside can, and a node that hands a file to something outside asks
`public_link` and inlines the bytes when it answers `None`. It is stripped
from everything you emit, park, or memoize, so the stored form is what
travels and the journal never holds a link.

Scope governs writes and lists. Key-addressed verbs act on the key's own
scope, so reading a handle works regardless of which scope you asked for.

If you pull a thing by a stable id (a message, a document at a provider),
name it: `.identified("<service>:<id>")` before the put. The same identity in
the same scope is then one file, however many runs ask for it: a
`put_from_url` asks the store first and fetches nothing when the file is
there, and two runs fetching at once cannot both land (the second sees a
conflict and retries). Pair it with `Project` scope so the copy outlives the
run that first pulled it. The identity is a label, scoped to the scope you
put in; choose one that names the source, never the content.

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

The `KeepFile` node extends or pins an execution file's lifetime after the fact.

## Files from the graph

A user-supplied file arrives as an ordinary typed input.

```json
"inputs": [
  { "name": "image", "type": "Image", "required": true }
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
every build and uploads what the code references. Current source files stay;
replaced or removed uploads expire after 30 days without access. Your node never
sees any of it: at run time the value is a normal media value, and `get` and
`get_bytes` read its bytes whichever handle it carries.

## Media in typed values

For converting whole typed values at a provider boundary, see
[Custom types](custom-types.md#media-inside-a-custom-type).

## The marker stays inside weft

The `__weft_image__` / `__weft_audio__` / `__weft_blob__` wrapper is how a
file travels between nodes: it carries the storage key the runtime reads
by. Anything you hand to something that is not weft (a provider's request
body, a bridge's action payload, a form spec a browser renders, a live
item) gets the plain thing that consumer reads: a URL string, a `data:`
URL, or a plain `{ url, mimeType, filename }` object. `externalize` does
this for a typed value, `public_link` and `presign` for one file. Wrapping
a link in a marker and sending it out puts weft's internal shape in an
external contract, and the consumer, which reads `value.url`, shows
nothing. The form image field did exactly that once, and the tasks app
rendered "(no image)" over a link that worked. A link you hand out also has
a life, so never store one: a form parks the stored file itself, and the
person who opens it gets a link minted at that moment through the
signal-token files door, however long the form waited.

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
