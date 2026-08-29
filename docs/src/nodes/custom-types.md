# Custom types

A `types` key in any `metadata.json`, a node's own or a package root's shared
partial, declares named types the whole project may use.

```json
"types": {
  "ChatHistory": "List[ChatMessage]",
  "ChatMessage": "{ role: String, content: String | List[Part], name?: String }",
  "Part": "{ type: String, text?: String, image_url?: { url: Image } }"
}
```

## The four rules

**Declarations are global.** Once any metadata declares `ChatHistory`, every
node's ports and every `.weft` inline signature may name it. Declare a type
next to the node that owns the concept.

**Named types are nominal.** Only a same-named value wires in. The value wires
**out** into `JsonDict` freely, so forgetting the name is always safe. The
user's escape hatch for a hand-built dict is the `Cast` node, which validates
at run time, so your node can trust that a named input already fits its
declared structure.

**Redeclaring an identical body is absorbed silently.** Two packages may ship
the same shared type without depending on each other. A **different** body
under the same name fails the catalog load loudly, so drift between two copies
is a build error.

**Record validation is strict.** A value carrying a key the record does not
declare is refused. So declare every field the real values carry, with `?` on
the optional ones. Half-declaring a shape produces a type that rejects real
data.

## What a name buys you

A named type is a contract that survives being passed around. Without one, a
chat history is a `JsonDict` and every node that touches it works out the shape
again for itself, and they do not all reach the same answer.

## Media inside a custom type

A field declared `Image`, `Audio`, `Video`, or `Blob` is a **media slot**. In
stored form, which is what rides edges and lands in the journal, it holds a
small stored-file reference, so a conversation carrying forty images stays
cheap to journal.

A provider wants bytes or URLs. Two storage verbs convert a **whole typed
value** at that boundary, driven by the declared type, so there is no per-node
walking code anywhere:

```rust
use weft::storage::media::{ExternalizePolicy, MediaForm};

let ty = ctx.output_type("history").expect("declared on the port");
let storage = ctx.storage(StorageScope::Project);

// Going out to a provider: each media slot becomes something it can use.
let wire = storage.externalize(
    &value,
    &ty,
    ExternalizePolicy { audio: MediaForm::Inline, ..ExternalizePolicy::urls() },
).await?;

// Coming back: raw media (a data: URL, an external URL) is stored, and every
// slot becomes a stored-file value again. Already-stored slots pass through.
let stored = storage.internalize(&response_value, &ty, None).await?;
```

`MediaForm::Url` is a **preference, not a promise**: a slot becomes a public
link when an internet-reachable address is configured, and falls back to inline
base64 bytes when none is. Declare `Inline` only for consumers that accept
nothing else.

**Emit only the internalized form.** Public links expire, so one sitting in a
journal row is a broken link when somebody opens that run later.

The chat nodes in `catalog/ai` are the worked example: `ChatHistory` carries
media through an arbitrarily long conversation with one externalize per call
and one internalize per reply.

## Sharing state across executions

If you are thinking of reaching for a plain Rust `static` in a node's module,
know that a worker process multiplexes many executions of the same project, so
every firing of every node in that file sees the same instance for as long as
the worker lives.

A worker only ever hosts one project, so nothing of another tenant's can reach
it. So executions reading and writing each other's state through it is a
feature: caches, pools, warmed clients, a shared buffer one execution fills
and others drain.

```rust
/// The process-shared `GeneratorInfo` for a model. Shared because the model's
/// published rates are cached on the generator, so the price sheet is fetched
/// once per TTL rather than once per execution.
fn shared_generator(model: &str) -> GeneratorInfo {
    static POOL: OnceLock<Mutex<HashMap<String, GeneratorInfo>>> = OnceLock::new();
    let fresh = GeneratorInfo::openrouter(model);
    let mut pool = POOL.get_or_init(Mutex::default).lock().expect("generator pool lock");
    pool.entry(fresh.pricing_key()).or_insert(fresh).clone()
}
```

One rule keeps it sound: **it is a per-process layer, not durable state.** It
dies with the worker, and workers shut down when idle. Other pods never see
it. Anything that must survive a restart or be visible across pods belongs in
the durable primitives (`ctx.run`, buses, storage), with the static as at most
a warm cache in front.

And hold locks only across map lookups, never across an `.await`.

If a piece of state should be private to one execution, key it by
`ctx.execution_id`.

The same pattern covers repeated storage reads: a node that inlines the same
bytes on every firing, such as an audio clip re-sent to a provider each
conversation turn, can keep a byte cache in a static keyed by the file's
storage key. Values themselves stay single-form, so the journal replays
without any cache, which makes this a pure optimization you add when a real
workload measures slow.
