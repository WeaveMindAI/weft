# Authoring Nodes

This guide covers patterns and gotchas when writing a node implementation
for Weft. Start with the existing nodes in `catalog/` for working
examples; this document explains the cross-cutting concerns that aren't
obvious from any single node.

## A node does ONE thing; Weft does the coordination

A node is a single, sharply-scoped capability: call this API, transcribe
this audio, write this row, render this PDF. It takes its inputs, does
that one thing, and pulses its result downstream. It does not orchestrate.

**Control flow is the graph's job, not the node's.** Looping, retrying,
branching, fanning out, gathering results, waiting for a human between two
steps: Weft expresses all of it declaratively with `Loop(...)`, edges, and
the wait primitives, and the engine gives you per-iteration journaling,
resumability, and cancellation for free. A node that reimplements that
control flow inside its own Rust body throws all of it away and takes on
correctness burdens (replay, idempotency, cancellation) that the engine
would otherwise have carried for it.

Concretely, when you feel the urge to write:

- a `for` / `loop` / `while` that drives a multi-step process, reach for a
  weft `Loop(...)` in the graph and let each iteration fire your node once;
- a retry-with-backoff around a call, express the retry in the graph;
- "call A, then depending on the result call B or C", wire A's outputs to
  B and C and let the null-propagation rule pick the branch;
- "do some work, then wait for a human, then do more work", split it into
  a work node, a human node, and a second work node.

The reward is not stylistic. A node that does one thing and returns is
trivially cancellable, trivially replayable, needs no `ctx.run`, cannot
double-charge anyone, and composes into graphs its author never imagined.
A node that owns a loop with a wait inside it is a small workflow engine
hiding inside a node, and it has to solve, by hand, every problem Weft
already solved. The closing section of this guide documents how to survive
that shape when a genuine constraint forces it. Treat needing it as a
signal that the work belongs in the graph.

## Writing a basic node

A node is a directory under the project's `nodes/` root:

```
nodes/my_node/
  mod.rs              # the Rust impl (a `Node` trait impl)
  metadata.json       # the node's declared surface (inputs, outputs, features)
  deps.toml           # optional: extra cargo deps beyond the codegen base
```

The trait (in `weft`). A node implements up to three separately
named bodies, and the ENGINE picks which to call from the manifest; a
node never inspects the lifecycle phase itself:

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    /// Infra nodes only (`requires_infra: true`): the desired infra shape.
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag)
        -> WeftResult<InfraSpec> { /* default: error */ }
    /// Triggers only (`features.isTrigger: true`): register the wake
    /// signal. Called INSTEAD of `run` at registration time.
    async fn setup_trigger(&self, ctx: ExecutionContext)
        -> WeftResult<()> { /* default: error */ }
    /// The node's normal body. The ONLY way to fire downstream is
    /// `ctx.pulse_downstream(output)`.
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>;
}
```

The `NodeManifest` supertrait carries the node's identity and surface,
and `#[derive(NodeManifest)]` on the node struct implements it by
embedding the `metadata.json` sitting next to the node's source file:
the node type comes from the json's `type` field, and a missing or
malformed json is a compile error. You never write `node_type` or
`metadata` by hand.

A complete minimal node (the stdlib `Text`):

```rust
//! Text: emit a literal string configured at design time.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.inputs.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

(Everything a node body needs imports from the `weft` crate; it is the
one author-facing name.)

### Reading values: `ctx.inputs`

A node reads its named values from ONE bag: `ctx.inputs`. However an
input got its value (a wire, a body literal, or the input's declared
default), and whether it is a metadata-declared input or an
INSTANCE-added custom port, the node reads it here; every input name is
unique on a node, so the read is always unambiguous. Precedence when
several sources could supply a value: a wire or body literal wins, then
the declared default. (A trigger's fire payload is the separate
`ctx.wake` bag, below.)

Both bags expose the same accessors:

- `.get::<T>("name")?`: required, typed, loud error when absent or
  mistyped (the error names the input).
- `.opt::<T>("name")?`: optional (`Ok(None)` when absent or null), but
  a PRESENT wrong-typed value still errors loud.
- `.get_or("name", default)?`: absent means the default, wrong type
  still errors. The blessed pattern for defaulted knobs. (Don't write
  `.get(..).unwrap_or(..)`: it swallows a real type error.)
- `.raw("name")`: the optional raw JSON for pass-through reads; a
  REQUIRED raw read is `.get::<Value>("name")?`.
- Three ITERATION projections (the named reads above cover every
  input uniformly; these are only for looping without knowing names):
  `.iter()` = every named value; `.declared()` = only the node type's
  own metadata-declared settings; `.custom()` = only the instance's
  extras (custom header ports, form-derived ports), the projection for
  nodes that treat "whatever the user wired in" as a dynamic set
  (script variables, form prefill).
- `.object()?`: the whole bag as one `serde_json::Map`, for nodes that
  consume or forward it as a record. Always answers on inputs; on the
  wake bag it fails loud when the fire delivered no keyed record (a
  broken delivery can never pass as an empty one).

File values are just types: `ctx.inputs.get::<FileHandle>("image")?`
parses the file value (loud when it has no readable handle), and the
storage verbs take the parsed handle directly.

### Declaring inputs: exposure, widget, default

Every entry in the metadata's `inputs` list is one input:

```json
{
  "name": "method",
  "type": "String",
  "required": true,
  "exposure": "config",
  "widget": { "kind": "select", "options": ["GET", "POST"] },
  "default": "GET",
  "label": "Method",
  "placeholder": "..."
}
```

`exposure` says where a value may come from. It governs LITERALS; wires
are a separate axis (every exposure is wireable except `config`):

- `"all"`: braces literal (`M { x: 5 }`), assignment literal
  (`n.x = 5`), or a wire. The default for plain data (strings, numbers,
  lists, dicts).
- `"assignment"`: literal only as `n.x = ...`, plus wires. The default
  for file types, e.g. `n.image = @asset("i.png", Image)`.
- `"config"`: literal only in the braces, and NOT wireable: a pure
  design-time setting (what a select, an API-key picker, a form builder
  configure). No type defaults to it; declare it.
- `"wire"`: wires only, no literal ever. The default for Bus inputs;
  declare it for an input that needs a real node wired, like an
  inference node's `config` input.

An input has exactly one driver; two is a compile error
(`double-driven-port`), and a wire on a `"config"` input is
`input-not-wireable`.

`widget` overrides the editor control. Absent, the control derives from
the TYPE through one central mapping (file-valued types get a drop/pick
control, Boolean a checkbox, Number a number box, everything else a
text area, with JSON typed as text for complex types). Declare a widget
for a richer control: `select`/`multiselect` (with `options`), `code`
(with `language`), `number` (with `min`/`max`/`step`, enforced by the
editor's clamp and the compiler's `literal-out-of-range`),
`password`, `form_builder`, `file_drop`, and the connection surface
(`access`, `remote_select`; see "Connections" below).

A field whose value names something enumerable is NEVER a bare text
field. If the vocabulary is small and fixed, declare `select` with the
options. If the provider can list the choices (model ids, voices,
channels, databases, repos), declare `remote_select` so the user gets
search-as-you-type instead of copying an id from the provider's docs;
add `free_text: true` when any pasted id is also valid (a model route
the list hasn't caught up with). The default text control is only for
genuinely free-form values (a prompt, a URL, a message body). See
`catalog/ai/llm/openrouter` (public model list) and
`catalog/ai/elevenlabs/speak` (signed voice list) for the two common
shapes.

`default` is the value the runtime supplies when nothing else drives
the input. It is consulted at run time and rendered by the editor as
the effective value, never written into source; `required` plus
`default` is satisfiable with no driver.

### The config-node pattern

A config node is nothing engine-special: it emits ONE plain object, and
the consuming node declares an ordinary object-typed input (usually
`exposure: "wire"`, so a real node must be wired) and reads that object
itself, deciding what each key means. `ctx.inputs.nested(name)` reads
an object-valued input as its own bag with the same typed accessors
(absent = an empty bag, every knob at its default; a present non-object
value errors loud):

```rust
let cfg = ctx.inputs.nested("config")?;
let model: String = cfg.get_or("model", "default-model".into())?;
```

No input name triggers hidden behavior; an object wired to an input
always arrives as that object.

### How the editor renders inputs

Every input renders as an inline field in the node body from its
resolved widget, except `"wire"` inputs (handle only) and wired inputs
(the edge is the driver, the field hides). `"config"` inputs render a
field but no handle on the edge rail. A small marker on a wireable
input's field toggles which source form the value is written in (braces
vs statement; locked to statement for `"assignment"` inputs).

### Emitting output, errors, HTTP, identity

- `ctx.pulse_downstream(NodeOutput)`: emit values on output ports and
  fire downstream. `NodeOutput::new().set("port", value)` takes anything
  that converts to JSON directly (bools, numbers, strings, an
  already-built `Value` passes through untouched); chain `.set` for more
  ports; `.extend_from_object(json)` fans a JSON object's keys onto
  same-named ports. A port not present in the output emits no pulse
  (downstream of it skips, the null-propagation rule). A
  `Generator[T]` output accepts repeated emissions (each one item of
  the stream); every other port at most once per firing.
- `ctx.yield_downstream(NodeOutput)`: the same emission, but
  it does not return until the values were TAKEN (the consumer
  dispatched; a stream item pulled). See the Streams section.
- `ctx.set_max_buffered_items(port, n)`: for a stream producer that
  emits without yielding, how many un-taken items its `Generator`
  output may buffer before an emission fails (default 4096). See the
  Streams section.
- `.node_err("doing X")?` on any non-weft `Result` (an HTTP call, a
  parser) turns its error into a node failure reading "doing X: ...";
  on an `Option`, `None` becomes a failure carrying the message
  verbatim. For a bad condition the node detects itself (nothing to
  wrap), `weft::node_bail!("bridge rejected: {reason}")` fails the
  node with that message in one statement; its expression cousin
  `node_error(message)` fits `map_err`/`ok_or_else` closures that build
  a rich message first. These are the only error doors: the accessors
  stamp input/config errors themselves, every ctx handle already
  returns `WeftResult`, and node code never names a `WeftError`
  variant.
- `ctx.http()`: the shared, pooled HTTP client for plain outbound
  calls. A call on a CONNECTION goes through `ctx.open` /
  `ctx.client` instead (that is what signs it and records its cost).
- Identity fields: `ctx.execution_id`, `ctx.project_id`, `ctx.node_id`,
  `ctx.node_type`, `ctx.node_label`, `ctx.color`, `ctx.frames`.

`metadata.json` declares the surface (see `weft::NodeMetadata` for
every field): `type`, `label`, `description`, `tags`, `icon`,
`color`, `inputs` (`{ name, type, required, exposure, widget, default,
label, placeholder, description }`), `outputs` (`{ name, type, required,
description }`), `requires_infra`, `images`, `features`, `display`,
`validate`. (`features` is for boolean-ish flags; anything with
structure, like `display`, is its own top-level key.)

One `features` flag every author must decide, not default: if your
node's firing IS the deliverable (it generates an artifact: an image,
a video, speech; or it performs the outward effect: sends the message,
creates the record, uploads the file), set
`features.isOutputDefault: true`. A run executes the union of the
upstream closures of the project's output nodes, so this flag is what
lets a user drop your node at the end of a chain and hit run with no
Debug node attached. Reads, transforms, lookups, and triggers leave it
unset; any project can override per instance with `is_output` in the
node's config.

### Showing a result on the node: `display`

A node whose firing produces (or receives) a FILE worth seeing
declares which port the editor renders inline on the node body,
per firing:

```json
"display": { "kind": "media", "output": "image" }
```

- `kind`: `media` renders the file by its OWN mime type: an image
  inline, audio and video with a real player, anything unplayable as
  the file card; a save button rides below. `link` renders the
  metadata + download card only. There is never a flag per media
  type; a new playable format is a renderer detail.
- The port is named WITH its side: exactly one of `output` (a
  generator shows what it emitted: the generated image, the spoken
  audio) or `input` (a display sink shows what was wired in). Naming
  the side is what keeps a node with a same-named input and output
  unambiguous; declaring both, neither, or a port that does not exist
  is refused when the catalog loads.

The generation nodes (`catalog/ai/fal`, the ElevenLabs audio nodes)
and the display sinks (`MediaDisplay`, `DownloadLink`) are the worked
examples.

`icon` names any [Lucide](https://lucide.dev/icons) icon in its
PascalCase form (`"BrainCircuit"`, `"KeyRound"`); the editor resolves
it dynamically against its installed `@lucide/svelte`, so every icon
that library ships just works. A name it does not ship renders as a
generic square with a loud console error, and the editor's test suite
pins every icon the editor asks for (each catalog metadata's, plus its
own builtins') to the installed set, so a typo or an icon a lucide
upgrade dropped fails the suite instead of shipping as a square.

`deps.toml` lists extra cargo dependencies beyond the always-available
base (weft, tokio, serde, serde_json, async-trait, anyhow, tracing,
uuid):

```toml
[dependencies]
reqwest = { version = "0.12", features = ["json"] }
```

A package (one `package.toml` root with member node subdirs) shares deps
and helper files across members; a bare node dir stands alone.

## Declaring custom types

A `types` key in any `metadata.json` (a node's own, or the package
root's shared partial) declares NAMED types the whole project may use:

```json
"types": {
  "ChatHistory": "List[ChatMessage]",
  "ChatMessage": "{ role: String, content: String | List[Part], name?: String }",
  "Part": "{ type: String, text?: String, image_url?: { url: Image } }"
}
```

The rules that matter when authoring:

- Declarations are GLOBAL: once any metadata declares `ChatHistory`,
  every node's ports (and every `.weft` inline signature) may name it.
  Declare a type next to the node that owns the concept.
- Named types are NOMINAL: only a same-named value wires in; the value
  wires OUT into `JsonDict` freely. The user's escape hatch for a
  hand-built dict is the stdlib `Cast` node, which validates at run
  time, so your node can trust that a named input already fits its
  declared structure.
- Redeclaring an identical body elsewhere is absorbed silently (two
  packages may ship the same shared type without depending on each
  other); a DIFFERENT body under the same name fails the catalog load
  loudly. Drift between two copies is therefore a build error, which is
  the point.
- Record validation is strict: declare every field the values really
  carry, optional ones with `?`.

### Media inside custom types

A field declared `Image` / `Audio` / `Video` / `Blob` is a MEDIA SLOT:
in stored form (what rides edges and the journal) it holds the small
stored-file value, never bytes or URLs, so a large conversation stays
cheap to journal and the editor can render it. Two storage verbs
convert a WHOLE typed value at an external boundary, driven by the
declared type (no per-node walking code, ever):

```rust
use weft::storage::media::{ExternalizePolicy, MediaForm};

let ty = ctx.output_type("history").expect("declared on the port");
let storage = ctx.storage(StorageScope::Project);

// Out to a provider: each media slot becomes something the consumer
// can use. `MediaForm::Url` is a PREFERENCE, not a promise: the slot
// becomes a public link when an internet-reachable address is
// configured (a publicly addressable bucket, or the relay under the
// public base), and falls back to inline data: bytes when none is.
// Declare `Inline` only for consumers that ONLY take base64 (e.g.
// chat-API audio).
let wire = storage.externalize(&value, &ty,
    ExternalizePolicy { audio: MediaForm::Inline, ..ExternalizePolicy::urls() }).await?;

// Back from a provider: raw media (a generated image's data: URL, an
// external URL) is stored and every slot becomes a stored-file value
// again; already-stored slots pass through untouched.
let stored = storage.internalize(&response_value, &ty, None).await?;
```

Emit only the internalized form: presigned URLs expire and never
belong in a stored value. The `catalog/ai` chat nodes
(`ChatHistoryAppend`, `LlmInference`) are the worked example:
the `ChatHistory` type carries media through arbitrarily long
conversations with one externalize per call and one internalize per
reply.

## Sharing state across executions: process-global statics

A worker process multiplexes many executions of the same project, so a
plain Rust `static` in a node's module is shared by ALL of them: every
execution (and every firing of every node in the file) sees the same
instance for as long as the worker lives. This is safe tenant-wise by
construction (a worker only ever hosts one project), so executions
reading and writing each other's state through it is a feature, not a
hazard: caches, pools, warmed clients, a shared task buffer one
execution fills and others drain. If a piece of state is meant to be
private to one execution instead, key it by `ctx.execution_id`.

```rust
/// The process-shared `GeneratorInfo` for a model. Shared because the
/// model's published rates are cached on the generator (clones share the
/// cache), so the price sheet is fetched once per TTL rather than once
/// per execution.
fn shared_generator(model: &str) -> GeneratorInfo {
    static POOL: OnceLock<Mutex<HashMap<String, GeneratorInfo>>> = OnceLock::new();
    let fresh = GeneratorInfo::openrouter(model);
    let mut pool = POOL.get_or_init(Mutex::default).lock().expect("generator pool lock");
    pool.entry(fresh.pricing_key()).or_insert(fresh).clone()
}
```

One rule keeps it sound: **it's a per-process layer, not durable
state**. It dies with the worker (workers idle-shutdown) and other Pods
never see it, so anything that must survive a restart or be visible
across Pods belongs in the language's durable primitives (`ctx.run`,
buses, storage), with the static as at most a warm cache in front. And
hold locks only for map lookups, never across `.await`.

The same pattern covers repeated STORAGE reads: a node that inlines the
same stored bytes on every firing (an audio clip re-sent base64 to a
provider each conversation turn) can keep a byte cache in a keyed
static, exactly like the generator pool above, keyed by the file's
storage key:

```rust
/// Process-shared bytes for a stored file. The provider needs the
/// full base64 on every call anyway; this only saves re-reading the
/// same bytes from the bucket while this worker lives.
static BYTES: OnceLock<Mutex<HashMap<String, bytes::Bytes>>> = OnceLock::new();
```

Values themselves stay single-form (the stored-file reference is the
one truth; the journal replays without any cache), so this is always a
pure optimization a node adds when a real workload measures slow, never
something the shape depends on.

## Cancellation

Every execution has a `CancellationFlag` attached to it. When the user
clicks Stop (or a project is deactivated, or the dispatcher decides to
tear an execution down), the flag is set. The engine's loop driver
checks the flag at every iteration and exits the execution; the
`JoinSet` holding all in-flight node tokio tasks gets dropped, which
aborts each task at its next `.await` point.

**The default behavior, with no node-side code, is that any normal
async Rust node is cancellable instantly.** A node has to do something
unusual to *escape* cancellation. The rest of this section covers the
default plus the few cases where a node should opt in for stronger
guarantees.

### Default: instant cancellation, no code

Any node written as straightforward async Rust gets cancellation for
free. When the engine drops the JoinSet, your future is dropped at its
current `.await`; tokio cancels every primitive that respects future
drop (HTTP via reqwest, DB via sqlx, sleep, file I/O via tokio::fs,
WebSocket streams, channel receives).

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let resp = ctx.http()
        .post("https://api.anthropic.com/v1/messages")
        .json(&body)
        .send()
        .await
        .node_err("posting to the API")?
        .json::<ApiResponse>()
        .await
        .node_err("decoding the API response")?;
    ctx.pulse_downstream(NodeOutput::new().set("response", resp.text)).await
}
```

When cancelled mid-call, the future at `.send().await` is dropped,
reqwest closes the underlying TCP socket, the HTTP request is
cancelled mid-flight, and the function exits. No further token billing,
no further work.

Same goes for retry loops, streaming receives, anything with regular
awaits: every iteration of the loop has a cancellation point.

### When the default isn't enough

The default fails when the node holds work that doesn't propagate
through future-drop:

- **External processes** spawned by the node (Python subprocess,
  shell-out to a CLI, etc).
- **CPU-bound work in `spawn_blocking`**: tokio cannot abort OS
  threads.
- **Resources requiring graceful cleanup** before drop (flush to disk,
  notify a peer, release a lock the node owns externally).

For these cases, `ExecutionContext` exposes the cancellation flag.

### Accessing cancellation in your node

```rust
let flag = ctx.cancellation(); // Arc<CancellationFlag>

// Sync check, atomic load. Cheap; safe in tight loops.
flag.is_cancelled() -> bool

// Future that resolves immediately if already cancelled, or on the
// next `.cancel()` call otherwise. Use in `tokio::select!`.
flag.cancelled().await
```

The flag is **persistent**: once `.cancel()` has been called, every
subsequent `is_cancelled()` returns true and every new `cancelled()`
future resolves immediately. There's no race window where you can
"miss" a cancellation.

The engine aborts in-flight node futures as soon as it observes the
cancel, so a `cancelled()` branch in your body only runs if it happens
to win that race. Paid calls need nothing from you here: the metering
runs BELOW your future and resolves an interrupted call's real cost on
its own, and the runtime gives your provider accesses back after the
abort. The patterns below are for the things an abort genuinely cannot
clean up by dropping (subprocesses, blocking threads, external
resources).

### Pattern: subprocess

Out of the box, dropping a `tokio::process::Child` does **not** kill
the underlying process. The OS process keeps running.

```rust
// BAD: drops the future, but `python` keeps running.
let mut child = tokio::process::Command::new("python")
    .arg(script_path)
    .spawn()?;
let status = child.wait().await?;
```

Add `.kill_on_drop(true)` to fix it. One line:

```rust
// GOOD: cancel drops the future, drop kills the process.
let mut child = tokio::process::Command::new("python")
    .arg(script_path)
    .kill_on_drop(true)
    .spawn()?;
let status = child.wait().await?;
```

If you need graceful shutdown (let the subprocess flush state before
SIGKILL):

```rust
let mut child = tokio::process::Command::new("python")
    .arg(script_path)
    .spawn()?;
let cancel = ctx.cancellation();
tokio::select! {
    status = child.wait() => Ok(format(status?)),
    err = cancel.cancelled_err() => {
        // SIGTERM, give it 2s to clean up, then drop kills it.
        let _ = child.start_kill();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            child.wait()
        ).await;
        Err(err)
    }
}
```

### Pattern: CPU-bound `spawn_blocking`

Dropping a `JoinHandle` from `spawn_blocking` does NOT kill the
worker thread. The thread runs the closure to completion; only its
result is discarded. From the user's POV the cancel "worked" (the
graph stops, the loop exits), but CPU keeps spinning until the
closure returns.

For long-running CPU work, pass the cancellation flag into the closure
and check it between chunks:

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let image: Vec<u8> = todo!("read the image bytes");
    let cancel = ctx.cancellation();
    let result = tokio::task::spawn_blocking(move || {
        for chunk in chunks_of(image) {
            if cancel.is_cancelled() {
                return Err("cancelled");
            }
            process_chunk(chunk);
        }
        Ok(...)
    }).await??;
    ctx.pulse_downstream(NodeOutput::new().set("out", result)).await
}
```

`is_cancelled()` is an atomic load; you can call it 10,000 times per
second without measurable overhead. Check at granular boundaries that
match how long you'd be willing for cancel to take to land (every
chunk for a chunked algorithm, every iteration of a tight inner loop,
etc).

### Pattern: stream / loop with cleanup

If you need to do something specific on cancel (notify a peer, log a
metric, release a resource the node owns), branch on the flag:

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let mut conn = open_connection().await?;
    let cancel = ctx.cancellation();
    loop {
        tokio::select! {
            msg = conn.recv() => {
                if let Some(m) = msg? { handle(m); } else { break; }
            }
            err = cancel.cancelled_err() => {
                conn.send_close_message().await.ok();
                return Err(err);
            }
        }
    }
    ctx.pulse_downstream(NodeOutput::new().set("done", json!(null))).await
}
```

If you don't have cleanup to do, you don't need this; the default
abort path closes `conn` at drop and you're done.

### Suspension is independent of cancellation

`ctx.await_signal(spec)` is not a tokio wait; the engine journals a
suspension and exits the worker. Cancel of a suspended execution goes
through the dispatcher's signal-strip path (drops the wake registration
so external events can't resume the dead execution) plus the cancel
task (the wake-up never reaches a worker, the journal records
ExecutionFailed). Nodes that use `await_signal` don't need to do
anything special for cancel; the engine handles it.

### Quick reference

| Node behavior                           | Cancellable?        | What you need to do        |
|-----------------------------------------|---------------------|----------------------------|
| Async HTTP / DB / sleep / file I/O      | Yes, instant        | Nothing                    |
| Async with retries                      | Yes, instant        | Nothing                    |
| Streaming receive (WS / SSE)            | Yes, instant        | Nothing                    |
| Suspended via `await_signal`            | Yes (engine path)   | Nothing                    |
| Subprocess                              | Process leaks       | `.kill_on_drop(true)`      |
| Measured call (a connection's client)   | Yes, instant        | Nothing (the metering settles on its own) |
| CPU-bound `spawn_blocking`              | Future returns, thread leaks | Pass flag, poll `is_cancelled()` |
| External resource needing cleanup       | Best effort         | `tokio::select!` branch on the flag |

**Bottom line**: write normal async Rust. Reach for `ctx.cancellation()`
only when you spawn a subprocess, run blocking CPU work, or hold a
resource that needs explicit cleanup before drop, and treat that cleanup
as best-effort (the abort races it).

## Connections: the one way a node calls a third party

`Access` is the weft port type for "authorized ability to call a third
party". An **access node** owns the connect for a service and emits an
`Access` value: a reference to a CONNECTION in the access store (the
credential material, the granted permissions, the identity, which app
was used; everything secret lives in the store, source carries an id
and nothing else). Action nodes wire the value in and open it per
firing:

```rust
let account = ctx.inputs.get("account")?;      // Access (the reference)
let conn = ctx.open(&account).await?;          // one resolve, one lease for this firing

conn.client()       // -> &ClientWithMiddleware: signed in, measured when a meter exists
conn.credential()?  // -> &str: ONLY when the sign-in is one string (a bearer key)
```

`ctx.client(&account)` is sugar for the overwhelmingly common case
(open + hand back the client), so a node that just makes HTTP calls is
one line. When the node's body finishes (any outcome), the runtime
releases the lease on its own; nothing node-facing closes it.

For a service's realtime API, the same connection opens a WebSocket:

```rust
let mut session = conn.socket("wss://api.example.com/v1/realtime?model=m").await?;
session.send(SocketMessage::Text(payload)).await?;
while let Some(frame) = session.recv().await? { /* ... */ }
session.close().await?;
```

Same rules as the client: the runtime signs the handshake (the
credential rides the handshake only, never a frame), routes the
session, and measures it when the service's meter prices sessions.
Never hand-roll a socket client for a provider.

**Secrets go through connections, never through config.** Never add a
config field that asks the user to paste an API key, a token, or a
password: node config and port values travel the execution journal and
render in the inspector in plaintext, while a connection's values are
sealed in the access store and only ever exist in the worker's memory while
your node runs. If your service needs a pasted key, declare it as an
access node with a paste acquisition (see
[the access system](access-system.md)) and the whole problem
disappears.

**A node declares only which service it needs.** Whether calls are
MEASURED follows from whether a price rule (a meter) is registered for
that service; whether they are BILLED follows from who owns the stored
credential (the user's own, or the runtime's). Neither is ever declared
on the node, node code cannot tell, and the same node, same metadata,
same source runs correctly in every combination.

```rust
// A library call: hand it the client (and the derived credential
// where the library insists on a raw string).
let generator = GeneratorInfo::openrouter(model)
    .with_api_key(conn.credential()?)
    .with_http_client(conn.client().clone());

// A hand-built request: just use the client.
let response = conn.client()
    .post("https://slack.com/api/chat.postMessage")
    .json(&body)
    .send()
    .await?;
```

Rules that matter:

- **Never construct your own HTTP client for a connection's calls**;
  always take it from the opened connection. A call on a hand-rolled
  client is invisible to the cost trail, and a runtime-supplied
  credential only works through the connection client's routing.
- Address the service's REAL API. The client does any routing a
  runtime-supplied credential needs; your code never rewrites a URL.
- Do not stash `.credential()` anywhere (an output port, a log, an
  error, a struct that outlives the call). It exists for libraries
  that insist on a raw string, nothing else, and it only exists at all
  when the service's resolved auth is one step interpolating one
  stored value; anything else fails loudly naming the service.
- A refusal to supply the runtime's own credential is a loud error
  naming the fix ("connect your own"); do not paper over it.
- Redirects behave like any ordinary HTTP client's, on every lane:
  followed, with the standard convention that the well-known
  `Authorization`/cookie headers do not cross a host change. Nothing
  weft-specific to learn.
- Sending media on a measured call? Declare what you know about it on
  the media objects (`AudioData::with_duration`,
  `ImageData::with_dimensions`, ...): it sharpens the pre-flight cost
  estimate. It never changes what is billed (always the measured cost).

Every connection call (`ctx.client` included; it opens the connection
for you) assumes your provider work fits the default window (15
minutes): that is how long a runtime-supplied credential stays usable
if your node crashes without finishing. On your own connected account
the window changes nothing. A node wrapping a genuinely long action on
a runtime-supplied credential declares its own:
`ctx.open_within(&access, Duration::from_secs(...))`.

### Measuring: meters, keyed by service name

A meter is the small piece of code that measures what a call really
cost, keyed by the SERVICE name; a registered meter for a service IS
the declaration that its calls are measured. Weft ships meters for the
services it supports, and **your project can define its own** for a
service weft does not ship yet (it lives beside the nodes that call it,
and works with your own connection right away). A meter never touches a
credential (its follow-up queries ride a signed-in client), which is
exactly why the same meter works on a pasted key and on a sign-in.
Writing one, and getting a project's meter promoted to a weft-shipped
one (so the platform keys in app.weavemind.ai can pay for it too), is
all in `docs/authoring-provider-meters.md`.

## The access node: a service is declared data

One node per service owns the connect. The whole feature is declared
data: a service is an **`AccessSpec`** in the access node's
`metadata.json` under the `service` key (how a credential is acquired,
how requests are signed, the permission catalogue, how it is verified,
how events arrive), and there is never per-service Rust. The runtime's
access store owns the sign-in, the credential storage, and the lazy
refresh; the node's body is a pure pass-through:

```jsonc
// metadata.json (the shape; the recipe language reference is
// docs/access-system.md)
{
  "type": "SlackAccess",
  "service": {
    "service": "slack",
    "doors": ["shared", "own"],
    "acquisition": { "kind": "oauth2", /* or "static", "mint_jwt" */ },
    "auth": [ { "kind": "header", "name": "Authorization", "value": "Bearer {token}" } ],
    "permissions": [ /* the catalogue, one human sentence each */ ],
    "test": { "url": "https://slack.com/api/auth.test", "method": "POST" },
    "identity": "{team}"
  },
  "inputs": [
    { "name": "account", "type": "JsonDict", "exposure": "config",
      "widget": { "kind": "access" }, "label": "Workspace" }
  ],
  "outputs": [ { "name": "access", "type": "Access" } ]
}
```

```rust
// mod.rs: the WHOLE body. Every access node's body is the same
// pass-through (the runtime sealed the handle + service into the bag
// value), so it is one macro declaration and can never drift:
weft::access_node!(SlackAccessNode);

// A node whose access input is not named `account` names it:
weft::access_node!(ElevenLabsAccessNode, "connection");
```

The full recipe language (acquisition kinds, auth steps, doors and the
"Your own" page, the verification ladder, event topics, the registered
apps file) is **[docs/access-system.md](access-system.md)**; that page
is the reference for writing a new service. Two authoring rules worth
repeating here:

- Templates interpolate STORED VALUES by name; the worker receives
  everything the connection stores except the store's own keep-alive
  material (refresh tokens, app secrets), whatever shape the
  credential takes. A mechanism no declared step expresses becomes a
  new typed variant in weft, never author code.
- Non-HTTP credentials (a Postgres connection string, mTLS certs) do
  NOT fit this model on purpose: take those as ordinary inputs.

### The consumer node

```rust
let access = ctx.inputs.get("account")?;
let repo: String = ctx.inputs.get("repo")?;
let gh = ctx.client(&access).await?;   // fetch + lazy refresh + auth steps + metering
gh.post(format!("https://api.github.com/repos/{repo}/issues")).json(&body).send().await.node_err("github")?;
```

Byte-identical across a pasted token, an OAuth grant, a minted
installation token, and the runtime's own credential. A connection
the provider revoked is a loud "needs reconnecting" error, never a
silent retry.

### Declaring what your node needs

A consumer node states its needs on its access INPUT, and the checks
run where the answer lives (live in the editor when a connection is
picked, at connect, and at run-time resolution; there is deliberately
no compile-time check, because source holds only a connection id):

```jsonc
{ "name": "account", "type": "Access", "required": true,
  "requiresScopes": ["chat:write"],                 // permissions
  "requiresValues": ["imap_host", "imap_port"] }    // stored values
```

- `requiresScopes`: declare ONLY what the node's own runtime calls
  need on every path. A permission that only one `remote_select`
  source needs (a browse-everything scope backing a `list` source)
  belongs on that source's `requires`, never here, or it locks out
  every connection that would have used the picker or a pasted link.
  A VERIFIED shortfall is a hard error; a claimed/unknown one is let
  through, because nobody actually knows (a pasted key on a service
  that reports nothing must not be refused).
- A required permission may be an OWN-ACCOUNT-ONLY capability (the
  service's catalogue marks it `own_only`, e.g. ElevenLabs'
  `voice_lab` / `agents`): the node's work creates or reads durable
  things INSIDE the connected account (minted voices, configured
  agents), so a runtime-supplied shared credential can never serve
  it. Declare it through the same `requiresScopes`; resolution
  refuses the shared credential with the capability's set-up guide,
  and the editor marks the node the moment the shared connection is
  picked. Marking entries and writing their guides is the access
  reference's job: see [access-system.md](access-system.md).
- `requiresValues`: for services whose optional fields decide what a
  connection can DO (a mailbox holding the incoming half, the
  outgoing half, or both; the service declares the groups as
  `capabilities`). Unlike a permission set the answer is never
  unknown (a value is stored or it is not), so a shortfall ALWAYS
  refuses, naming the value to add. Use this whenever one service's
  optional fields unlock separate capabilities; do not build two
  access nodes for one account.

How permissions are recorded (verified vs claimed), the verification
ladder, the `capabilities` rules, and grant coexistence
(`coexisting` / `exclusive`) are all in
[docs/access-system.md](access-system.md).

### Working without a connection at all

Some providers serve a link-shared resource with no sign-in at all:
Google's spreadsheet CSV export, GitHub's normal API. **When a
provider does, the node should support that path.** It costs the
author little and turns "connect your Google account" into "paste the
link" for the many people whose file is already shared. State the two
limits in the same breath: it only ever works for a resource you
already have the link to, never for browsing or searching, and it only
exists where the provider genuinely serves anonymously (Google's
export address and GitHub's API do; Notion and Airtable never do).

Best case, one address serves BOTH worlds (GitHub's API answers with
and without a token) and the body has no branch at all. Verify that
before assuming it: Google's CSV export is anonymous-only (it is the
browser's cookie endpoint and ignores a bearer token, answering 404
for a private sheet), while its Sheets API is signed-in-only, so the
sheets node branches on whether an account is connected. Both branches
must answer identically (share the parsing).

This needs no new metadata: an access input declared
`"required": false` IS the declaration. `ctx.client` accepts the
absent connection (`ctx.client(account.as_ref())` on an
`Option<Access>`) and answers a plain client; on a `required: true`
input an absent value is an ordinary missing-input error, never a
quiet bare request. See `catalog/google/sheets_read` for the worked
example: works signed in (Sheets API) and with nothing but a share
link (public CSV export), one shared parsing step.

### Picking resources: the `remote_select` widget

A connection says WHICH ACCOUNT; almost every real node then needs
WHICH THING in it (a spreadsheet, a channel, a repo). One field, a
declared list of SOURCES in preference order; the editor uses the
richest one the picked connection actually supports and silently drops
each source whose requirement is not met:

| kind | where the options come from | needs |
|---|---|---|
| `granted` | recorded on the connection during sign-in (`from` names the capture; `label`/`value` address one item) | nothing, no call |
| `list` | call the service and enumerate (today's declarative lookup) | its `requires` permissions |
| `picker` | the provider's own chooser, declared entirely by YOU (`script` + `code`, below); choosing GRANTS the picked resource | a connection |
| `from_url` | paste a link; `pattern`'s first capture group is the id | nothing at all |

`from_url` needing nothing is what leaves it standing with NO
connection: the works-without-signing-in path. See
`catalog/google/sheets_read` again for a field declaring all of
`list` + `picker` + `from_url`:

```jsonc
{ "name": "spreadsheet", "type": "String", "required": true,
  "widget": { "kind": "remote_select", "access": "account", "sources": [
    { "kind": "list", "requires": ["https://www.googleapis.com/auth/drive.readonly"],
      "get": "https://www.googleapis.com/drive/v3/files?...", "items": "files",
      "label": "name", "value": "id",
      "page": { "cursor_param": "pageToken", "cursor_path": "nextPageToken" } },
    { "kind": "picker",
      "script": "https://apis.google.com/js/api.js",
      "code": "await new Promise((r) => gapi.load('picker', r)); ...",
      "grants": ["https://www.googleapis.com/auth/drive.file"],
      "mime_types": ["application/vnd.google-apps.spreadsheet"] },
    { "kind": "from_url", "pattern": "/spreadsheets/d/([a-zA-Z0-9_-]+)" } ] } }
```

A `picker` is yours end to end, and it is the one place a node carries
**browser JavaScript** (the same way an ExecPython node carries
Python): your `mod.rs` stays pure Rust and never sees any of this. The
fields:

- `script`: the https address of the provider's own chooser library
  (the URL their "embed our picker" docs tell every web developer to
  load).
- `code`: plain browser JavaScript you write, usually adapted straight
  from the provider's own sample. It runs on a small page weft serves,
  opened in the user's browser (the same pattern as the sign-in
  consent), AFTER `script` has loaded, inside an async function, so
  `await` works at the top level.
- `grants`: the permissions that choosing through this chooser grants
  on the picked resource, recorded on the connection when the pick
  lands (Google's `drive.file`: the picker itself is what hands the
  app access to that one file, so the grant only exists once a pick
  happened).
- `mime_types`: narrows the chooser to these MIME types, threaded to
  your glue as `weft.mimeTypes`.

Your code talks to weft through one object, `weft`, already in scope:

| | |
|---|---|
| `weft.token` | the connection's access token (the user's own), the string you hand the chooser where its docs say "your OAuth token" |
| `weft.clientId` | the PUBLIC client id of the OAuth app behind the connection, or `null` when no app made it (a pasted key). Some choosers require an app identifier (Google's picker needs `setAppId(weft.clientId.split('-')[0])`, its Cloud project number, or picking grants the app nothing and later reads 404) |
| `weft.mimeTypes` | your declared `mime_types`, for choosers that filter |
| `weft.done({id, label})` | the user picked this resource: the field fills with it |
| `weft.cancel()` | the user closed the chooser without picking: the field closes quietly |
| `weft.fail(message)` | the chooser could not work: `message` shows on the field in red |

Call exactly one of the three enders; the first call wins and later
ones are ignored. A thrown exception (or a rejected `await`) is caught
and becomes `weft.fail` automatically, so an unexpected provider error
still surfaces on the field instead of hanging it. The recipe for a
new provider is therefore: open the provider's "picker/chooser embed"
documentation, take their sample, replace their API key / token slot
with `weft.token`, and route their picked/cancelled callbacks into
`weft.done` / `weft.cancel`. See
`catalog/google/sheets_read/metadata.json` for the complete Google
Picker glue written exactly this way. No weft change is ever needed
for a new provider's chooser; the declaration is the integration.

One environment note: many choosers (Google's included) also lean on
the provider's own browser session, which is exactly why the page
opens in the user's real browser: their session is already there, and
the chooser signs in on its own.

The stored value is the bare id, which is exactly what your node
reads and what the field's declared `String` type holds; the human
label the editor shows is a display cache, never source. A pasted raw
id is first-class; a
runtime value arriving on the wire bypasses all of it. Note that a
picked resource is as person-scoped as the connection itself: both are
re-chosen when a project changes hands, and an unresolvable one is a
loud node error, never a silent pointer to something the new owner
cannot open.

## Durable execution: `await_signal`, `register_signal`, `ctx.run`

Three primitives let a node body interact with the outside world in a
way that survives the worker dying and a fresh worker resuming
hours or days later.

### `ctx.register_signal(kind)`

For a trigger's `setup_trigger` body. Tells the listener to watch for a
wake signal. You pass a **typed signal kind** (one of the structs in
`weft::signal`, see the kinds table below), not an untyped spec;
the framework projects it onto the wire shape. Returns `()` once the
dispatcher acknowledges. Each external fire later spawns a fresh
execution of the project; this registration is NOT bound to the current
execution's firing. Any public URL is derived from the signal's path on
the dispatcher, so nodes don't get a URL handed back.

A trigger writes two bodies and never inspects any phase; the engine
calls the right one:

```rust
use weft::signal::{ApiEndpoint, LiveConnectionConfig};

#[async_trait]
impl Node for MyTriggerNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Build the typed kind from the node's values, then register
        // it. (The live-caller kinds share a config body built by
        // `LiveConnectionConfig::from_node_fields`.) The runtime saves
        // a snapshot of `ctx.inputs` with the registration.
        let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?);
        ctx.register_signal(ApiEndpoint { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Runs when an external fire arrives, exactly once per fire.
        // The fire's payload fields are on the `ctx.wake` bag (same
        // accessors); `ctx.inputs` replays the values the trigger's
        // inputs held at setup time.
        let value: serde_json::Value = ctx.wake.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

**What a trigger's `run` sees, and when it runs.** A trigger's two
value sources at fire time:

- `ctx.inputs`: a SNAPSHOT of what the trigger's inputs held when it
  registered (its upstream runs during trigger setup, the values land,
  and the runtime saves them with the registration). Upstream nodes do
  not re-run for the trigger's sake at fire time, and wires into a
  trigger deliver nothing during a fire; re-activating the project
  re-registers and refreshes the snapshot.
- `ctx.wake`: this fire's event payload as a bag of named fields (the
  HTTP body, the SSE event JSON, the form submission, the timer info).
  A trigger that forwards the whole payload reads it in one go via
  `ctx.wake.object()?`, which fails loud when the fire delivered no
  keyed record (so a broken delivery can never pass as an empty one).

Per fire, the engine runs the subgraph the FIRED trigger reaches: its
downstream outputs plus everything those outputs depend on (stopping
at triggers). The fired trigger's body runs exactly once. Any other
trigger in that subgraph does not run at all: the engine closes its
output ports, so a node fed by several triggers sees the idle branches
as structurally dead and proceeds with the firing one.

A simpler kind takes its fields directly:

```rust
use weft::signal::SseSubscribe;

ctx.register_signal(SseSubscribe {
    url: events_url,
    event_name: "message.received".into(),
}).await?;
```

### Wake-signal kinds

Each kind is a struct in `weft::signal`. A node constructs one and
passes it to `register_signal` (entry trigger) or `await_signal`
(mid-flow resume).

The two FAMILIES below answer two different questions and never share a
name (the `socket` distinction is the trap: outbound "we dial them" vs
inbound "they dial us").

**Outbound event sources** (the listener reaches OUT to something and
fires a fresh execution per event):

| Kind | What it does | Use for |
| --- | --- | --- |
| `SseSubscribe { url, event_name }` | Holds a one-way Server-Sent-Events stream; fires per matching event. Receive-only. | A service that pushes an SSE feed (the WhatsApp bridge). |
| `PollEndpoint { url, interval_secs, method?, body?, format?, delta? }` | Hits a URL on a timer; fires with the response body, or (with `delta`) once per NEW item. `method: Post` + `body` polls a query endpoint (Notion's data-source query); `format: Feed` parses RSS/Atom into `{ "items": [...] }`. No held connection. | A "give me what's new" endpoint (a bot getUpdates loop, a database query, a feed). |
| `SocketListen { url, handshake?, heartbeat?, heartbeat_secs }` | Holds a bidirectional WebSocket alive, sends an optional handshake on open and an optional heartbeat frame on a schedule; fires per inbound frame. | A gateway that needs login + keepalive or it drops you (Discord, Slack socket mode). The service-specific protocol (op-codes) is YOUR concern, expressed as the literal `handshake` / `heartbeat` frames. |
| `StreamListen { address, framing, script, replies?, heartbeat?, fire }` | Holds a raw TCP/TLS pipe for services that speak neither HTTP nor WebSocket (IMAP, MQTT, Redis, XMPP). Runs a declared connect dialogue (send frame, wait for a matching line), cuts the byte stream by the declared framing (delimiter, length prefix, or varint prefix), and fires every unit matching the `fire` pattern. Text frames interpolate `{placeholders}` from the attached connection, so credentials ride the dialogue without sitting in the spec. | Any wire protocol. The watch is the trigger; the fired body then talks the protocol properly itself (fetch the mail, decode the packet) with its own library, where code is unrestricted. See `catalog/email/receive_email` for the worked example (IMAP IDLE). |

**Inbound live-caller endpoints** (an outside caller dials IN and holds
the connection; nodes talk back via `ctx.caller()`, see the live-caller
section):

| Kind | What it does | Use for |
| --- | --- | --- |
| `ApiEndpoint { common }` | An HTTP endpoint people call; a node replies once or streams a response. | A live HTTP API a program serves. |
| `LiveSocket { common }` | An inbound WebSocket; a node holds a two-way conversation. | A live chat / interactive socket a program serves. |

Both live-caller kinds share `LiveConnectionConfig` (the `common`
field): build it from the node's merged named values with
`LiveConnectionConfig::from_node_fields(ctx.inputs.object()?)`. The wire
protocol is the KIND, not a config field (the runtime derives it from
the tag), which is why there is no `protocol:` knob to set.

**Plus the always-present kinds**: `Timer { spec }` (cron / after / at)
and `Form { .. }` (human-in-the-loop submission, used with
`await_signal`).

### `ctx.await_signal(kind)`

For mid-flow waits (HumanQuery and similar). Parks THIS firing until
the signal fires. Worker exits while parked; a fresh worker spawns
when the fire arrives. Other firings of the same execution at
different frame stacks keep going independently. Like `register_signal`,
it takes a typed `Signal` kind.

```rust
use weft::signal::Form;

async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let answer = ctx.await_signal(Form {
        form_type: "human-query".into(),
        schema: my_form_schema(),
        title: Some("Approve?".into()),
        description: None,
        consumer_kind: Some("human_in_the_loop".into()),
    }).await?;
    // After the user submits, `answer` is the form payload.
    ctx.pulse_downstream(NodeOutput::new().set("answer", answer)).await
}
```

(The exact `Form` fields are in `weft::signal::Form`; the point is
you pass the typed kind, not a wrapper spec. Whether a registration is a
fresh entry or a resume is decided by which method you call,
`register_signal` vs `await_signal`, not by a flag on the kind.)

The body unwinds via `?` when the worker has no value yet. When the
fire arrives, the next worker re-runs the body from the top; the
`await_signal` call returns instantly with the journaled value.

### Multiple `await_signal` calls in one body

You can stack as many as you want. Each call is sequenced by its
position in the body. On replay, each call returns its own fire's
value in order.

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let approval = ctx.await_signal(approval_spec()).await?;
    if approval["accepted"].as_bool() != Some(true) {
        return ctx.pulse_downstream(NodeOutput::new().set("decision", "rejected")).await;
    }
    let confirmation = ctx.await_signal(confirmation_spec()).await?;
    ctx.pulse_downstream(NodeOutput::new().set("final", confirmation)).await
}
```

Three runs of the body happen across the lifetime of this node:
1. First dispatch: hits `await_signal #0`, suspends.
2. Approval fires: re-dispatch. `await_signal #0` returns the value.
   Logic runs. `await_signal #1` (if reached) suspends.
3. Confirmation fires: re-dispatch. Both awaits return their values.
   Body completes.

The branch on `approval["accepted"]` runs every time the body
re-dispatches. The result MUST be the same on every replay. See the
deterministic-replay rule below.

### `ctx.run("name", closure)`: deterministic-replay escape hatch

Anything between awaits that's non-deterministic OR has side effects
must be wrapped in `ctx.run`. Examples: random tokens, `now()`,
calling an external API, writing to a database.

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    // Mint an idempotency token ONCE; replays return the same token.
    let idem = ctx.run("idem", || async {
        Ok(json!(uuid::Uuid::new_v4().to_string()))
    }).await?;

    let approval = ctx.await_signal(approval_spec()).await?;

    // Call an external API ONCE; replays return the same response.
    let http = ctx.http();
    let api_resp = ctx.run("call_billing", || async {
        let resp = http
            .post("https://api.billing/charge")
            .json(&json!({ "idem": idem, "approved_by": approval["who"] }))
            .send().await?
            .json::<serde_json::Value>().await?;
        Ok(resp)
    }).await?;

    ctx.pulse_downstream(NodeOutput::new().set("receipt", api_resp)).await
}
```

The closure runs at most once across the lifetime of this (color,
node, frames) firing. On every subsequent replay, the journaled
value comes back without invoking the closure. Idempotency, signed
URLs that need to be stable across replays, expensive computation,
external side effects: all live behind `ctx.run`.

The author-supplied `name` is for traceability in the journal. The
runtime keys on call-site ORDER, not on the name: two `ctx.run` calls
may share the same name (a `ctx.run("charge", ..)` inside a loop reuses
that name every iteration and is fine), and you may rename any call
freely, as long as the sequence of `ctx.run` / `ctx.await_signal` calls
stays the same across replays.

### Deterministic-replay rule

A node body is replayed from the top whenever a fire arrives. **The
sequence of `ctx.await_signal` and `ctx.run` calls must be identical
across every replay.** The runtime checks the next call against the
journaled sequence; a mismatch fails the node loudly with a clear
error.

What's safe between awaits:
- Pure logic, branching on values that came from awaits or runs.
- Reading named values via `ctx.inputs`.

What's NOT safe between awaits (wrap in `ctx.run`):
- `rand::random()`, `Uuid::new_v4()`, `Instant::now()`.
- Network calls, DB writes, file I/O.
- Reading environment variables that might change.
- Anything that could differ between two runs of the same code.

Emitting on (or closing) an output port BEFORE an `await_signal` is
refused outright, not wrapped: the resume replays the body from the
top and would touch the port twice. Emit and close after all awaits,
or (for a co-alive node) stay warm with `bus.recv()` instead.

If the runtime detects a drift (`ctx.run` at the same index where
the journal has `ctx.await_signal`, or vice versa), the node fails
with `NodeExecution` error explaining the issue. No silent
desyncing.

### When to use which

| Author intent                                       | Primitive               |
|-----------------------------------------------------|-------------------------|
| Trigger node declaring a persistent endpoint/cron/feed | `ctx.register_signal` |
| Mid-flow wait for human input or external event     | `ctx.await_signal`      |
| Non-deterministic / side-effecting work between awaits | `ctx.run`            |
| Pure logic, branching, computing from journaled values | nothing, just write Rust |

### Worker lifetime

A worker pod dies whenever every live firing is parked on `await_signal`.
This is the multiplexing model: thousands of suspended HumanQuery
flows cost no compute, just journal rows. When a fire arrives, a
fresh worker pod spawns, folds the journal, and re-runs every node
that has a fire to deliver. The body re-runs from the top; each
prior `await_signal` and `ctx.run` returns instantly from the
journal.


## Reacting to provider events: `ProviderEvents`

A trigger that fires "when something happens at the service" (a Slack
message, a Drive change, a mail arrival) registers ONE kind, whatever
the service and however its events travel:

```rust
async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let account: Access = ctx.inputs.get("account")?;
    ctx.register_signal(ProviderEvents::new(&account, "messages", vec![
        Predicate { field: "type".into(), op: PredicateOp::Eq, value: Some("message".into()) },
        Predicate { field: "channel".into(), op: PredicateOp::Eq, value: Some(channel) },
    ]))
    .await
}

async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    // The wake payload is the event as the service's NAMED fields.
    let data = serde_json::Value::Object(ctx.wake.object()?.clone());
    ctx.pulse_downstream(ctx.fan_declared(&data)).await
}
```

The parts, and where each one's knowledge lives:

- **The connection** (`&account`) says whose events. The node never
  picks a transport: whether weft holds an outbound line to the
  service or the service pushes to weft's public events surface is
  decided from what the connection can do, by the runtime. Same node
  code everywhere.
- **The topic** (`"messages"`) names one of the event topologies the
  service's recipe declares (`service.events` in the access node's
  metadata: the named fields of one event, which account it concerns,
  and the transport recipes). One service may declare several
  (`google` declares `drive_changes` and `mailbox`).
- **The filters** are [`Predicate`]s over the topic's NAMED fields,
  evaluated BEFORE anything fires, so a non-matching event costs no
  execution. Translate the node's plain config inputs (a keyword box,
  an include-bots checkbox) into predicates here; anything the
  predicate grammar cannot say runs as ordinary code in `run`, after
  the fire.
- Topics whose subscribe call needs node-supplied values (the Drive
  file to watch) pass them with `.with_params(...)`.

Registration fails LOUDLY when the trigger cannot be served: the
connection lacks a value the dial-out transport needs, or the install
has no public address for a push-only service (the error names
`./setup.sh --public-url` and docs/event-triggers.md). A trigger never
activates into a silent dead state.

Everything mechanical (holding the socket, acknowledging frames,
verifying push signatures, subscribing/renewing/stopping provider-side
watch channels) is the runtime's; a body that finds itself doing any
of it is wrong.

### One node per expectation, never an auto-detecting hybrid

A node embodies ONE user expectation. When one capability serves two
genuinely different expectations, they are two nodes, even when the
machinery underneath is shared; a node that silently answers a
different question depending on what was wired into it is convoluted,
not convenient. The test is what the USER expects, not what the code
does:

- `GoogleSheetsRead` signed in vs public-link is ONE node: the
  expectation ("read this sheet's rows") is identical either way;
  only the mechanics differ.
- `SlackReceiveMessage` (your bot, your workspace, a picked channel)
  vs `SlackAppMessages` (you own the app as a product; every
  install's messages, tagged with which workspace) are TWO nodes: the
  same event stream, but different questions with different inputs
  and different outputs. The subscription scope
  (`ProviderEvents::app_wide()`) is how the app-owner node states
  which one it is.

Corollary: which TRANSPORT serves a trigger is never a node split
(same expectation, environment decides); which SCOPE it subscribes at
always is (different expectation, the node decides).

## Live caller connections: `ctx.caller()`

The durable primitives above (`await_signal`) are a DISCONNECTED wait:
the worker parks and dies. A live caller is the opposite world. When an
outside caller hits an `ApiEndpoint` (HTTP) or `LiveSocket` (WebSocket)
trigger, the dispatcher routes the held connection to one worker and the
worker stays alive on the open socket for the life of the request or
session. Any node downstream of the trigger can talk back to that caller
over the held connection. This is NOT durable: the connection is pinned
to the one worker and dies with it.

### Gating and the handle

A node downstream of a live trigger reaches the caller via `ctx`:

- `ctx.http_caller().await?` / `ctx.ws_caller().await?`: the one-call
  form for a node that only makes sense on one protocol. Each folds the
  whole chain (caller present, right protocol, connection barrier
  passed) and fails loud naming the trigger to wire it under.
- `ctx.caller() -> Option<CallerHandle>`: the protocol-typed handle, or
  `None` on a run with no live caller. `CallerHandle` is an enum over the
  two protocol shapes, so the type is honest about what each can do. For
  nodes that branch per protocol.
- `ctx.is_api_call()` / `ctx.is_websocket()`: status reads. A node may
  branch three ways (http / websocket / neither), so these are two
  separate queries, never one enum.
- `ctx.caller_data_type()`: the declared inbound/outbound shape (JSON /
  Text / Bytes) so a node can branch on what it sends.

The talk methods are protocol-specific:

- **HTTP** (`CallerHandle::Http(http)`): `request_parts()` (the inbound
  request), `write(chunk)` (stream a chunk), `respond(body)` (send the
  final body), `close()`. Respond/close are terminal: first one wins,
  a second errors loud.
- **WebSocket** (`CallerHandle::Websocket(ws)`): `send(chunk)`,
  `recv_next()` (next inbound message, or `None` when the stream ends),
  `receive()` (the typed-error form of the same read), `request(chunk)`
  (send then await one reply), `close()`. A read is UNBOUNDED: a node may
  wait minutes or hours for the caller's next message; only a disconnect or
  the trigger's session cap ends the wait.
- Both share `is_connected()` and one `ensure_connected().await?`
  barrier (wait for the caller's socket to actually attach before
  talking).

Inbound on a WebSocket is BROADCAST and forward-only by default, the
same model as the bus.

**Reading the stream.** `ws.receive()` reads messages that arrive after
you got the handle; the position is pinned at the moment you obtain it,
so there is no missed-message race between attach and your first read.
Every reader has its own position: two nodes both see every message, so
a responder and an observer can run off the same socket.

For the common loop, use `recv_next()`. It yields `Some(msg)` for each
message and `Ok(None)` when the stream ends (caller gone, session
capped, fell behind), so you write
`while let Some(msg) = ws.recv_next().await? {}` and the language does
the end-of-stream classification for you. A real failure (wrong
protocol, transport) propagates via `?`. When you need to distinguish
the exact outcome, `receive()` returns the typed `CallerError` so you
can `match` every case.

**Reading history.** To read earlier messages, mint a positioned
**cursor** (same concept as the bus): `ws.cursor_from_start()`
(everything still retained in RAM), `ws.cursor_at(offset)`, or
`ws.cursor_including_last()` (forward plus the single most recent
message, e.g. to grab the latest state on a late join). Ask
`ws.now_offset()` / `ws.retained_floor()` to position relative to now.
Offsets are absolute over the connection's whole life, so a saved
offset keeps naming the same message as the retention window moves.

A cursor reads the in-RAM window only. When a cursor's offset has been
trimmed out of the window, the read returns
`CallerError::FellBehind { oldest_resident }` and the cursor is moved
to `oldest_resident` (the earliest message still retained), so the next
read resumes there. The built-in forward cursor stays ahead of the
window in normal use, so it does not hit this.

### Pattern: a WebSocket conversation

```rust
use weft::caller::{InboundMessage, OutboundChunk};

async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    // Caller present, WebSocket, connected; loud otherwise.
    let ws = ctx.ws_caller().await?;
    // recv_next() yields each message, or None when the stream ends (caller
    // gone, session capped, fell behind). A real failure propagates via `?`.
    while let Some(msg) = ws.recv_next().await? {
        let v = match msg {
            InboundMessage::Json(v) => v,
            InboundMessage::Text(s) => Value::String(s),
            InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
        };
        ws.send(OutboundChunk::Json(json!({ "echo": v }))).await?;
    }
    let _ = ws.close().await;
    ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
}
```

The HTTP shape is the same idea with `respond`/`write` instead of
`send`/`receive`. Working examples:
`crates/weft-e2e/fixtures/web_trigger/nodes/http_responder`,
`crates/weft-e2e/fixtures/live_chat/nodes/ws_echo`.

### Lifetime: caller-tied vs survives

The `canSuspend` field on the trigger is the single lifetime axis (it
seeds `LiveConnectionConfig`):

- **off (default)**: the run is tied to the caller. A node that hits a
  durable `await_signal` holds the worker briefly then the run is killed;
  a caller disconnect cancels the run. This is the interactive default.
- **on**: the run may suspend and resume later without the caller (it
  becomes a background job); a disconnect does not kill it, and further
  sends go into the void.

A disconnect's meaning is derived purely from this one axis, there is no
separate disconnect setting to contradict it.

## Buses: live channels between co-alive nodes

A bus is an in-process message channel: one node creates it and emits
its marker on a `Bus`-typed output; downstream nodes resolve the marker
and exchange messages in RAM. The producer ritual (create, emit the
marker, register a name, and close on EVERY exit so readers never park
forever) is one call, and so is the consuming side:

```rust
// Producer: the returned guard closes the bus when dropped.
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
bus.send("msg", json!("hello"))?;
drop(bus); // the close IS the end-of-stream signal

// Consumer that participates (registers + closes on exit):
let bus = ctx.join_bus("channel", "guest")?;
let mut cursor = bus.cursor();
while let Some((from, value)) = cursor.next_json("msg").await? { /* ... */ }

// Observer that must NOT close the bus (a debug tap):
let bus = ctx.bus_from_input("channel")?;
```

`BusOptions` declares the channel's shape at creation, and every
consumer reads it back off the handle (or the marker):

- `payload`: what messages carry. `Json` (default) for chat-shaped
  traffic; `Bytes` for media frames (`send_bytes` / `next_bytes`, raw
  bytes end to end, no base64 between nodes). Frozen; the wrong shape
  is refused loudly.
- `meta`: creator-declared stream metadata (an audio stream's sample
  rate and encoding), read via `bus.meta()`, so a consumer knows the
  format before the first message instead of every frame repeating it.
- `ephemeral`: `true` keeps payloads out of the journal entirely (a
  sliding in-RAM window; a consumer that falls behind silently resumes
  at the oldest retained frame) for streams where the bytes are
  transient by nature.
- `journal_window`: the journal granularity (default 1s). The pump
  writes ONE journal row per bus per window: a journaled bus's window
  carries every message in it (nothing lost, fewer rows), an ephemeral
  one carries the rollup (count + bytes per sender/kind). What travels
  the bus is untouched; this is only how the trail is stored.

## Streams: `Generator[T]` ports

A `Generator[T]` port is an ordinary typed port that accepts being
emitted into repeatedly. That makes a PRODUCER trivial: yield items
with the emission call you already use, keeping any state (an open
connection, say) in plain Rust locals, and the stream ends when your
body returns:

```rust
// metadata.json: { "name": "rows", "type": "Generator[Row]" }
for row in read_rows(&file) {
    if keep(&row) {
        // The lock-step yield: returns once the consumer pulled this
        // item. Plain `pulse_downstream` instead keeps your body
        // running and buffers the item (bounded; see below).
        // `set` takes a JSON value, so a custom struct goes through
        // `serde_json::to_value` (or the `json!` macro).
        ctx.yield_downstream(NodeOutput::new().set("rows", serde_json::to_value(row)?))
            .await?;
    }
}
// body returns: the engine closes the stream (an error closes it as
// FAILED: the consumer's pull gets your error instead of a clean end).
```

The CONSUMER reads the stream from the input bag like any other input;
the type does the work. The node fires once, on the first item, and
pulls in its own code:

```rust
let rows = ctx.inputs.get::<Generator<Row>>("rows")?;
while let Some(row) = rows.next().await? { /* one item at a time */ }
```

On the handle: `next()` (waiting take: `Ok(Some(item))`, `Ok(None)` on
a clean end, the producer's error on a failed one), `try_next()` (no
wait; distinguishes "nothing buffered yet" from "finished"), `drain()`
(the whole stream as a `Vec`, erroring on a failed end rather than
handing back a truncated list), and `end()` (the end marker, once the
producer's side ended). Waiting is the language's job: none of these
spin or make you re-check readiness. A pull that can never be
satisfied (every remaining node provably waits on the others) is
resolved by the engine's stuck-check as a FAILED stream, surfacing
through `?` like any producer failure, never as a hang.

An EMPTY stream still runs the consumer: a producer that closes
without yielding delivers a stream whose first `next()` answers
`Ok(None)`, so your post-loop code (a summary over zero rows, say)
runs the same as over one row.

A PRODUCER that emits with plain `pulse_downstream` runs ahead of the
consumer's pulls; the un-taken items buffer on the edge. That buffer
is bounded (`DEFAULT_MAX_BUFFERED_ITEMS`, currently 4096), and the
emission past the bound fails the producer loudly (silent unbounded
buffering would be a memory leak). A producer that deliberately runs
far ahead declares its own bound:

```rust
ctx.set_max_buffered_items("rows", 100_000)?;
```

Only legal on a `Generator` output, refused for 0; it applies to the
emissions that follow the call (before or between emissions both
work). A `yield_downstream` producer never needs it (each yield waits
for its pull, so the buffer never grows past one).

Plain-`pulse_downstream` items are fire-and-forget: a consumer that
returns early (took what it wanted and stopped pulling) simply never
takes the rest, and the run completes with the leftovers dropped, like
any value a finished node never read. When your producer must KNOW its
items were consumed, that is what `yield_downstream` is for: a yield
whose consumer finishes without taking it fails your body loudly.

Rules to know: exactly one consumer per stream (broadcast is `Bus`
territory), a `Loop` consumes a stream by naming the port in `over`
(one item per iteration), a stream cannot cross a group boundary or
sit inside a container, a `Generator` INPUT must be required (an
unwired stream has no meaning), `close_port` on a generator output is
the early end-of-stream verb (legal after any number of yields), and a
stream consumer's body cannot `await_signal` (its resume would replay
a body whose pulled items cannot be replayed).

`yield_downstream` is not stream-specific: on an ORDINARY
port it suspends your body until the downstream consumer of the value
has actually been dispatched, a real synchronization point ("do not
continue until the next stage started", e.g. a phone-call node that
must not proceed until the answering node is live). It fails loudly
when the delivery can never happen (the consumer skipped or finished
without taking the value) instead of waiting forever.

## Storage: `ctx.storage`

A running node can read and write files through `ctx.storage`. Every
write takes a **scope** that decides WHERE the file lives and, crucially,
**how long it lives**. Pick the scope by how long you need the data, not
as an afterthought: the scope is the file's lifetime contract.

| Scope                      | Lives under              | Lifetime |
|----------------------------|--------------------------|----------|
| `StorageScope::Execution`  | `exec/<run>/`            | One run. Swept when the run terminates, UNLESS you flag it kept (a kept file survives until its keep-TTL or an explicit clean). The default scope. |
| `StorageScope::Project`    | `project/<project_id>/`  | Tied to the PROJECT. Outlives individual runs and is shared across the project's executions, but is **deleted when the project is deleted** (or by an explicit clean). |
| `StorageScope::Shared { name }` | `shared/<name>/`    | Tied to the OWNER (tenant), not any project. Survives runs AND project deletion. Projects that name the same `name` meet in the same space; first use auto-grants it. |

The lifetime distinction is the thing to get right:

- Use **`Execution`** for scratch a run needs and nothing else cares about
  after (intermediate files, temp downloads). It cleans itself up.
- Use **`Project`** for a project's own persistent state (a cache, an
  index, accumulated outputs) that should exist for as long as the project
  does and **go away with it**. Deleting the project reclaims these files;
  that is intended, not a bug.
- Use **`Shared { name }`** for data that must **outlive the project** (a
  dataset the owner reuses across projects, a model the user paid to build,
  anything they would be upset to lose when they delete a project). A
  `Shared` file is the owner's, addressed by a name they choose, and a
  project's deletion never touches it.

**An Execution-scoped file your node EMITS must be kept.** Every
Execution write (`put`, `put_stream`, `put_response`, `put_from_url`,
`internalize`) takes a `keep: Option<KeepTtl>`. `None` means the file is
swept shortly after the run ends: right for scratch, wrong for anything
the node pulses downstream, because an emitted reference lands in the
journal and renders in the editor long after the run, and a swept file
shows up there as "media expired". So a node that produces a user-facing
artifact (a generated image, synthesized speech, downloaded or received
media) passes `Some(KeepTtl::Default)`: kept 30 days, and every access
bumps the clock, so artifacts still in use never expire while abandoned
ones age out. A node whose file is cheaply re-fetchable (a plain
download) may instead expose a `keep` boolean config input (default off)
and pass `keep.then_some(KeepTtl::Default)`, letting the user decide.
The `KeepFile` node extends or pins any stored file's lifetime after the
fact.

So "do I want this file to survive deleting the project?" is answered
entirely by the scope on the write call: `Project` = no, `Shared` = yes.
There is no separate setting; changing the scope argument is the whole
knob. The files are reachable independently of the editor (e.g. the `weft`
CLI lists/downloads/removes them by scope), so `Shared` data a project
wrote remains accessible after the project is gone.

## Media config: the file-drop widget and project assets

When your node needs a user-supplied file (an image to display, an audio
clip to transcribe), declare a file-typed input; the drop/pick control
derives from the type automatically:

```json
"inputs": [
  { "name": "image", "type": "Image", "required": true, "exposure": "all" }
]
```

The input's type (`Image`, `Audio`, `Video`, `Blob`, or the `File` union)
drives the editor's file filter, validates drops, and is what gets written
into source. A declared `"widget": { "kind": "file_drop", "accept": "image/png" }`
narrows the filter further. The value is delivered on the input; the node
reads it via `ctx.inputs.get::<FileHandle>`. (`exposure: "all"` opens the
braces form; a file type's default is assignment-only.)

What lands in source is ONE clean line, never a storage key:

```
pick = ImagePick {
  image: @asset("assets/photo.png", Image)
}
```

The file lives WITH the project (the editor's drop/pick writes it under
`assets/`; a dev can equally copy a file there and type the line). The
`@asset` source can also be a path outside the project (local runs only),
an `http(s)` URL (never uploaded: the worker fetches it at run time), or a
stored runtime file's short address (`project/<project-id>/<file-id>`,
picked from the editor's "Stored files" browser).

`@asset` is the PULL-ONLY sibling of `@file`: `@file` injects a text
file's content and lets the editor write field edits back to that file
(so it requires a text-shaped type), while nothing ever writes back
through an `@asset`. A text-typed `@asset` is legal too: same inline read
as `@file`, just read-only in the editor.

Right before every build, the **asset sync** makes storage mirror exactly
what the code references: it hashes each referenced file, uploads new or
changed content, deletes content no longer referenced, and the compile
substitutes the resolved stored-file value. Your node code never sees any
of this: at run time the value on the port (or in config) is a normal media
value, identical to one an upstream node emitted, and `ctx.storage`
`get`/`get_bytes` read its bytes whichever handle it carries (a bucket key
or an external URL).

## Package-level metadata defaults

A package (a dir with `package.toml`) may hold a PARTIAL `metadata.json`
at its root: defaults every member node inherits. The merge is top-level
and key-by-key: for each key in the package file, a member gets it
unless its own `metadata.json` carries that key, in which case the
member's value wins wholesale (no deep merge). `type` can never be a
default (it is one node's identity); setting it at the package level is
an error.

Use it for whatever a package's nodes share: the `formFieldSpecs`
vocabulary the `human` package's trigger and query both speak, a
shared `types` block, and any future shared key. A
bare node has no package level; its own `metadata.json` is already the
whole story.

```
catalog/human/                     # package (has package.toml)
  package.toml
  metadata.json                    # PARTIAL: { "formFieldSpecs": [...] }, shared by BOTH members
  form_helpers.rs                  # shared: fields -> FormSchema/output mapping
  trigger/  metadata.json          # HumanTrigger  (hasFormSchema: true)
  query/    metadata.json          # HumanQuery    (hasFormSchema: true)
```

Any `.rs` file at the package root is a shared file like `form_helpers.rs`
above (`super::<filename>` from a member). This is also where a package
defines its own **provider meter**, when its nodes call a paid service weft
does not ship: a shared `.rs` file that ends in `weft_providers::register_meter!(...)`.
A bare node (no package) has no package root, so its meter goes at the bottom
of the node's own `mod.rs` instead. See `authoring-provider-meters.md`.

## Form-field nodes: `formFieldSpecs`

A form-field node (a human trigger/query: the user fills a form, its
fields become ports) does NOT hardcode its ports. Instead it declares a
VOCABULARY of field types under the `formFieldSpecs` metadata key, and
the node's real ports are DERIVED from whatever fields the graph author
configured, at parse/enrich time. So a `HumanTrigger` with an
`approve_reject` field named `review` gets two Boolean outputs
`review_approved` / `review_rejected` automatically; the author never
writes those ports.

### Turning it on

Set `hasFormSchema: true` in the node's `metadata.json` `features`, and
declare `formFieldSpecs` (usually once at the package level; see
"Package-level metadata defaults"). That flag is the gate: the enrich
pass only materializes form ports for nodes that declare it.

```json
// metadata.json
{ "type": "HumanTrigger", "features": { "hasFormSchema": true }, ... }
```

### The spec entries

Each entry defines one field type: its `field_type` token (what a
graph author's field `fieldType` must equal), a `render` hint for the
task UI, the config keys the editor collects, and the ports the field
adds. `{key}` in a `name_template` is substituted with the field's
user-supplied key; `T_Auto` requests a per-field type variable.

```json
// metadata.json (package-level, shared by every member)
{
  "formFieldSpecs": [
    {
      "field_type": "approve_reject",
      "label": "Approve / Reject",
      "render": { "component": "buttons", "source": "static" },
      "required_config": [],
      "optional_config": ["label", "approveLabel", "rejectLabel"],
      "adds_inputs": [],
      "adds_outputs": [
        { "name_template": "{key}_approved", "port_type": "Boolean" },
        { "name_template": "{key}_rejected", "port_type": "Boolean" }
      ]
    },
    {
      "field_type": "text_input",
      "label": "Text input",
      "render": { "component": "text" },
      "adds_outputs": [ { "name_template": "{key}", "port_type": "String" } ]
    }
  ]
}
```

The on-disk entries may use snake_case (`field_type`, `adds_outputs`,
`name_template`, `port_type`); the loader accepts that AND the camelCase
wire form, so you can write either.

### What derivation reads (and what it ignores)

Port derivation reads ONLY each configured field's `fieldType` and
`key`, matches the spec by `field_type`, and emits its `adds_inputs` /
`adds_outputs` with `{key}` resolved. It does NOT read a field's
`render` or `config` from the graph source: those are inherited from the
spec (a field may override `render`, but it need not, and the editor
emits the minimal `{ fieldType, key }` so the source stays lean). A
field's `key` becomes a port name, so it must be a legal identifier
(`[A-Za-z_][A-Za-z0-9_]*`).

## Infra nodes: long-running backing services

Some nodes need a long-running process the user can't easily run
themselves: a WhatsApp bridge daemon, a headless browser, a local LLM
server, a database. Weft calls those *infra nodes*. The pattern: the
node returns a typed `InfraSpec` from `provision_infra()`, the supervisor
compiles it to Kubernetes manifests and applies them, and the node
talks to the running pod(s) over HTTP at fire time.

You never write Kubernetes YAML by hand. You build an `InfraSpec` with
typed Rust structs (`Container`, `Endpoint`, `Volume`, ...); the
compiler turns it into Deployments, Services, PVCs, NetworkPolicies,
HPAs, and stamps all the `weft.dev/*` labels for you.

### Two methods: `provision_infra` and `run`

An infra node sets `requires_infra: true` in `metadata.json` and
implements two methods:

- **`provision_infra(ctx, input) -> InfraSpec`**: returns the desired
  infra shape. Pure: it emits no pulses, it just describes what should
  run. Called at provisioning time, before `run`. The `ctx`
  (`InfraProvisionContext`) carries `project_id`, `node_id`,
  `namespace`, `tenant_id`. The default impl returns an error, so only
  infra nodes override it.
- **`run(ctx)`**: the node's actual logic, same as any node. By the
  time `run` executes, the infra is applied and the node can resolve
  its endpoints.

The split is the rule: **provision_infra describes infra, run produces
pulses.** Provisioning can do async work (a registry lookup) but its
job is the spec.

```rust
async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag)
    -> WeftResult<InfraSpec>
{
    const PORT: u16 = 8090;
    Ok(InfraSpec {
        units: vec![Unit {
            name: "bridge".into(),
            on_upgrade: UpgradeBehavior::Recreate,
            containers: vec![
                Container::new("whatsapp", Image::Local { name: "bridge".into() })
                    .with_env(vec![EnvEntry::Literal { name: "PORT".into(), value: PORT.to_string() }])
                    .with_ports(vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(5)),
            ],
            ..Default::default()
        }],
        endpoints: vec![Endpoint {
            name: "api".into(), unit: "bridge".into(), container: "whatsapp".into(),
            port: "http".into(), expose: Expose::ClusterInternal,
        }],
        ..Default::default()
    })
}
```

### The `InfraSpec` reference

The fields you fill in when you build an `InfraSpec`.

**`InfraSpec`** (all fields default, so `InfraSpec::default()` is valid):
- `units: Vec<Unit>`: pod templates. Most nodes have one.
- `volumes: Vec<Volume>`: PVCs / emptyDir / mounted ConfigMaps+Secrets.
- `config: Vec<ConfigSource>`: Secrets / ConfigMaps to create, inline or by ref.
- `endpoints: Vec<Endpoint>`: named ports exposed via Services.
- `access: Access`: NetworkPolicy ingress/egress (default: workers in, internet out).
- `lifecycle: Lifecycle`: terminate policy (PVC preservation).
- `extras: Vec<Value>`: raw k8s manifests for things the typed surface doesn't model. Labels get stamped automatically.

**`Unit`** (one Pod template; the *operational* unit, see lifecycle below):
- `name` (required), `kind: UnitKind` (`Deployment` default / `StatefulSet` / `DaemonSet` / `Job`).
- `containers`, `init_containers`, `pod_options`.
- `scaling: ScalingPolicy`: `replicas` + optional `autoscale` (HPA). Per-unit.
- `on_upgrade: UpgradeBehavior`: `Rolling{...}` (default) or `Recreate`. Per-unit; only honored for Deployments.
- `on_stop: StopBehavior`: `ScaleToZero` (default) or `NoOp`. Per-unit. See "Stop behavior" below.
- `health: UnitHealth`: per-unit flaky/recovery window overrides (`flaky_after_seconds`, `recovery_after_seconds`); unset = supervisor defaults (30s).

**`Container`** (no `Default`, image is mandatory): build with
`Container::new(name, image)` then chain `.with_env / .with_ports /
.with_resources / .with_mounts / .with_readiness / .with_liveness /
.with_startup / .with_command / .with_args / .with_security_context /
.with_pre_stop`. `pre_stop` is a k8s preStop hook for graceful
shutdown; **Weft calls no Rust callback at stop time**, graceful
shutdown lives entirely in the container.

**`Image`**: `Image::Local { name }` (built from a dir in the node's
`images`, hash-tagged by the CLI) or `Image::Upstream { reference }`
(e.g. `"postgres:16"`). The local name is the directory basename of an
entry in `metadata.images`.

**`Endpoint`**: `name`, `unit`, `container`, `port` (the named
`ContainerPort`), `expose` (`ClusterInternal` default / `TenantPublic{path}`
/ `NodePort{port}`). The (unit, container, port) chain is validated at
compile.

**`Volume`** (`VolumeKind`): `Persistent { size, storage_class?, access_modes }`
(PVC, preserved across stop+upgrade, deleted on terminate unless in
`preserve_pvcs`), `EmptyDir`, `ConfigMap`, `Secret`.

**`Access`**: ingress rules (`FromWorkers` default, `FromNode`,
`FromInternet`, `FromCidrs`, `FromLabel`) + egress (`ToInternet`
default, `ToNode`, `ToCidrs`). Compiles to one NetworkPolicy on top of
the namespace baseline.

**`ScalingPolicy` / `AutoscaleSpec`**: static `replicas`, or an HPA
(`min/max_replicas`, `metrics` of CPU/Memory/Custom utilization).

The compiler validates DNS-1123 names, uniqueness per kind, and the
endpoint chains; a bad spec fails the apply loud (the node shows
`Failed` with the reason).

### Talking to your infra: `ctx.endpoint(name)`

At fire time, the node resolves an endpoint **by name**:

```rust
let api = ctx.endpoint("api").await?;     // one broker round-trip, caches the URL
let out = api.call(EndpointMethod::Get, "/outputs", None).await?;  // HTTP
let url = api.url();                       // bare service URL (no path, no trailing /)
```

`ctx.endpoint("api")` returns an `EndpointHandle` that caches the
cluster-internal URL (`<scheme>://<instance>-<name>.<ns>.svc.cluster.local:<port>`).
`.url()` is the bare URL; `.call(method, path, body)` does one HTTP
round-trip against it. The endpoint resolves only when the **whole
node is running** (all its units up): an endpoint is a front door to
the node, so a request must not land while a sibling unit is degraded.

**Endpoint resolution is by-name and per-node.** Only the node that
*declared* the endpoint can call `ctx.endpoint(name)`. A sibling node
(e.g. a "send" node targeting a "bridge" node) gets the URL by the
bridge **explicitly exporting it as an output port** and wiring it
downstream:

```rust
// bridge node, in run:
let api = ctx.endpoint("api").await?;
ctx.pulse_downstream(NodeOutput::new().set("apiUrl", api.url())).await

// send node, in run: reads the wired URL, appends its own path
let base: String = ctx.inputs.get("apiUrl")?;  // the bridge's exported URL
let resp = post(format!("{}/action", base.trim_end_matches('/')), body).await?;
```

The author chooses what to send downstream; there's no magic
auto-exported URL. With multiple endpoints, export each by name.

### Container-exposed HTTP routes (the capability contracts)

These are routes your container serves; Weft (or other nodes) call
them. Implement the ones you need.

| Route | Method | Who calls it | Contract |
| --- | --- | --- | --- |
| `/health` (or any path) | GET | the k8s readiness probe | Return 2xx when ready. Wire it via `Probe::http("/health", port)` on the container. The path is whatever you pass to `Probe`. |
| `/live` | GET | the dispatcher proxy (graph body panel) | Return `{ "items": [{ "type": "text"\|"image", "label": "...", "data": "..." }] }`. The graph polls this every 3s and renders the items (a QR code, a status line). Opt in by setting `features.liveEndpoint` to the endpoint name (below). |
| `/outputs` | GET | the declaring node's own `run` | Return a flat JSON object; the node folds each key into an output port. The key set must match the node's declared `outputs` in metadata.json (a hand-maintained contract, not enforced). |
| `/action`, `/events`, ... | any | sibling nodes via the wired URL | Your own convention. e.g. a send node POSTs `/action`, a receive node opens an SSE stream at `/events`. The route names live in the node code, the URL comes from the wired endpoint export. |

`/live` is special because the *dispatcher* (not a node) calls it, so
it needs to know which endpoint serves it. Declare it in metadata:

```json
"features": { "liveEndpoint": "api" }
```

`liveEndpoint` names the endpoint whose URL the dispatcher appends
`/live` to. Unset = no `/live` (the graph shows no body panel). There
is no separate `hasLive` flag: naming the endpoint *is* opting in.

### Node packaging

```
catalog/whatsapp/
  package.toml          # shared cargo deps for all nodes in the package
  bridge/
    mod.rs              # the node impl
    metadata.json       # requires_infra, images, features, ports
    deps.toml           # per-node cargo deps
    images/bridge/      # Dockerfile + source for the Image::Local "bridge"
```

`metadata.json` for an infra node:

```json
{
  "type": "WhatsAppBridge",
  "requires_infra": true,
  "images": ["images/bridge"],
  "outputs": [ { "name": "apiUrl", "type": "String" }, ... ],
  "features": { "liveEndpoint": "api" }
}
```

Each entry in `images` is a directory (relative to the package root)
containing a `Dockerfile`; its basename is the `Image::Local { name }`.
The CLI hashes the dir, tags `weft-infra-{name}:{hash}`, and (for kind)
loads it into the cluster.

### Lifecycle: stop, start, upgrade, terminate

The unit is the *operational* granularity. Each `Unit` independently
has a status (running / stopped / flaky / ...) and a `on_stop`.

**`weft infra start`** brings *down* units up to spec. Units that are
already up are left untouched (something downstream may depend on them
running). So a plain start never disturbs a running unit.

**`weft infra stop`** takes units down per their `on_stop`:
- `ScaleToZero` (default): scale the unit's workloads to 0. PVCs and
  Services are kept (the endpoint URL stays stable), so a later start
  brings it back fast.
- `NoOp`: the unit **stays up**. Use it for a unit that's expensive or
  slow to recreate (a model that took an hour to download, a license
  server with live sessions) and that downstream work depends on. Only
  terminate, or an explicit force-stop, takes a NoOp unit down.

**`weft infra upgrade`** is just **stop then start**: it stops
(respecting `on_stop`) then starts. ScaleToZero units cycle onto the
new spec; NoOp units stayed up through the stop, so start leaves them
**frozen at their current version**.

**Updating a frozen (NoOp) unit** requires an explicit force-stop:

```
weft infra node-stop <node_id> --force   # ignores on_stop, scales the node's units to 0
weft infra start                         # recreates them at the new spec
```

`--force` is the conscious "I accept the downtime, take it down so I can
update it" override. The graph's per-node right-click "stop" uses
`--force` automatically (you explicitly picked one node, so its NoOp
units come down too).

**`weft infra terminate`** deletes everything for the node (all units,
PVCs unless listed in `lifecycle.on_terminate.preserve_pvcs`). Deleting
a node from the graph terminates it on the next sync (orphan reap);
removing a single *unit* from a node's spec terminates that unit's
workloads on the next apply.

### Health

The supervisor watches each unit's replicas. A unit continuously
below its readiness threshold for `flaky_after_seconds` (default 30)
is marked `flaky`; continuously ready for `recovery_after_seconds`
returns it to `running`. Health is per-unit: one flaky sidecar doesn't
drag a healthy primary down, and a project's HealthProtocols can target
a specific unit (`NodeReadyReplicas { node_id, unit, ... }`) for
remediation (bounce pods, scale, park triggers). Override the windows
per unit via `Unit.health`.

### Security and resources: your call

The compiler stamps labels and namespaces but adds **no security
context or resource limits on your behalf**. Cross-tenant isolation
comes from the namespace boundary and the baseline NetworkPolicies; a
runaway container can still starve its node. To be a good citizen, set
`Resources` (cpu/memory requests+limits) on the container and a
`ContainerSecurityContext` / `PodSecurityContext` (run as non-root,
read-only root fs, drop capabilities, `RuntimeDefault` seccomp) to
satisfy the k8s `restricted` baseline. Your image has to cooperate
(run as the chosen UID, tolerate a read-only root). If it can't, skip
it; within your own namespace you can do what you want.

### Limitations to know

- **Upstream mutable image tags don't trigger drift.** `Image::Upstream`
  with a tag like `:latest` is passed through verbatim; Weft does not
  resolve it to a digest. If the tag rolls underneath you, the spec
  hash doesn't change, so the graph won't surface "upgrade available".
  `weft infra upgrade` still re-applies manually if you know there's a
  new version. Use a digest (`@sha256:...`) for reproducibility.
- **`/outputs` <-> output ports is a convention**, not enforced: keep
  your container's `/outputs` keys in sync with `metadata.json`
  `outputs` by hand.
- **Keep bus work on your node's own task.** If your node uses a message
  bus (`ctx.create_bus` / `ctx.bus`), do all of its reads and waits
  (`cursor.next`, `wait_for`, `recv`) directly in your node body. Do NOT
  move a bus handle or cursor into a `tokio::spawn`ed background task. The
  engine decides "every node is stuck, close the buses" by tracking
  whether each node EXECUTION is waiting or working, and it assumes a
  node's bus waits run on the node's own task. A wait happening on a task
  you spawned is invisible to that accounting and can make the engine
  wrongly tear down a live conversation (or hang). If you need concurrent
  or background work, model it as another node and exchange with it over
  the bus, rather than spawning a task yourself. This is a convention, not
  enforced.

## Last warning: a loop with a wait inside a node

Don't. A `loop` in your `run` body that contains an `await_signal` means
the node is orchestrating, which is the graph's job. Use weft's `Loop(...)`
and let each iteration fire a single-responsibility node.

If you do it anyway: a resume re-runs the WHOLE body from the top, so the
loop restarts at iteration zero. Past `await_signal` and `ctx.run` calls
replay instantly from the journal, but every other call runs for real
again, once per replay. An un-wrapped paid API call or payment inside that
loop is charged again on every human response. Wrap every side-effecting
or non-deterministic call in `ctx.run` (the same name each iteration is
fine, the runtime keys on call order, not name).

## Testing your node

Every node can declare its own tests in a `tests.rs` next to its
`mod.rs`: pure `basic` tests, `fake` tests running the full body
against canned provider responses (no credentials, no cost), and
`live` tests through the real access path. Run them with
`weft test-node`. Full guide: [node-tests.md](node-tests.md).
