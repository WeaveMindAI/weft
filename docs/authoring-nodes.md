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
  metadata.json       # the node's declared surface (ports, fields, features)
  deps.toml           # optional: extra cargo deps beyond the codegen base
```

The trait (in `weft_core`). A node implements up to three separately
named bodies, and the ENGINE picks which to call from the manifest; a
node never inspects the lifecycle phase itself:

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    /// Infra nodes only (`requires_infra: true`): the desired infra shape.
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: InputBag)
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

use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft_core::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.config.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

### Reading values: `ctx.ports` and `ctx.config`

A node reads its named values from two separate bags. `ctx.ports` holds
what arrived on the node's input ports; `ctx.config` holds the node's
design-time config fields. Each name lives in exactly one of the two, so
the read is always unambiguous: a port value comes from `ctx.ports`, a
config field from `ctx.config`.

Both bags expose the same accessors:

- `.get::<T>("name")?`: required, typed, loud error when absent or
  mistyped (stamped as an input or config error per bag).
- `.opt::<T>("name")?`: optional (`Ok(None)` when absent or null), but
  a PRESENT wrong-typed value still errors loud.
- `.get_or("name", default)?`: absent means the default, wrong type
  still errors. The blessed pattern for defaulted knobs. (Don't write
  `.get(..).unwrap_or(..)`: it swallows a real type error.)
- `.raw("name")`: the optional raw JSON for pass-through reads; a
  REQUIRED raw read is `.get::<Value>("name")?`.
- `.iter()`: every named value, for nodes that treat their ports as a
  dynamic set (script variables, form fields) or forward them.
- `.object()?`: the whole bag as one `serde_json::Map`, for nodes that
  consume or forward it as a record. Always answers on ports and
  config; on the wake bag it fails loud when the fire delivered no
  keyed record (a broken delivery can never pass as an empty one).

File values are just types: `ctx.ports.get::<FileHandle>("image")?`
parses the file value and fails loud when it has no readable handle.

### How a port gets its value in weft source

A port has exactly one driver, written in one of three forms: an edge
(`n.x = other.y`), an assignment literal (`n.x = 5`), or a braces
literal (`M { x: 5 }`). Giving a port two drivers is a compile error
(`double-driven-port`).

Where a LITERAL may be written is the port's `literal` level, declared
on the port in `metadata.json` (an explicit level wins, otherwise the
port type's default applies):

- `"anywhere"`: braces or assignment. The default for plain data
  (strings, numbers, lists, dicts).
- `"assignment"`: only `n.x = ...`. The default for file types, e.g.
  `n.image = @asset("i.png", Image)`.
- `"none"`: wires only. The default for Bus ports; declare it for a
  port that needs a real node wired, like an inference node's `config`
  port.

Edges are legal on every port, in both spellings: `n.x = other.y` and
`M { x: other.y }` produce the same wire.

Whichever form drove the port, the node reads the value from
`ctx.ports`. A port with `literal: "assignment"` or `"none"` may share
a name with a config field; the two hold independent values
(`ctx.ports.get(name)` vs `ctx.config.get(name)`). A field sharing the
name of a `literal: "anywhere"` port is a metadata error: declare only
the port, it alone carries the value.

### The config-node pattern

A node may declare an input port named `config`. A configuration object
wired to it (from a config node) is consumed by the ENGINE before
dispatch: each key overlays the node's config bag, and a key naming one
of the node's `literal: "anywhere"` ports fills that port instead (a
directly wired value on that port wins). The node body just reads
`ctx.config` and `ctx.ports` as usual.

### How the editor renders ports

The editor shows an inline field in the node body for every port that
can take a literal. The control kind derives from the port TYPE through
one central mapping: file types get a drop/pick control, Boolean a
checkbox, Number a number box, everything else a text area. A small
marker on the field toggles which source form the value is written in
(braces vs statement; locked to statement for `"assignment"` ports). A
`literal: "none"` port shows a handle only, and wiring an edge hides
the field (one driver).

### Emitting output, errors, HTTP, identity

- `ctx.pulse_downstream(NodeOutput)`: emit values on output ports and
  fire downstream. `NodeOutput::new().set("port", value)` takes anything
  that converts to JSON directly (bools, numbers, strings, an
  already-built `Value` passes through untouched); chain `.set` for more
  ports; `.extend_from_object(json)` fans a JSON object's keys onto
  same-named ports. A port not present in the output emits no pulse
  (downstream of it skips, the null-propagation rule).
- `.node_err("doing X")?` on any non-weft `Result` (an HTTP call, a
  parser) turns its error into a node failure reading "doing X: ...";
  on an `Option`, `None` becomes a failure carrying the message
  verbatim. For a bad condition the node detects itself (nothing to
  wrap), `weft_core::node_bail!("bridge rejected: {reason}")` fails the
  node with that message in one statement; its expression cousin
  `node_error(message)` fits `map_err`/`ok_or_else` closures that build
  a rich message first. These are the only error doors: the accessors
  stamp input/config errors themselves, every ctx handle already
  returns `WeftResult`, and node code never names a `WeftError`
  variant.
- `ctx.http()`: the shared, pooled HTTP client for plain (unpaid)
  outbound calls. A PAID provider call goes through
  `ctx.provider_access` + `ctx.metered_client` instead (that is what
  records its cost).
- Identity fields: `ctx.execution_id`, `ctx.project_id`, `ctx.node_id`,
  `ctx.node_type`, `ctx.node_label`, `ctx.color`, `ctx.frames`.

`metadata.json` declares the surface (see `weft_core::NodeMetadata` for
every field): `type`, `label`, `description`, `category`, `tags`, `icon`,
`color`, `inputs`/`outputs` (`{ name, type, required, description }`),
`fields` (config the editor collects: `{ key, label, field_type, required,
description }`), `requires_infra`, `images`, `features`, `validate`.

`deps.toml` lists extra cargo dependencies beyond the always-available
base (weft-core, tokio, serde, serde_json, async-trait, anyhow, tracing,
uuid):

```toml
[dependencies]
reqwest = { version = "0.12", features = ["json"] }
```

A package (one `package.toml` root with member node subdirs) shares deps
and helper files across members; a bare node dir stands alone.

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
| Paid call (metered client)              | Yes, instant        | Nothing (the metering settles on its own) |
| CPU-bound `spawn_blocking`              | Future returns, thread leaks | Pass flag, poll `is_cancelled()` |
| External resource needing cleanup       | Best effort         | `tokio::select!` branch on the flag |

**Bottom line**: write normal async Rust. Reach for `ctx.cancellation()`
only when you spawn a subprocess, run blocking CPU work, or hold a
resource that needs explicit cleanup before drop, and treat that cleanup
as best-effort (the abort races it).

## Paid calls: the access and the metered client

A node that spends money on a third-party API (an LLM call, a search
credit, an enrichment lookup) does exactly two things:

```rust
// 1. Your access to the provider: what to authenticate with. A key on
//    the node's key input is the user's own; an empty input (or the
//    platform sentinel) asks the runtime for its configured key
//    (<PROVIDER>_API_KEY on the runtime's broker). Your code is the
//    same lines either way; nothing branches.
let access = ctx.provider_access("openrouter", ctx.config.opt("apiKey")?).await?;

// 2. An ordinary HTTP client to make the calls with. Use it directly,
//    or hand it to any library that accepts an injected client.
let http = ctx.metered_client(&access)?;

// An HTTP library: give it the credential and the client.
let generator = GeneratorInfo::openrouter(model)
    .with_api_key(access.credential())
    .with_http_client(http);

// A hand-built request: same two values.
let response = ctx.metered_client(&access)?
    .post("https://api.tavily.com/search")
    .bearer_auth(access.credential())
    .json(&body)
    .send()
    .await?;
```

That is the whole surface: open the access, make the calls. **The runtime
measures what each call really cost** (the provider's meter runs around the
call, behind the client) and records it on the execution's cost trail. A
node states no cost and has no way to; the number is always the runtime's
measurement, so you cannot misbill no matter what your code does. A Stop
mid-generation is equally not your problem: the metering outlives your
future and still resolves the interrupted call's real cost.

When the node's body finishes (any outcome), the runtime gives a
runtime-granted access back on its own; nothing node-facing closes it.

Rules that matter:

- **Never construct your own HTTP client for a paid call**; always take it
  from `ctx.metered_client(&access)`. A call on a hand-rolled client is
  invisible to the cost trail, and a runtime-granted credential only
  works through the metered client's routing. (A library that refuses an
  injected client cannot be used for a paid call, and that is a smell in
  that library.)
- Address the provider's REAL API. The client does any routing a
  runtime-granted access needs; your code never rewrites a URL for it.
- Do not stash `.credential()` anywhere (an output port, a log, an error,
  a struct that outlives the call).
- A missing runtime key is a loud error naming the fix ("set your own
  key"); do not paper over it.
- On a runtime-granted access, requests do not follow redirects: a
  provider that answers "go ask this other address instead" is an error,
  not a second request. Every API we support answers directly. (A user's
  own key is unaffected: those requests are yours.)
- Sending media? Declare what you know about it on the media objects
  (`AudioData::with_duration`, `ImageData::with_dimensions`, ...): it
  sharpens the pre-flight cost estimate for the call. It never changes
  what is actually billed (that is always the measured cost).

`provider_access` assumes your provider work fits the default window (15
minutes): that is how long a runtime-granted credential is guaranteed
usable if your node crashes without finishing. A node wrapping a genuinely
long action declares its own:
`ctx.provider_access_within(provider, key_input, Duration::from_secs(...))`.

### Which providers

The provider name you pass to `ctx.provider_access` is the key's identity:
the runtime's key for it lives in `<NAME>_API_KEY` (uppercased). Any
provider works with the user's own key. The runtime only supplies ITS
configured key for providers whose spend it can measure: the ones weft
ships a reviewed meter for.

A meter is the small piece of code that measures what a call really cost.
Weft ships meters for the providers it supports, and **your project can
define its own meter** for a provider weft does not ship yet (it lives
beside the nodes that call it, and works with your own key right away).
Writing one, and getting a project's meter promoted to a weft-shipped one
(so the platform keys in app.weavemind.ai can pay for it too), is all in
`docs/authoring-provider-meters.md`.

The key input itself is an ordinary config field with the `api_key` field
type, naming the provider so the editor renders the "Credits / Own key"
choice:

```jsonc
// metadata.json, in configFields
{ "key": "apiKey", "label": "API key",
  "field_type": { "kind": "api_key", "provider": "tavily" } }
```

## Durable execution: `await_signal`, `register_signal`, `ctx.run`

Three primitives let a node body interact with the outside world in a
way that survives the worker dying and a fresh worker resuming
hours or days later.

### `ctx.register_signal(kind)`

For a trigger's `setup_trigger` body. Tells the listener to watch for a
wake signal. You pass a **typed signal kind** (one of the structs in
`weft_core::signal`, see the kinds table below), not an untyped spec;
the framework projects it onto the wire shape. Returns `()` once the
dispatcher acknowledges. Each external fire later spawns a fresh
execution of the project; this registration is NOT bound to the current
execution's firing. Any public URL is derived from the signal's path on
the dispatcher, so nodes don't get a URL handed back.

A trigger writes two bodies and never inspects any phase; the engine
calls the right one:

```rust
use weft_core::signal::{ApiEndpoint, LiveConnectionConfig};

#[async_trait]
impl Node for MyTriggerNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Build the typed kind from the node's values, then register
        // it. (The live-caller kinds share a config body built by
        // `LiveConnectionConfig::from_node_fields`.) The runtime saves
        // a snapshot of `ctx.ports` with the registration.
        let common = LiveConnectionConfig::from_node_fields(ctx.config.object()?);
        ctx.register_signal(ApiEndpoint { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Runs when an external fire arrives, exactly once per fire.
        // The fire's payload fields are on the `ctx.wake` bag (same
        // accessors as ports/config); `ctx.ports` replays the values
        // the trigger's inputs held at setup time.
        let value: serde_json::Value = ctx.wake.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

**What a trigger's `run` sees, and when it runs.** A trigger's three
value sources at fire time:

- `ctx.config`: the node's design-time config, as always.
- `ctx.ports`: a SNAPSHOT of what the trigger's inputs held when it
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
use weft_core::signal::SseSubscribe;

ctx.register_signal(SseSubscribe {
    url: events_url,
    event_name: "message.received".into(),
}).await?;
```

### Wake-signal kinds

Each kind is a struct in `weft_core::signal`. A node constructs one and
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
| `PollEndpoint { url, interval_secs }` | Hits a URL on a timer; fires with the response body. No held connection. | A "give me what's new" endpoint (a bot getUpdates loop). |
| `SocketListen { url, handshake?, heartbeat?, heartbeat_secs }` | Holds a bidirectional WebSocket alive, sends an optional handshake on open and an optional heartbeat frame on a schedule; fires per inbound frame. | A gateway that needs login + keepalive or it drops you (Discord, Slack socket mode). The service-specific protocol (op-codes) is YOUR concern, expressed as the literal `handshake` / `heartbeat` frames. |

**Inbound live-caller endpoints** (an outside caller dials IN and holds
the connection; nodes talk back via `ctx.caller()`, see the live-caller
section):

| Kind | What it does | Use for |
| --- | --- | --- |
| `ApiEndpoint { common }` | An HTTP endpoint people call; a node replies once or streams a response. | A live HTTP API a program serves. |
| `LiveSocket { common }` | An inbound WebSocket; a node holds a two-way conversation. | A live chat / interactive socket a program serves. |

Both live-caller kinds share `LiveConnectionConfig` (the `common`
field): build it from the node's merged named values with
`LiveConnectionConfig::from_node_fields(ctx.config.object()?)`. The wire
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
use weft_core::signal::Form;

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

(The exact `Form` fields are in `weft_core::signal::Form`; the point is
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
- Reading named values via `ctx.ports` / `ctx.config`.

What's NOT safe between awaits (wrap in `ctx.run`):
- `rand::random()`, `Uuid::new_v4()`, `Instant::now()`.
- Network calls, DB writes, file I/O.
- Reading environment variables that might change.
- Anything that could differ between two runs of the same code.

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
use weft_core::caller::{InboundMessage, OutboundChunk};

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
`send`/`receive`. Working examples: `catalog/live/http_responder`,
`catalog/live/ws_echo`, `catalog/live/ws_listener`.

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

So "do I want this file to survive deleting the project?" is answered
entirely by the scope on the write call: `Project` = no, `Shared` = yes.
There is no separate setting; changing the scope argument is the whole
knob. The files are reachable independently of the editor (e.g. the `weft`
CLI lists/downloads/removes them by scope), so `Shared` data a project
wrote remains accessible after the project is gone.

## Media config: the `file_drop` field and project assets

When your node needs a user-supplied file (an image to display, an audio
clip to transcribe), declare a `file_drop` field with the weft file type it
picks:

```json
"inputs": [
  { "name": "image", "type": "Image", "required": true }
],
"fields": [
  { "key": "image", "label": "Image", "field_type": { "kind": "file_drop", "type": "Image" } }
]
```

The declared `type` (`Image`, `Audio`, `Video`, `Blob`, or the `File` union)
drives the editor's file filter, validates drops, and is what gets written
into source. An optional `accept` narrows the filter further (e.g.
`"image/png"`). A field whose `key` matches an input port makes that port
fillable from config, and the value is delivered ON the port: the node
reads it via `ctx.ports.get`.

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
vocabulary the `human` package's trigger and query both speak, a shared
`category`, and any future shared key. A
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
async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: InputBag)
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
let base: String = ctx.ports.get("apiUrl")?;   // the bridge's exported URL
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
