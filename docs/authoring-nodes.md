# Authoring Nodes

This guide covers patterns and gotchas when writing a node implementation
for Weft. Start with the existing nodes in `catalog/` for working
examples; this document explains the cross-cutting concerns that aren't
obvious from any single node.

## Writing a basic node

A node is a directory under the project's `nodes/` root:

```
nodes/my_node/
  mod.rs              # the Rust impl (a `Node` trait impl)
  metadata.json       # the node's declared surface (ports, fields, features)
  deps.toml           # optional: extra cargo deps beyond the codegen base
```

The trait (in `weft_core`):

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    /// Infra nodes only (`requires_infra: true`): the desired infra shape.
    async fn provision(&self, ctx: InfraProvisionContext, input: InputBag)
        -> WeftResult<InfraSpec> { /* default: error */ }
    /// The node's logic. The ONLY way to fire downstream is
    /// `ctx.pulse_downstream(output)`.
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()>;
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
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft_core::node::NodeOutput;

#[derive(NodeManifest)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.config.get("value")?;
        ctx.pulse_downstream(NodeOutput::with("value", Value::String(value))).await
    }
}
```

What `execute` works with:

- `ctx.config: ConfigBag`: the node's `.weft` config values;
  `ctx.config.get::<T>("key")?` deserializes one (loud error when missing
  or mistyped).
- `ctx.input: InputBag`: the values that arrived on the input ports this
  fire; `ctx.input.get::<T>("port")?`.
- `ctx.pulse_downstream(NodeOutput)`: emit values on output ports and fire
  downstream. `NodeOutput::with("port", value)` for one port; chain
  `.set("other", value)` for more (`NodeOutput::empty()` fires nothing;
  `.extend_from_object(json)` fans a JSON object's keys onto same-named
  ports). A port not present in the output emits
  no pulse (downstream of it skips, the null-propagation rule).
- `ctx.phase: Phase`: which lifecycle phase this invocation is
  (`InfraSetup` / `TriggerSetup` / `Fire`); non-trigger, non-infra nodes
  only ever see `Fire`.
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
async fn execute(ctx: ExecutionContext, ...) -> WeftResult<()> {
    let resp = reqwest::Client::new()
        .post("https://api.anthropic.com/v1/messages")
        .json(&body)
        .send()
        .await?
        .json::<ApiResponse>()
        .await?;
    ctx.pulse_downstream(NodeOutput::with("response", resp.text)).await
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
    _ = cancel.cancelled() => {
        // SIGTERM, give it 2s to clean up, then drop kills it.
        let _ = child.start_kill();
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            child.wait()
        ).await;
        Err(WeftError::Cancelled)
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
async fn execute(ctx: ExecutionContext, image: Vec<u8>) -> WeftResult<()> {
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
    ctx.pulse_downstream(NodeOutput::with("out", result)).await
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
async fn execute(ctx: ExecutionContext, ...) -> WeftResult<()> {
    let mut conn = open_connection().await?;
    let cancel = ctx.cancellation();
    loop {
        tokio::select! {
            msg = conn.recv() => {
                if let Some(m) = msg? { handle(m); } else { break; }
            }
            _ = cancel.cancelled() => {
                conn.send_close_message().await.ok();
                return Err(WeftError::Cancelled);
            }
        }
    }
    ctx.pulse_downstream(NodeOutput::with("done", json!(null))).await
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
//    platform sentinel) asks the deployment for its configured key
//    (<PROVIDER>_API_KEY on the deployment's broker). Your code is the
//    same lines either way; nothing branches.
let access = ctx.provider_access("openrouter", cfg.get_optional("apiKey")?).await?;

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
deployment-granted access back on its own; nothing node-facing closes it.

Rules that matter:

- **Never construct your own HTTP client for a paid call**; always take it
  from `ctx.metered_client(&access)`. A call on a hand-rolled client is
  invisible to the cost trail, and a deployment-granted credential only
  works through the metered client's routing. (A library that refuses an
  injected client cannot be used for a paid call, and that is a smell in
  that library.)
- Address the provider's REAL API. The client does any routing a
  deployment-granted access needs; your code never rewrites a URL for it.
- Do not stash `.credential()` anywhere (an output port, a log, an error,
  a struct that outlives the call).
- A missing deployment key is a loud error naming the fix ("set your own
  key"); do not paper over it.
- On a deployment-granted access, requests do not follow redirects: a
  provider that answers "go ask this other address instead" is an error,
  not a second request. Every API we support answers directly. (A user's
  own key is unaffected: those requests are yours.)
- Sending media? Declare what you know about it on the media objects
  (`AudioData::with_duration`, `ImageData::with_dimensions`, ...): it
  sharpens the pre-flight cost estimate for the call. It never changes
  what is actually billed (that is always the measured cost).

`provider_access` assumes your provider work fits the default window (15
minutes): that is how long a deployment-granted credential is guaranteed
usable if your node crashes without finishing. A node wrapping a genuinely
long action declares its own:
`ctx.provider_access_within(provider, key_input, Duration::from_secs(...))`.

### Which providers

The provider name you pass to `ctx.provider_access` is the key's identity:
the deployment's key for it lives in `<NAME>_API_KEY` (uppercased). Any
provider works with the user's own key. A deployment only supplies ITS
configured key for providers whose spend it can measure: the ones with a
meter in `weft-providers` (see `docs/authoring-provider-meters.md`, which
is also the path to adding one).

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

For trigger nodes during `Phase::TriggerSetup`. Tells the listener to
watch for a wake signal. You pass a **typed signal kind** (one of the
structs in `weft_core::signal`, see the kinds table below), not an
untyped spec; the framework projects it onto the wire shape. Returns
`()` once the dispatcher acknowledges. Each external fire later spawns
a fresh execution of the project; this registration is NOT bound to the
current execution's firing. Any public URL is derived from the signal's
path on the dispatcher, so nodes don't get a URL handed back.

```rust
use weft_core::signal::{ApiEndpoint, LiveConnectionConfig};

async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    match ctx.phase {
        Phase::TriggerSetup => {
            // Build the typed kind from the node's config fields, then
            // register it. (The live-caller kinds share a config body
            // built by `LiveConnectionConfig::from_node_fields`.)
            let common = LiveConnectionConfig::from_node_fields(&ctx.config.values);
            ctx.register_signal(ApiEndpoint { common }).await?;
            ctx.pulse_downstream(NodeOutput::with("started", json!(true))).await
        }
        Phase::Fire => {
            // Run when an external fire arrives.
            // ctx.wake_payload() returns the wake event's data (the
            // parsed SSE event JSON, the poll body, the timer info,
            // etc.). Returns None for non-trigger nodes and for trigger
            // setup; trigger Fire bodies that REQUIRE a payload should
            // `ok_or_else` with a clear error.
            ...
        }
        _ => Ok(()),
    }
}
```

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
field): build it from the node's config map with
`LiveConnectionConfig::from_node_fields(&ctx.config.values)`. The wire
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

async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let answer = ctx.await_signal(Form {
        form_type: "human-query".into(),
        schema: my_form_schema(),
        title: Some("Approve?".into()),
        description: None,
        consumer_kind: Some("human_in_the_loop".into()),
    }).await?;
    // After the user submits, `answer` is the form payload.
    ctx.pulse_downstream(NodeOutput::with("answer", answer)).await
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
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let approval = ctx.await_signal(approval_spec()).await?;
    if approval["accepted"].as_bool() != Some(true) {
        return ctx.pulse_downstream(NodeOutput::with("decision", json!("rejected"))).await;
    }
    let confirmation = ctx.await_signal(confirmation_spec()).await?;
    ctx.pulse_downstream(NodeOutput::with("final", confirmation)).await
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
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    // Mint an idempotency token ONCE; replays return the same token.
    let idem = ctx.run("idem", || async {
        Ok(json!(uuid::Uuid::new_v4().to_string()))
    }).await?;

    let approval = ctx.await_signal(approval_spec()).await?;

    // Call an external API ONCE; replays return the same response.
    let api_resp = ctx.run("call_billing", || async {
        let resp = reqwest::Client::new()
            .post("https://api.billing/charge")
            .json(&json!({ "idem": idem, "approved_by": approval["who"] }))
            .send().await?
            .json::<serde_json::Value>().await?;
        Ok(resp)
    }).await?;

    ctx.pulse_downstream(NodeOutput::with("receipt", api_resp)).await
}
```

The closure runs at most once across the lifetime of this (color,
node, frames) firing. On every subsequent replay, the journaled
value comes back without invoking the closure. Idempotency, signed
URLs that need to be stable across replays, expensive computation,
external side effects: all live behind `ctx.run`.

The author-supplied `name` is for traceability in the journal. The
runtime keys on call-site ORDER, not on the name; you can change a
name freely as long as the call sequence stays the same.

### Deterministic-replay rule

A node body is replayed from the top whenever a fire arrives. **The
sequence of `ctx.await_signal` and `ctx.run` calls must be identical
across every replay.** The runtime checks the next call against the
journaled sequence; a mismatch fails the node loudly with a clear
error.

What's safe between awaits:
- Pure logic, branching on values that came from awaits or runs.
- Reading inputs/configs from `ctx.input` / `ctx.config`.

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

- `ctx.is_api_call()` / `ctx.is_websocket()`: status reads. A node may
  branch three ways (http / websocket / neither), so these are two
  separate queries, never one enum. A node that REQUIRES a caller fails
  loud when neither is true.
- `ctx.caller() -> Option<CallerHandle>`: the protocol-typed handle, or
  `None` on a run with no live caller. `CallerHandle` is an enum over the
  two protocol shapes, so the type is honest about what each can do.
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

Inbound on a WebSocket is BROADCAST and **forward-only by default**, the
same model as the bus:

- `ws.receive()` reads messages that arrive AFTER you got the handle, not
  prior history. The position is pinned when you obtain the handle (no
  missed-message race between attach and your first read).
- Every reader has its OWN position, so two nodes both see every message
  (neither steals from the other), a responder and an observer can run off
  the same socket.
- To read earlier messages, mint a positioned **cursor** (same concept as
  the bus): `ws.cursor_from_start()` (everything still retained in RAM),
  `ws.cursor_at(offset)`, or `ws.cursor_including_last()` (forward plus the
  single most recent message, e.g. to grab the latest state on a late
  join). Ask `ws.now_offset()` / `ws.retained_floor()` to position
  relative to now. Offsets are ABSOLUTE over the connection's whole life
  (never relative to the sliding window), so a saved offset always names
  the same message even as the window moves.
- For the common loop, use `recv_next()`: it yields `Some(msg)` for each
  message and `Ok(None)` when the stream ends (caller gone, session capped,
  fell behind), so you write `while let Some(msg) = ws.recv_next().await? {}`
  and the language does the end-of-stream classification for you. A real
  failure (wrong protocol, transport) propagates via `?`.
- If you need the exact outcome, `receive()` returns the TYPED
  `CallerError` so you can `match` every case. A cursor only ever reads the
  in-RAM window, never the DB; if your cursor's offset has been trimmed out
  of the window you get `CallerError::FellBehind { oldest_resident }` (NOT a
  silent substitution): the cursor is MOVED to `oldest_resident` (the
  earliest message still retained), so your next read resumes there. One
  field, because the inbound log is dense, the resume point and the window
  floor are the same. (The bus's `FellBehind` carries a second `resumed_at`
  because its log is sparse and can resume past the floor; the caller never
  can.) The built-in forward cursor cannot hit this in normal use (it stays
  ahead of the window).

### Pattern: a WebSocket conversation

```rust
use weft_core::caller::{CallerHandle, InboundMessage, OutboundChunk};

async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    if !ctx.is_websocket() {
        return Err(WeftError::NodeExecution(
            "wire this under a LiveSocket trigger".into()));
    }
    let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
        return Err(WeftError::NodeExecution("no WebSocket caller".into()));
    };
    ws.ensure_connected().await?;
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
    ctx.pulse_downstream(NodeOutput::with("done", json!(true))).await
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
node returns a typed `InfraSpec` from `provision()`, the supervisor
compiles it to Kubernetes manifests and applies them, and the node
talks to the running pod(s) over HTTP at fire time.

You never write Kubernetes YAML by hand. You build an `InfraSpec` with
typed Rust structs (`Container`, `Endpoint`, `Volume`, ...); the
compiler turns it into Deployments, Services, PVCs, NetworkPolicies,
HPAs, and stamps all the `weft.dev/*` labels for you.

### Two methods: `provision` and `execute`

An infra node sets `requires_infra: true` in `metadata.json` and
implements two methods:

- **`provision(ctx, input) -> InfraSpec`**: returns the desired infra
  shape. Pure: it emits no pulses, it just describes what should run.
  Runs in `Phase::InfraSetup`, before `execute`. The `ctx`
  (`InfraProvisionContext`) carries `project_id`, `node_id`,
  `namespace`, `tenant_id`. The default impl returns an error, so only
  infra nodes override it.
- **`execute(ctx) -> NodeOutput`**: the node's actual logic, same as
  any node. By the time `execute` runs, the infra is applied and the
  node can resolve its endpoints.

The split is the rule: **provision describes infra, execute produces
pulses.** Provision can do async work (a registry lookup) but its job
is the spec.

```rust
async fn provision(&self, _ctx: InfraProvisionContext, _input: NodeInput)
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
// bridge node, in execute:
let api = ctx.endpoint("api").await?;
ctx.pulse_downstream(NodeOutput::with("apiUrl", Value::String(api.url().to_string()))).await

// send node, in execute: reads the wired URL, appends its own path
let base = ctx.input.get("apiUrl")?;       // the bridge's exported URL
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
| `/outputs` | GET | the declaring node's own `execute` | Return a flat JSON object; the node folds each key into an output port. The key set must match the node's declared `outputs` in metadata.json (a hand-maintained contract, not enforced). |
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
