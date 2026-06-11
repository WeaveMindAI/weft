# Authoring Nodes

This guide covers patterns and gotchas when writing a node implementation
for Weft. Start with the existing nodes in `catalog/` for working
examples; this document explains the cross-cutting concerns that aren't
obvious from any single node.

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
| Subprocess needing graceful shutdown    | Yes, with delay     | `tokio::select!` + signal  |
| CPU-bound `spawn_blocking`              | Future returns, thread leaks | Pass flag, poll `is_cancelled()` |
| External resource needing cleanup       | Yes, with cleanup   | `tokio::select!` branch    |

**Bottom line**: write normal async Rust. Reach for `ctx.cancellation()`
only when you spawn a subprocess, run blocking CPU work, or hold a
resource that needs explicit cleanup before drop.

## Durable execution: `await_signal`, `register_signal`, `ctx.run`

Three primitives let a node body interact with the outside world in a
way that survives the worker dying and a fresh worker resuming
hours or days later.

### `ctx.register_signal(spec)`

For trigger nodes during `Phase::TriggerSetup`. Tells the listener to
watch for a wake signal (Webhook URL, cron schedule, SSE subscription,
form). Synchronous: returns the user-facing URL (if the kind mints
one) and the worker keeps executing. Each external fire later spawns
a fresh execution of the project. This signal is NOT bound to the
current execution's firing.

```rust
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    match ctx.phase {
        Phase::TriggerSetup => {
            let url = ctx.register_signal(WakeSignalSpec {
                kind: WakeSignalKind::Webhook { path: "".into(), auth: WebhookAuth::None },
                is_resume: false,
                consumer_kind: None,
                live_rendering: false,
            }).await?;
            // url is Some("https://dispatcher/.../signal/<token>") for webhook;
            // None for kinds that don't expose a URL (Timer, SSE).
            ctx.pulse_downstream(NodeOutput::with("url", json!(url))).await
        }
        Phase::Fire => {
            // Run when an external fire arrives.
            // ctx.wake_payload() returns the wake event's data (the
            // HTTP body for webhooks, the parsed SSE event JSON, the
            // timer info, etc.). Returns None for non-trigger nodes
            // and for trigger setup; trigger Fire bodies that REQUIRE
            // a payload should `ok_or_else` with a clear error.
            ...
        }
        _ => Ok(()),
    }
}
```

### `ctx.await_signal(spec)`

For mid-flow waits (HumanQuery and similar). Parks THIS firing until
the signal fires. Worker exits while parked; a fresh worker spawns
when the fire arrives. Other firings of the same execution at
different frame stacks keep going independently.

```rust
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let answer = ctx.await_signal(WakeSignalSpec {
        kind: WakeSignalKind::Form {
            form_type: "human-query".into(),
            schema: my_form_schema(),
            title: Some("Approve?".into()),
            description: None,
        },
        is_resume: true,
        consumer_kind: Some("human_in_the_loop".into()),
        live_rendering: false,
    }).await?;
    // After the user submits, `answer` is the form payload.
    ctx.pulse_downstream(NodeOutput::with("answer", answer)).await
}
```

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
| Trigger node declaring a persistent webhook/cron    | `ctx.register_signal`   |
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

Everything reachable from `InfraSpec` (in `weft-core/src/infra/types.rs`).
Maps are `BTreeMap` (not `HashMap`) on purpose: the compiled spec is
hashed for drift detection and HashMap iteration order would randomize
the hash.

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
