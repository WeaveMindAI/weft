# Authoring Nodes

This guide covers patterns and gotchas when writing a node implementation
for Weft. Start with the existing nodes in `crates/weft-nodes` for
working examples; this document explains the cross-cutting concerns
that aren't obvious from any single node.

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
async fn execute(ctx: ExecutionContext, ...) -> WeftResult<NodeOutput> {
    let resp = reqwest::Client::new()
        .post("https://api.anthropic.com/v1/messages")
        .json(&body)
        .send()
        .await?
        .json::<ApiResponse>()
        .await?;
    Ok(NodeOutput::single("response", resp.text))
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
async fn execute(ctx: ExecutionContext, image: Vec<u8>) -> WeftResult<NodeOutput> {
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
    Ok(NodeOutput::single("out", result))
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
async fn execute(ctx: ExecutionContext, ...) -> WeftResult<NodeOutput> {
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
    Ok(NodeOutput::single("done", json!(null)))
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
current execution's lane.

```rust
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
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
            Ok(NodeOutput::with_value("url", json!(url)))
        }
        Phase::Fire => {
            // Run when an external fire arrives.
            // ctx.input["__seed__"] carries the fire payload.
            ...
        }
        _ => Ok(NodeOutput::empty()),
    }
}
```

### `ctx.await_signal(spec)`

For mid-flow waits (HumanQuery and similar). Stops THIS lane until
the signal fires. Worker exits while parked; a fresh worker spawns
when the fire arrives.

```rust
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
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
    Ok(NodeOutput::with_value("answer", answer))
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
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
    let approval = ctx.await_signal(approval_spec()).await?;
    if approval["accepted"].as_bool() != Some(true) {
        return Ok(NodeOutput::with_value("decision", json!("rejected")));
    }
    let confirmation = ctx.await_signal(confirmation_spec()).await?;
    Ok(NodeOutput::with_value("final", confirmation))
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
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
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

    Ok(NodeOutput::with_value("receipt", api_resp))
}
```

The closure runs at most once across the lifetime of this (color,
node, lane) execution. On every subsequent replay, the journaled
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

A worker pod dies whenever every lane is parked on `await_signal`.
This is the multiplexing model: thousands of suspended HumanQuery
flows cost no compute, just journal rows. When a fire arrives, a
fresh worker pod spawns, folds the journal, and re-runs every node
that has a fire to deliver. The body re-runs from the top; each
prior `await_signal` and `ctx.run` returns instantly from the
journal.

If your node holds in-process state that can't be replayed cheaply
(a browser session with thousands of cookies, a long-lived ML model
load), the durable-replay model works against you. A future
`ctx.hold_signal` primitive (see `docs/v2-held-suspensions.md`) will
let you opt into a warm-worker model where the future actually
awaits and the worker pod stays alive. Not yet implemented.

## Infra nodes: declaring a sidecar

Some nodes need a long-running process the user can't easily run
themselves: a WhatsApp bridge daemon, a headless browser session, a
local LLM server. Weft calls those *infra nodes*. The pattern: the
node declares Kubernetes manifests in its `metadata.json`, the
dispatcher applies them during `weft infra start`, and the node
talks to the running pod over HTTP at fire time.

### The lifecycle

`weft infra start` spawns a worker in `Phase::InfraSetup`. Only
nodes with `requires_infra: true` and their upstream dependencies
run. Each infra node's body calls `ctx.provision_sidecar(spec)`;
the dispatcher applies the manifests through `kubectl apply` and
returns a handle with the pod's ClusterIP service URL.

In `Phase::Fire` and `Phase::TriggerSetup`, the same node calls
`ctx.sidecar_endpoint()` to fetch the URL and makes HTTP requests
to the running sidecar. The pod is shared across every execution;
the node never spawns it itself.

`weft infra stop` scales the Deployment to 0 (keeps state on PVC,
keeps the Service so the URL stays stable). `weft infra terminate`
sweeps everything by the `weft.dev/instance=<id>` label the
dispatcher injected at apply time. `weft infra upgrade` is a
scale-to-zero plus a fresh provision with a new image hash.

### Node code

```rust
async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
    match ctx.phase {
        Phase::InfraSetup => {
            let meta: NodeMetadata = serde_json::from_str(METADATA_JSON)?;
            let spec = meta.features.sidecar.ok_or_else(|| {
                WeftError::Config("missing sidecar spec".into())
            })?;
            let handle = ctx.provision_sidecar(spec).await?;
            Ok(NodeOutput::empty()
                .set("endpointUrl", Value::String(handle.endpoint_url)))
        }
        Phase::Fire | Phase::TriggerSetup => {
            let url = ctx.sidecar_endpoint().await?;
            let resp = reqwest::Client::new()
                .get(format!("{url}/outputs"))
                .send().await?;
            // ... forward fields to output ports
        }
    }
}
```

### The manifest contract

You write `metadata.json` by hand. `features.sidecar.manifests` is a
list of raw Kubernetes documents. At minimum you need a `Deployment`
running your container and a `Service` exposing the port. You can
add anything else (`PersistentVolumeClaim`, `ConfigMap`, `Secret`,
`Ingress`, ...) and the dispatcher will apply each one.

The dispatcher substitutes three placeholders in every document
before `kubectl apply`:

| Placeholder         | Replaced with                                            |
| ------------------- | -------------------------------------------------------- |
| `__INSTANCE_ID__`   | Per-instance pod name (used as Deployment/Service name). |
| `__NAMESPACE__`     | Tenant namespace (e.g. `wm-tenant-foo`).                 |
| `__SIDECAR_IMAGE__` | Hash-tagged image `weft-sidecar-{name}:{hash}`.          |

The dispatcher also injects four labels into every document's
`metadata.labels`:

```
weft.dev/role: infra
weft.dev/project: <project_id>
weft.dev/node: <infra_node_id>
weft.dev/instance: <instance_id>
```

The `weft.dev/instance` label is what `weft infra terminate` uses
to sweep every resource owned by one provision. Anything you ship
in `manifests` gets that label, so a custom PVC or ConfigMap is
cleaned up alongside the Deployment.

### Minimal example

A stateless HTTP sidecar needs three documents in `manifests`:

```json
[
  {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": { "name": "__INSTANCE_ID__" },
    "spec": {
      "replicas": 1,
      "selector": { "matchLabels": { "app": "__INSTANCE_ID__" } },
      "template": {
        "metadata": { "labels": { "app": "__INSTANCE_ID__" } },
        "spec": {
          "containers": [{
            "name": "sidecar",
            "image": "__SIDECAR_IMAGE__",
            "ports": [{ "containerPort": 8080 }]
          }]
        }
      }
    }
  },
  {
    "apiVersion": "v1",
    "kind": "Service",
    "metadata": { "name": "__INSTANCE_ID__" },
    "spec": {
      "selector": { "app": "__INSTANCE_ID__" },
      "ports": [{ "port": 8080, "targetPort": 8080 }]
    }
  }
]
```

That's enough to provision and reach the pod from your node. See
`catalog/whatsapp/bridge/metadata.json` for a full example with
persistence and a readiness probe.

### Security and resources: your call

The dispatcher applies whatever you ship verbatim. **No security
context, resource limits, or capability drops are added on your
behalf.** A sidecar that runs as root with no CPU limit will start
without complaint.

Cross-tenant isolation comes from the namespace boundary and the
NetworkPolicies the dispatcher applies once per tenant; nothing
about your pod's internal posture affects other tenants. But a
runaway sidecar can starve the kubernetes node it lands on, and a
compromised sidecar that runs as root can do more damage to itself
than a hardened one.

If you want to be a good citizen, add the following to the
container spec:

```json
"resources": {
  "requests": { "cpu": "100m", "memory": "128Mi" },
  "limits":   { "cpu": "500m", "memory": "512Mi" }
},
"securityContext": {
  "allowPrivilegeEscalation": false,
  "readOnlyRootFilesystem": true,
  "capabilities": { "drop": ["ALL"] }
}
```

And at the pod level (inside `spec.template.spec`):

```json
"securityContext": {
  "runAsNonRoot": true,
  "runAsUser": 65532,
  "runAsGroup": 65532,
  "fsGroup": 65532,
  "seccompProfile": { "type": "RuntimeDefault" }
}
```

These satisfy the Kubernetes Pod Security `restricted` baseline.
Your image has to cooperate: it must run as UID 65532 with `/`
mounted read-only. Most Go and Rust images do this trivially; many
Python images need a writable `/tmp` mounted via `emptyDir`. If
your image can't comply, skip the hardening; within your own
namespace you can do whatever you want.

### Network policy

Each tenant namespace gets a set of NetworkPolicies applied at
provisioning time (see `tenant_namespace.rs`):

- **default-deny**: drops every pod's ingress and egress by default.
- **worker-policy**: workers can reach the broker, sidecars in their
  own namespace, and the internet. They cannot reach pods in other
  tenant namespaces.
- **listener-policy**: listeners can reach the broker and accept
  HTTP from the dispatcher.
- **sidecar-policy**: sidecars accept ingress from workers in the
  same namespace. They can call out to the internet.

You don't need to add anything; the labels the dispatcher injects
(`weft.dev/role: infra`) are the selectors the policies match. If
your sidecar needs to talk to another pod inside the same
namespace, that traffic is allowed by default; cross-namespace is
blocked.

### Image building

Sidecar images are built by the CLI from a `Dockerfile` in the
node's directory. The CLI hashes the build context, tags the
image as `weft-sidecar-{name}:{shorthash}`, and writes the hash
into the `/infra/start` request body. The dispatcher uses that
hash to construct `__SIDECAR_IMAGE__`. Drift detection compares
running vs desired hash to light up the "Upgrade infra"
affordance.

If you change the sidecar source, the hash changes and `weft infra
upgrade` is the path to apply the new image (scales the old pod
to zero, provisions a new one, returns the new handle).
