# Slice 5 progress (second session)

## What landed

### Part 1 (earlier this session, still true)

- Fire-time subgraph rule: triggers are terminators; only upstream
  nodes that reach outputs via a non-trigger path run at fire time.
  Five unit tests.
- `Phase` enum (`InfraSetup` / `TriggerSetup` / `Fire`) threaded
  through `ExecutionContext`.
- `weft-listener` crate: generic per-project listener service. 4
  integration tests.
- Dispatcher rewired around the listener: new `listener` module,
  `SubprocessListenerBackend`, `SignalTracker`, `/signal-fired`
  endpoint, SuspensionRequest goes via listener.
- Old `/w/{token}`, `/f/{token}`, `scheduler.rs` removed.

### Part 2 (this push)

**Node-side self-registration.** Nodes now register their own
entry signals during `Phase::TriggerSetup` via `ctx.register_signal(spec)`.
The dispatcher no longer iterates `node.entry_signals` at activate
time. Per your rule: the node owns its logic; the language exposes
the primitive.

- New `ctx.register_signal(spec)` primitive on `ExecutionContext`.
- New WS frames: `RegisterSignalRequest` / `RegisterSignalAck`.
- Engine-side `DispatcherLink::request_register_signal`.
- Dispatcher `ws.rs` handles the frame: registers with the
  project's listener, tracks in `SignalTracker`, replies with the
  user URL.
- `ApiPost`, `Cron`, `HumanTrigger` all rewritten to:
  - `Phase::TriggerSetup` → build WakeSignalSpec from config, call
    `ctx.register_signal`.
  - `Phase::Fire` → emit payload through output ports.

**Infra runtime.**

- `SidecarSpec` declarative type on `NodeMetadata` (mirrors v1's
  `InfrastructureSpec`). Carried through to `NodeDefinition` via
  enrich.
- New `weft_dispatcher::infra::InfraRegistry`: per-(project, node)
  handle tracking. On DispatcherState.
- `POST /projects/{id}/infra/up`: iterates every `requires_infra`
  node, calls `InfraBackend::provision` with its metadata spec,
  stores handle in registry.
- `POST /projects/{id}/infra/down`: deprovisions all.
- `InfraHandle` now carries `endpoint_url: Option<String>`.
  KindInfraBackend computes cluster-internal service DNS.
- `KindInfraBackend::provision` updated to apply declarative
  manifests from `SidecarSpec.manifests` with placeholder
  substitution (`__INSTANCE_ID__`, `__NAMESPACE__`, `__SIDECAR_IMAGE__`),
  matching v1's infra_helpers pattern.

**`ctx.sidecar_endpoint()` primitive.**

- New primitive on `ExecutionContext`. Node code asks the dispatcher
  for its sidecar endpoint URL. Never touches k8s directly.
- New WS frames: `SidecarEndpointRequest` / `SidecarEndpoint`.
- Dispatcher resolves via `InfraRegistry`, returns None if infra
  isn't provisioned (node surfaces a clear "run `weft infra up`"
  error).

**Activate rewritten.**

- Pre-flight: every `requires_infra: true` node must have an entry
  in `InfraRegistry`. Otherwise `412 PRECONDITION_REQUIRED` with
  the missing node list.
- Spawns listener.
- Runs the TriggerSetup sub-execution: `compute_trigger_setup_seeds`
  (target = trigger nodes, upstream closure with no terminators).
  Waits for `ExecutionCompleted` on the event bus. 30s timeout.
- Collects URLs from `signal_tracker` (populated by nodes'
  self-registration) and returns.
- No more dispatcher-side signal-kind branching. Zero node-specific
  code in activate.

**Signal tracker stores the URL too.** `RegisteredSignalMeta` got
`user_url` + `kind` fields so activate can surface URLs without a
second round-trip to the listener.

## Verified

End-to-end cron flow:
- `weft run --detach` registers a project with a Cron trigger.
- `POST /projects/{id}/activate` spawns a listener, runs TriggerSetup
  sub-exec (visible in journal: execution_started → node_started for
  Cron → register_signal happens over WS → execution_completed).
- Listener's cron task fires every 5s. Each fire POSTs `/signal-fired`.
  Dispatcher spawns a worker with Phase=Fire. Cron node emits
  `scheduledTime` + `actualTime` to downstream.

All 230+ tests still green. Zero warnings.

### Part 3 (this push, after your "keep going" message)

**Infra verbs.**

- `weft infra up` → `POST /projects/{id}/infra/up`: reads every
  `requires_infra: true` node's `sidecar` metadata, calls
  `InfraBackend::provision`, populates `InfraRegistry`. Idempotent:
  reuses handles if already provisioned.
- `weft infra down` → `POST /projects/{id}/infra/down`: deprovisions
  and clears the registry.
- Sidecar spec is 100% declarative in metadata JSON. Dispatcher
  never runs node code to provision.

**`ctx.sidecar_endpoint()` primitive.**

- Added to the Node-facing API. Node calls it during Fire phase to
  get its own sidecar's cluster-local URL. Zero k8s knowledge in
  node code.
- New WS frames: `SidecarEndpointRequest` / `SidecarEndpoint`.
- Dispatcher resolves via `InfraRegistry`, returns None if infra
  isn't provisioned.

**SSE wake signal kind.**

- New `WakeSignalKind::Sse { url, event_name }` + tag.
- Listener has a new `kinds/sse.rs` that opens a long-lived
  subscription, parses `data: ...` lines, relays matching events
  to the dispatcher.
- Port of v1's WhatsAppReceive keep_alive loop, generalized.

**WhatsApp package ported.**

- `catalog/whatsapp/{bridge,receive,send}/`.
- **Bridge**: `requires_infra: true`, declarative sidecar spec
  (PVC + Deployment + Service) with `__INSTANCE_ID__` /
  `__NAMESPACE__` / `__SIDECAR_IMAGE__` placeholders. Fire-phase
  execute queries `/outputs` via `ctx.sidecar_endpoint()`.
- **Receive**: TriggerSetup phase computes the SSE URL from the
  bridge's `endpointUrl` input and registers an Sse signal. Fire
  phase maps the WhatsApp event data to output ports.
- **Send**: POST `/action` with `sendMessage` payload. Pure
  Fire-phase.
- Package-level `package.toml` declares shared deps (reqwest,
  futures-util, chrono, etc.).

Hello project using all three WhatsApp nodes + Debug compiles and
links successfully. I haven't run it end-to-end against an actual
WhatsApp sidecar (that needs `dev.sh` to load the sidecar image
into kind + a real phone pairing), but the Rust side is sound.

### Part 4 (this push, still)

Kept the WhatsApp port deliberately minimal:
- No media download (requires `ctx.store_temp_media` primitive
  which doesn't exist in v2 yet).
- No WhatsAppSendMedia, no groups, no reactions, etc. Just
  Bridge/Receive/Send.

## K8s architecture decisions (just discussed)

**Everything runs in kind locally and real k8s in cloud. Same
logic both places.**

- **Dispatcher**: k8s Deployment. One instance (Phase B adds
  multi-instance).
- **Listener**: k8s Deployment per active project. Same namespace
  as sidecars.
- **Sidecars**: k8s Deployment per infra node. Same namespace.
- **Workers**: k8s Pod per execution. Same namespace.
- **Namespace convention**: `wm-{user_id}` (v1 convention). Local
  dev uses `wm-local`.
- **Internal comms**: cluster DNS only. No port-forward hacks
  anywhere.
- **External edges**:
  - CLI + Extension → Dispatcher: ingress.
  - External users → Listener (webhook/form URLs): ingress.
- **Worker binary distribution**: build produces an image (like
  sidecars do today via `dev.sh`), `kind load docker-image` locally,
  registry push for cloud. One code path.

### Part 5 (this push): full kind-native deployment

**All ten k8s tasks landed and verified end-to-end.** Cron now
fires from inside kind, through a real listener pod, into a real
worker pod, every five seconds. No more SubprocessListenerBackend
hack; no more `weft-dispatcher` binary on the host.

What's new:

- `deploy/docker/dispatcher.Dockerfile`, `deploy/docker/listener.Dockerfile`: multi-stage Rust builds against `rust:1.85-bookworm`, runtime on `debian:bookworm-slim`. Dispatcher image bakes `/catalog` so it doesn't depend on `CARGO_MANIFEST_DIR` paths.
- `deploy/k8s/namespace.yaml`, `dispatcher.yaml`, `ingress.yaml`: namespace `wm-local`, ServiceAccount + Role + RoleBinding (pods, services, deployments, replicasets, configmaps, pvcs, jobs, ingresses), PVC for journal, dispatcher Deployment + Service, Ingress rules for the dispatcher and a wildcard `*.listener.weft.local` for project listeners.
- `weft daemon start`: creates the kind cluster if missing (with port 80/443 mapped for ingress), installs nginx-ingress, applies the manifests, builds+loads dispatcher/listener images, port-forwards `svc/weft-dispatcher` to `127.0.0.1:9999`. `weft daemon stop` tears the port-forward and scales the dispatcher to zero while preserving the PVC.
- `weft build`: now produces `weft-worker-{project-id}:latest` alongside the binary and `kind load`s it. Dockerfile template lives at `.weft/target/Dockerfile.worker`.
- `crates/weft-cli/src/images.rs`: central image build + `kind load` helpers. Replaces `dev.sh`; no shell scripts touched.
- `listener.rs`: new `K8sListenerBackend`. Applies Deployment+Service+Ingress per project via kubectl, resolves internal admin URL over cluster DNS, external URL over ingress hostname. Cleanup on `stop()` deletes the three.
- `backend/k8s_worker.rs`: new `K8sWorkerBackend`. Spawns a one-shot Pod per execution using `weft-worker-{project-id}:latest`, container args connect it to the dispatcher via cluster DNS.
- `main.rs`: both the listener backend and worker backend default to the k8s variants (k8s is now the default, subprocess is opt-in via `WEFT_LISTENER_BACKEND=subprocess` / `WEFT_WORKER_BACKEND=subprocess`).
- `config.rs`: `WEFT_DATA_DIR` env var is honored so the PVC mount at `/var/lib/weft` actually carries the journal + project store.
- `api/project.rs`: `activate` now hands the listener a cluster-DNS dispatcher URL (`http://weft-dispatcher.{ns}.svc.cluster.local:9999`) when `WEFT_NAMESPACE` is set, rather than 127.0.0.1.
- `weft-catalog/src/lib.rs`: `stdlib_root` now honors `WEFT_CATALOG_ROOT` so the dispatcher image's baked catalog works without any cargo-compile-time path knowledge.
- Dispatcher image includes `kubectl` so the in-cluster ServiceAccount's kubeconfig drives KindInfraBackend / K8sListenerBackend / K8sWorkerBackend.

Smoke test that ran cleanly:
1. `weft daemon start` (cluster up, images built, dispatcher rolling).
2. `weft run --detach` (project registered, one-shot worker Pod ran, completed in ~8s with Debug output "ticked").
3. `POST /projects/{id}/activate` (listener Pod spawned, trigger-setup subgraph ran, cron signal registered).
4. Listener fires every 5s → dispatcher spawns a worker Pod → Pod completes, prints the timestamp, terminates. Five fires observed in 25 seconds.

## Still open (carried from the slice plan)

### A. Full kind-native deployment (large)

Architecture decided (see above). What needs to be built:

1. **Dockerfile for weft-dispatcher.**
2. **Dockerfile for weft-listener.**
3. **Per-project worker image build pipeline.** Extend `weft build`
   to produce an OCI image alongside the binary. Load into kind
   via `kind load docker-image` locally; push to registry for cloud.
4. **K8s manifests** (Helm chart or raw YAML) for dispatcher +
   service + ingress.
5. **K8sListenerBackend**: replaces `SubprocessListenerBackend`.
   Applies a Deployment+Service per project via `kubectl apply` (same
   pattern as KindInfraBackend).
6. **K8sWorkerBackend**: replaces `SubprocessWorkerBackend`. Each
   execution spawns a one-shot Pod or Job that runs the project's
   worker image.
7. **Ingress controller**: nginx-ingress installed into kind on
   first `weft daemon start`. Second ingress (or subdomain-based
   routing) for listener URLs.
8. **`weft daemon start` rewrite**: applies dispatcher manifest,
   waits for ready, sets up local port-forward or ingress to
   expose the HTTP port to CLI.
9. **Image build pipeline in the CLI.** `dev.sh` is v1 scaffolding;
   the CLI is the only user surface. Image build + `kind load`
   moves into `weft` itself:
     - `weft daemon start` builds the dispatcher image if missing
       and loads it into kind.
     - `weft daemon start` also ensures the listener image is
       loaded (one-time; the listener binary is stable).
     - `weft build` additionally produces the per-project worker
       image and loads it into kind.
     - `weft infra up` ensures each sidecar image is built and
       loaded (same pattern as v1's dev.sh, moved into Rust).
   For cloud deployment the same code paths push to a registry
   instead of calling `kind load`. `INFRASTRUCTURE_TARGET=local`
   env var or similar switches the two.
10. **CLI config**: how it learns the dispatcher URL. Probably
    `~/.config/weft/dispatcher-url` written once by daemon start.

This is 10+ hours of careful work. I stopped before starting
because the shape (Dockerfile base image for a Rust binary with
tokio + axum, manifest conventions, ingress choice) deserves your
input before committing to a direction. Doing it autonomously
risks going down a path you'd rework.

### B. WhatsApp port

Port `WhatsAppBridge` / `WhatsAppReceive` / `WhatsAppSend` as the
three-runtime acceptance test. Depends on A because WhatsApp is
where the real listener pod matters (SSE to bridge).

### C. Durable state (postgres)

`SignalTracker`, `ListenerRegistry`, `InfraRegistry` are all
in-memory. Lost on dispatcher restart. Per your earlier comment:
this is handled by the planned postgres migration, not urgent now.

### D. Subprocess cleanup on SIGKILL

When the dispatcher is killed without graceful shutdown, listener
subprocesses don't die via `kill_on_drop` reliably. Low-priority
(local dev annoyance); fixed properly by moving to k8s listener
pods in A.

## Files changed since last commit (6b06579)

New files (not yet tracked):
- `crates/weft-dispatcher/src/api/signal.rs`
- `crates/weft-dispatcher/src/api/infra.rs`
- `crates/weft-dispatcher/src/listener.rs`
- `crates/weft-dispatcher/src/infra.rs`
- `crates/weft-listener/` (entire crate)
- `docs/v2-slice-5-plan.md`
- `docs/v2-slice-5-progress.md` (this file)

Deleted:
- `crates/weft-dispatcher/src/api/form.rs`
- `crates/weft-dispatcher/src/api/webhook.rs`
- `crates/weft-dispatcher/src/scheduler.rs`

Modified (mostly):
- `crates/weft-core/src/{context,primitive,node,project}.rs`
- `crates/weft-engine/src/{context,dispatcher_link,loop_driver}.rs`
- `crates/weft-compiler/src/{enrich,weft_compiler}.rs`
- `crates/weft-dispatcher/src/{state,main,api/{mod,project,ws}}.rs`
- `crates/weft-dispatcher/src/backend/{mod,kind_infra}.rs`
- `catalog/triggers/api/post/mod.rs`
- `catalog/triggers/cron/mod.rs`
- `catalog/human/trigger/mod.rs`
- `crates/weft-dispatcher/tests/webhook_flow.rs`

## Overall Slice 5 status

Semantic model ~ 100% done. Infrastructure to run it at k8s
parity ~ 40% done (subprocess listener is the temporary gap).

My read: this is a good handoff point. The node-level contract
is right (no plumbing, phase-aware execute, ctx primitives).
Next session can focus entirely on A (K8sListenerBackend) and
B (WhatsApp port) with the semantic foundation already in place.
