# Slice 5: Listener service + three-runtime lifecycle

## Goal

Make triggers and suspensions fire through a generic listener
service that lives alongside project infrastructure. Bring back the
three v1 runtimes (infra setup, trigger setup, execution) cleanly,
without node-specific code leaking into the dispatcher.

## Non-goals in this slice

- Full infra backend (k8s cluster, sidecar provisioning). We need
  it eventually but we can land the listener architecture against a
  stub backend first and layer real k8s after.
- Node mocks. Slice 6.
- Extension UI changes beyond what the new URL shape forces.

## Architecture

### The listener

One binary, one image. Compiled from weft code, shipping only our
signal-kind implementations. Never loads user node code.

Inputs at start:
- Dispatcher URL (for relaying fires back).
- A per-listener auth token (shared with the dispatcher).
- Public base URL of this listener (for minting user-facing signal
  URLs).

HTTP surface:
- `POST /signal/{token}`: user-facing signal fire endpoint.
  Webhook/form/any externally-triggered kind land here. GET also
  accepted (for OAuth callbacks, GET-based triggers).
- `GET /signal/{token}`: kind-dependent read. Form returns schema;
  others return the minimal metadata they want to expose.
- `POST /register`: dispatcher-internal endpoint. Dispatcher calls
  this to tell the listener "add signal X with spec Y to your
  routing table."
- `POST /unregister`: dispatcher tears down a signal.
- `GET /signals`: lists everything registered (for debugging).

Internal state: `HashMap<token, RegisteredSignal>` where a
RegisteredSignal holds the kind, the spec, and any runtime handles
(tokio task for timers, connection handles for SSE/socket).

Kind implementations (one module per kind, inside the listener):
- `webhook`: no outbound loop. Token is just a routing entry.
- `form`: same as webhook plus exposes schema via GET.
- `timer::{cron,after,at}`: each adds a tokio task that sleeps to
  the next fire time, POSTs the dispatcher, loops (cron) or exits.
- `sse`: opens a subscription loop to the configured URL, filters
  events, POSTs matches.
- `socket`: long-lived WebSocket, same pattern.

### The dispatcher

Loses:
- `/w/{token}` and `/f/{token}` routes. Gone.
- `scheduler.rs`'s Cron-specific tokio tasks. Gone. Cron becomes a
  generic Timer signal registered with the listener.
- The per-kind URL branching in `ws.rs`'s SuspensionRequest
  handler. The listener mints URLs now.

Gains:
- `POST /signal-fired`: the listener posts here when ANY signal
  fires. Body: `{project_id, token, payload}`. Dispatcher looks up
  the token in its journal, decides entry vs suspension, spawns or
  resumes accordingly.
- `ListenerHandle` abstraction: per active project, the dispatcher
  holds a handle to its listener (URL + auth token). Used for
  register/unregister RPC.
- `ListenerBackend` trait: how the listener gets spawned. Two
  implementations: `InProcessListenerBackend` (runs the listener
  code in the same tokio runtime; for unit tests and trivial local
  dev) and `K8sListenerBackend` (later; spawns a pod in the
  project's namespace).

Still keeps:
- Slot state machine, worker lifecycle, journal, WebSocket link to
  workers. Unchanged.
- Token minting and suspension journaling. Unchanged.
- The WebSocket `SuspensionRequest` flow. Changed only so the URL
  comes from the listener, not hardcoded.

### The three runtimes

Activation of a project runs (in order):

1. **Infra runtime**: sub-execution whose target set is nodes that
   declare `requires_infra: true`. Those nodes produce sidecar
   specs / endpoint URLs as outputs. Sidecars get provisioned via
   an infra backend (later: k8s). Outputs are stored in a
   per-project "infra outputs" map in the journal.

2. **Trigger-setup runtime**: sub-execution whose target set is
   trigger nodes (defined as any node with non-empty
   `entry_signals`). Upstream of each trigger runs as usual;
   infra-output URLs are injected as inputs to the trigger-subgraph
   boundary nodes. Each trigger node's execute reads its inputs
   and produces outputs that, together with its declared kind,
   define a `WakeSignalSpec`. Dispatcher collects every spec.

3. **Listener provisioning**: dispatcher asks the `ListenerBackend`
   to spawn a listener for this project. Hands it every resolved
   `WakeSignalSpec`. Listener registers them internally, returns
   the user-facing URLs for externally-reachable kinds. Dispatcher
   journals the URLs for the extension + CLI to display.

4. **Project is now Active**: any fire arriving at the listener
   relays to dispatcher, dispatcher spawns a worker for the full
   execution runtime.

Deactivation reverses:
1. Listener torn down (kills all registered signals).
2. Infra sidecars torn down.
3. Project state flipped to Inactive.

### Fire path (post-activation)

Cron tick (example):
1. Listener's timer task for token T wakes up.
2. Listener POSTs `{dispatcher}/signal-fired { project_id, token: T, payload }`.
3. Dispatcher resolves T in the journal: it's an entry token for
   node N in project P.
4. Dispatcher runs the existing trigger-fire logic: compute
   firing-trigger subgraph, mint a color, journal execution start,
   spawn a worker with the seeds.
5. Worker runs, execution completes, worker exits.

Webhook fire (example): identical path, just the listener got the
event via inbound HTTP instead of internal timer.

Form submission / other resume: listener POSTs to dispatcher with
the token, dispatcher sees it's a suspension token, respawns the
worker if not live, passes the value as a queued delivery.

### Suspension registration (new flow)

1. Worker calls `ctx.await_signal(spec)`.
2. Engine sends `SuspensionRequest { spec }` to dispatcher over WS.
3. Dispatcher mints a token, journals the suspension, calls the
   listener's `/register` with `{token, spec}`.
4. Listener registers internally, returns `{user_url}` (or
   `user_url: null` for internal-only kinds).
5. Dispatcher sends `SuspensionToken { token, user_url }` to
   worker.
6. Worker returns `Err(Suspended)`, engine stalls.
7. When the signal fires, normal fire path runs; dispatcher
   respawns worker with the delivery seeded.

## Migration order

To avoid breaking everything at once, land this as sub-slices:

### 5.1 Listener crate, in-process backend, feature parity

- New crate `weft-listener` with the generic listener binary.
- `InProcessListenerBackend` runs the listener inside the
  dispatcher's tokio runtime (no separate process). This lets us
  test the architecture end-to-end without k8s.
- Port webhook + form + cron + the listener's timer/webhook/form
  handlers.
- Dispatcher routes `/w/*` and `/f/*` to the in-process listener
  instead of its own code.
- `scheduler.rs` deleted; Cron signals are registered with the
  listener at activate.
- `/signal-fired` endpoint on the dispatcher, used internally by
  the in-process listener.
- All existing tests still pass.

### 5.2 Signal URL consolidation

- Listener routes `/signal/{token}` for all externally-triggered
  kinds.
- Old `/w/*` and `/f/*` deleted from the listener (and from the
  dispatcher, where they were proxying through).
- Extension updated to use `/signal/{token}` for form submission.

### 5.3 Three-runtime lifecycle

- Add `requires_infra: true` handling at activate (produces infra
  outputs, stubbed provisioning via a `StubInfraBackend` that just
  invents URLs).
- Add trigger-setup sub-execution at activate. Trigger nodes gain
  a setup-time execute pass that reads inputs, emits outputs
  describing their signal.
- Dispatcher collects trigger specs, registers them with the
  listener.

### 5.4 Real infra backend

- `KindInfraBackend` wired up for local dev.
- Port WhatsAppBridge + WhatsAppReceive as the integration test.
- Listener pod provisioning via k8s backend (real separate pod).

## Decided

1. **Trigger setup-phase signature**: v1 style. `ctx.phase: Phase`
   enum (`Infra`, `TriggerSetup`, `Normal`). Same `execute`, branches
   on phase.

2. **Listener is a separate pod from day one.** No in-process
   backend.

3. **Slice 5 scope includes real infra.** Port `WhatsAppBridge`,
   `WhatsAppSend`, `WhatsAppReceive` as the end-to-end test. Live
   endpoint streaming (QR code) required.

4. **Fire-time subgraph rule**: walk upstream from output nodes,
   **treat trigger nodes as terminators**. A node runs at fire time
   iff it's reachable upstream from an output without passing
   through a trigger. Triggers themselves are seeded (firing one
   gets payload, others null).
   - If A only connects to TriggerX, A doesn't run at fire time.
   - If A connects to both TriggerX and B (where B → Output), A
     runs at fire time via the A → B path.

5. **Trigger-setup subgraph**: target set = trigger nodes, normal
   upstream closure (no terminators). Uses same helper.

6. **Activation order** blocks on infra readiness: "click Start"
   runs infra runtime. When infra healthy, Activate is allowed.
   Activate runs trigger-setup runtime, then listener registration.

## Open questions

1. **Infra output storage.** Journal (durable) vs in-memory. Leaning
   journal so dispatcher restart doesn't lose URLs.

2. **Listener authentication.** One shared token per project, minted
   at listener-spawn, stored in listener env + dispatcher state.

3. **Listener restart survival.** Re-push all active signals when
   listener comes up. Protocol: listener-ready → dispatcher pushes
   its signal list.

## Done criteria for this slice

- [ ] All webhook/form/cron fires go through the listener (even
      if in-process during 5.1).
- [ ] `/signal/{token}` is the sole externally-addressable URL
      for wake signals.
- [ ] Three-runtime activation (infra → trigger-setup → execution)
      works end-to-end against the WhatsApp pair.
- [ ] No node-kind-specific code in the dispatcher. All kind
      logic lives in the listener crate.
- [ ] Existing tests pass unchanged; new tests cover the listener
      path for each kind.
- [ ] Extension uses `/signal/{token}` for form submission.
- [ ] Nothing named "Mock" or "overrides" appears. Mocks are
      Slice 6.
