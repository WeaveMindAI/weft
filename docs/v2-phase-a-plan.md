# v2 Phase A: Full Plan

Phase A ships the complete open-source v2 vision. When it lands,
the open-source side of weft matches the v2 design doc end to end.
No intermediate interpreter shapes, no "works for now, fix later"
shortcuts. `weft run` compiles the project to a native binary,
the dispatcher spawns it, the binary runs, snapshots when it stalls,
and exits. Waiting costs zero server time.

---

## 1. Locked decisions

**Compile model.** Each weft project compiles to one native Rust
binary. Only the nodes the project references are linked in
(cargo features on `weft-stdlib`). Codegen emits a cargo crate
under `.weft/target/build/`; `cargo build --release` produces the
binary. That binary IS the worker.

**Dispatcher.** Always-on daemon (local or cloud). Owns:
- HTTP API (run, webhook, form submit, SSE).
- Per-color slot state (Idle / Starting / Live) in RAM.
  Flagged for Phase B externalization.
- Journal (SQLite locally; Restate or equivalent later).
- Worker spawning (subprocess locally).
- Wake-signal handlers (webhook, timer, form, ...).

**Worker.** Short-lived. Runs until it completes, stalls, or fails.
Stall = nothing can make progress + at least one lane is waiting.
On stall, serializes `ExecutionSnapshot` and exits. Dispatcher wakes
a new worker with the snapshot + the incoming wake when a signal fires.

**Per-color serial access.** The dispatcher guarantees one worker
per color at a time. Wakes for a color that's Starting or Idle are
queued and delivered when the worker reaches Live (or within the
`Start` message).

**IPC: WebSocket.** Worker connects to dispatcher at startup. One
socket per worker. Messages both ways. Uniform for local subprocess
and future remote pods / E2B.

**Wake signals.** One unified concept (design doc 3.1):
- Dispatcher-owned closed set of kinds (Webhook, Timer, Form, Socket, ...).
- Parameters set per-instance by the node (path, schedule, form fields, ...).
- `is_resume: bool` selects fresh-run vs resume-paused-lane.
  - `false` = entry/trigger, persistent, every fire spawns a run.
  - `true` = wait/suspension, single-use, fire resumes the lane.
- Node API: `entry_signals` in metadata + `ctx.await_signal(spec)` for wait.

**Run semantics.**
- Manual run: upstream-of-`is_output` subgraph, all roots seeded,
  triggers in the subgraph pulse null.
- Trigger fire: upstream-of-outputs-reachable-from-firing-trigger;
  firing trigger pulses payload, others pulse null.
- Mock run: per-trigger mocks = N executions; per-node mocks replace
  outputs at dispatch time across all executions.
- Non-firing trigger null rule: downstream handles null explicitly
  (required port skips, optional accepts null).

**10 stdlib nodes for Phase A, all working flawlessly:**
Text, Debug, Output, ApiPost, Cron, HumanQuery, HumanTrigger,
HttpRequest, LlmConfig, LlmInference. Remaining catalog = Phase B.

**Not in Phase A:** Restate migration, multi-machine coordination,
Loom runner UI, `weft add` package ecosystem, `await_callback` /
`await_first` / timer-only suspensions (none of the 10 nodes need
them).

---

## 2. Target crate layout

- **`weft-core`**: pure types + pure exec functions. No tokio, no HTTP.
  Pulses, lanes, ProjectDefinition, Node trait, ExecutionContext,
  exec/*, WakeSignalKind/Spec, ExecutionSnapshot.
- **`weft-engine`** (new): the runtime. Loop driver + context impl +
  WebSocket client. One entry point: `run(project, catalog, wake, dispatcher_url)`.
- **`weft-stdlib`**: the 10 nodes + their signal usages. One cargo
  feature per node type (`node-text`, `node-api-post`, ...).
- **`weft-compiler`**: parse → enrich → validate → codegen → cargo
  invoke. Emits the project crate. Dynamically discovers user
  nodes from `nodes/` and vendor nodes from `vendor/`.
- **`weft-dispatcher`**: daemon. Per-color slot machine, WebSocket
  server, wake-signal handlers, journal, HTTP API.
- **`weft-cli`**: thin client. Calls `weft-compiler` directly for
  build, talks HTTP to dispatcher for everything else.
- **Emitted project crate** (per-project): `main.rs`, `project.rs`,
  `registry.rs`. Depends on `weft-engine`, `weft-core`, `weft-stdlib`
  (subset of features), user `nodes/`, vendor `vendor/`.

`weft-runner` crate (current generic interpreter) is DELETED.

---

## 3. Execution model (the part we got wrong before)

### Per-color slot

```
enum Slot {
    Idle {
        snapshot: Option<ExecutionSnapshot>,
        queued: VecDeque<Delivery>,
    },
    Starting {
        worker_handle: WorkerHandle,
        queued: VecDeque<Delivery>,
    },
    Live {
        socket: WebSocketHandle,
        suspensions: HashMap<Token, SuspensionInfo>,
    },
}
```

`Delivery` = either a fresh wake (initial run kick) or a
`Deliver { token, value }` (a wake signal fired, resume the lane).

All transitions under a per-color `tokio::Mutex`.

### WebSocket protocol

Dispatcher → worker:
- `Start { wake, snapshot: Option<Snapshot>, queued_deliveries }`: once,
  after Ready.
- `Deliver { token, value }`: a wake signal fired while worker is Live.
- `Cancel`.

Worker → dispatcher:
- `Ready` on connect.
- `NodeEvent`, `Log`, `Cost`: observability.
- `SuspensionRequest { request_id, spec }` → dispatcher registers the
  wake signal, replies `SuspensionToken { request_id, token, user_url }`.
- `Stalled { snapshot }` → dispatcher persists, acks, worker exits.
- `Completed { outputs }` / `Failed { error }` → terminal.

### Worker main loop (pseudocode)

```
connect_ws()
send(Ready)
Start { wake, snapshot, queued } = recv()
if snapshot is Some: restore(snapshot)
apply_wake(wake)
for d in queued: deliver(d)

loop {
    preprocess_input()
    ready = find_ready_nodes()
    if ready.empty() {
        if done() {
            send(Completed { outputs }). exit
        }
        if stalled() {
            send(Stalled { serialize() }). await ack. exit
        }
        // in-flight async futures (e.g. an HTTP call); park on them
        select! {
            msg = recv_ws() => handle(msg)
            _ = any_pending_future() => continue
        }
    }
    for (node, group) in ready: run_or_skip(node, group)
}
```

### `await_signal` inside the worker

```
async fn await_signal(&self, spec: WakeSignalSpec) -> Value {
    let (request_id, token_rx, value_rx) = register_pending(...);
    ws.send(SuspensionRequest { request_id, spec });
    let token = token_rx.await;   // dispatcher replied SuspensionToken
    // node can surface token_url to user (log, Slack, etc.)
    let value = value_rx.await;    // resolved by the ws loop when
                                   // Deliver { token, ... } arrives
    value
}
```

The main loop's `select!` routes `Deliver` messages to the right
`value_rx` by token.

### Dispatcher slot transitions

Incoming wake for color C:

```
lock(slot[C]);
match slot[C] {
    Idle { snapshot, queued } => {
        queued.push(wake);
        spawn_worker(C);
        slot[C] = Starting { handle, queued };
    }
    Starting { queued, .. } => {
        queued.push(wake);
    }
    Live { socket, .. } => {
        socket.send(Deliver { token, value });
    }
}
```

Worker Ready:

```
match slot[C] {
    Starting { queued, handle } => {
        socket.send(Start { wake: first(queued), snapshot, queued_deliveries: rest(queued) });
        slot[C] = Live { socket, suspensions: HashMap::new() };
    }
    ...
}
```

Worker Stalled:

```
journal.save_snapshot(C, snapshot);
socket.send(Ack);
slot[C] = Idle { snapshot: Some(snapshot), queued: VecDeque::new() };
```

### `ExecutionSnapshot`

```rust
pub struct ExecutionSnapshot {
    pub color: Color,
    pub pulses: PulseTable,
    pub executions: NodeExecutionTable,
    pub suspensions: HashMap<Token, SuspensionInfo>,
}
```

All three fields already exist or are trivially derivable from the
current v2 types. Just bundle + serde.

---

## 4. Wake-signal kinds for Phase A

Built into the dispatcher:

- **Webhook**: mint URL `/w/{token}/{path}`. POST body = payload.
  Used by ApiPost (entry, is_resume=false). Also usable as wait
  ("click this URL to continue").
- **Timer**: `After(Duration)` / `At(DateTime)` / `Cron(String)`.
  Tokio task; fires at deadline; `Cron` repeats until torn down.
  Used by Cron node (entry, Cron variant, is_resume=false).
- **Form**: webhook + schema + `form_type`. Used by HumanQuery
  (`form_type: "human-query"`, is_resume=true) and HumanTrigger
  (`form_type: "human-trigger"`, is_resume=false). Extension filters
  by `form_type` to render in the right panel.

Socket kind is reserved in the enum (Phase B: Discord gateway,
Telegram long-poll).

Adding a new kind = add variant + handler code in the dispatcher.
Framework-level change, not user-extensible.

---

## 5. What we keep / rewrite / delete

### Keep

- Parser, Enrich, Validate in `weft-compiler`.
- `weft-core`'s `exec/*` (readiness, preprocess, postprocess, skip,
  completion, typecheck).
- Pulse + Lane + NodeDefinition + ExecutionContext trait shape.
- VS Code extension scaffolding (graph view, execution panel).

### Rewrite

- `weft-compiler/src/codegen.rs`: full emission of the project crate.
  Emits `project.rs` as Rust literals (not embedded JSON).
- `weft-compiler/src/build.rs`: orchestrate pipeline + `cargo build`.
- `weft-cli/src/commands/{build,run}.rs`: wire to compiler + dispatcher.
- `weft-dispatcher/src/api/project.rs`: run handler returns dispatcher
  slot delivery instead of spawn-with-entry-node.
- `weft-dispatcher/src/backend/subprocess.rs`: spawn the project binary,
  not a generic runner.
- `weft-dispatcher/src/api/form.rs` + webhook: translate to Wake signal
  fires routed through slot machine.
- `weft-core/src/primitive.rs`: replace `EntryPrimitive` enum with
  `WakeSignalKind` + `WakeSignalSpec`. Delete `await_form`/`await_timer`/
  `await_callback` on `ContextHandle`, replace with `await_signal`.

### New

- `weft-engine` crate (moves + extends `weft-runner/src/{loop_driver,context}.rs`).
- Per-color slot machine on dispatcher.
- WebSocket server on dispatcher + client in engine.
- Wake-signal handlers (webhook, timer, form) in dispatcher.
- `ExecutionSnapshot` type in `weft-core`.
- Snapshot storage methods on the `Journal` trait.

### Delete

- `weft-runner` crate entirely (binary + lib).
- Old `EntryPrimitive` enum.
- Old `await_form`/`await_timer`/`await_callback` on ExecutionContext.
- `WakeKind::Fresh` single-entry path.
- Every mention of "runner as generic interpreter" in docs and code.

No deletion under `crates-v1/`, `catalog-v1/`, etc. Kept as reference.

---

## 6. v1 parity checklist

Semantics ported or confirmed ported. Unchecked = Slice 3 work.

- [x] Pulse structure (color, lane, port, value, status, gathered).
- [x] Lane matching, broadcast suppression, shape-mismatch detection.
- [x] Expand / Gather input preprocessing.
- [x] Gather output (sibling counting, emit on parent lane).
- [x] Broadcast rule for wired ports.
- [x] Required/wired/config-filled readiness.
- [x] oneOfRequired skip.
- [x] Null propagation on failure and skip.
- [ ] Group In-boundary skip cascade (partial; needs end-to-end test).
- [x] Group Out-boundary forwarding.
- [ ] Infra endpoint injection (`_endpointUrl` to infra nodes).
- [ ] Cost reporting via ExecutionContext → dispatcher journal.
- [ ] Log shipping via ExecutionContext → dispatcher SSE.
- [x] Trigger payload injection (via fresh wake seeding).
- [ ] Node-level mock (output replacement + sanitize).
- [ ] Group-level mock short-circuit.
- [ ] Mock file format + multi-run coordination.

---

## 7. Slice order

### Slice 0: clean house (1-2 hrs)

44yet (WakeSignal is defined but not yet used end to end).

### Slice 1: codegen (3-5 days)

- Feature flags on `weft-stdlib` (one per node type).
- `weft-compiler/src/codegen.rs`: emit `Cargo.toml`, `project.rs`
  (Rust literals), `registry.rs`, `main.rs`. Dynamic node discovery.
- `weft-compiler/src/build.rs`: parse → enrich → validate →
  codegen → `cargo build --release`. Return binary path.
- `weft-cli/src/commands/build.rs` wired.

Acceptance: `weft build hello/` produces a small binary
(`.weft/target/build/target/release/hello`). Manually invokable
with a test WakeSpec JSON; exits cleanly.

### Slice 2: dispatcher spawns project binary (1-2 days)

- Dispatcher `project/register`: runs `weft-compiler::build`, stores
  binary path.
- `SubprocessWorkerBackend` spawns the project binary.
- Run handler computes `WakeSpec::FreshMulti` from upstream-of-outputs,
  hands it to slot machine (which then spawns).
- Existing HTTP-based start/status/cancel adapted.

Acceptance: `weft run hello` runs through dispatcher → binary →
Output logs → binary exits. SSE sees started + completed.

### Slice 3: stall + snapshot + WebSocket + wake signals (5-7 days)

This is the big slice. It puts the model from §3 into code.

1. WebSocket server on dispatcher (axum WS), client on engine.
   `Ready` handshake. Bidirectional message types.
2. Per-color `Slot` state machine. In-RAM `DashMap<Color, Mutex<Slot>>`.
   Marked with comments flagging Phase B external lock.
3. Engine `await_signal`: sends `SuspensionRequest`, awaits token,
   parks on value_rx oneshot. Main loop `select!` routes `Deliver`.
4. Stall detection: all lanes terminal or WaitingForInput + no
   in-flight futures. Engine sends `Stalled { snapshot }`, awaits ack,
   exits.
5. Snapshot storage: `Journal::save_snapshot`, `load_snapshot`,
   `clear_snapshot`. SQLite impl.
6. Wake-signal handlers on dispatcher:
   - Webhook: URL mint, routing, payload delivery.
   - Timer: tokio task, deadline fire.
   - Form: webhook + schema + `form_type` stored for extension query.
7. Form endpoint (`POST /f/{token}`) translates to Wake signal fire
   through slot machine.
8. Resume path: dispatcher loads snapshot from journal, spawns worker,
   sends Start with snapshot + Deliver.

Acceptance: end-to-end test with HumanQuery. Manual run → HumanQuery
suspends → worker exits → `curl` form submit → new worker spawns,
loads snapshot, Deliver resolves lane → worker runs downstream →
completes. Second end-to-end: Cron entry signal fires at schedule
→ fresh run.

### Slice 4: trigger fire + multi-subgraph (2-3 days)

- Webhook entry signal fires → dispatcher resolves entry node →
  computes upstream-of-outputs-reachable-from-this-trigger →
  spawns worker with FreshMulti wake seeded accordingly.
- HumanTrigger ported as Form entry signal.
- End-to-end with two triggers: each fires, correct subgraph runs,
  null pulse on non-firing trigger.

### Slice 5: mocks (2-3 days)

- Mock file format: `{ triggers: {...}, nodes: {...} }`.
- Dispatcher mock parser.
- Per-trigger mock = N worker spawns, each with one trigger's mock
  as its seed, others null.
- Per-node mock = `overrides` map in WakeSpec, engine short-circuits
  `node.execute` when node_id has an override. Sanitization ported
  from v1.
- Group-level mock: overrides keyed on group id → engine skips
  inner nodes, Out-boundary emits mock.
- Session_id groups multi-run executions for extension UI.
- `weft run --mock path.json` CLI wiring.

### Slice 6: v1-parity remaining + extension polish (2-3 days)

- Group In-boundary skip cascade: end-to-end test + fix.
- Infra endpoint injection.
- Cost reporting.
- Log shipping.
- Extension: run button calls new endpoint shape, logs panel,
  execution detail panel, mock run menu, session grouping in history.

---

## 8. Definition of Phase A done

- [ ] `weft build hello` → native binary, small (only linked nodes).
- [ ] `weft run hello` → binary runs through dispatcher, completes,
  exits. Waiting costs zero worker time.
- [ ] `weft run multi_trigger --mock fixtures/happy.json` →
  N executions, one per trigger mock; extension groups them.
- [ ] HumanQuery: suspends, worker exits, form submit spawns resume
  worker, execution completes. No server was running during the wait.
- [ ] Cron entry signal fires every N seconds, fresh execution each time.
- [ ] Dispatcher restart (while no worker is live): pending snapshots
  persist, next wake resumes from snapshot.
- [ ] VS Code extension: live execution state, logs panel, execution
  detail, mock picker.
- [ ] v1 parity checklist: every box checked.
- [ ] `weft-runner` binary does not exist.
- [ ] No TODO/FIXME/"Phase A2"/stub comments in v2 tree.
- [ ] `v2-design.md` reflects Phase A final shape.

---

## 9. Risks (actual, not performative)

**Cargo build cost.** First build per project: 5-30s. Incremental
builds: sub-second. No fallback interpreter.

**WebSocket disconnect mid-run.** Worker detects socket drop → treat
as crash → dispatcher marks execution Failed. Phase B: worker could
retry-connect + re-handshake with dispatcher by color. Not Phase A.

**Snapshot size.** Large pulse tables in pathological graphs. No
compression Phase A. Revisit if we hit an issue.

**Port space for simultaneous workers locally.** WebSocket
connections are outbound from worker to dispatcher, so one port
(the dispatcher's) serves everyone. No port-per-worker problem.

---

## 10. Execution rules

- Slices run sequentially. Each slice's acceptance criteria must
  be green before the next begins.
- Between slices I pause and hand back to Quentin for review.
- Design questions mid-slice → stop, surface, resume after decision.
- No scope creep inside a slice. If I find additional work, log it
  and defer.
