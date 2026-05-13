# Held suspensions (Epic 2): warm-worker await primitive

Status: design, not implemented.
Companion to: Epic 1 (multi-await in deterministic mode), shipped separately.

## Problem

Today every `await_signal` follows the deterministic-replay model:

1. Node body parks on `await_signal`. Worker writes `SuspensionRegistered` to the journal and exits.
2. Fire arrives later. Dispatcher writes `SuspensionResolved`, spawns a fresh worker.
3. Worker folds the journal, re-enters the node body from the top, every prior `await_signal` returns its journaled value instantly (Restate-style replay).

Epic 1 generalizes this so a body can have multiple `await_signal` calls in sequence, each replaying in order. Variables between awaits get re-derived on every replay. Non-determinism between awaits is handled by wrapping the work in `ctx.run("name", || ...)` which journals its output (also Restate-style).

This works for orchestration-shaped flows: a HITL approval chain, a webhook → process → confirm pattern, anything where the node body's logic is deterministic or instrumentable.

It breaks for **agents with rich in-process state**. A browser-use agent that:
- holds a `Browser` handle with cookies, DOM caches, viewport state,
- accumulates `Vec<Action>` of what it has done,
- reads pixel data and decides next action based on it,

cannot run in this model without ctx.run-wrapping every observable step (pixel reads, DOM queries, action decisions). The wrapping is invasive and easy to get wrong: forget one and the replay desyncs silently.

The author's intuition is "I want the body to actually pause, with its variables intact, and resume when the fire arrives." Today there is no way to express that.

## What we considered and rejected

**Restate-style ctx.run blanket coverage** (Epic 1's solution generalized to arbitrary non-determinism). Author has to wrap every non-deterministic op. For an agent with dozens of decision points per loop iteration, this is unergonomic and error-prone. Rejected as the only mode.

**WASM-based snapshots**. Compile node code to WebAssembly, snapshot the WASM store, restore on resume. Node author writes plain async Rust, the runtime handles snapshot/restore generically. This is what Lunatic, Cloudflare Durable Objects, and similar frameworks do. Rejected because:
- Compile-pipeline complexity (every node compiles native + WASM).
- WASI bridges for sidecar calls, journal writes, dispatcher comms are non-trivial.
- Perf overhead (1.5x to 3x depending on workload, mostly negligible for I/O-bound, but feels wrong for agents that do actual CPU work).
- Versioning headache: snapshot taken on engine vN can't restore on vN+1 cleanly.

**CRIU (Checkpoint/Restore In Userspace)**. Linux-level process snapshotting. Promising in theory: dump the whole process state on suspend, restore on resume, the futures resume at exactly where they were. Rejected because:
- Open file descriptors (Postgres pool, dispatcher websocket) don't migrate cleanly across pods. Workarounds exist (drop FDs before snapshot, reopen on restore) but they're invasive.
- Tokio runtime time anchoring: a future blocked on `tokio::time::sleep(1h)` thinks 0 seconds passed after restore. Real correctness issue.
- Snapshot size: full process memory per snapshot. For executions with fat in-process state (browser session) this is hundreds of MB. Per execution. Doesn't scale.
- Versioning: snapshot ties to exact binary version. Hard to combine with deploys.
- Operational complexity in K8s: CRIU + runc checkpoint exists (Kata, Podman, runc) but is non-mainstream tooling.

**Coroutine / generator-based serializable futures**. Custom executor where suspension points are explicit yields with serializable state types. Author writes generator-style code instead of `async fn`. Rejected because it forces every node to be written in a foreign style, and the existing async ecosystem (sqlx, reqwest, tokio) doesn't compose.

## The design we chose: two-mode split

Two `ctx` primitives, author picks per-call:

- **`ctx.await_signal(spec)`**: durable parking await. The Epic 1 model. Worker dies on stall. Resume re-enters the body and replays. Cheap suspension (just journal entries), cheap resume in absolute terms but body re-runs (with cached awaits + ctx.run journaled).
- **`ctx.hold_signal(spec)`**: warm-worker holding await. Worker stays alive. Future literally awaits. Fire arrives → dispatcher routes the value into the live worker over a side channel → future resolves → body continues with all locals intact.

The semantic difference is honest:
- `await_signal`: "I'm OK pausing, see you when fired. The body must be replay-safe."
- `hold_signal`: "Stay warm and deliver directly to me. State is in-process; if the worker dies the execution dies too."

The tradeoff is honest:
- `await_signal`: cheap to park (any number of long-pending suspensions cost ~bytes each), expensive replay-cost-paid-once.
- `hold_signal`: pod stays warm (compute cost), no replay cost.

Author makes the cost-vs-ergonomics call per call site.

## Why two modes instead of unifying

We considered "Type 2 falls back to Type 1 if the worker dies." Decided against:
- Mixed-mode bodies are hard to reason about: which awaits replay, which fail?
- Author's mental model gets muddied: "is my node deterministic?" becomes a runtime property, not a static one.
- Failure modes are clearer when split: `hold_signal` is "best-effort durability; if the pod dies, the execution dies." `await_signal` is "fully durable; survives any pod death."

Strict split, opted into per call. Author owns the choice.

## Architecture

### Setup (unchanged)

Both `await_signal` and `hold_signal` go through the same setup chain:

```
ctx.{await,hold}_signal(spec)
  → enqueue_register_signal_task
  → dispatcher claims task
  → POST /register on listener (listener stores spec in registry)
  → POST /render on listener (cache consumer payload)
  → INSERT signal row { token, color, is_resume, parkable, hold, ... }
  → return { token, user_url } to worker
```

A new column `hold: BOOLEAN` denormalizes `spec.hold` onto the signal row so the dispatcher can branch at fire time without parsing spec_json.

### Fire-time branching

`dispatch_listener_outcome` (the shared post-park-gate processor) gains a sub-branch when the listener returns `ProcessTarget::Resume`:

```
Resume { color, ... }
  → SELECT hold FROM signal WHERE token = $1
  → if hold = false:  (Type 1)
       journal SuspensionResolved + ensure_worker  ← today's path
  → if hold = true:   (Type 2)
       look up live worker pod for this color
       if found: route value over the live channel; the worker's
                 oneshot resolves; body continues
       if not found: journal ExecutionFailed("held worker died");
                     execution terminates
```

The Type 2 branch is the new logic. Type 1 stays unchanged.

### Live delivery channel

The worker needs to receive deliveries mid-execution, not just at boot. Two implementation options:

**Option A: extend task polling.** Worker already polls the `task` table for new claims. Add a `signal_delivery` task kind: when the dispatcher needs to deliver to a live worker, INSERT into task table; worker's poll picks it up; worker matches token against in-memory `held_suspensions: HashMap<token, oneshot::Sender<Value>>`; sends; the future resolves. Pros: no new infra. Cons: poll latency (currently a few hundred ms per cycle).

**Option B: dedicated websocket.** Worker opens a WS to dispatcher at boot. Dispatcher pushes `signal_delivery` events directly. Pros: low latency. Cons: new connection management, lease handling, retry logic.

**Recommendation: A first.** Latency of ~500ms per delivery is fine for HITL flows. Browser agents that need sub-100ms delivery can motivate Option B later. Don't over-engineer.

### Worker-side state

Each worker maintains a `held_suspensions: Arc<DashMap<String, oneshot::Sender<Value>>>` keyed by token. `ctx.hold_signal(spec)`:

1. Calls `enqueue_register_signal_task` (same as await_signal but with `hold = true`).
2. Creates a oneshot channel; stores sender under the returned token.
3. Returns the receiver future.

The body `await`s the receiver. When delivery arrives via the live channel:

```
worker receives signal_delivery task { token, value }
  → held_suspensions.remove(&token).unwrap().send(value)
  → the body's await resolves with value
```

On worker shutdown (cancel, eviction, dispatcher decision), all senders get dropped, all receivers see "channel closed," all `hold_signal` futures return Err. The execution surfaces as Failed.

### Reaper interactions

The worker-pod reaper currently kills idle workers (no in-flight tasks, no claimed work). With Type 2 holds, "idle" gets a richer definition:

- Pod is idle iff `held_suspensions.is_empty()` AND no in-flight tasks.
- Reaper queries the pod's `held_suspensions` count via a status endpoint OR via DB-side denormalization (e.g., a `held_suspension_count` column on `worker_pod`).

Either works; DB-side denormalization is simpler (no extra HTTP).

### Cancellation interaction

If the user cancels an execution that has held suspensions, the dispatcher:

1. Sends cancel task to the worker (existing path).
2. Worker drops all `held_suspensions` senders.
3. Each held `hold_signal` future returns "cancelled."
4. Body propagates the error up; node fails; execution journals NodeCancelled + ExecutionFailed.

No new cancellation logic. Cancellation already kills futures via the existing CancellationFlag plumbing.

### Worker lifetime

A pod with at least one held suspension cannot be evicted by the reaper. Per-tenant max-warm-pods limits (cloud) become a real concern at scale. For OSS / kind, no limit.

In cloud, a future quota system would let users see "you have N warm pods costing $X/hour" and make the cost visible.

## Failure modes

**Pod dies while holding** (k8s eviction, OOM, kernel panic).
- All held senders drop.
- Held futures return Err.
- Worker dies before journaling; the cancel/fail flow doesn't run cleanly.
- Worker reaper detects stale pod, marks dead, executions for that color get marked failed.

**Dispatcher pod dies while routing.**
- Live delivery in flight gets re-claimed by another dispatcher pod.
- The signal_delivery task survives in the task table.
- New dispatcher routes; worker receives.
- No data loss; brief latency.

**Network partition between dispatcher and worker.**
- Worker can't be reached. Live delivery times out.
- Dispatcher journals SuspensionFailed (new variant) or ExecutionFailed.
- Author sees "held worker unreachable."

These match the documented contract: Type 2 = best-effort durability.

## Schema changes

```sql
ALTER TABLE signal ADD COLUMN IF NOT EXISTS hold BOOLEAN NOT NULL DEFAULT FALSE;
```

Plus the existing `parkable` column from Epic 1 stays; `parkable` and `hold` are independent (a held signal could in principle still be parkable, but in practice held = stay-alive and parking is meaningless; we'd derive `parkable = !hold && !live_rendering`).

## Wire-protocol changes

`WakeSignalSpec` gets a new field:

```rust
pub struct WakeSignalSpec {
    // existing fields...
    
    /// True iff the worker should keep the future warm in-process
    /// rather than journaling-and-replaying. Author opt-in for
    /// non-deterministic node bodies (browser agents, etc).
    /// Implies the worker pod stays alive across the suspension;
    /// if the pod dies, the execution fails.
    #[serde(default, rename = "hold", alias = "hold")]
    pub hold: bool,
}
```

ctx primitive:

```rust
async fn hold_signal(&self, spec: WakeSignalSpec) -> WeftResult<Value>;
```

Same return type as `await_signal`. The runtime distinguishes by checking `spec.hold` (or having two primitives at the trait level). Two primitives is more honest naming.

## Open questions

- **Multiplexing under Type 2:** if a worker holds suspensions for executions A, B, C, and A's body finishes, can the worker still die? Only if B and C are also done. So multiplexing reduces total warm pods at scale, but the per-execution latency is unchanged. Worth keeping multiplexing in this model.
- **Mixing `await_signal` and `hold_signal` in one body.** Should be allowed: a HITL approval (await) followed by a real-time agent loop (hold). Implementation just dispatches each call to its own path.
- **Hold + park interaction.** A held signal cannot be parked (the worker is alive, holding the future, the dispatcher has nowhere to park to that the worker would notice). Park gate needs a check: if `hold = true`, refuse the park branch even when project is hibernated. (In practice, hibernating a project with held workers is itself contradictory; deactivate-with-hibernate could refuse if any worker holds anything for the project.)
- **Quota / cost surfacing.** The CLI/extension's `weft status` should report "N warm pods, M held suspensions" so the user sees what they're paying for.

## Implementation phases (Epic 2)

1. Add `hold` field on `WakeSignalSpec` + `parkable` derivation update + signal column.
2. Add `ctx.hold_signal` primitive on the trait + RunnerHandle implementation that registers a oneshot.
3. Add `signal_delivery` task kind. Worker poll picks it up and resolves the matching oneshot.
4. Update `dispatch_listener_outcome`'s Resume branch: read `hold` flag, route accordingly.
5. Update worker-pod reaper to skip pods with held suspensions.
6. Cancel path: worker drops all senders on shutdown.
7. Documentation + examples (browser-agent skeleton).

Estimated: 1-2 weeks of focused work, depending on whether multiplexing stays.

## When to do this

Not now. After:
- Epic 1 (multi-await deterministic mode) lands and stabilizes.
- A real use case forces the issue (browser agent integration, or any node that can't be made replay-safe ergonomically).

The split design is preserved here so we don't repaint the bikeshed when we're ready.
