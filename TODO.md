# Weft TODO

Larger design work that's been surfaced but deferred. Not bugs (those
get fixed inline); these are architecture decisions that need design
before implementation. Each entry carries its own what/why; a written
plan lives in `docs/` while it is being worked.

## Unify journal holes: a missing/corrupt event is a HOLE, fatal only on the resume frontier

### Mental model first (read this, the rest follows from it)

- A live worker runs the whole execution **in RAM**. Pulses flow out of
  nodes and trigger downstream nodes; the worker holds all of that
  state in memory.
- The **journal is a write-as-you-go RECORD of what the in-RAM run
  did**, NOT the thing driving each live step. The drive loop folds the
  journal ONCE at boot, then works off the in-RAM snapshot. It only
  reads the journal back to DRIVE on a **respawn** (a fresh worker
  rebuilding state after a crash/eviction, or to resume a suspension).
- So during a live run, every journal write is just "save a checkpoint
  so a future respawn can rebuild this." A write failing does NOT break
  the live run; it only leaves a HOLE that a future respawn would have
  to deal with.

### The problem: two failure paths, both wrong, and they're the same thing

There are two ways a journal event can be bad, handled by two unrelated
mechanisms today:

1. **Write fails** (saving e.g. `NodeCompleted(B)` is rejected by
   Postgres / a fencing trigger). The event is now MISSING from disk.
   Today: `PoisonOnWriteFailure` latches a flag and the drive loop
   **bails the whole worker** at the next iteration.
   **Why this is wrong:** in RAM, B finished and already fired
   downstream; the live run is fine. Killing it because a *save* failed
   is a sledgehammer. It converts "couldn't save B" into "kill the run,"
   which forces a respawn that re-folds from the last good prefix and
   re-runs everything since (including B) anyway.

2. **Read/fold hits a malformed stored row** (the bytes are there but
   garbage, e.g. a corrupt pulse id). Today: `fold_to_snapshot` SKIPS
   the row, logs it, adds a `JournalCorruption { site, reason }` to the
   snapshot (rendered as a corruption marker in the graph), keeps
   folding. Non-fatal.
   **Why this is incomplete:** skip-and-continue is correct for DEAD
   history (a corrupt row in a long-settled branch only degrades the
   replay view), but the SAME skip runs when the corrupt row is on the
   RESUME FRONTIER (a `PortEmitted` feeding a suspended node's input,
   a `NodeResumed` for it); there it would rebuild the suspended
   node's state wrong. The worker now refuses to resume over ANY
   corruption (it journals `ExecutionFailed` naming the rows and `weft
   clean`); the display read still skips and marks.

Both (1) and (2) are the SAME underlying thing: **the journal has a
HOLE at some position** (a missing event, or an unusable one). The only
question that matters is WHERE the hole is, not how it got there.

### Direction: one "hole" concept, frontier-aware

- **A failed write becomes a HOLE, not a kill.** When `record_event`
  fails, record that "an event of kind K for (node, frames) at this
  point could not be persisted" (a hole marker, the write-time analog
  of the fold-time corruption marker). Then KEEP RUNNING the live
  execution on its real in-RAM state. Drop `PoisonOnWriteFailure`'s
  worker-bail entirely.
- **A malformed stored row is the same kind of HOLE**, discovered at
  fold instead of at write. Unify it with the above: one concept ("the
  journal cannot give a correct event at position P"), two discovery
  sites (write-time, fold-time).
- **Fatality is decided by POSITION, not discovery site.** A hole is:
  - **cosmetic** (current marker behavior) when it's in dead history /
    off the resume path: replay view degrades, execution/resume
    unaffected.
  - **fatal** (fail loud, refuse to resume, surface for inspection)
    when it intersects the **resume frontier**: the set of
    {suspended node(s), the resolution of their current token, the
    pulses their resume will consume}. There, "we lost/corrupted the
    very thing that drives the resume" → the execution cannot be
    resumed correctly and must say so loudly, not silently resume on
    bad state.

### The hard part to design

- **Define the resume frontier precisely at fold time.** Which exact
  (node, frames) + pulses + token-resolutions does a correct resume
  depend on? A hole touching that set is fatal; anything else is
  cosmetic.
- **The write-time hole marker.** A failed write produces NO row, so
  there's nothing for the fold to "skip" later. Need a way to PERSIST
  "there is a hole here" (or reconstruct that a hole exists) so a later
  respawn's fold knows an event is missing at position P and can decide
  fatal-vs-cosmetic. Open question: a dedicated hole row, vs detecting
  the gap structurally (e.g. an in-RAM exec that's Running/Completed
  with no corresponding journal row), vs something else.
- **Respawn re-run safety still matters.** Even a cosmetic hole on a
  completed node B means a respawn re-runs B (double side effect). That
  at-least-once re-run is the existing crash semantics and is
  acceptable, but the design should be explicit that "cosmetic hole" =
  "may re-run on respawn," distinct from "fatal hole" = "cannot resume."

### Why deferred

Real design pass: defining the frontier, the unified hole
representation, the write-time persistence of a hole, and the
fatal-vs-cosmetic classifier. Surfaced from the bus+suspension round
(the resume path made the frontier case concrete). The current
`PoisonOnWriteFailure` is the placeholder sledgehammer until this
lands.

[Update Notice Warning] If we touch `PoisonOnWriteFailure`,
`fold_to_snapshot`'s `report_corruption` path, `CorruptionSite`, or the
suspension-resume fold (`SuspensionRegistered` / `NodeSuspended` /
`NodeResumed` / `SuspensionResolved`), revisit this entry.

## Unified error / degraded-state surfacing to the user

**Problem.** When something goes wrong that the runtime can't auto-fix
(a health-recovery action that keeps failing, an infra node stuck
Failed, a trigger-setup that errors, a worker that crashloops), the
user has no consistent, actionable surface. Today it's scattered:
`InfraEvent::Flaky` / `Recovered` / `ProtocolConfigError` events go to
the graph view; some failures are bare 500s; some are log-only; the
new exponential backoff on a failing health action retries silently
forever with no "this is stuck, do something" signal. The recently
added action timeout + backoff close the *wedge* (the slot always
frees, retries are paced) but don't *tell the user* recovery is
struggling.

**Direction.** One reusable "degraded / needs-attention" surface,
usable anywhere in the system (health recovery, infra lifecycle,
trigger setup, worker spawn), not a per-subsystem one-off. Shape to
design:

- A single structured event/state ("X is degraded: reason, attempt
  count, next retry, suggested remediation") that rides the existing
  `InfraEvent` -> `infra_event_bridge` -> SSE -> graph-view rail.
- Surfaced in the graph view with: what's failing, why (the actual
  error, not a stack), how many retries / when the next is, and
  **concrete remediation steps** as helpful as possible.
- A human off-ramp so the user is never stuck in an infinite
  auto-retry: bail buttons (Stop / Terminate the infra, Deactivate the
  project) plus, where possible, a "fix" affordance. These map to verbs
  that already exist (`infra_stop` / `infra_terminate` / `deactivate`
  in `compute_available_actions`); the surface just needs to present
  them in context.
- A **diagnosis layer** (its own sub-design): inspect *why* an action
  failed (pod status = ImagePullBackOff -> "bad image, rebuild"; scale
  rejected -> "RBAC"; readiness timeout -> "node crashing, check
  logs") and emit a specific remediation per cause, instead of a raw
  error string. This is the hard, valuable part.

**Requirements.**
- One representation reused everywhere errors/degradation surface; no
  per-subsystem error shapes. The goal is "wire a new failing
  subsystem into the surface in a few lines."
- Honest: never a silent infinite retry; the user always sees that
  something needs attention and can always bail.
- Composes with the existing event rail (don't invent new transport).

**Why deferred.** This is a cross-cutting UX + diagnosis system that
touches every subsystem that can fail; it deserves a unified design
pass rather than being bolted onto health-recovery alone. Surfaced
from the health-action backoff: the backoff paces retries and is
honest in logs, but the user-facing "recovery is failing, here's what
to do" surface is the real fix and is general, not health-specific.

## Function callbacks (node-to-node, bottom-to-top)

**Problem.** There's no way to connect nodes "from the bottom back to
the top": a node can't invoke a subgraph as a function and get a result
back. This blocks higher-order nodes (`map` / `filter` / "run this
subgraph per element") and is a core piece of the "Weft as a real
language" arc (the callback primitive alongside richer types and
compilation).

**Direction.** A node declares **circuits** (function sockets): named
entry points it can call as functions, plus the matching **return**
edge so the side-pass knows where to hand results back. At fire time
the runtime injects a callable into the node; the node calls it, the
pulse flows through the connected subgraph (the "side pass"), and when
that pass completes it calls back into the node with the result. From
the node's perspective this feels synchronous: invoke, await, get the
result. Mechanically it resembles a signal suspension (suspend, process
on the side, resume), but the side-pass is INTERNAL and the node drives
it, not an external wake event.

**Requirements.**
- The injected callable crosses the node/language boundary cleanly: the
  node calls a function, it does NOT reach into the runtime (the "nodes
  do no plumbing" rule).
- The circuit's argument + return types are part of the node's declared
  interface (typed, not untyped-JSON).
- Preserves the compile-to-standalone-binary path (a callback is an
  in-process call in the compiled form, not an HTTP round-trip).

**Why deferred.** Design is open. The central architectural call: does
this SHARE the journal/replay + suspend/resume machinery (a callback is
a suspension whose resolver is an internal subgraph) or need its own
path? Likely design this BEFORE loops (loops may fall out of it).

## Per-node / per-unit infra drift detection

**Problem.** Infra drift is a single project-wide bool
(`project.running_infra_hash` vs the CLI-supplied `desired_infra_hash`).
It can't say WHICH node changed, only that "something in the infra
changed". So the UI's "Infrastructure has changed, click Upgrade/Start"
banner fires for any infra change, including a node DELETION (orphan to
be reaped), where "upgrade" is misleading. There's no way to surface
"node X's spec changed" vs "node Y is orphaned" vs "unchanged" per node.

We already store `applied_spec_hash` per `infra_node` row, so the data
to compare against exists; the desired side is what's project-wide.

**Direction.** Make drift per-node (and ideally per-unit, matching the
rest of the per-unit model). The CLI computes a desired hash per
infra node (it already compiles each node's spec); the dispatcher
compares each node's desired hash against its row's `applied_spec_hash`
and classifies: unchanged / changed / orphaned (in cluster, not in
source). Status carries per-node drift; the UI shows precisely which
nodes changed and a correct per-node affordance.

**Why deferred.** Real work (CLI per-node hashing, dispatcher per-node
compare, status shape, UI consumption) for a polish-level signal that
rarely bites in practice. The coarse project-wide bool + "click
Start/Upgrade to apply" messaging is acceptable for now. A removed
per-node `(?)` "frozen units" hint was the first casualty of the coarse
signal (it fired on the wrong node); reintroduce a correct per-node
version when this lands.

[Update Notice Warning] If we touch `compute_drift` /
`running_infra_hash` / `desired_infra_hash` or `ProjectInfraEntry`,
revisit this entry.

## Infra namespace-escape hardening

A node author ships arbitrary container specs + raw `extras` k8s
manifests that get applied into the project namespace. Node packages are
untrusted third-party code, so verify a node CANNOT escape its namespace.
Audit: `extras` namespace forced + cluster-scoped kinds rejected;
`PodOptions.service_account` constrained; Pod Security admission rejects
privileged / hostPath / host-namespaces; NetworkPolicy blocks
cross-tenant traffic; supervisor RBAC not turnable into a cross-namespace
write. Goal: compiler rejects escaping specs loudly + namespace admission
enforces, not "we assume they can't".

[Update Notice Warning] If we touch `compile.rs` extras/namespace
stamping, `PodOptions`, `project_namespace.rs` policies/RBAC, or the
supervisor apply path, revisit.

## A closed port carries no reason

**Problem.** A closure says "nothing will arrive here" and nothing
more: `Pulse::closure` sets `closed: true` with `value: Null` and no
other field (`crates/weft-core/src/pulse.rs:82-98`). Three unrelated
situations produce a byte-identical signal at the consumer:

1. the producer FAILED (its body errored, so its ports closed),
2. the producer deliberately declined to emit (a node whose permission
   was false, an unselected branch),
3. the producer was never going to run (a different trigger fired and
   this branch is dead, so its ports closed at Fire).

Downstream they are the same nothing. A consumer can branch on ABSENCE
(that's the null-propagation model working as designed) but not on
CAUSE. So the graph cannot express "retry this when it actually broke,
but leave it alone when the gate deliberately said no", or "route real
failures to an alert and ignore deliberate skips": the retry loop
cannot tell the two apart and retries both.

Second half of the same gap: node code cannot observe a closure at all.
An optional port that was closed is indistinguishable, from inside the
body, from an optional port that was never wired. The bag accessors
answer "do I have a value", never "did something upstream close this".

**Direction.** Deliberately unspecified. Two shapes are visible (a
closure that carries a reason tag readable by node code; or failure
propagating as a genuinely distinct signal from deliberate-skip) and
neither is clearly right, so this entry records the problem and the
evidence WITHOUT picking a shape. Park it until a real use case bites
and shows which distinction actually needs to be drawn; designing it
from the hypothetical would bake in the wrong seam.

**Why deferred.** No design yet, on purpose (see above). Surfaced while
mapping the node-request corpus (`discord-export/node-requests-ranked.md`)
against the language: retry-on-real-failure is wanted by essentially
every flaky-external-API node, and it is the retry/error-handling item
`ROADMAP.md` names, reached from the other end. Note this is NOT a
missing retry construct: retry is expressible today as a `Loop` with a
stop condition, and that stays the explicit way to do it. The gap is
only that the loop cannot see WHY it got nothing.

[Update Notice Warning] If we touch `Pulse::closure` / the `closed`
flag, the skip-cascade in `handle_node_skip`, or the `ValueBag`
accessors, revisit this entry.

## Held suspensions (warm-worker model)

**Problem.** The durable-replay model dies-and-resumes the worker pod
on every suspension: a worker dies whenever all lanes park on
`await_signal`, and a fresh pod folds the journal to resume. That's the
right trade for thousands of cheap parked flows (a HumanQuery waiting
days costs only journal rows). But it works against a node that holds
in-process state too expensive to rebuild on replay: a browser session
with thousands of cookies, a long-lived local model load, a warm
connection pool. Replaying the journal doesn't reconstruct that state;
it's gone with the pod.

**Direction.** A `ctx.hold_signal` primitive that opts a node into a
warm-worker model: the future actually awaits in place and the worker
pod stays alive across the suspension, instead of unwinding and dying.
The node keeps its in-process state; the await is a real await, not a
replay boundary.

**Requirements.**
- Opt-in per call site. The default stays die-and-resume (it's correct
  for the common case and is what makes massive park-fanout cheap);
  holding is the exception a node asks for when it has unreplayable
  state.
- Crosses the node/language boundary cleanly (the "nodes do no
  plumbing" rule): the node awaits a future, it does not reach into the
  worker lifecycle or the dispatcher.
- Honest about the cost: a held suspension pins a worker pod for its
  whole duration, so the language should make that trade visible (this
  is no longer free parking).
- Composes with the existing wake-signal contract: a held await resolves
  on the same fire path as a parked one, the difference is only whether
  the worker stayed warm.

**Why deferred.** Needs a design pass on how a held await coexists with
the journal/replay machinery (the worker that holds is the same worker
that would otherwise have died and refolded) and on the lifecycle/leasing
implications of a pinned worker. Surfaced from the node-authoring docs,
which promised this primitive before it existed.

## setup.sh cross-version upgrade path [DORMANT until MVP]

**Status: OFF.** Inactive while pre-users (no install base to protect).
Turns ON when Quentin says "I am opening the MVP" (or equivalent); at
that point start enforcing it. Until then a corrupted-state-on-rerun is
acceptable, the fix is just `setup.sh --uninstall --purge` then
`setup.sh`.

**The rule (when ON).** `setup.sh` must support a clean upgrade from ANY
shipped version (every version from the MVP launch onward) to current,
with NO manual purge and NO corrupted state left behind. When something
cross-cutting changes (image/tag naming scheme, k8s manifest shape,
on-disk project layout, DB lifecycle), the upgrade path must detect the
old shape and migrate or clean it automatically.

**Why.** Once there is an install base, an upgrade that silently breaks
state is a production incident for every user who reruns setup.
Pre-users it costs nothing, so the work is deferred, but the obligation
is recorded so it isn't forgotten at launch. Past incident (pre-MVP,
harmless then): the image/resource tagging scheme changed between two
builds; rerunning `setup.sh` left stale state mismatched with the new
code (`weft run` failed with "project not found" / status-gate errors);
only `--uninstall --purge` + reinstall fixed it. With users, that same
situation would corrupt their install on a routine upgrade.

When this flips ON, revisit alongside setup.sh's flag set and the
image/tag + manifest + project-layout conventions; the migration logic
lives wherever setup.sh sequences install/upgrade.

## E2e parallelization: one cluster per e2e, keep failed clusters

**Problem.** The e2e suite runs sequentially against ONE shared kind
cluster, so tests cannot overlap (they share the dispatcher, the
project namespace pool, the ingress/seaweed ports) and a failing test's
cluster state is torn down or reused before it can be inspected.

**Direction (agreed shape, not yet designed in detail).**
- Each e2e gets its OWN kind cluster. The knobs already exist as env
  vars (`WEFT_CLUSTER_NAME`, `WEFT_INGRESS_PORT`, `WEFT_SEAWEED_PORT`,
  `WEFT_DISPATCHER_URL`), so a per-test cluster is "pick a unique name
  + unique ports, export, run"; no code seam needed, the work is in the
  runner.
- `run-e2e.sh` grows the same `--parallel [N]` contract as
  `run-node-tests.sh`: bare = all at once, N = batches of N, outputs
  buffered per test and printed in suite order, a failure stops after
  its whole batch (every failure in that batch visible). Realistic
  batch size is 3-5 (each cluster is a full control plane; RAM/CPU
  bound).
- **A failed test's cluster is KEPT for inspection** (named after the
  test, printed in the failure banner with the kubectl context to poke
  it and the delete command); passing tests' clusters are deleted as
  soon as they pass. A sweep must never leave passing clusters behind.
- Node-test and e2e suites already share nothing (own scratch projects,
  own build dirs, own cluster projects), so they stay runnable
  simultaneously; per-e2e clusters only strengthen that.

**Why deferred.** Quentin parked it explicitly ("let's wait for the e2e
parallelization and having their own cluster after we are done with the
other stuff") after the node-test `--parallel` work landed. Pick it up
when he calls for it.

[Update Notice Warning] If we touch `run-e2e.sh`, the e2e harness's
cluster bootstrap, or the WEFT_CLUSTER_NAME/port env knobs, revisit
this entry.

## Project-scoped meta log (observability outside the journal)

Design a per-project log surface for everything that is ABOUT a project
but is not execution data, so debugging held connections stops meaning
kubectl. What goes there:

- What the listener sees for the project's signals: connect cycles,
  dialogue progress on a raw pipe (which step, what matched), fires,
  reconnects, why a fire was filtered out. Today this lands in the
  pooled listener's pod logs, interleaved across tenants and invisible
  to the project owner. Debugging "my email trigger never fires" needs
  this legible per project.
- A node-facing info log (`ctx.log(...)`-shaped): breadcrumbs a node
  author wants while developing, deliberately NOT journal events
  (journaling every log line would bloat the durable record; these are
  ephemeral, ring-buffered, lossy by design).
- Later candidates: subscription renewals, access refreshes, tunnel
  address changes.

Shape to think through: one ring buffer per project (bounded, lossy,
queryable via dispatcher + shown in the dashboard/extension), what the
pooled tiers may write to it (tenant isolation: a pooled listener
writes only to the project the signal belongs to), and rate-limiting
so a chatty loop cannot flood it. Related: "Note Q" above (ephemeral
journal buffering) and the unified error-surfacing entry; a design
should look at all three together before building any one of them.

## Delegated end-customer connections (embed weft in someone else's product)

**Problem.** An operator builds a product on top of weft (say a
sheet-analysis workflow) and serves it to THEIR end customers from
their own website. Each end customer needs to connect their own
third-party account (their Google, their Slack) and run the workflow
against it: pick their sheet, run on their data, on their behalf. The
access system today has no seam for this: connections belong to the
tenant who owns the project (the operator), created through the
editor's connect flow by that tenant. There is no way for the
operator's website to mint a connection FOR one of its end customers,
no way to keep two end customers' grants apart inside one project, and
no way to point a run at "customer X's connection" at fire time.

**Direction (rough, needs a real design pass).**
- The operator registers their own OAuth app once (that part exists:
  it is an app entry, the operator's client id/secret).
- Their website drives a connect flow for a LOGGED-IN end customer:
  the operator's backend asks weft (server-to-server, operator
  credential) to mint a consent link scoped to an operator-chosen
  subject id ("customer-123"); the end customer approves at the
  provider; the resulting grant lands in weft tagged with that
  subject, not as an operator-wide connection.
- Credential custody stays in weft the whole way: the end customer's
  tokens are held and resolved by weft like any grant, never returned
  to the operator's backend and never exposed to the end customer's
  browser, so neither side can exfiltrate the other's credentials.
  The operator can list/revoke by subject, never read.
- A run then names its subject (fire-time input or signal payload) and
  the access resolution picks that subject's grant for the service,
  instead of "the project's connection". Remote-select pickers (pick a
  sheet) need the same subject-scoped resolution to work in the
  operator's embedded UI.

**Requirements.**
- The subject is an OPAQUE operator-chosen string; weft does not know
  or care about the operator's user model.
- Per-subject isolation is enforced by weft, not by operator
  discipline: a run bound to subject A can never resolve subject B's
  grant.
- The existing single-tenant flow stays untouched: a project with no
  subjects behaves exactly as today (the substitution test says extend
  the grant concept with an optional subject, not fork a sibling
  concept).

**Why deferred.** Real design pass across the access store (grant
shape), the consent flow (embeddable, operator-driven), fire-time
binding, and the picker path. Surfaced by a real ask from a potential
operator; recorded so the access system's next design round takes it
as a first-class use case.

[Update Notice Warning] If we touch the access grant schema, the
connect/consent flow, or fire-time access resolution, revisit this
entry.

## Model-list filtering by capability (inference vs embeddings vs rerank)

**Problem.** The provider nodes' `model` input is now a `remote_select`
fetching the provider's model list (free-typing allowed). The list is
unfiltered: OpenRouter's `/models` answers every model (chat,
embedding, rerank alike) with the capability as fields on each item,
and there is no server-side "only embedding models" parameter. An
inference provider suggesting embedding models (and the reverse) is
noise, and today one shared provider node feeds inference, embed,
rerank, and moderate, so a single model field cannot carry
per-consumer filters anyway.

**Direction.** Add an optional declarative per-item filter to the
`remote_select` `list` source (a dotted field path plus an expected or
contained value, applied store-side while paging, same vocabulary as
`Lookup`'s label/value paths). Then decide where differently-filtered
lists live: either the endpoint nodes (LlmEmbed, LlmRerank) get their
own model input with their own filtered widget, or per-capability
provider nodes. The filter mechanism is generic (any listing service
whose items carry a type field); the split question is the real design
call.

**Why deferred.** The filter needs the split decision to be useful,
and free-typing already unblocks every model today.

[Update Notice Warning] If we touch the remote_select widget, the
Lookup shape, or split the LLM provider nodes per capability, revisit
this entry.

## Parallel loops: a `max_parallel` concurrency bound

**Problem.** A parallel loop launches a lane per item with NO bound on
how many run at once: a 10k-element list launches 10k concurrent
iterations, and a stream-driven parallel loop launches a lane per
arriving item the same way (the producer's buffer cap gives no
backpressure, because items launch instead of buffering). `max_iters`
caps the TOTAL, which is a different question from "how many in
flight".

**Direction.** One loop config knob (`max_parallel`) bounding in-flight
lanes for BOTH sources: `ready = in_flight < max_parallel` where
`in_flight = launched - out_fired`; items past the bound buffer (the
stream state already has the buffer; lists would derive the next index
the same way sequential does) and a completed lane launches the next
buffered/pending item from `record_loop_out`'s parallel arm, which
today never pops the buffer (it never needs to; that changes with the
bound). Compiler side: a `KNOWN_LOOP_KEYS` entry + type check, default
unbounded (today's semantic).

**Why deferred.** Decided with Quentin during the Generator[T] review
round: unbounded-per-item IS the intended parallel semantic for now
("process each item as it arrives without waiting on the previous
one"), and the bound is wanted later for lists and streams together,
not as a stream-only patch.

[Update Notice Warning] If we touch `LoopRuntime::stream_push`'s ready
check, `parallel_completion`, or the loop config keys in the compiler,
revisit this entry.

## Suspendable live channels (bus + generator)

**What.** A live channel (a `Bus`, and once implemented a
`Generator[T]` stream, see `docs/generator-design.md`) is pinned to
one worker: both endpoints must stay co-alive, and `await_signal` is
forbidden while the channel is open. Make these channels survive
suspension and worker death: journal enough of the channel state that
a fresh worker can resume both endpoints mid-stream.

**Why deferred.** Durable mid-stream resume needs a replay story for
partially-consumed streams (which yields were delivered, which pulls
were answered) and interacts with the deterministic-replay rule.
Design it after Generator[T] lands in its non-durable form; the
no-suspension-while-open rule keeps the gap honest until then.

[Update Notice Warning] If we touch the BusCoordinator, implement
Generator[T], or rework await_signal journaling, revisit this entry.

## Concurrent builds have no order for the asset reference set
Every build publishes "the files this project uses now". Two builds of one
project at once can land in either order, so a slow older build can
overwrite a newer one's set: the newer files get a 30-day expiry countdown
they should not have, which the next successful build clears. Fixing it
needs a version on builds to compare against, and the publish happens
before the definition is registered, so nothing carries one yet.

## A time type?
`Cron` takes a cron string plus a `timezone`, `WaitUntil` an ISO-8601
string, `Wait` a number of seconds: three spellings of "a moment" with
no type behind them. Worth deciding whether time (and a repeating time)
should be a WeftType of its own, or whether three string-ish inputs on
three nodes is fine.

## Rename color to exec
Color was a concept I was experimenting with for mutliple execution in the same runtime but I changed my mind and never ended up changing the name.

## Native branching and retries: should `if` / `else` / retry become language constructs?

Branching is one rule: a closed port skips the node it lands on, which
closes its outputs, which cascades. `_should_flow` is that rule with a
handle on it (a node runs unless its permission says no), `Switch` picks
which permission is granted, and `FirstInOrder` brings the branches back
to one wire. Retries are a different story: whatever a node does about
them, it does inside itself.

That is enough to express branching, and it may not be the nicest way to
write it. An author who wants "call this, and if it fails three times,
take the other path" is writing node code for something that reads like
control flow. The question is whether the language should grow a native
`if` / `else` and a native retry, and if so what a retry means when the
thing being retried is a subgraph rather than a call (what re-fires,
what keeps its state, what the journal records, and what a person
watching the graph sees while it happens).

Two pieces of the old version of this question are now built, and what
they taught is worth keeping:

- Waiting for every wired port is not the problem it looks like. A
  branch that was turned off does not keep anyone waiting: the node at
  the head of it is skipped the moment its permission closes, and the
  skip reaches the join in the same tick. A join stalls only while a
  branch is genuinely still running, which is the honest answer anyway.
- `FirstInOrder` picks by WRITTEN order, never by arrival, because a
  race would replay differently from the run it recorded and the journal
  is supposed to be the truth.

What is still open is the RACE: two live branches, and the first answer
wins. Nothing fires a node once per arriving value. The closest is a
live channel (a stream or a bus), which delivers items to a node that is
ALREADY running rather than firing it again. Whether ordinary ports
should ever have a per-arrival mode is the question that decides whether
a race is expressible without a bus.

Decide before the release: a native form added later changes how every
program is written, so it is cheaper to know now whether it is coming.

## Inverting a decision: SETTLED, `_should_not_flow`

This section used to answer "run this when the other branch did not"
with: the node that decides emits an optional port that says nothing on
success, and the other branch hangs off that port. That answer was
wrong, and it was wrong in a way worth writing down, because it cost a
whole build session.

It only covers absence that a node you wrote DECIDED on. It does not
cover absence that is just data: a key missing from a request body, an
optional input nobody filled. Nothing decided there, so there is no node
to add a port to. And every other node in the language skips when its
inputs close, so nothing downstream is left alive to notice. Following
the old advice meant inventing a node whose whole job was to survive the
closure and announce it, which is exactly the convoluted shape the
advice was supposed to avoid.

The language now carries a second spelling of the gate,
`_should_not_flow`: the same decision read the other way round, where a
CLOSED input is the yes. It is the one port in the language that fires
on a closure, which is what makes "act on the thing that did not happen"
writable at all. A node has one gate; wiring both spellings is a compile
error (`two-gates`). In the editor it is the same triangle with a small
circle where it meets the node, the way a negated input is drawn in a
logic diagram, and right-clicking the gate toggles it.

What is still open is the plain boolean flip: holding a `true` and
wanting to act on `false` still means writing Python, since nothing in
the catalog turns a boolean around. A `Not` node (boolean in, boolean
out) would cover it and compose anywhere a boolean goes. That is a
smaller question than this section used to be, and it is the only part
of it left.

## Killing tagged NODES inside one execution

The execution-level half of this idea shipped. For the mechanism, go and
read `docs/src/nodes/steering-executions.md`.

**The same verb, one scope down.** Tags name nodes too (`_tags`), so the
same idea points INSIDE one execution: "stop every node tagged `pathB`".
Where it pays is a fork whose two branches race, one short and one long:
the moment the short one wins, the long one is dead weight, and killing
it saves the model calls and the compute it was about to spend rather
than discarding its answer at the end.

Whether that is the same function with a scope, or two functions, is
part of the decision. What a killed branch leaves behind is the harder
half: its nodes have to close their outputs so whatever was waiting on
them skips cleanly instead of hanging. A node that is mid-call when the
kill lands is the same in-flight problem the execution-level stop already
answers: the flag flips, the node stops at its next await. For how a race
between branches becomes possible at all, go and read
[fire-on-arrival](#fire-on-arrival-should-a-node-be-able-to-run-before-all-its-inputs-are-in).

Not doing it now: without fire-on-arrival there is no race to lose.

## Nothing tests the editor: a rig that drives the real webview

**Problem.** Every gesture in the graph is verified by a person looking
at it. The webview's tests cover plain modules only (projection, the
edit engine, layout, the protocol shapes): there is no jsdom, no
component rendering, so nothing can click a button, open a form, drag a
wire, or assert that a field is absent. The e2e rig cannot help: it
drives the dispatcher over HTTP and never opens a browser. So the whole
editor, which is most of what a user touches, has no test layer at all.

What that costs, from one afternoon: a config field silently became a
JSON text area instead of a list editor, a port dock offered a wire the
compiler refuses, and an entry could be edited into a name a neighbour
already owned. Each was found by eye.

**What it has to test, and this is the point.** The gesture AND the
source it produces. An edit in the graph rewrites the `.weft` file
through the edit ops, so the assertion is a round trip: mount the real
webview, act, read the source back, and check both what the file says
and what the graph now shows. Half a rig that only asserts pixels would
be worse than none.

**Not only the graph.** The same harness is what would let us test the
extension host: the action bar's states, the streaming AI edits landing
in the open file, the sidebar's project and execution lists, the live
panel, the graph reopening on the right file. Whatever shape it takes,
it should be able to stand up an extension host as well as a webview.

**Open questions.**
- Which harness: jsdom plus a Svelte testing library (fast, runs in the
  same vitest as everything else, but it is not a browser), a real
  headless browser over the built bundle (honest, slower, new tooling),
  or VS Code's own extension test runner (the only one that gives a
  REAL extension host, and the heaviest).
- The host bridge is a message channel, so a fake host is dumb and
  hand-rolled, the same rule the other fakes follow. What it has to
  answer: parse, validate, edit ops, file reads.
- Where it lives: the renderer is `packages/weft-graph`, so its rig
  belongs beside it; anything about the extension host belongs to
  `extension-vscode`.
- Whether it runs on every save or on the pre-release pass, which
  decides how much it may cost.

Not now: it is a real piece of infrastructure, not an afternoon.

## Type rules in metadata: an output typed from the node's other ports

Two nodes want an output whose type the metadata cannot write down as a
single type, and today each fakes it: `FirstInOrder` emits whichever
branch survived, so its output is really the UNION of what its created
inputs carry, and `LlmInference.response` is a `String` without
`parseJson` and a record or `JsonDict` with it. Both are `MustOverride`
or a type variable now, and the author restates the type in the
signature every time.

The feature is a small rule language in `metadata.json` (an output typed
as "the union of these inputs", "this type when that input is true"),
read by enrich the way `portsFromConfig` is. Nothing of it exists in the
code on purpose: it is the whole feature or nothing, since a half rule
would send the editor, the validator and the runtime three different
answers about one port. Design it before writing the first rule.

## Fire-on-arrival: should a node be able to run before all its inputs are in?

**This entry is a decision to make, not a task to do.** Think it
through, then decide whether it is worth building at all.

Today a node fires once, when every wired port has arrived. A closed
port counts as an arrival, which is what makes branching work, and it
is also what makes this shape slow: a fork where one branch is three
nodes and the other is thirty, both landing on the same node. The short
branch's value is there in a second, and the node sits on it until the
skip has walked all thirty nodes of the branch nobody took. It already
holds everything it needs and it waits anyway.

**The shape being considered.** A flag on a node: fire me every time one
of my inputs is filled. Each firing sees the input bag AS IT STANDS, not
just the value that arrived, so the body always has the whole picture.
A first-to-arrive node then emits on its first firing and does nothing
on the later ones. A node with five inputs that wants the sum of the
first two waits through one firing, emits on the second, and ignores the
rest.

**What it drags in with it.** The body has to remember what it already
did, across firings of the SAME execution, which weft gives a node no
way to do. Something like a scratchpad on the ctx, scoped to this node
in this execution, and the question of whether it lives in the worker's
memory (lost the moment the execution suspends or the pod dies) or in
the journal (durable, replayable, another thing on the write path).

**The questions to answer before anything is built.**
- Replay. Firing order becomes arrival order, and arrival order is a
  race: the same program could pick a different branch on a replay than
  it did on the original run. Either the journal records which firing
  emitted and replay follows that record rather than the clock, or the
  feature breaks the one property the whole system rests on.
- What a firing IS in the journal. One node execution per firing, or one
  execution that received several deliveries? The inspector, the metering,
  and the replay all read that shape, and a node that ran four times and
  emitted once has to be legible to a person looking at the graph.
- Termination. The runtime still has to know when a node's inputs are
  settled, so a node that never emitted can close its outputs and let the
  skip cascade finish. Fire-on-arrival adds firings; it cannot remove
  that.
- Does a CLOSED arrival count as a fill? For the fork it must not (the
  branch that lost has nothing to say), but "the first two that arrive"
  needs the same answer stated deliberately rather than falling out.
- Loops. A firing is per (color, frames), so this is per iteration; is
  there any case where it should be otherwise?
- Cost. A node that fired four times bills as what.

It pairs with [tags stopping work](#killing-tagged-nodes-inside-one-execution):
fire-on-arrival is what makes a race expressible, and cancelling the
losing branch by tag is what stops it costing money.

## A node whose outputs nobody reads

There used to be a warning for it, `orphan-outputs`, and it was removed
because it fired on the last node of nearly every real program. This
entry is what would have to be true to bring it back.

**What the warning was for.** A node that computes a value nobody uses
is usually a mistake: a `Cast` left over from an edit, a `Format` whose
result was meant to go somewhere. Catching that is worth a line of
advice in the editor.

**Why it fires on correct programs.** A program ends by DOING
something: sending the message, writing the row, uploading the file.
Those nodes have outputs (a message id, a row count) that nobody has to
read, so every one of them looked like the mistake above. `_is_output:
true` used to mark them and is gone, because every reached node runs
now and the marker meant nothing to the runtime. So today the last node
of a Telegram bot, a Slack bot and a Postgres writer all warn, which
teaches people to ignore warnings, which costs us the two cases where
the warning was right.

**Why a metadata flag is not enough.** The obvious fix is a per-node
flag in the catalog ("this node's effect is the point"), set on send,
write, upload and react nodes. It works for those, and it breaks on the
nodes that are both: `ExecPython` is usually a computation whose result
matters, and sometimes the script itself is the whole point (it calls
something, it writes a file). Whichever way the flag is set on such a
node, half its uses are wrong.

**So the shape it needs.** A default in the node's metadata, plus a way
for a program to override the default on one instance. Which raises the
questions to answer before writing any of it:

- What is the override's spelling, and is it a config key (the language
  reading a `_`-reserved key again, which is the thing `_is_output` did
  and we removed) or something else entirely?
- Does the override belong on the node at all, or is it really a
  property of the WIRE that is missing (this output is a receipt) so
  the check is per-port rather than per-node?
- Is the editor a better home than the compiler? A node with nothing
  leaving it is visible at a glance in the graph, and a diagnostic that
  is only ever advice may not belong in the compile output at all.
- What does it do inside a group? A member whose outputs feed nothing
  and no `self.x` is the same mistake one level down.

Until that is answered there is no warning, and a leaf is just a leaf.

## Show the version tree in the sidebar the way git tools draw history

`weft tree` already knows everything: every version, what changed
against its parent, the runs beneath each one, head marked, and
`weft branch` restores any of them. The sidebar shows none of that
shape. It fetches `tree --json` and uses it to decorate a flat list of
executions, so a person cannot see that two runs sit on different
branches, that head moved back, or that a checkpoint exists at all.

**What it should look like.** A graph the way a git client draws one:
one row per version, a lane per branch with the connecting lines, the
runs of a version nested under it, head and the seed run marked. A
right-click or inline action on a version runs `weft branch` (with the
dirty-tree refusal surfaced as the prompt it already is), and on a run
sets it as the seed. Hovering a version shows the file diff summary the
CLI already prints.

**Open questions.** Whether the tree replaces the executions list or
sits beside it (a run is reachable through both today); how much of a
long history to load before the view goes lazy; and whether a version's
diff should open the file diff in the editor rather than a tooltip.

## Cloud deployment, and a route's URL that reaches the internet

Deployment beyond one machine is undesigned. What exists: a kind
cluster per machine, set up by `./setup.sh`, and an opt-in Cloudflare
quick tunnel (`--public-url`) whose nginx proxy allowlists exactly the
provider-events receiver, the per-signal fire door, the file relay and
the OAuth callback. A `k8s` backend exists in `weft daemon start` and
demands `WEFT_GATEWAY_HOST`, `WEFT_GATEWAY_BASE_URL` and
`WEFT_CALLER_TOKEN_SECRET`, and nothing has ever been deployed with it.

**The concrete gap that surfaced it.** `Route` and `Socket` are reached
through `/connect/<tenant>/<path>`, and the dispatcher answers by
redirecting the caller to the worker's own address on the live gateway:
`<pod>.<namespace>.<gateway host>`. Locally that host is
`127-0-0-1.nip.io`, so the redirect only resolves on the same machine,
and the public proxy does not forward `/connect/` at all. `weft activate`
prints no URL, so nothing points this out: anyone who takes the tunnel
address and appends `/connect/<tenant>/<path>`, the way every other
minted link is built, gets a 404 from the proxy's catch-all. Decision for now: routes stay local-only; do not
special-case the tunnel.

**Rate limiting a public route belongs here.** A `Route` on the open
internet has nothing in front of it: no per-caller ceiling, no burst
cap, nothing that says one address is asking too often. Today the only
way is counters in the author's own Postgres, which is exactly the
plumbing the language is supposed to own, so a program that goes public
either ships without a limit or hand-rolls one. It sits with the
deployment questions rather than beside them: where the limit is
enforced (the gateway, the dispatcher's entry, the worker), and what one
caller even means, both fall out of how workers are reached from outside.

**What has to be decided, together, before any of it is built.**
- Where the control plane runs (dispatcher, broker, Postgres, the
  object store) and who owns it: one shared multi-tenant install, or
  one per customer.
- Rate limiting: where a per-caller ceiling is enforced, and what
  identifies a caller once a request has been through a tunnel or an
  ingress and its source address is the proxy's.
- How workers are reached from outside. The per-pod subdomain scheme
  needs a wildcard DNS record and a wildcard certificate; a quick
  tunnel offers one random hostname and no subdomains. The alternative
  is the pod in the path (`/live/<pod>/<namespace>/<rest>`) so a single
  hostname serves every worker, which changes the Envoy rewrite rule,
  the redirect the dispatcher mints, the proxy allowlist and the
  daemon's gateway variables.
- What a URL handed to a third party is built from once there are
  several public addresses (MEMORY.md has the rule for the local case:
  the request's own host when the requester fetches, the configured
  external base otherwise).
- Secrets and identity: the caller-token HMAC, the broker's sealing
  key, the tunnel token, the database credentials, all fixed dev values
  today.
- Images: workers, listeners and infra nodes are built on the machine
  and loaded into kind; a cloud cluster needs a registry and a build
  that publishes to it.
- Upgrades and migrations against a database that is not disposable
  (the `setup.sh cross-version upgrade path` entry above is the local
  half of this).
- Cost and isolation: what one tenant can consume, and what of another
  tenant's a worker can reach (today: one project per pod, egress
  denies private ranges).
- Who may come through a door. A `SameNetwork` endpoint compiles to a
  NodePort plus a NetworkPolicy rule admitting `0.0.0.0/0` on that one
  port (`compile_network_policy` in `crates/weft-core/src/infra/compile.rs`),
  which in-cluster means every pod in every other tenant's namespace.
  It has to be an address rule locally: traffic from the machine
  arrives SNATed to the node's address, so no pod selector can match
  it, and that path is how a person's own frontend reaches their
  database. What holds the line today is the loopback binding on every
  mapped node port, which a real cluster does not have. The shape to
  design: the door carries who may use it, defaulting to the project's
  own namespace, with the machine admitted by address only where the
  node ports are bound to loopback.

## `[T]` instead of `List[T]`

A list is the only type whose name you have to write out. A record is
written as the shape itself, `{ id: String, qty: Number }`, with nothing
in front of it, and nesting already works to any depth in any position:

```weft
type Orders = List[{ id: String, lines: List[{ sku: String, qty: Number }] }]
```

That line is legal today and says everything it needs to. `List` is the
only word in it that carries no information: a bracket can only ever be
a list, because every other bracketed type is spelled with its name in
front (`Dict[String, Number]`, `Generator[String]`). So `[T]` would be
unambiguous, and it is what Swift and TypeScript readers already expect.

The decision that matters is not whether to add it, it is whether to
carry two spellings. Two ways to write one type is how documentation
starts rotting, so if `[T]` goes in, `List[T]` comes out in the same
change: the parser stops accepting it, and the catalog, the fixtures,
the docs and both highlighters move over. That sweep is mechanical and
it is cheap now, while the catalog is this size.

Decided: worth doing, deliberately deferred so it can be one clean pass
of its own rather than a rename tangled into unrelated work. Until then
`List[T]` is the spelling.

## Trying a route without a cluster: BUILT

`weft run --fire` on a Route runs the whole program, answer included,
with no cluster and no activation. The payload is the request to serve:
the envelope the trigger declares in its `firesWith`, plus a `body`.

```bash
weft run --fire 'hello={"method":"POST","path":"hello","body":{"name":"ada"}}'
```

A stand-in caller serves the body and records what the program answered,
so the status, the headers and the body land in the journal the way a
real exchange does. It implements the same `CallerConnection` the real
one does, so the trigger, the Reply, the Stream and the Close all run
their ordinary code and never learn the difference; a loop that
exercised a different path from production would teach you about the
fake instead of the program.

`body` is deliberately outside the `firesWith` contract: a real listener
never delivers one, because a real caller sends it over the wire, so
declaring it would make every Route's contract describe something
production never does. It is split off before the envelope is checked.

A Socket still cannot be fired. Its shape is a conversation over time
and there is nothing honest to invent for the caller's next message, so
it says so and points at `weft activate`.

## A node's display has no limits, and every number in it is hardcoded

Reading what a node is showing works and costs nothing at the size a
local install runs at. Every bound it will need on a real deployment is
either missing or a literal in the middle of a function, and the doors
are on the internet-facing surface (the public proxy's allowlist passes
the whole `/signal-token/` prefix, so a client holding a `--display`
token reads from anywhere).

**The one that is not about scale.** The dispatcher reads a container's
`/live` answer with `resp.json::<Value>()` and no byte cap
(`api/infra.rs`, `read_live`). The only bound is a 3 second deadline on
the whole exchange, and three seconds of pod-to-pod bandwidth is a lot
of megabytes landing in the dispatcher's memory. A dispatcher Pod is
shared across tenants, so one tenant's buggy or hostile container can
spike the RSS of the Pod serving everybody. This one bites at a single
user, not at a thousand.

**The knobs, and what they are today.**

| Knob | Today | Wants |
|---|---|---|
| Bytes the dispatcher will read from a `/live` answer | unbounded | a cap, with a 502 naming the cap and the node; a `Content-Length` refusal before reading a byte |
| Deadline on that read | `Duration::from_secs(3)`, a literal in `read_live` | configurable, and probably shorter than the poll interval by construction |
| Deadline on a display's `/action` press | none at all, on purpose (the work is the container's and the wait the user's) | a cap anyway on the token-facing door, where nobody is watching a spinner and a held connection is just a held connection |
| Editor poll interval | `liveIntervalMs = 3000`, a literal in `graphView.ts` | configurable, and ideally not a fixed timer at all (see below) |
| Polling while the graph tab is in the background | keeps running; only a disposed panel stops it | stop, or slow down, when nobody is looking |
| Rate limit on `/signal-token/displays*` | none, and the dispatcher has no rate limiting anywhere | a limit per token, which is the first such limit the dispatcher would have, so it is a decision about the whole outside surface rather than about displays |

**What it should look like.** Every row above is an option with a
default that suits a local install, set where the rest of the
dispatcher's deployment knobs are set, not a literal in a handler. The
defaults are what a person running `./setup.sh` gets and never thinks
about; a cluster operator moves them.

Two changes would also cut most of the traffic without touching the
freshness guarantee, which is that a display is read on every render
and never stored (a QR code expires in under a minute):

- **Conditional reads.** Hash the feed, answer 304 on `If-None-Match`.
  The common case is a display that has not changed, and it becomes a
  few bytes instead of the whole payload.
- **Coalescing.** A shared entry per project and node with a TTL around
  a second, so a hundred readers of one bridge cost one container hit
  rather than a hundred.

For the shape of the load: a display is polled every 3 seconds per open
graph, whole payload every time. The WhatsApp example has two displays,
so about 5 MB an hour per open graph to show a picture that changes
twice. Ten thousand concurrent viewers with five displays each is
roughly 16,000 requests a second, every one of them a dispatcher to
container round trip and load on the tenant's own container.

**One documentation gap that comes with the cap.** A `data:` URI in an
`image` item is fine for something a container generates in memory (a
300px QR measures about 1.5 KB as a data URI, and base64's 33% on top
of that is noise). Nothing stops a node author putting a real image
there instead, and `image` items already accept a plain URL, so the
stored-file path with its expiring links is the right answer for
anything big. Once there is a cap to name, say all of that in the
node-authoring skill with the number in it.

**Open questions.** Whether the poll becomes a push (the dispatcher
already has an SSE surface) or stays a poll with the two optimizations
above; whether the cap is per answer or per item, since one oversized
image among four good items could come back as one unreadable line
rather than a failed read; and whether a rate limit belongs per token
or per (token, node), given that one client legitimately watches one
bridge closely and has no reason to sweep every display it can see.
