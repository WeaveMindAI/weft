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
   RESUME FRONTIER (a `PulseEmitted` feeding a suspended node's input,
   a `NodeResumed`'s absorbed-pulse list); there it silently rebuilds
   the suspended node's state wrong and resumes on garbage.

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

## Inverting a decision: is there still a hole?

`_should_flow` reads BOTH shapes of "no": a `false` value and a closure
(nothing ever answered). So "run this when the other branch did not" is
already writable: the node that decides emits an optional port that says
nothing on success, and whatever reads that port runs only in the other
case. The README's bigger example does exactly that with its `refusal`.

What is still missing is the plain boolean flip. Holding a `true` and
wanting to act on `false` means writing Python to invert it, since
nothing in the catalog turns a boolean around. A `Not` node (boolean in,
boolean out) would cover it and compose anywhere a boolean goes.

Decide whether that node is worth adding, or whether "emit nothing on
the branch you do not want" is the one way it should be said.

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
