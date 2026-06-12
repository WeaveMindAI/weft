# Weft TODO

Larger design work that's been surfaced but deferred. Not bugs (those
get fixed inline); these are architecture decisions that need design
before implementation. An entry that already has a full written plan
links to a file in `todo_plans/` (the plan holds every detail; the
entry here is just the pointer + one-paragraph what/why).

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
   a `NodeResumed`'s absorbed-pulse list) — there it silently rebuilds
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

## Project-scoped file storage (large-data plane)

**Problem.** Workers and infra nodes need to store and exchange large
data (audio/video/documents from WhatsApp, model outputs, uploads,
intermediate artifacts) that's too big to live inline in the journal /
task payloads / pulse values. The v1 design had a `store_temp_file`
split: local OSS mode wrote to local disk, cloud (weavemind) wrote to
Cloudflare R2. That split is exactly the kind of "two code paths for
the same role" we're trying to eliminate in v2.

**Direction.** Tie storage to the **project namespace**, the same way
infra nodes already are. A project gets a storage resource scoped to
its namespace; whether we're local (kind) or cloud (real k8s), it's the
same mechanism and the same code path. Candidates to evaluate:

- A per-project storage backend declared like an infra node (PVC-backed
  locally, object-store-backed in cloud) that the language exposes a
  generic handle for (`ctx.storage()` returning put/get/url, analogous
  to `ctx.endpoint()`).
- A MinIO/S3-API sidecar per project namespace (one API, kind locally
  via a MinIO pod, real object store in cloud), so workers and infra
  nodes both speak S3 and the local/cloud difference is just the
  endpoint.

**Requirements.**
- One code path for local and cloud (no `#[cfg]` / OSS-vs-cloud split
  in the language; the cloud-only behavior, if any, wraps a generic
  weft mechanism per the open/closed repo rule).
- Workers AND infra nodes can read/write; data outlives a single
  worker pod (workers are ephemeral, `restartPolicy: Never`).
- User-manageable: the user can see/manage stored data for a project
  (CLI + extension surface), and `weft clean <project>` wipes it with
  the rest of the project's namespaced state.
- Large objects never travel through the journal/task/pulse path; those
  carry references (keys/URLs), not bytes.

**Why deferred.** Needs a design pass (which backend shape, how the
language exposes it, how it composes with the existing infra-node /
endpoint machinery) before implementation. Surfaced from the WhatsApp
audio path: the bridge already produces base64 audio inline, which is
fine for small clips but won't scale to real media.

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

## Edge-owned transform + type pipeline (with Error-as-value)

**Problem.** The expand/gather/single transform and the type checking
are modeled as NODE concerns (lane_mode lives on `PortDefinition`,
preprocess iterates nodes-and-their-input-ports) when they are
fundamentally EDGE concerns: the operation is inferred from the
type pair across an edge (output port type vs input port type).
Symptoms today: type enforcement is scattered across multiple sites
with inconsistent behavior, transforms carry type knowledge, and the
fail-vs-null decision is made at the check site instead of where
required-vs-optional is known. A node should ONLY do its action and
emit/receive on its ports; everything between two ports (the pipeline)
belongs to the edge.

**Direction.** Move the whole pipeline onto the edge. The operation is
decided once, from `(source output port type, target input port type)`:
`T -> List[T]` = gather, `List[T] -> T` = expand, `T -> T` = single.
`lane_mode`/`lane_depth` move from `PortDefinition` to `Edge`; the
runtime drives the pipeline per-edge, not per-node-input-port. The
node never knows about lanes/expand/gather.

**The exact chain (per edge):**
1. **Emit** with the source output port type known. Type-check the
   emitted value against the OUTPUT port type. On mismatch -> the value
   becomes `Error(TypeMismatch { node, port, expected, got })`, NOT
   null (null loses the locality; the Error carries where it happened).
2. **Transform** (expand / gather / single) on the edge. An `Error`
   value is a poison value: it flows through the transform UNTOUCHED
   (an expanded Error stays Error per lane; a gathered list can contain
   Error entries). Transforms never type-check and never re-check.
3. **Input check** with the target input port type known. Type-check
   the (post-transform) incoming value against the INPUT port type. On
   mismatch -> the value becomes `Error(TypeMismatch { ... })`.
4. **Consumer readiness** is the SINGLE place the consequence is
   decided, by required-vs-optional (extend the existing required/null
   check):
   - required port whose value is an `Error` -> the node fails loudly,
     aggregating all error messages across ports.
   - optional port (not required, or type admits null) whose value is
     an `Error` -> coerce to `Null`, the node proceeds.
   The node then takes its NORMAL path: it never sees an `Error`; by
   the time it executes, every port is a real value or `Null`.

**The Error type.** Make the type mismatch an actual value variant:
`Error(TypeMismatch(...))`, extensible to other error kinds later
(`Error(...)`). The runtime pulse value becomes a wrapper (e.g.
`PulseValue { Json(serde_json::Value), Error(WeftError) }`) since
`serde_json::Value` can't be extended. This touches every pulse-value
site, which is why it's deferred. The key safety property: `Error` is
contained to the engine/pulse transport layer; readiness resolves it
to a real value or `Null` before node exec, so the node-facing API
never sees it.

**Why this shape.** It removes ALL expand/gather special-casing from
type enforcement (the check is uniform: value vs port type), moves the
fail-vs-null decision to the one place that knows required-vs-optional,
and keeps full error locality (the Error names its origin even when it
surfaces at a downstream consumer). The transform stays dumb (no type
knowledge), the node stays dumb (no transport knowledge), the edge owns
the pipeline.

**Coupled with the callback primitive.** A callback is an edge that
goes "down and back," so edge-owns-the-pipeline is the right foundation
for it. Design these two together.

**Interim state (already shipped, a strict subset of this design).**
Type enforcement was unified at the CONSUMER input boundary only
(`ready::check_input`): one required/optional-aware check, no
expand/gather special-case, fail-vs-null decided at readiness. The
output-side and transform-side checks were removed (a node ships its
output as produced; a bad value surfaces at the consumer). This is
correct under the current port-based model and every line survives the
edge move; what's left for this entry is (a) moving ownership
port -> edge and (b) Error-as-flowing-value so the failure can surface
with producer-side locality instead of only at the consumer.

[Update Notice Warning] If we touch `lane_mode`/`PortDefinition`,
`preprocess`, `ready::check_input`, or the pulse value type, revisit
this entry.

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

## Sensitive values: encrypt in journal, redact in inspector

**Problem.** A node config field can hold a secret (an LLM `apiKey`, a
third-party API token typed directly into a node). Today those values
flow through the journal verbatim (in the `NodeStarted` input/config)
and show up in the execution inspector in plaintext. A secret must
SURVIVE resume (the worker re-folds the journal and needs the real
value to make the call) but must NEVER be readable in logs or the
inspector. The webhook-trigger API key already solves a NARROWER
version of this (sha256 on the signal row, plaintext only in the
listener's in-RAM cache, served via `/display`), but that path is
specific to listener-minted auth, not to arbitrary node config a user
types in.

**Direction.** A language-generic "sensitive value" mechanism, NOT an
LLM-node special case:

- A way for the language to KNOW a value is sensitive. This is the
  central design fork (pick before building): a `sensitive: true` flag
  on the metadata field definition, a dedicated `Secret` weft-type, or
  a naming convention. The flag-on-field-definition shape is the
  current front-runner (most local, no new type-system surface) but
  needs deciding.
- Encrypt-at-journal / decrypt-at-fold: the engine encrypts a sensitive
  value before it's written to the journal and decrypts after fold, so
  the worker has the real value to make its call but the at-rest journal
  row carries only ciphertext.
- Inspector redaction: the inspector shows `••••` (or similar) for a
  sensitive value, never the plaintext or the ciphertext.

**Reuse, don't reinvent.** The crypto primitive already exists in the
old code: `crates-v1/weft-api/src/crypto.rs` is a working AES-256-GCM
implementation keyed off `CREDENTIAL_ENCRYPTION_KEY` (already present in
`.env.example`, with a cloud-mode-panics / local-dev-key fallback).
Port that shape into a v2 home rather than writing new crypto.

**Requirements.**
- Language-generic: any node config field can be marked sensitive; the
  engine/journal/inspector handle it uniformly. No per-node code.
- Survives resume: the decrypt-at-fold path gives the worker the real
  value on replay.
- Never readable outside the worker: not in the journal at rest, not in
  logs, not in the inspector.
- Key from the environment (`CREDENTIAL_ENCRYPTION_KEY`), cloud-mode
  panics if unset, local-dev fallback key with a loud warning (the v1
  policy, kept).

**Why deferred.** It's a real subsystem spanning weft-core (the marker
+ crypto), weft-engine (encrypt-before-journal / decrypt-after-fold),
weft-journal (the encrypted field), and the extension inspector
(redaction), and the marker mechanism is itself a design fork. Surfaced
from the feat-bus review (FORK-2): LLM `apiKey` and similar node-typed
secrets currently land in the journal and inspector in plaintext.

[Update Notice Warning] If we touch how node config is journaled
(`NodeStarted`), the metadata field-definition shape, or the inspector's
config rendering, revisit this entry.

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
