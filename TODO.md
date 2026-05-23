# Weft TODO

Larger design work that's been surfaced but deferred. Not bugs (those
get fixed inline); these are architecture decisions that need design
before implementation.

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

## Loops

**Problem.** No way to express iteration in the graph.

**Direction.** Ideas exist, deferred. Open question: is a loop a
distinct primitive, or does it fall out of function callbacks +
Expand/Gather (a callback invoked N times, or an Expand that feeds
back, is already most of a loop)?

**Why deferred.** Decide after the callback shape is clear; the two are
coupled and callbacks probably come first.

## File import / multi-file projects

**Problem.** A project is one `main.weft`. There's no way to split it
across files or compose pieces, which doesn't scale as projects grow.

**Direction.** Let a project import other `.weft` files into values,
into a group, or anywhere (imports inject at any scope). The project
becomes a tree of files the **compiler inlines into one executable at
compile time** (a compile-time include/inline, NOT a runtime module
system); the whole thing still compiles to one standalone artifact.

**Requirements.**
- Import granularity: a value, a group, or an arbitrary node/subgraph
  (probably all three).
- Name resolution + collision rules across files.
- Distinct axis from the node-library question (see "standard library
  as cloned user nodes"): file import is `.weft` SOURCE composition;
  node discovery is the node LIBRARY. Don't conflate them.

**Why deferred.** Needs a design pass on granularity + name resolution.

## Standard library as cloned user nodes (not built into weft)

**Problem.** The standard library shipping inside the weft binary/repo
as a special case is a maintenance nightmare and a special-casing seam.

**Direction.** Stop shipping it built in. On `weft new`, **clone the
stdlib into the project's `nodes/` folder**, and have execution read
ONLY the user's `nodes/` folder. The stdlib then is just ordinary user
nodes: the user can delete what they don't use, fork, or replace any of
it with their own. Everything is a user node in `nodes/`; the model
becomes uniform with no built-in/user split.

**Same task: nested node discovery.** Discovery must detect nodes
anywhere nested under `nodes/`, in BOTH forms: bare single-node
directories AND package form (multi-node packages with a `package.toml`
unioned with each node's `deps.toml`).

**Requirements.**
- Per-project `nodes/` discovery REPLACES the built-in catalog as the
  source of nodes at compile time.
- One discovery path that handles both the bare-node and package forms
  at any nesting depth.

**Why deferred.** This reshapes the catalog-loading path (`weft-catalog`,
the compiler's node registry, `Image::Local` resolution) and is a real
migration of how compile finds nodes, not a tweak. Plan it as such.

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
