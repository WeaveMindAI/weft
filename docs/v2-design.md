# Weft v2: Compiler + Runtime Redesign

Status: **design draft**. This is a redesign of the whole Weft runtime
model. v1 (the current codebase on `main`) accumulated state in many
places with no reconciler; v2 rebuilds around a single primitive
(colored pulses with durable suspension) and compiles projects to native
rust binaries.

This doc covers the language, the compiler, and the primitives. How
third parties host Weft programs at scale is their problem; this doc
stays runtime-agnostic. Local single-user hosting via `weft run` is in
scope and fully specified.

An AI agent should be able to implement any section in isolation using
only this doc and the current weft codebase as reference.

---

## 0. Motivation: why v2 exists

v1 has real bugs that are symptoms, not causes:

- **Webhook triggers fire after deactivation.** The URL stays mounted
  after `status` flips to `stopped`. Band-aid in PR #9 added a status
  check.
- **Cancelled executions keep running.** The retry loop does not check
  the `cancelled` flag during sleeps.
- **Form requests duplicate.** Various retry paths create multiple
  registrations per logical human-in-the-loop wait.

All three trace back to one structural rot: **runtime state is
fragmented across multiple independent stores with no single source of
truth**. Each store assumes the others agree; nothing enforces it.
Drift is invisible until it bites.

Every roadmap item (callbacks, loops, outputs-as-endpoints,
compile-to-binary, distributed subprograms, multi-file imports) makes
this worse because each assumes a coherent runtime model that does not
exist today.

v2 rebuilds the runtime model around a single primitive (colored pulses)
and gets the fragmented stores out of the critical path.

---

## 1. Decisions already locked in

Treat these as hard constraints.

### 1.1 Single rust binary as the compile target
`weft build myproject/` produces a native rust binary. No IR-plus-runtime
hybrid, no interpreter path. Compile time is acceptable; having two
execution paths is not.

### 1.2 Durability always on
Executions can suspend (human-in-the-loop, timers, callbacks). When they
suspend, state is journaled durably so the worker can exit and another
worker can resume later. This is true both for `weft run` (local sqlite
journal) and for any hosted environment. No "simple mode without
durability"; it would diverge.

### 1.3 Suspension primitives are language-level
The language exposes typed suspension primitives (`await_form`,
`await_timer`, `await_callback`, ...). Nodes are rust code that calls
these; the framework handles journaling and wake routing.

### 1.4 No `triggerCategory` enum
v1 has `TriggerCategory = Webhook | Socket | Polling | Schedule | Local
| Manual` baked into node metadata, and the framework branches on the
string. v2 drops this. What kind of trigger a node is falls out of
which primitives it calls; the framework does not need to be told.

### 1.5 Every node is rust
No per-node microservices in the language itself. Nodes are linked into
the compiled binary at build time. Python execution nodes, if any,
shell out from a rust node body (rust drives the subprocess).

### 1.6 Persistent external connections are infra, not triggers
Anything needing a long-held outbound connection (Slack Socket Mode,
Discord Gateway, WhatsApp SSE bridge) is an infra node with its own
lifecycle. Webhooks and cron do NOT need persistent connections (an
always-on dispatcher routes HTTP, a timer service ticks crons), so they
are never infra. Infra is explicit and user-provisioned.

---

## 2. Core primitive: colored pulses

Everything in v2 runs on pulses. A pulse is a message carrying data
between nodes. The v1 pulse model is extended in two ways: pulses carry
an execution identity (color) and the framework exposes typed
suspension primitives.

### 2.1 Pulse structure

```rust
pub struct Pulse {
    /// Execution identity. Minted at entry (external event triggers a
    /// new run). Propagates unchanged through normal edges, callbacks,
    /// loops. Suspended pulses resume with the same color. Globally
    /// unique (UUID).
    pub color: Uuid,

    /// Parallel/loop sub-dimension within a color. v1's `wave-N` lane
    /// index becomes a vector: one entry per enclosing expand, loop,
    /// or parallel-map. Empty = the root dimension. Used to correlate
    /// pulses at gather points.
    pub lane: Vec<LaneIndex>,

    /// Destination port on the target node.
    pub target: PortRef,

    /// The actual payload.
    pub value: serde_json::Value,
}
```

Color and lane are orthogonal. Color says *which run is this*. Lane
says *which sub-branch of that run*. A pulse inside the third iteration
of a loop inside a parallel-expanded lane 5 has color `C42` and lane
`[5, 2]`.

### 2.2 Color semantics (the rule)

A new color is minted **only** when an external event starts a new
program run. Every internal mechanism (edges, callbacks, loops,
parallel expansion, suspension/resume) propagates the color unchanged.

| Situation | Color |
|-----------|-------|
| Normal edge between two nodes | Unchanged |
| Parallel expansion (List[T] to T) | Unchanged, lane extended |
| Callback invocation | Unchanged, isolated subregion |
| Loop iteration | Unchanged, lane extended with iteration index |
| Mid-execution suspend + resume (form, timer) | Unchanged |
| External webhook arriving | New color |
| External cron fire | New color |
| External socket event kicking off a run | New color |

Analogous to OpenTelemetry's trace-id model: one trace ID per request;
child spans inherit; new requests get fresh IDs. Here "request" maps to
"external event kicking off a run."

### 2.3 Why color is central

- **Correlation at gather points.** A node with two incoming edges
  fires when it has a pulse on each, **of the same color** (and
  matching lane vector).
- **Callback isolation (see 3.3).** Callbacks run in a sub-subgraph
  under the caller's color. Both sides share color, so gather still
  works.
- **Suspension identity.** When a pulse suspends, the journal records
  `(color, lane, node, port, metadata)`. Resuming finds the entry by
  color.
- **Cancellation.** Cancelling an execution is "drop all pulses and
  journal entries with this color." Clean and atomic.
- **Distributed execution (future).** Serializing a pulse for wire
  transfer means serializing color + lane + value. Color is the trace
  ID across machine boundaries.

### 2.4 Lane semantics

A lane vector is a path through nested expansions. Lane `[]` is the
root (no expansion). Lane `[3]` is the fourth element of one expansion.
Lane `[3, 0]` is the first iteration/element inside lane `[3]`.

- An expansion node (`List[T] -> T`) emits one pulse per element with
  the lane extended by the element index.
- A gather node (`T -> List[T]`) collects pulses with the same color
  and lane-prefix, emits one aggregated pulse with the lane trimmed.
- Loop iterations and parallel expansions produce the same kind of
  sub-lane structure. They are unified under one mechanism.

The implicit expand/gather currently active in v1 stays available but
becomes opt-in (see ROADMAP.md: explicit `expand` / `gather`
keywords). Default becomes "type error if List[T] meets T without
explicit keyword." The mechanism itself is unchanged; only the default
is reversed.

---

## 3. Primitives exposed to nodes

Nodes are rust code implementing a trait. They interact with the
runtime through two families of primitives: **entry** (starts a new
run) and **suspension** (pauses the current run to wait on an external
event).

### 3.1 Entry primitives

Declared at node level, not called from `execute`. The compiler sees a
node has an entry primitive and knows: this node is an entry point;
external events matching the declaration start a new execution with a
fresh color.

```rust
pub enum EntryPrimitive {
    /// Incoming HTTP POST. Framework mints a URL, routes matching
    /// calls.
    Webhook {
        /// Path pattern, e.g. "apipost/*". Framework prefixes with a
        /// project-specific random token so URLs are unguessable.
        path: &'static str,
        /// Optional authentication config. Signature + api-key
        /// validation is framework-level, not per-node.
        auth: WebhookAuth,
    },

    /// Cron schedule.
    Cron {
        /// Standard cron expression, validated at compile time.
        schedule: &'static str,
    },

    /// Subscription to a long-running infra connection (Slack,
    /// Discord, etc).
    Event {
        /// The infra node providing the event stream. Wired in at
        /// graph level via a typed port.
        connection_port: &'static str,
        /// Optional filter pattern.
        filter: FilterSpec,
    },

    /// Manual/UI-initiated run.
    Manual,
}
```

Entry primitives are NOT called from within `execute`. They are part
of the node's metadata. The framework reads them at compile time,
wires the routing layer accordingly, and at runtime invokes the
node's `execute` with the event payload as input.

A node can declare multiple entry primitives (e.g. both `Webhook` and
`Cron`). Each fires independently and produces a fresh color.

### 3.2 Suspension primitives

Called from inside `execute` via the `ExecutionContext`. Each pauses
the current execution, journals the pulse state, and waits for a
matching event. The execution is suspended in durable storage and the
worker may die. When the event arrives, a new worker spins up,
replays the journal to reconstruct state, and the suspension
primitive returns the resolved value.

```rust
impl ExecutionContext {
    /// Wait for a form submission. Framework mints a token, returns a
    /// URL the caller can surface to humans. When a POST arrives at
    /// that URL, the primitive returns with the form data.
    pub async fn await_form(&self, schema: FormSchema) -> FormSubmission;

    /// Wait for a duration. Framework schedules a timer in the
    /// journal; worker dies after this call. On wake, returns ().
    pub async fn await_timer(&self, duration: Duration);

    /// Invoke a callback subgraph synchronously. Framework runs the
    /// subgraph under the current color, returns its output.
    pub async fn await_callback<I, O>(
        &self,
        subgraph: SubgraphRef,
        input: I,
    ) -> O;

    /// Wait for N of a set of events (select!-style). Not ship-day
    /// priority but the primitive exists so composition works.
    pub async fn await_first(&self, primitives: &[SuspensionPrimitive])
        -> usize;
}
```

Suspension primitives do not start new executions. They pause and
resume the current one. Color is preserved across the suspend.

### 3.3 Callback isolation rule (compile-time)

A callback subgraph is an isolated subregion with exactly one entry
edge (caller's input to the body) and one exit edge (body's output
back to caller). Anything inside the body subgraph MUST NOT have
edges leaking to nodes outside the body.

The compiler validates this at build time. If a graph has a callback
where the body reaches a node that's also reachable from the caller's
downstream, compilation fails with a clear error.

Why: without isolation, a callback invoked N times would produce N
pulses at a shared downstream node, corrupting the gather semantics.
Isolation makes callbacks semantically equivalent to function calls:
one input, one output, no side effects on the outer graph.

Loops use the same rule (loops are callbacks with iteration count,
see 3.4).

### 3.4 Loops are callbacks

A `ForLoop` node takes a count (or a list) and a body port. The body
port connects to a subgraph. Internally the node invokes the body
via the callback primitive N times. Each iteration extends the lane
vector with the iteration index. When all iterations complete, the
loop emits its downstream output.

No new primitive. Loops are sugar over callbacks, the same isolation
rule applies, same pulse propagation.

---

## 4. Runtime architecture (single-process)

The minimum runtime needed to execute a Weft program. This section is
what `weft run` uses and what any host would need at minimum.

### 4.1 Components

- **Pulse queue**: holds pulses waiting to be delivered to nodes.
- **Pulse dispatcher (in-process)**: pops from the queue, routes pulses
  to node handlers, collects outputs, re-enqueues.
- **Journal**: durable record of (color, lane, node, port, value) for
  every pulse emitted and every suspension entered. Used for replay
  and for resumption after worker restart.
- **Wake index**: mapping from external-event tokens (webhook path,
  form token, timer wake time) to `(color, suspended_pulse)`. Used
  when an external event arrives.
- **HTTP facade**: receives webhook/form URL hits, looks them up in
  the wake index, schedules wake events.

For `weft run`, all five live in a single process. Journal + wake
index back onto a local sqlite file. HTTP facade binds to a local
port.

### 4.2 Execution loop (pseudocode)

```
loop {
    match next_work() {
        Pulse(p) => {
            let node = resolve_target(p.target);
            let result = node.execute(ctx(p.color, p.lane), p.value).await;
            journal.record(p, &result);
            for out in result.outputs {
                enqueue(make_pulse(p.color, p.lane, out));
            }
        }
        Wake { color, suspended_at, value } => {
            let pulse = journal.restore_suspended(color, suspended_at);
            deliver_resume_value(pulse, value);
            enqueue(pulse);
        }
        ExternalEvent { token, payload } => {
            match wake_index.lookup(token) {
                Entry(node) => {
                    let color = Uuid::new_v4();
                    enqueue(Pulse::new(color, [], node.entry_port, payload));
                }
                Suspension(color, suspend_id) => {
                    enqueue(Wake { color, suspended_at: suspend_id, value: payload });
                }
                NotFound => respond_404(),
            }
        }
        Idle => wait_for_next_event().await,
    }
}
```

### 4.3 Journal layout

One journal entry per pulse emitted and per suspension entered.
Entries are keyed by `(color, sequence_number)`. Replay for a given
color reads all entries for that color in sequence and rebuilds the
in-memory pulse queue state.

For `weft run` (single-user, single-process), the journal is a sqlite
table. For multi-user hosted environments, backends can swap in a
distributed journal (out of scope for this doc).

### 4.4 Wake index

Separate sqlite table. Rows are `(token, kind, color, suspend_id)`
where `kind` is `Entry | Suspension`. TTL-managed: entry tokens live
until the project is deactivated; suspension tokens live until the
execution completes or is cancelled.

### 4.5 Cancellation

Cancelling color C:
- Mark all journal entries for C as cancelled.
- Drop all queued pulses with color C.
- Remove all wake-index rows for C.
- Deliver a cancellation signal to any in-flight `execute` call under
  C (via `ExecutionContext::cancellation_token`).

After cancel, no future event for C will wake anything.

---

## 5. The `weft` CLI

Public UX.

```
weft new <name>          Scaffold a new project directory.
weft build               Compile to native binary.
weft run [--output N]    Run locally. Embedded dispatcher, sqlite journal.
weft infra up            Provision long-lived infra nodes.
weft infra down          Tear down infra.
```

Project structure:

```
myproject/
  weft.toml              Manifest: name, version, dependencies.
  main.weft              Entrypoint (the graph).
  nodes/                 User-authored rust nodes.
    my_custom.rs
  vendor/                Imported third-party nodes (via `weft add`).
  .weft/                 Local state (sqlite journal when running locally).
```

`weft.toml` lists node dependencies (stdlib + user + vendored). The
compiler resolves at build time. Node impls are linked into the binary.

### 5.1 `weft build` pipeline

1. Parse all `.weft` files (main + imports).
2. Resolve imports, build the combined graph.
3. Validate (type check, callback isolation, entry-point detection).
4. Enrich (type inference, TypeVar resolution, cf. v1 weft-nodes/enrich).
5. Codegen rust source that embeds the graph + links all referenced
   node impls (from stdlib, user nodes/, and vendor/).
6. Invoke cargo to produce the binary.

### 5.2 `weft run`

Runs the compiled binary with a local sqlite journal. Single process.
Binary serves webhook / form URLs on a local port by default
(configurable). Ctrl-C shuts down gracefully (completes in-flight
pulses, persists journal, exits).

If the project has no entry points that require network (e.g. just
`Manual` entry), the binary runs to completion and exits without
serving URLs.

---

## 6. Node API

Nodes are rust structs implementing `Node`. The trait exposes metadata
and `execute`. Primitives are called through `ExecutionContext`.

```rust
#[async_trait]
pub trait Node {
    /// Node type identifier, must be unique across the catalog.
    fn node_type(&self) -> &'static str;

    /// Metadata: ports, fields, entry primitives if any.
    fn metadata(&self) -> NodeMetadata;

    /// Run this node. Input contains resolved input values; ctx
    /// provides suspension primitives, logging, cost tracking.
    async fn execute(&self, ctx: ExecutionContext, input: Input)
        -> NodeResult;
}

pub struct NodeMetadata {
    pub label: &'static str,
    pub inputs: &'static [PortDef],
    pub outputs: &'static [PortDef],
    pub fields: &'static [FieldDef],
    /// Entry primitives declared at metadata level. Empty = normal
    /// node.
    pub entry: &'static [EntryPrimitive],
    /// Whether this node needs wiring to an infra node. Falls out of
    /// entry primitives containing Event.
    pub requires_infra: bool,
}
```

Compare to v1's `features: NodeFeatures` struct with `isTrigger: bool`,
`triggerCategory: Option<TriggerCategory>`,
`requiresRunningInstance: bool`. In v2 all of these are derived from
the presence of entry primitives.

### 6.1 Example: `ApiPost` in v2

```rust
pub struct ApiPostNode;

#[async_trait]
impl Node for ApiPostNode {
    fn node_type(&self) -> &'static str { "ApiPost" }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "API Endpoint (POST)",
            inputs: &[],
            outputs: &[
                PortDef::new("receivedAt", "String", false),
            ],
            fields: &[
                FieldDef::password("apiKey"),
            ],
            entry: &[
                EntryPrimitive::Webhook {
                    path: "",
                    auth: WebhookAuth::OptionalApiKey { field: "apiKey" },
                },
            ],
            requires_infra: false,
        }
    }

    async fn execute(&self, _ctx: ExecutionContext, input: Input)
        -> NodeResult
    {
        let mut output = input["body"].clone();
        output["receivedAt"] = input["receivedAt"].clone();
        NodeResult::completed(output)
    }
}
```

Zero hardcoded URL logic in the node. The framework sees the Webhook
primitive and wires routing.

### 6.2 Example: `HumanQuery` in v2

```rust
impl Node for HumanNode {
    fn node_type(&self) -> &'static str { "HumanQuery" }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Human",
            inputs: &[PortDef::new("context", "String", false)],
            outputs: &[],
            fields: &[],
            entry: &[],
            requires_infra: false,
        }
    }

    async fn execute(&self, ctx: ExecutionContext, input: Input)
        -> NodeResult
    {
        let form = build_form_schema(input);
        let submission = ctx.await_form(form).await;
        NodeResult::completed(submission.into())
    }
}
```

No more `FormInputChannels`, oneshot channels,
`NodeCallbackRequest::WaitingForInput`, callback URL construction.
One primitive, `await_form`, handles it.

---

## 7. How v1 features map to v2

| v1 feature | v2 replacement |
|------------|----------------|
| `triggers` table with `status`, `pending_action`, etc. | Wake-index entries keyed by color. |
| `TriggerService` + `TriggerHandle` + `keep_alive` | Infra nodes (for connections) + entry primitives (for everything else). |
| `TaskRegistry` as a separate service for human tasks | Unified with execution journal under `await_form`. |
| Orchestrator `ExecutorState.executions: DashMap` | Journal is the source of truth; workers are ephemeral. |
| `NodeCallbackRequest::WaitingForInput` + oneshot channels | `ctx.await_form` primitive. |
| Implicit expand/gather | Opt-in via explicit `expand` / `gather` (see ROADMAP). |
| `triggerCategory` enum read in ten places | Derived from entry primitives declared on the node. |

---

## 8. Migration plan (weft side)

v1 users need a path forward. v2 is not drop-in; it's a rewrite. v1
and v2 can run side by side during a transition.

### 8.1 Phase 1: v2 minimal end-to-end
- Implement core primitives (pulse, color, lane, await_*, entry).
- Implement pulse queue + journal + sqlite backend.
- Port stdlib nodes (Text, Gate, LLM, HumanQuery, ApiPost, Cron) to
  the v2 Node trait.
- `weft run` works for simple programs.
- `weft run` works for programs with HumanQuery (exercises
  suspend/resume).

### 8.2 Phase 2: broader stdlib + compiler maturity
- Port remaining stdlib nodes.
- Multi-file imports.
- `weft build` produces binaries other hosts can run.
- `weft new` scaffolding.

### 8.3 Phase 3: migration tooling
- `weft migrate-from-v1 <project>` converts a v1 weft file to v2
  format where automatic conversion is safe.
- Some constructs need manual conversion (anywhere v1 relied on
  implicit mechanisms that v2 makes explicit).

### 8.4 Phase 4: v1 shutoff
- Deprecation notice with date.
- v1 codebase frozen, bug fixes only.
- Eventually, v1 endpoints retired.

---

## 9. Open questions

### 9.1 Entry-point detection
The compiler determines a node is an entry point by seeing it has
entry primitives in its metadata. Is that enough, or do we also need
an explicit `#[entry_point]` attribute on the impl? Preference: derive
from metadata. If ambiguous cases emerge, add the attribute as an
override.

### 9.2 `await_first` composition
`tokio::select!`-style waiting on multiple primitives. Not ship-day
but the primitive needs to exist in the design so we don't paint
ourselves in a corner.

### 9.3 Error handling primitive
Roadmap says try/catch. Does catching use the suspension primitive
shape (await_result with error variant) or a separate mechanism?
Probably separate because errors are cross-cutting, but worth
thinking about.

### 9.4 Distributed subprograms
Cross-machine color propagation, serialization format for pulses.
Out of scope for v2 ship; architected in via color+lane being
serializable.

### 9.5 `weft.toml` schema
Manifest format, dependency declaration, node registration.
Probably copy `Cargo.toml` closely. No strong opinion yet.

### 9.6 Compile-time vs runtime node registration
v1 uses `register_node!` to build a static registry. v2 can build
a `const` table at compile time from imports. Requires the compiler
to discover and link all nodes a project uses.

### 9.7 Testing strategy
- Unit tests on primitive impls (pulse propagation, color rules,
  isolation validation).
- Integration tests running compiled binaries end-to-end (HumanQuery
  roundtrip, callback isolation, cron fire).
- Property tests on journal replay: for any sequence of pulses +
  suspensions, replay reconstructs identical state.

---

## 10. What v2 does NOT do (deliberately)

- No interpreter, no bytecode, no hybrid compile. Single rust target.
- No per-node microservice mesh. Nodes are rust, linked into the
  binary.
- No dynamic node loading at runtime. Node set is fixed at compile
  time.
- No DSL for defining nodes outside rust. Nodes are rust; weft programs
  are weft.
- No backwards compatibility with v1 graph files. Migration is
  explicit (Phase 3).

---

## 11. Reading order for implementers

1. Section 0 (motivation) and 1 (locked decisions) for context.
2. Section 2 (pulse primitive) for the core mental model.
3. Section 3 (node primitives) for the API surface.
4. Section 6 (node examples) to see how nodes look in code.
5. Section 4 (runtime) for the execution loop.
6. Section 5 (CLI + project structure) for user-facing shape.
7. Section 8 (migration) for phasing.
8. Section 9 (open questions) for things still to figure out.

---

## 12. What to build first

The first implementation milestone is the minimal end-to-end loop: a
weft program with one entry node (ApiPost) and one mid-execution
suspend (HumanQuery), compiled via `weft build`, run via `weft run`
with a local sqlite journal, webhook triggered by `curl`, form
answered via second `curl`. Everything else builds on this.

Concrete sub-tasks for the first milestone:

1. Define core types: `Pulse`, `Color`, `Lane`, `ExecutionContext`,
   `EntryPrimitive`, `SuspensionPrimitive`.
2. Implement the pulse queue + journal interface + sqlite backend.
3. Port `ApiPost` and `HumanQuery` to the v2 Node trait.
4. Implement `weft build` producing a binary (hardcoded graph is
   fine for the milestone; full compiler comes after).
5. Implement `weft run` loop: dispatcher + in-process worker +
   sqlite journal.
6. Manual testing: curl the webhook URL, verify execution starts,
   curl the form URL, verify execution resumes.

This milestone validates the core primitives and the suspend/resume
path. Multi-node programs, callbacks, loops, multi-file imports,
all come after.
