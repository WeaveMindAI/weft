# Weft v2: Compiler + Runtime Redesign

Status: **design draft**. This is a redesign of the whole Weft runtime
model. v1 (the current codebase on `main`) accumulated state in many
places with no reconciler; v2 rebuilds around a single primitive
(colored pulses with durable suspension) and compiles projects to
native rust binaries.

This doc covers the language, the compiler, the runtime primitives,
the local dispatcher, the VS Code extension, and the local dashboard.
Cloud-specific details (multi-tenancy, billing, hosted workspaces,
specific isolation runtimes in a hosted context) live in the
weavemind repo's v2-cloud-design doc.

An AI agent should be able to implement any section in isolation
using only this doc and the current weft codebase as reference.

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
fragmented across multiple independent stores with no single source
of truth**. Each store assumes the others agree; nothing enforces it.
Drift is invisible until it bites.

Every roadmap item (callbacks, loops, outputs-as-endpoints,
compile-to-binary, distributed subprograms, multi-file imports) makes
this worse because each assumes a coherent runtime model that does
not exist today.

v2 rebuilds the runtime model around a single primitive (colored
pulses) with a unified dispatcher, and gets the fragmented stores
out of the critical path.

---

## 1. Decisions locked in

Treat these as hard constraints.

### 1.1 Single rust binary as the compile target
`weft build myproject/` produces a native rust binary. No
IR-plus-runtime hybrid, no interpreter path. Compile time is
acceptable; having two execution paths is not.

### 1.2 Durability always on
Executions can suspend (human-in-the-loop, timers, callbacks). When
they suspend, state is journaled durably so the worker can exit and
another worker can resume later. Restate is the durability substrate
everywhere: embedded as a single-node instance locally, managed
multi-node in cloud. No sqlite-for-local / restate-for-cloud split;
one code path everywhere.

### 1.3 Suspension primitives are language-level
The language exposes typed suspension primitives (`await_form`,
`await_timer`, `await_callback`, ...). Nodes are rust code that calls
these; the framework handles journaling and wake routing.

### 1.4 No `triggerCategory` enum
v1 has `TriggerCategory = Webhook | Socket | Polling | Schedule |
Local | Manual` baked into node metadata, and the framework branches
on the string. v2 drops this. What kind of trigger a node is falls
out of which primitives it calls; the framework does not need to be
told.

### 1.5 Every node is rust
No per-node microservices in the language itself. Nodes are linked
into the compiled binary at build time. Python execution nodes, if
any, shell out from a rust node body (rust drives the subprocess).

### 1.6 Persistent external connections are infra, not triggers
Anything needing a long-held outbound connection (Slack Socket Mode,
Discord Gateway, WhatsApp SSE bridge) is an infra node with its own
lifecycle. Webhooks and cron do NOT need persistent connections (the
always-on dispatcher routes HTTP, restate ticks timers), so they are
never infra. Infra is explicit and user-provisioned.

### 1.7 The dispatcher is a universal daemon
Not a cloud-only component. The dispatcher is a long-lived process
that owns:
- Event routing (webhook URLs, form URLs, cron, infra events).
- Worker spawning (how executions actually run).
- Infrastructure orchestration (how persistent resources are
  provisioned and where they live).
- Journal management (durable state for the above).
- The ops dashboard (web UI for watching live execution state).

Local: `weft start` launches a dispatcher daemon on the user's
machine. Cloud: the same dispatcher binary runs on our infra. Same
source, same binary, different backend drivers selected at startup.

### 1.8 Pluggable backend drivers
The dispatcher has pluggable implementations for the two things that
differ by environment:
- **Worker backend**: where execution workers actually run.
- **Infrastructure backend**: where infra nodes (long-lived
  connections, user-provisioned services) actually run.

OSS weft ships with local-only drivers (subprocess for workers, kind
for infra). The closed-source weavemind repo adds cloud drivers.
Same source tree, different deployment flavors.

### 1.9 CLI is a client, dispatcher is the server
The `weft` CLI never owns execution lifecycle. It makes HTTP calls
to the dispatcher, same as `kubectl` to a k8s API server. This
means Ctrl-C on `weft run` disconnects the CLI but does NOT kill
the execution (kill is `weft stop <color>`). Same shape for pure
programs, triggered programs, and HITL programs: all go through
one registration path (`POST /projects/{id}/run`), dispatcher
handles the rest. Projects stay registered after completion until
manually pruned (`weft rm`).

---

## 2. Core primitive: colored pulses

Everything in v2 runs on pulses. A pulse is a message carrying data
between nodes. The v1 pulse model is extended in two ways: pulses
carry an execution identity (color) and the framework exposes typed
suspension primitives.

### 2.1 Pulse structure

```rust
pub struct Pulse {
    /// Execution identity. Minted at entry (external event triggers
    /// a new run). Propagates unchanged through normal edges,
    /// callbacks, loops. Suspended pulses resume with the same
    /// color. Globally unique (UUID).
    pub color: Uuid,

    /// Parallel/loop sub-dimension within a color. v1's `wave-N`
    /// lane index becomes a vector: one entry per enclosing expand,
    /// loop, or parallel-map. Empty = the root dimension. Used to
    /// correlate pulses at gather points.
    pub lane: Vec<LaneIndex>,

    /// Destination port on the target node.
    pub target: PortRef,

    /// The actual payload.
    pub value: serde_json::Value,
}
```

Color and lane are orthogonal. Color says *which run is this*. Lane
says *which sub-branch of that run*. A pulse inside the third
iteration of a loop inside a parallel-expanded lane 5 has color
`C42` and lane `[5, 2]`.

### 2.2 Color semantics (the rule)

A new color is minted **only** when an external event starts a new
program run. Every internal mechanism (edges, callbacks, loops,
parallel expansion, suspension/resume) propagates the color
unchanged.

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

Analogous to OpenTelemetry's trace-id model: one trace ID per
request; child spans inherit; new requests get fresh IDs.

### 2.3 Why color is central

- **Correlation at gather points.** A node with two incoming edges
  fires when it has a pulse on each, **of the same color** (and
  matching lane vector).
- **Callback isolation** (see 3.3). Callbacks run in a sub-subgraph
  under the caller's color. Both sides share color, so gather still
  works.
- **Suspension identity.** When a pulse suspends, the journal
  records `(color, lane, node, port, metadata)`. Resuming finds the
  entry by color.
- **Cancellation.** Cancelling an execution is "drop all pulses and
  journal entries with this color." Clean and atomic.
- **Distributed execution (future).** Serializing a pulse for wire
  transfer means serializing color + lane + value. Color is the
  trace ID across machine boundaries.

### 2.4 Lane semantics

A lane vector is a path through nested expansions. Lane `[]` is the
root (no expansion). Lane `[3]` is the fourth element of one
expansion. Lane `[3, 0]` is the first iteration/element inside lane
`[3]`.

- An expansion node (`List[T] -> T`) emits one pulse per element
  with the lane extended by the element index.
- A gather node (`T -> List[T]`) collects pulses with the same
  color and lane-prefix, emits one aggregated pulse with the lane
  trimmed.
- Loop iterations and parallel expansions produce the same kind of
  sub-lane structure. They are unified under one mechanism.

The implicit expand/gather currently active in v1 stays available
but becomes opt-in (see ROADMAP.md: explicit `expand` / `gather`
keywords). Default becomes "type error if List[T] meets T without
explicit keyword." The mechanism itself is unchanged; only the
default is reversed.

---

## 3. Primitives exposed to nodes

Nodes are rust code implementing a trait. They interact with the
runtime through two families of primitives: **entry** (starts a new
run) and **suspension** (pauses the current run to wait on an
external event).

### 3.1 Entry primitives

Declared at node level, not called from `execute`. The compiler sees
a node has an entry primitive and knows: this node is an entry
point; external events matching the declaration start a new
execution with a fresh color.

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

Entry primitives are NOT called from within `execute`. They are
part of the node's metadata. The framework reads them at compile
time, wires the routing layer accordingly, and at runtime invokes
the node's `execute` with the event payload as input.

A node can declare multiple entry primitives (e.g. both `Webhook`
and `Cron`). Each fires independently and produces a fresh color.

### 3.2 Suspension primitives

Called from inside `execute` via the `ExecutionContext`. Each pauses
the current execution, journals the pulse state, and waits for a
matching event. The execution is suspended in durable storage and
the worker may die. When the event arrives, a new worker spins up,
replays the journal to reconstruct state, and the suspension
primitive returns the resolved value.

```rust
impl ExecutionContext {
    /// Wait for a form submission. Framework mints a token, returns
    /// a URL the caller can surface to humans. When a POST arrives
    /// at that URL, the primitive returns with the form data.
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
where the body reaches a node that's also reachable from the
caller's downstream, compilation fails with a clear error.

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

## 4. The dispatcher (universal daemon)

The dispatcher is the heart of v2. One binary, one source tree, runs
locally as a daemon and in cloud as a deployed service. The CLI and
extension talk to it; it owns runtime state.

### 4.1 Responsibilities

- **Event routing**: serves webhook/form URLs and cron triggers.
  When an event arrives, looks up the matching execution (new or
  suspended) and enqueues a wake.
- **Worker spawning**: when a wake needs to happen, asks the worker
  backend to run the user's compiled binary with the wake context.
- **Infrastructure orchestration**: provisions long-lived infra
  nodes (Postgres, bridges, etc) via the infra backend. Manages
  their lifecycle.
- **Journal (via restate)**: the durable source of truth for all
  runtime state. Dispatcher reads and writes; workers write their
  own progress.
- **Ops dashboard**: serves a web UI for watching live execution
  state, project management, trigger URLs, infra status.

### 4.2 Stateless in cloud, embedded state locally

In cloud, the dispatcher is stateless and horizontally scaled; all
state goes through restate. Any instance can handle any request;
HPA scales on RPS. Autoscaling works because nothing is pinned to
any instance.

Locally, the dispatcher runs restate embedded (single-node) inside
the same process, along with sqlite-backed indices for hot lookups.
Operationally it looks like one binary the user runs; internally the
architecture is the same (dispatcher talks to "a restate").

### 4.3 Backend driver architecture

Two traits define the pluggable boundaries:

```rust
trait WorkerBackend {
    async fn spawn_worker(
        &self,
        binary_path: &Path,
        wake_ctx: WakeContext,
    ) -> WorkerHandle;
    async fn kill_worker(&self, handle: WorkerHandle);
}

trait InfraBackend {
    async fn provision(&self, spec: InfraSpec) -> InfraHandle;
    async fn deprovision(&self, handle: InfraHandle);
}
```

OSS weft implementations:
- `SubprocessWorkerBackend`: spawns the binary as a local process.
- `KindInfraBackend`: creates containers in a local `kind` cluster.

Closed-source weavemind implementations live in the weavemind repo
and plug into the same traits.

### 4.4 Local dashboard

The dispatcher serves a web dashboard for managing the local weft
runtime. Not a code editor; just ops:

- List of registered projects.
- Per project: active executions, suspended executions, trigger
  URLs, infra node status.
- Click an execution to see its graph, which nodes have fired,
  which are waiting, logs per node.
- Buttons: activate / deactivate a project, force-retry, cancel an
  execution.

Served at `localhost:PORT/dashboard/` (or whatever the user
configures). No auth by default on localhost; `weft start --public`
or similar adds a password for shared-machine scenarios.

The dashboard is built as part of the dispatcher. Same binary, same
build, same deploy model (bundled assets via `rust-embed` or
similar).

### 4.5 Event-stream API

Clients (the VS Code extension, the hosted web UI, the local
dashboard) subscribe to runtime state via SSE. One stream per
project:

```
GET /events/project/{id}
```

Server pushes events:
- `execution.started { color, entry_node }`
- `execution.suspended { color, node, wait_metadata }`
- `execution.resumed { color, node }`
- `execution.completed { color, outputs }`
- `execution.failed { color, error }`
- `trigger.url_changed { node_id, url }`
- `infra.status_changed { node_id, status }`

Clients maintain local `Map<NodeId, NodeRuntimeState>` and render
from it. Replaces v1's zoo of polling loops.

### 4.6 Auth on the dispatcher

- **Local default**: bind to localhost, no auth required.
- **Local shared machine**: explicit password/token.
- **Cloud**: auth layer in front (JWT via the website's user
  management). Covered in the weavemind cloud doc.

---

## 5. Execution workers

### 5.1 Lifecycle

A worker is a single invocation of the user's compiled binary. The
dispatcher tells the worker backend to spawn one when a wake needs
to happen. Shape:

1. Dispatcher decides to wake color `C42` at node `N` with value
   `V`.
2. Dispatcher calls `worker_backend.spawn_worker(binary,
   WakeContext { project_id, color, node, value })`.
3. Backend launches the binary (subprocess locally, sandboxed pod
   in cloud, etc).
4. Binary loads, reads the project's journal entries for color C42
   from restate (via a shared restate client configured by the
   dispatcher), replays to reconstruct state.
5. Binary resumes at node N with value V.
6. Binary runs until it suspends or completes, writing journal
   entries as it goes.
7. On suspend: binary writes the suspension entry, tells restate
   "wake me when event X arrives," exits.
8. On complete: binary writes the completion entry, exits.
9. Backend tears down the worker.

The binary is ignorant of which backend is running it. It talks to
restate via configured URL and credentials, that's it.

### 5.2 Isolation

Every worker runs untrusted user code (the weft graph's nodes,
including potentially ExecPython, HTTP calls, whatever the user
wrote). Isolation is mandatory.

**Locally**: worker is a subprocess. The local machine trusts the
user who wrote the code. Process isolation is acceptable.

**Cloud and enterprise**: each worker runs in its own microVM. No
shared kernel between workers. Attack surface is the hypervisor,
which is minimal. Specific implementation details (which hypervisor,
which orchestrator) live in the weavemind cloud doc. From the weft
side, the contract is: `WorkerBackend` implementations for hosted
environments MUST provide microVM-strength isolation per worker.

---

## 6. Project structure and the CLI

### 6.1 Project layout

```
myproject/
  weft.toml              Manifest: name, version, dependencies.
  main.weft              Graph definition (entrypoint).
  main.loom              Runner UI definition (optional).
  nodes/                 User-authored rust nodes.
    my_custom.rs
  vendor/                Imported third-party nodes (via `weft add`).
  .weft/                 Local state cache (binary builds, etc).
  .git/                  Standard git repo. Project is a git repo.
```

Loom and weft coexist as sibling files. Same project, one file
authors the graph, another authors the runner view (like having
a frontend and backend in the same repo). The VS Code extension
renders `.weft` files as graph previews and `.loom` files as
runner previews.

`weft.toml` is a cargo-style manifest:
- Package metadata.
- Node dependencies (stdlib + user `nodes/` + `vendor/` + external
  packages if the ecosystem grows).
- Dispatcher connection (e.g. `dispatcher = "http://localhost:9999"`
  locally, a cloud URL in hosted contexts).

### 6.2 The CLI is a client, the dispatcher is the server

This is load-bearing and worth naming explicitly. The `weft` CLI
does not own execution. The dispatcher does. Every CLI command
maps to an HTTP call against the dispatcher. This is the same
relationship as `kubectl` to a k8s API server, or `docker` CLI to
the daemon.

Consequences:
- `weft run` does NOT own the execution's lifecycle. Ctrl-C on
  the terminal disconnects the CLI, it does NOT kill the execution.
- To actually kill an execution, `weft stop <color>`.
- The VS Code extension is a parallel client talking to the same
  dispatcher HTTP API. CLI and extension are peers, not a stack.
- Same binary, same API whether the dispatcher is local
  (laptop daemon) or cloud (our hosted dispatcher). CLI switches
  targets via config.

### 6.3 CLI commands

```
weft new <name>                  Scaffold a new project (git init,
                                  main.weft, main.loom, weft.toml).
weft build                       Compile to a native rust binary.
weft run [--detach]              Compile if needed. Register the
                                  project with the dispatcher and
                                  start a new execution. Blocks in
                                  the terminal, streaming logs,
                                  until the execution completes or
                                  suspends. Ctrl-C disconnects the
                                  log stream; the execution keeps
                                  running. --detach returns
                                  immediately without streaming.
weft follow <project|color>      Subscribe to the dispatcher's SSE
                                  stream and render live state in
                                  the terminal. Works against the
                                  project id (shows all active
                                  executions for that project) or
                                  a specific color (shows one).
weft stop <color>                Cancel a running or suspended
                                  execution. Drops pulses, removes
                                  wake entries, marks cancelled.
weft deactivate <project>        Take the project offline.
                                  Triggers stop firing, URLs die,
                                  pending suspended executions are
                                  cancelled. Project stays
                                  registered (see `weft rm`).
weft activate <project>          Bring a previously-deactivated
                                  project back online with fresh
                                  trigger URLs.
weft ps                          List every project registered
                                  with the dispatcher. Shows
                                  status (active / deactivated),
                                  active execution count, last run.
weft rm <project>                Remove a project from the
                                  dispatcher entirely. Journal is
                                  gone, logs are gone.
weft logs <project|color>        Historical + live logs for a
                                  project or a specific execution.
weft start                       Start the local dispatcher daemon
                                  (if not already running). No-op
                                  in hosted workspaces (cloud
                                  dispatcher is already there).
weft daemon-stop                 Stop the local dispatcher daemon.
                                  Named distinctly from `weft stop`
                                  which cancels an execution.
weft status                      Terminal view of the dashboard
                                  for the connected dispatcher.
weft infra up / down             Provision or tear down infra
                                  nodes for the current project.
```

### 6.4 Execution lifecycle via the CLI

What actually happens when the user types `weft run`:

1. CLI parses `weft.toml` for the project id and dispatcher URL.
2. If the binary isn't built or is stale, compile (see 6.6).
3. CLI uploads the binary to the dispatcher (if the dispatcher
   doesn't already have it; usually content-addressed).
4. CLI calls `POST /projects/{id}/run`. Dispatcher mints a new
   color, spawns a worker, returns the color and an SSE stream URL.
5. Unless `--detach`, CLI opens the SSE stream, renders log lines
   and state changes until the stream closes (execution completed,
   failed, or the user Ctrl-C'd).
6. On Ctrl-C: the CLI closes its stream and exits. The dispatcher
   does not hear about it; the execution keeps going.
7. Execution can later be watched again via `weft follow <color>`
   or cancelled via `weft stop <color>`.

This is the only code path. Pure programs, triggered programs,
and suspended-then-resumed programs all go through the same
`POST /projects/{id}/run` shape (triggered programs just have
the dispatcher invoking it internally on event arrival; pure
programs complete quickly; HITL programs suspend and resume).
No "simple mode" that bypasses the dispatcher.

### 6.5 Project lifecycle

Projects in the dispatcher have a simple state machine:

```
  (not registered) -- weft run  --> active
                   \  weft run --> active
  active -- weft deactivate --> deactivated
  deactivated -- weft activate --> active
  (any) -- weft rm --> (not registered)
```

A project stays registered with the dispatcher after its last
execution completes. You can `weft logs` a completed execution
days later. Manual prune only, no auto-GC of projects. (Individual
suspended executions may have their own GC policy, see open
questions; projects themselves are persistent.)

Analogy: `docker ps -a` showing exited containers until `docker rm`.
Same intuition.

### 6.6 Dispatcher connection semantics

`weft` CLI talks to the dispatcher at the URL in `weft.toml` (or
`WEFT_DISPATCHER_URL` env var). Default = `http://localhost:9999`.

Local dev: dispatcher is the laptop daemon (`weft start`). Works
offline. Binary is uploaded to the daemon on `weft run`.

Hosted workspace: dispatcher URL is pre-set by the workspace
template to the cloud dispatcher's endpoint. Same CLI binary, same
commands, no behavior change.

CI / automation: set the env var, same CLI works.

### 6.7 `weft build` pipeline

1. Parse all `.weft` files (main + imports).
2. Resolve imports, build the combined graph.
3. Validate (type check, callback isolation, entry-point detection,
   required-port coverage).
4. Enrich (type inference, TypeVar resolution, cf. v1's
   weft-nodes/enrich).
5. Codegen rust source: the graph is emitted as a static structure,
   all referenced nodes are linked in (from stdlib, `nodes/`,
   `vendor/`).
6. Invoke cargo to produce the binary.

Output is written to `.weft/target/`. `weft run` uses the cached
binary if the source hash matches.

---

## 7. Node API

Nodes are rust structs implementing `Node`. The trait exposes
metadata and `execute`. Primitives are called through
`ExecutionContext`.

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
    /// Whether this node needs wiring to an infra node. Falls out
    /// of entry primitives containing Event.
    pub requires_infra: bool,
}
```

Compare to v1's `features: NodeFeatures` struct with
`isTrigger: bool`, `triggerCategory: Option<TriggerCategory>`,
`requiresRunningInstance: bool`. In v2 all of these are derived from
the presence of entry primitives.

### 7.1 Example: `ApiPost` in v2

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

### 7.2 Example: `HumanQuery` in v2

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

## 8. VS Code extension

The VS Code extension is the primary authoring UX for Weft. It is
distinct from the dispatcher's ops dashboard: the extension is for
building, the dashboard is for watching.

### 8.1 Surfaces

- **Tangle side panel** (left): AI assistant, chat with Tangle
  about the current project. Reads the open files, helps write
  nodes and graphs. Same shape as the GitHub Copilot or Claude
  Code side panels in VS Code.
- **Graph view** (editor tab, opens from a `.weft` file): renders
  the graph visually. Nodes, edges, ports. Live-updates as the
  user edits the `.weft` source. Click "visualize" on a `.weft`
  file to open it.
- **Loom view** (editor tab, opens from a `.loom` file): renders
  the runner UI preview. Shows what end-users will see when
  running the project.
- **Right sidebar panel**: dispatcher connection + live project
  state. Similar to the MongoDB extension's resource browser.
  Shows:
  - Dispatcher connection status (green/red, URL).
  - Registered projects.
  - Per project: active executions, infra state, trigger URLs,
    suspended forms.
  - Action buttons: run, stop, deactivate, view logs.
  - Driven by the dispatcher's SSE stream.

### 8.2 Local + hosted parity

Same extension works locally and in hosted workspaces. The
dispatcher URL determines the target:
- Local: extension talks to `localhost:9999` (the user's daemon).
- Hosted: extension talks to the cloud dispatcher's URL
  (pre-configured in the hosted workspace).

Everything else is identical. The extension does not know (or care)
whether it's local or hosted.

### 8.3 Distribution

Published to the VS Code Marketplace. Open-source. In hosted
workspaces, pre-installed in the workspace image.

---

## 9. Runtime architecture (single-process local)

The minimum runtime needed to execute a Weft program locally. This
is what `weft run` against a local dispatcher uses.

### 9.1 Components (all inside the dispatcher process)

- **Pulse queue**: holds pulses waiting to be delivered to nodes.
- **Pulse dispatcher (in-process)**: pops from the queue, routes
  pulses to node handlers, collects outputs, re-enqueues.
- **Journal** (embedded restate): durable record of everything for
  replay and resumption.
- **Wake index**: mapping from external-event tokens (webhook path,
  form token, timer wake time) to `(color, suspended_pulse)`.
- **HTTP facade**: receives webhook/form URL hits, looks them up
  in the wake index, schedules wake events. Also serves the ops
  dashboard and the SSE event stream.

### 9.2 Execution loop (pseudocode)

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

### 9.3 Journal layout

One journal entry per pulse emitted and per suspension entered.
Entries keyed by `(color, sequence_number)`. Replay for a given
color reads all entries for that color in sequence and rebuilds the
in-memory pulse queue state.

Backed by restate (embedded locally, managed in cloud). Same API
both sides.

### 9.4 Wake index

Table inside restate's store. Rows are `(token, kind, color,
suspend_id)` where `kind` is `Entry | Suspension`. TTL-managed:
entry tokens live until the project is deactivated; suspension
tokens live until the execution completes or is cancelled.

### 9.5 Cancellation

Cancelling color C:
- Mark all journal entries for C as cancelled.
- Drop all queued pulses with color C.
- Remove all wake-index rows for C.
- Deliver a cancellation signal to any in-flight `execute` call
  under C (via `ExecutionContext::cancellation_token`).

After cancel, no future event for C will wake anything.

---

## 10. How v1 features map to v2

| v1 feature | v2 replacement |
|------------|----------------|
| `triggers` table with `status`, `pending_action`, etc. | Wake-index entries in restate, keyed by color. |
| `TriggerService` + `TriggerHandle` + `keep_alive` | Infra nodes (for connections) + entry primitives (for everything else). |
| `TaskRegistry` as a separate service for human tasks | Unified with execution journal under `await_form`. |
| Orchestrator `ExecutorState.executions: DashMap` | Journal is the source of truth; workers are ephemeral. |
| `NodeCallbackRequest::WaitingForInput` + oneshot channels | `ctx.await_form` primitive. |
| Dashboard as a separate deployable | Dashboard is served by the dispatcher. |
| Website as project editor | Website is just auth + project launcher; authoring moves to VS Code (local or hosted workspace). |
| Implicit expand/gather | Opt-in via explicit `expand` / `gather` (see ROADMAP). |
| `triggerCategory` enum read in ten places | Derived from entry primitives declared on the node. |

---

## 11. Migration plan (weft side)

v1 users need a path forward. v2 is not drop-in; it's a rewrite. v1
and v2 can run side by side during a transition.

### 11.1 Phase 1: v2 minimal end-to-end (local)
- Implement core primitives (pulse, color, lane, await_*, entry).
- Implement pulse queue + journal + embedded restate.
- Port stdlib nodes (Text, Gate, LLM, HumanQuery, ApiPost, Cron)
  to the v2 Node trait.
- `weft build` produces a working binary.
- `weft run` registers with a local dispatcher, simple programs
  work.
- `weft run` works for programs with HumanQuery (exercises
  suspend/resume).
- Local dispatcher serves the ops dashboard.

### 11.2 Phase 2: VS Code extension
- Tangle panel in VS Code.
- Graph view from `.weft` files.
- Loom view from `.loom` files.
- Right sidebar showing dispatcher state.

### 11.3 Phase 3: broader stdlib + compiler maturity
- Port remaining stdlib nodes.
- Multi-file imports.
- `weft new` scaffolding.
- `weft add` for dependencies.

### 11.4 Phase 4: migration tooling
- `weft migrate-from-v1 <project>` converts a v1 weft file to v2
  format where automatic conversion is safe.
- Some constructs need manual conversion (anywhere v1 relied on
  implicit mechanisms that v2 makes explicit).

### 11.5 Phase 5: v1 shutoff
- Deprecation notice with date.
- v1 codebase frozen, bug fixes only.
- Eventually, v1 endpoints retired.

---

## 12. Open questions

### 12.1 Entry-point detection
The compiler determines a node is an entry point by seeing it has
entry primitives in its metadata. Is that enough, or do we also
need an explicit `#[entry_point]` attribute on the impl? Preference:
derive from metadata. If ambiguous cases emerge, add the attribute
as an override.

### 12.2 `await_first` composition
`tokio::select!`-style waiting on multiple primitives. Not ship-day
but the primitive needs to exist in the design so we don't paint
ourselves into a corner.

### 12.3 Error handling primitive
Roadmap says try/catch. Does catching use the suspension primitive
shape (await_result with error variant) or a separate mechanism?
Probably separate because errors are cross-cutting, but worth
thinking about.

### 12.4 Distributed subprograms
Cross-machine color propagation, serialization format for pulses.
Out of scope for v2 ship; architected-in via color+lane being
serializable.

### 12.5 `weft.toml` schema
Manifest format, dependency declaration, node registration.
Probably copy `Cargo.toml` closely. No strong opinion yet.

### 12.6 Compile-time vs runtime node registration
v1 uses `register_node!` to build a static registry. v2 can build a
`const` table at compile time from imports. Requires the compiler
to discover and link all nodes a project uses.

### 12.7 Testing strategy
- Unit tests on primitive impls (pulse propagation, color rules,
  isolation validation).
- Integration tests running compiled binaries end-to-end
  (HumanQuery roundtrip, callback isolation, cron fire).
- Property tests on journal replay: for any sequence of pulses +
  suspensions, replay reconstructs identical state.

### 12.8 Restate licensing
Restate is BSL-licensed (converts to Apache 2.0 four years after
each release). We use it through a thin adapter so an exit later
is "swap the journal backend" rather than a rewrite. Worth
documenting this adapter boundary explicitly in the implementation
so migration is painless if it ever happens.

### 12.9 Worker startup cost for hosted environments
Locally, subprocess startup is ~20ms. In cloud, workers need real
isolation (microVM per worker), which has a higher cold-start
cost. The exact cold-start number depends on the hosted runtime
(this decision lives in the weavemind cloud doc). Design-wise, the
weft contract is "workers are ephemeral, startup is not zero-cost";
nodes should not assume instant workers.

---

## 13. What v2 does NOT do (deliberately)

- No interpreter, no bytecode, no hybrid compile. Single rust
  target.
- No per-node microservice mesh. Nodes are rust, linked into the
  binary.
- No dynamic node loading at runtime. Node set is fixed at compile
  time.
- No DSL for defining nodes outside rust. Nodes are rust; weft
  programs are weft.
- No backwards compatibility with v1 graph files. Migration is
  explicit (Phase 4).
- No "simple mode without durability." All programs journal. Pure
  programs just happen to never suspend.

---

## 14. Reading order for implementers

1. Sections 0-1 (motivation and locked decisions) for context.
2. Section 2 (pulse primitive) for the core mental model.
3. Section 3 (node primitives) for the API surface.
4. Section 7 (node examples) to see how nodes look in code.
5. Section 4 (dispatcher) for the daemon architecture.
6. Section 5 (workers) for the execution lifecycle.
7. Section 6 (project structure and CLI) for user-facing shape.
8. Section 8 (VS Code extension) for authoring UX.
9. Section 9 (single-process runtime) for the execution loop
   internals.
10. Section 11 (migration plan) for phasing.
11. Section 12 (open questions) before making unilateral decisions.

---

## 15. What to build first

The first implementation milestone is the minimal end-to-end loop:
a weft program with one entry node (ApiPost) and one mid-execution
suspend (HumanQuery), compiled via `weft build`, run via
`weft run` against a local dispatcher, webhook triggered by `curl`,
form answered via second `curl`. Everything else builds on this.

Concrete sub-tasks for the first milestone:

1. Define core types: `Pulse`, `Color`, `Lane`,
   `ExecutionContext`, `EntryPrimitive`, `SuspensionPrimitive`.
2. Implement the pulse queue + journal interface + embedded
   restate.
3. Implement the dispatcher binary with subprocess worker backend
   and kind infra backend.
4. Port `ApiPost` and `HumanQuery` to the v2 Node trait.
5. Implement `weft build` producing a binary (hardcoded graph is
   fine for the milestone; full compiler comes after).
6. Implement the dispatcher HTTP API: `POST /projects/{id}/run`,
   `POST /executions/{color}/cancel`, SSE stream on
   `GET /events/project/{id}`.
7. Implement the `weft` CLI as a thin client of that API
   (`weft run`, `weft stop`, `weft follow`).
8. Serve the ops dashboard at `localhost:PORT/dashboard/`.
8. Manual testing: curl the webhook URL, verify execution starts,
   curl the form URL, verify execution resumes, watch state in
   the dashboard.

This milestone validates the core primitives, the dispatcher
daemon, and the suspend/resume path. Multi-node programs,
callbacks, loops, multi-file imports, VS Code extension, all come
after.
