# Glossary

Terms weft uses in a specific way. If a page used a word and you were not sure
it meant what you assumed, it is here.

**Access.** The port type for the authorized ability to call a third party. One
type for every service. What flows on the wire is a small reference, never a
credential. See [How connections work](../connections/overview.md).

**Access node.** The node that owns the connection for one service and emits an
`Access` value. Most bodies are one macro; the `connection_optional` ones
write their own.

**Activation.** Turning a project's triggers on: registers every trigger and
mints its address. A trigger whose address cannot be served fails the
activation, loudly.

**Broker.** The scoped HTTP front door to the database that every tenant-side
component uses. The dispatcher bypasses it. See
[How the runtime is built](../running/architecture.md).

**Bus.** A live channel between nodes alive at the same time. Any number of
participants, any direction. See
[Live channels](../language/live-channels.md).

**Closed pulse.** A pulse carrying no value, meaning "nothing will ever arrive
here, at this color, at these frames". On a required input it skips the node
and cascades. This is how branching works. See
[the closure rule](../language/mental-model.md#how-a-branch-stops-the-steps-after-it).

**Color.** One execution. A re-run is a new color, so "per color" always means
per execution.

**Connection.** An account somebody hooked up to a service. It lives in the
access store and holds everything secret. A project's source holds a bare id.

**Dispatcher.** The control plane. Routes events, manages lifecycle, owns the
journal, hosts the trigger and form URLs. Never runs user code.

**Door.** How a connection is obtained. `shared` means a credential this weft
holds; `own` means the user brings or creates their own.

**Example.** A run spec saved as `examples/<name>.json`: which part of the
graph to run, what to hand it, and which trigger to fire. `weft run <name>`
runs it again. See [Versions, seeded runs and frozen
examples](../running/versions.md).

**Firing.** One call to a node's body, at one color and one frame stack. A node
can be firing several times at once inside a parallel loop.

**Frames.** A stack of loop iteration indices. Two pulses only meet at a node
if their frames match, which is what keeps iterations from mixing.

**Frozen example.** Saved starting parameters plus accepted output history
in `expected`, with optional nodes to focus on during review. Run it on
current code, then inspect its diff. See
[Freezing an accepted run](../running/versions.md#freezing-an-accepted-run).

**Gather port.** A loop output that collects one value per iteration. Typed
`List[T | Null]`, because an iteration can fail to write.

**Generator[T].** A typed one-way terminating stream. Exactly one producer,
exactly one consumer.

**Group.** A subgraph with typed boundary ports, behaving as one node from
outside. See [Groups](../language/groups.md).

**HEAD.** The version your next checkpoint or run is recorded beneath, and
`weft branch` moves it too. It is also where `--seed` starts looking: see the
**Seed** entry.

**Infra node.** A node that needs a long-running process, declared as a typed
spec that the supervisor compiles to Kubernetes manifests.

**Journal.** The append-only record of an execution: one row per event. Not a
log. It is the state, in replayable form. See
[The journal](../running/the-journal.md).

**Listener.** The tier that holds live event sources. The only tier that knows
about signal kinds. Never touches the database.

**Meter.** The per-provider code that computes the real cost of a paid call
from the bytes. A node never states a cost.

**Pulse.** One emission travelling to one input port, carrying a value, a
color, and a frame stack. The only thing that moves in a running program.

**Supplied input.** A backup input handed to a node at a `--from` or
`--group` start, or an output supplied through `--emit`. A supplied input is
a backup at a named start, nothing more: real execution input takes
precedence over it, and changed supplied values invalidate affected reuse.

**Recipe.** The `service` block in an access node's metadata: how a credential
is acquired, how a request is signed, what the permissions are, how events
arrive.

**Registered app.** One OAuth application this weft signs users in with, living
in the operator's trusted apps file. Its credential stays in the access store
and can never be extracted from it: a recipe may use the credential, never read
it.

**Root.** A node no wire feeds. A manual run kicks ordinary roots in its
selection. Triggers require an explicit fire or supplied outputs.

**Scope** (run). Which part of the graph a run executes, set by `--from`,
`--emit`, `--target`, `--before` or `--group`, or by saved parameters. See
[Running one group, or one node onward](../running/versions.md#running-one-group-or-one-node-onward).

**Scope** (storage). Which of `Execution`, `Project`, or `Shared` a file is
written under. It is a lifetime contract, not a folder name.

**Seed.** The run a `--seed` run inherits from. By default it is head's run,
once that run has finished or parked on a signal; if head has no run, it is
the newest finished or parked run on head's version, or on the nearest
ancestor version that has one. For which of its nodes are taken and which run
again, go and read the **Reusable work** entry below; for how weft picks a seed when
head has no run, go and read
[Seeding](../running/versions.md#seeding-run-only-what-changed).

**Sequential Diffusion Programming.** Building a program stage by stage against
a real example, then a second, then a third, until new inputs just work.
[The chapter](../thinking/sdp.md).

**Signal.** A wake source: a timer, a form, an endpoint, a subscription, a held
socket. Registered by a trigger, or awaited mid-flow.

**Reusable work.** What a `--seed` run may inherit
rather than run itself. A seeded run reuses eligible completed work, and work
is no longer reusable when:

- you edited it, or anything upstream of it;
- it is new since the seed, or the seed's run never covered it;
- it is a root whose kick payload changed, or one the seed never kicked;
- its supplied starting inputs changed;
- the seed ran it but it failed, was cancelled, is still running, or is
  parked waiting on somebody: the question it asked belongs to the seed's
  run, so answering it would wake the seed rather than this run, and the
  node asks again;
- it fired several times inside a loop and did not complete or get skipped
  in every one of them;
- it is beyond the permitted `--seed-before` or `--seed-until` boundary;
- its output contains a live handle tied to the earlier run.

Anything downstream of a reusable node that changed is not reusable either,
and a loop is reused whole or not at all.

**Supervisor.** The tier that runs kubectl for user infrastructure. One holds a
lease per project.

**Suspension.** A parked firing waiting on a signal. The worker exits. The
execution costs rows and no compute.

**Trigger.** A node that starts an execution from outside. Two phases: setup
at activation, then a fire per event.

**Unit.** One pod template inside an infra spec. Each has its own status and
its own stop behavior, and the infra verbs act on one at a time.

**Version.** The project's program files (`src/`, `weft.toml`, `nodes/`,
`assets/`, `examples/`, plus the installed weft's own version). A version is
named by a hash of those contents, so the same code is always the same version
however many times you run it. Your `layouts/` and your notes are not in it.
The seeded `nodes/base_catalog/` is not listed file by file, but its content
hash rides along with the installed weft's version, so upgrading the catalog
changes the version like any edit. Every run and every checkpoint records one.

**Worker.** The compiled project binary, running as a pod, multiplexing
executions and shutting down when idle.
