# Glossary

Terms weft uses in a specific way. If a page used a word and you were not sure
it meant what you assumed, it is here.

**Access.** The port type for the authorized ability to call a third party. One
type for every service. What flows on the wire is a small reference, never a
credential. See [How connections work](../connections/overview.md).

**Access node.** The node that owns the connect for one service and emits an
`Access` value. Most bodies are one macro; the `connection_optional` ones
write their own.

**Activation.** Turning a project's triggers on: registers every trigger and
mints its address. One that cannot be served refuses here, loudly.

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

**Firing.** One call to a node's body, at one color and one frame stack. A node
can be firing several times at once inside a parallel loop.

**Frames.** A stack of loop iteration indices. Two pulses only meet at a node
if their frames match, which is what keeps iterations from mixing.

**Gather port.** A loop output that collects one value per iteration. Typed
`List[T | Null]`, because an iteration can fail to write.

**Generator[T].** A typed one-way terminating stream. Exactly one producer,
exactly one consumer.

**Group.** A subgraph with typed boundary ports, behaving as one node from
outside. See [Groups](../language/groups.md).

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

**Recipe.** The `service` block in an access node's metadata: how a credential
is acquired, how a request is signed, what the permissions are, how events
arrive.

**Registered app.** One OAuth application this weft signs users in with, living
in the operator's trusted apps file. A recipe may use one and can never extract
from it.

**Root.** A node no wire feeds. A manual run kicks every root at the top level;
a scope's own roots are kicked when the scope starts.

**Scope** (storage). Which of `Execution`, `Project`, or `Shared` a file is
written under. It is a lifetime contract, not a folder name.

**Sequential Diffusion Programming.** Building a program stage by stage against
a real example, then a second, then a third, until new inputs just work.
[The chapter](../thinking/sdp.md) is the whole methodology.

**Signal.** A wake source: a timer, a form, an endpoint, a subscription, a held
socket. Registered by a trigger, or awaited mid-flow.

**Supervisor.** The tier that runs kubectl for user infrastructure. One holds a
lease per project.

**Suspension.** A parked firing waiting on a signal. The worker exits. The
execution costs rows and no compute.

**Trigger.** A node that starts an execution from outside. Two phases: setup
at activation, then a fire per event.

**Unit.** One pod template inside an infra spec. Each has its own status and
its own stop behavior, and the infra verbs act on one at a time.

**Worker.** The compiled project binary, running as a pod, multiplexing
executions and shutting down when idle.
