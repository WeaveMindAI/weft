# Glossary

**Access.** The port type for permission to call a service. One access node
holds the connection and emits an `Access`; every node that calls that service
takes one as an input. Nothing secret is in it.

**Access node.** The node that owns the connection for one service. Its body is
one line; everything it does is declared in its
[service recipe](../connections/declaring-a-service.md).

**Activation.** Turning a project's triggers on. It runs each trigger's setup,
which registers subscriptions with providers and dials sockets, then leaves the
listeners running. `weft activate`.

**Asset.** A file pulled into your project at build time with `@asset`. Never
written back to.

**Bake.** A trigger's settings, prepared and saved without anything listening.
`weft bake`. What you want while building.

**Broker.** The scoped front door to the database that every tenant-side
process uses. Workers, listeners and supervisors ask it; only it and the
dispatcher touch Postgres.

**Bus.** A live channel between nodes that are running at the same time. Any
number of participants, each with its own position. It lives exactly as long as
its worker.

**Closure.** A pulse carrying no value, meaning nothing will ever arrive here
for this run. It is how a step says no rather than saying nothing. Not `null`,
which is data.

**Color.** One execution. A re-run is a new color, so "per color" always means
per run. The first eight characters are enough to name one.

**Connection.** One account somebody connected to a service. Its credential
lives in weft's store, sealed, and never in your program.

**Dispatcher.** The control plane. It routes events, decides what runs where,
and answers every request about a project or a run.

**Door.** How a connection is obtained. `shared` uses a credential weft holds;
`own` means you bring your own.

**Drift.** Your source having moved ahead of what is running. `weft status`
names which kind and which verb fixes it.

**Example.** A run's starting parameters, saved as `examples/<name>.json`. A
**frozen** example also holds the outputs you accept as right.

**Firing.** One go at one step, identified by its run, its step and its
frames.

**Frames.** The stack of loop iteration numbers a firing sits inside. weft only
combines inputs whose run and frames match, which is what stops iteration three
eating iteration four's answer.

**Gather port.** A loop output that collects one value per iteration. It has to
be `List[T | Null]`, because a failed iteration leaves an empty slot.

**Generator.** A stream: one producer, one consumer, ordered, typed, and it
ends. A closure on one is its end rather than a skip.

**Group.** Several steps under one name, with declared inputs and outputs. It
does not exist when the program runs.

**head.** The version your next checkpoint sits under, and the run a `--seed`
inherits from.

**Infra node.** A node that needs a container of its own. Nothing starts it for
you.

**Journal.** The append-only record of a run, one row per event. It is what the
graph shows you and what rebuilds a run that was interrupted.

**Keep.** Marking a file to survive the sweep that clears a run's storage.
Additive, and there is no un-keep.

**Listener.** The tier that holds the timers, the sockets and the
subscriptions. The only tier that tells one kind of event source from another.

**Meter.** The code that works out what one provider's call really cost. A node
never states a cost.

**Node.** One step of a program. On disk, a folder with a `metadata.json` and a
`mod.rs`.

**Pulse.** One emission travelling to one input, carrying a value, a color and
a frame stack. The only thing that moves in a running program.

**Recipe.** The `service` block in an access node's metadata: how a credential
is obtained, how a request is signed, what the permissions are, how events
arrive.

**Registered app.** One OAuth application this installation signs people in
with, living in the operator's apps file. Its secret never leaves the store.

**Resync.** Deactivate and re-activate in one go, against your current program.
What you run after editing anything a live trigger reads.

**Root.** A step no wire feeds. A manual run starts every root at the top
level, plus every trigger.

**Scope (run).** Which part of the graph a run covers, set by `--from`,
`--target`, `--before`, `--group` or `--fire`.

**Scope (storage).** Which of execution, project, shared or asset a file
belongs to. It decides where new files go and how long they live.

**Seed.** The run a `--seed` run inherits from. By default head's run.

**Sequential Diffusion Programming.** Building a program stage by stage against
a real case, rather than describing the whole thing and hoping.

**Signal.** Something that wakes a step: a timer, a form, an endpoint, a
subscription, a held socket. Registered by a trigger, or awaited mid-flow.

**Slug.** The stable name on a compiler finding, like `type-mismatch`. The slug
names the rule; the message names the fix.

**Stuck.** A run where steps are holding values that will never add up to
enough to fire them, and nothing is parked on a signal. Nobody can answer, so
weft ends it and names every step involved.

**Supervisor.** The tier that runs kubectl for your infrastructure. One holds
an exclusive lease per project.

**Tag.** A label a run puts on itself, which another run can use to stop it.

**Tangle.** The weft specialist `weft new --assistant` copies into your
project.

**Trigger.** A node that starts a run from outside. Two bodies: setup, which
runs once at activation, and run, which fires on each event.

**Unit.** One pod template inside an infrastructure spec. Most nodes have
exactly one.

**Version.** A snapshot of your project's files, recorded on every run and by
`weft checkpoint`.

**Waiting for input.** A run parked on a person or a service. The worker shuts
down and costs nothing, and a fresh one picks the run up when the answer lands.

**Worker.** Your compiled program, running as a pod, serving as many runs at
once as it can. It shuts itself down 30 seconds after it has nothing left.
