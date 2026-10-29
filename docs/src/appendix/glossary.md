# Glossary

**`_error`.** The output every node has for a failure of its own work (a
service refusing, a script raising, a wait that gave up). If it is wired, that
failure goes down it as text and the node's other outputs close; if it is not,
the failure fails the run. A mistake in the program, like a bad setting, fails
the run either way. A node that already sent something on a `Bus` or
`Generator` output fails the run either way.

**Access.** The port type for permission to call a service. One access node
holds the connection and emits an `Access`; every node that calls that service
takes one as an input. Nothing secret is in it.

**Access node.** The node that owns the connection for one service. Its body is
one line; everything it does is declared in its
[service recipe](../connections/declaring-a-service.md).

**Activation.** Turning a trigger on. It runs the trigger's setup, which
registers subscriptions with providers and dials sockets, then leaves the
listener running. Each trigger is turned on separately, and a trigger that
exists once per instance is turned on separately for each instance. The command is
`weft activate`, which turns on what is off and leaves what is on alone.

**Asset.** A file pulled into your project at build time with `@asset`. Never
written back to.

**Bake.** A trigger's settings, prepared and saved without anything listening.
The command is `weft bake`, and it is what you want while building.

**Baked output.** An output of an infrastructure node that weft makes when the
infrastructure is applied and then saves. A run that reads nothing else from
the node uses the saved value instead of running it. `weft infra rebake
<node>` makes it again. Unrelated to `weft bake`.

**Behind.** A trigger that is on but fires an older version of what it runs,
or an infra copy running from older source than you have now. `weft resync`
brings a trigger up to date, and `weft infra upgrade` does the same for infra.

**Broken part.** A piece of the program that does not compile: a file that does
not parse, an include that does not resolve, a node type neither the catalog nor your project has, a wire
whose types do not fit. A command refuses only when what it runs needs a broken
part, and names it; otherwise it goes on and prints the broken part as a
warning.

**Broker.** The scoped front door to the database that every tenant-side
process uses. Workers, listeners and supervisors ask it; only it and the
dispatcher touch Postgres.

**Bus.** A live channel between nodes that are running at the same time. Any
number of participants, each with its own position. It lives exactly as long as
its worker.

**Closure.** A pulse carrying no value, meaning nothing will ever arrive here
for this run. It is how a node says no rather than saying nothing. Not `null`,
which is data.

**Conflict copy.** A file the sync writes beside one of yours that changed
both in your folder and in the install: `tone.weft-conflict-1a2b3c4d.txt`
beside `tone.txt`. It is never compiled and never in a version; delete it
once you have taken what you want from it.

**Connection.** One account somebody connected to a service. Its credential
lives in weft's store, sealed, and never in your program.

**Cut.** The part of the program a run actually runs: the nodes the walk keeps
and the wires between them, built as a smaller program. Every run has one, and
so does each trigger, from the trigger down to what it reaches and back
up for what that needs.

**Dispatcher.** The control plane. It routes events, decides what runs where,
and answers every request about a project or a run.

**Door (connection).** How a connection is obtained. `shared` uses a credential
weft holds; `own` means you bring your own.

**Door (trigger).** What a trigger that is down does with what arrives:
`take` holds it, `hide` holds it without listing it for the people who
answer your program's questions, `refuse` takes nothing. You pick one with `--door` on `weft deactivate`, `weft resync` or the
`weft infra` commands that stop infra; the default is `take`.

**Drift.** Your source having moved ahead of what is running. `weft status`
names which kind and which verb fixes it.

**Durable run.** A run whose trigger has `durable` on: before each node
that is not pure starts, everything the run did so far is written down, so if its worker dies
another one picks it up.

**Example.** A run's starting parameters, saved as `examples/<name>.json`. A
**frozen** example also holds the outputs you accept as right.

**External link.** A link to a stored file for something outside the install
to open: a browser, an app, a provider. It says whether the open internet can
open it (`public`).

**Fast run.** A run whose trigger has `durable` off, the default: it goes as
fast as the program and its record follows behind it. If its worker dies, it
ends cancelled and is not run again.

**Firing.** One go at one node, identified by its run, its node and its
frames.

**Frames.** The stack of loop iteration numbers a firing sits inside. weft only
combines inputs whose run and frames match, which is what stops iteration three
eating iteration four's answer.

**Gather port.** A loop output that collects one value per iteration. It has to
be `List[T | Null]`, because a failed iteration leaves an empty slot.

**Generator.** A stream: one producer, one consumer, ordered, typed, and it
ends. A closure on one is its end rather than a skip.

**Group.** Several nodes under one name, with declared inputs and outputs. No
node stands for it when the program runs: it is a scope that opens or skips as
one.

**head.** Where one working copy is: the version its next version is recorded under,
and the run a `--seed` from it inherits from. Your folder has its own.

**Health rules.** Rules in an infra node's `InfraSpec` saying when to take down
the triggers that need it, because one of its containers stopped being ready,
and when to bring them back. With none
written, while a trigger of the project is on and a container has stayed not
ready past its flaky window, the node takes the triggers that need it down, and
brings them back once it is healthy.

**Held.** The work a trigger kept while it was down: the arrivals its door took,
and the runs it fired that are paused on a wait. `weft activate` runs it,
paced so new arrivals still get through, or drops it with `--held drop`.

**Held back.** A trigger the health check or an infra command took down that
cannot come back by itself, because other triggers are on another build,
and only one build at a time answers a project's routes. `weft resync --all` brings it back.

**Infra node.** A node that needs a container of its own. Nothing starts it for
you.

**Instance.** One separate running copy of part of a program, under an id the
program picks. It can stand for one person, one session, or one of several
things a person owns; which person owns which instances is kept in the
program's own database. A node marked `@per_instance` gets one copy per
instance, and every run is for one instance or for none. See
[programs with instances](../running/instances.md).

**Instance token.** A signal token that acts inside one instance of one
program and can do nothing else: it starts that instance's runs, answers its
waits, shows its displays and connects its accounts. It always expires.

**Internal link.** A link to a stored file for code inside the install, which
goes straight to storage.

**Join.** A node that meets several branches and goes on with what arrived,
like `FirstInOrder`. Its inputs are optional and it needs no gate.

**Journal.** The append-only record of a run, written in rows that each hold
one or more of its events. It is what the graph shows you, and what a worker
reads to pick up a durable run or one that was waiting.

**Keep.** Marking a file to survive the sweep that clears a run's storage.
Additive, and there is no un-keep.

**Left out.** A node folder weft could not load (bad metadata, a name two
folders claim), which `weft describe-nodes` lists with why, or a node package
whose Rust does not compile, which the command that built it prints as a
warning. The worker is built
without it, a run that needs it is refused naming its first error and its build
log, and it is built again once its files change.

**Listener.** The tier that holds the timers, the sockets and the
subscriptions. The only tier that tells one kind of event source from another.

**Meter.** The code that works out what one provider's call really cost. A node
never states a cost.

**Metadata port.** A port whose value is written in the program and never
computed by a run: a group writes it in its body (`self.tool = { ... }`), a
node type in its `metadata.json`. The graph shows a group's in the group's
Config strip, and a node type's nowhere: neither is a port on the box.

**Metadata wire.** A wire leaving a metadata port. It never runs: the compiler
writes the value into the input it points at before the run starts, so it may
point backwards without making a cycle. The graph does not draw it: the input
it fills shows the value and where it comes from.

**Narrowable output.** An output whose catalog type is broad (say `JsonDict`) and
which a program may declare narrower where it uses the node:
`-> (response: Verdict)`.

**Network address.** This machine's IP on its local network, set with
`WEFT_NETWORK_ADDRESS`. The paths outside callers use answer there too, so a
phone on the same wifi can open what a program hands out.

**Node.** One box in a program's graph. On disk, a folder with a `metadata.json` and a
`mod.rs`.

**Open record.** A record type ending in `..`, like `{ email: String, .. }`. It
accepts keys it does not declare, and drops them from the value that flows on.

**Pulse.** One emission travelling to one input, carrying a value, a run id and
a frame stack. The only thing that moves in a running program.

**Recipe.** The `service` block in an access node's metadata: how a credential
is obtained, how a request is signed, what the permissions are, how events
arrive.

**Registered app.** One OAuth application this installation signs people in
with, living in the operator's apps file. Its secret never leaves the store.

**Resync.** Building again and moving the shared triggers that are on and
behind onto the new build (`--instance` or `--all-instances` for instances'
triggers): each is taken down, its runs on the old version finish (or are
cancelled, if you asked for that), and it comes back on the new one. A trigger that is off stays off.
What you run after editing anything a live trigger reads.

**Root.** A node no wire feeds. A manual run starts every root at the top
level, plus every trigger.

**Run.** One go of a program, under a run id given when it starts. Running
again is a new run with a new id.

**Scope (run time).** A group, an included file, or one loop iteration, as the
runtime sees it: each copy of it opens or skips as one.

**Scope (storage).** Which of run, project, shared, asset or instance a
file belongs to. It decides where new files go and how long they live.

**Seed.** The run a `--seed` run inherits from: head's run, or the run you name.

**Sequential Diffusion Programming.** Building a program stage by stage against
a real case, rather than describing the whole thing and hoping.

**Signal.** Something that wakes a node: a timer, a form, an endpoint, a
subscription, a held socket. Registered by a trigger, or awaited mid-flow.

**Slug.** The stable name on a compiler finding, like `type-mismatch`. The slug
names the rule; the message names the fix.

**Snapshot.** What a command reads of your project's files, once, at its start,
and builds from. A file you edit while it runs is not in the build, and
the command ends by naming the files that changed after it.

**Stuck.** A run where nodes are holding values that will never add up to
enough to fire them, and nothing is parked on a signal. Nobody can answer, so
weft ends it and names every node involved.

**Supervisor.** The tier that creates and watches your infrastructure's
containers. One holds an exclusive lease per project.

**Tag.** A label a run puts on itself, which another run can use to stop it.

**Tangle.** The weft specialist `weft new --assistant` copies into your
project.

**Trigger.** A node that starts a run from outside. Two bodies: setup, which
runs once at activation, and run, which fires on each event.

**Unit.** One set of containers inside an infrastructure spec, running
together on one machine. Most nodes have exactly one.

**Upload slot.** A place in storage a program gives a web page, so a visitor's
big file goes straight there, in parts that resume after a dropped connection.
The page then sends the slot to a route instead of the file.

**Version.** Your project's files at one moment, recorded whenever you do
something with the code (a run, a build, an activate, `weft checkpoint`) and
never on a save. Recording the same files again gives the version they
already are.

**Visitor address.** The address where a project's own site passes its
visitors' calls on to weft (`weft frontend visitor-address`). Links to the project's stored files
then go through the site.

**Waiting for input.** A run parked on a person or a service. The worker shuts
down and costs nothing, and a fresh one picks the run up when the answer lands.

**Walk.** How weft works out a run's cut: down along the wires from where the
run starts, then back up from where it ends, stopping at a node you started from (`--from`), at a
trigger nobody fired, and at a node with no inputs.

**Worker.** A process (on your machine, in the project's folder) or a Cloud
Run service (on a cloud install) running your compiled program. For how it starts and stops, go and read
[the worker](../running/architecture.md#the-worker).

**Working copy.** The install's copy of one place a project's code is
edited: your folder (the install keeps the two the same in both directions),
or the program copy, which programs write into when the project
has no folder. Each has its own head.
