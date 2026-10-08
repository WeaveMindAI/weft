# Weft TODO

Larger design work that's been surfaced but deferred. Not bugs (those
get fixed inline); these are architecture decisions that need design
before implementation. Each entry carries its own what/why; a written
plan lives in `docs/` while it is being worked.

## Unify journal holes: a missing or unreadable event is a HOLE, fatal only on the resume frontier

**Problem.** The journal records what the in-RAM run did; it only drives
anything on a respawn. Two ways an event can be bad, two unrelated
sledgehammers. A failed WRITE latches `PoisonOnWriteFailure` and exits
the worker, even though the live run is fine and the node already fired
downstream. An unreadable stored ROW makes `fold_journal` refuse the
whole snapshot and tell the user to `weft clean`, even when the bad row
sits in dead history nothing will resume over. Meanwhile the
dispatcher's read path skips the same row and paints a marker.

**Direction.** One concept: the journal has a hole at some position,
found either at write time or at fold time. Fatality is decided by
POSITION, not by which mechanism found it. A hole off the resume path
is cosmetic (the replay view degrades, the run carries on, and a
respawn may re-run that node, which is the existing at-least-once
semantics). A hole touching the resume frontier (the suspended nodes,
their token resolutions, the pulses their resume consumes) is fatal and
says so. Drop the worker-bail entirely.

**Why deferred.** Three things need designing: the resume frontier
defined precisely at fold time, a way to persist "an event is missing
here" when the failed write left no row to skip, and the
fatal-versus-cosmetic classifier itself.

[Update Notice Warning] If we touch `PoisonOnWriteFailure`,
`fold_journal`'s corruption bail, `fold_to_snapshot`'s
`report_corruption` path, `CorruptionSite`, or the suspension-resume
fold (`SuspensionRegistered` / `NodeSuspended` / `NodeResumed` /
`SuspensionResolved`), revisit this entry.

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
and classifies: unchanged / changed / orphaned (running, not in
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

## Infra isolation between projects

A node author ships container specs, and node packages are untrusted
third-party code. The neutral infra spec (`weft_core::infra::types`) has
no field for a privileged container, a host path or a host namespace, so
a node cannot ask for one. What is not audited yet: on a local install,
which other containers a unit's network reaches (another project's
units, the install's own), and on GCP, what a unit's machine can do with
the project account it runs as beyond pulling its images and reading the
ticket secret. Goal: each answer checked, and the refusals at the
compiler or the host, not "we assume they can't".

[Update Notice Warning] If we touch `crates/weft-platform-local/src/infra_host.rs`
(the unit's network), `crates/weft-platform-gcp/src/accounts.rs` (the
project account's grants), or the infra spec's fields, revisit.

## Held suspensions (warm-worker model)

**Problem.** The durable-replay model dies-and-resumes the worker
on every suspension: a worker dies whenever all lanes park on
`await_signal`, and a fresh worker folds the journal to resume. That's the
right trade for thousands of cheap parked flows (a HumanQuery waiting
days costs only journal rows). But it works against a node that holds
in-process state too expensive to rebuild on replay: a browser session
with thousands of cookies, a long-lived local model load, a warm
connection pool. Replaying the journal doesn't reconstruct that state;
it's gone with the worker.

**Direction.** A `ctx.hold_signal` primitive that opts a node into a
warm-worker model: the future actually awaits in place and the worker
stays alive across the suspension, instead of unwinding and dying.
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
- Honest about the cost: a held suspension pins a worker for its
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
memory (lost the moment the execution suspends or the worker dies) or in
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
- Loops. A firing is per (execution, frames), so this is per iteration; is
  there any case where it should be otherwise?
- Cost. A node that fired four times bills as what.

It pairs with [tags stopping work](#killing-tagged-nodes-inside-one-execution):
fire-on-arrival is what makes a race expressible, and cancelling the
losing branch by tag is what stops it costing money.

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

## Cloud installs: what is still open

Weft installs on GCP (`deploy/terraform/gcp/`, `.github/workflows/install-gcp.yml`):
one machine running `weft-runtime` and Postgres, project workers on Cloud
Run, builds on Cloud Build, wakes on Cloud Tasks, infra nodes on Compute
Engine, and a front door on the machine's own address with Let's
Encrypt certificates. What is left:

- **Nobody has run it on a real GCP project yet.** The Terraform, the
  machine's boot, the Cloud Run, Cloud Build, Cloud Tasks and Compute
  Engine clients, and the certificate issuance are covered by unit tests
  on their request bodies only.
- **The machine's memory.** An e2-micro has 1 GB for Postgres and every
  role; nobody has measured weft's resident size there. If it does not
  fit, the default machine becomes e2-small, which is not free.
- **A connection whose provider refuses a bare IP as its callback**
  (Google) needs the install to have a domain; nothing yet checks with a
  real Slack app whether Slack's request URL verification accepts one.
- **AWS and Azure**, on the same shape: Terraform per cloud and a
  platform crate each.
- **A frontend's database address in CI, on its own.** The deploy workflow
  hands the frontend whatever the `WEFT_FRONT_ENV` secret holds, so the
  program's database door reaches it once someone writes it there
  (`weft infra env --on <target> --into <file>`, then `weft target export
  --front-env <file>`). Nothing fills it in automatically when the program's
  infrastructure comes up.

## To consider: `@install_filled`, a value filled once per install

A connection a program's own node uses is picked per install, in the
install's store, never in the source. A plain value can differ between
installs too (a channel id, a sheet id, an API base address), and today it
can only be written in the source, the same on every install. A marker
like `@install_filled`, the sibling of `@instance_filled` ("filled once per
install, for everybody"), would put such a value in the same store. Not
decided: whether it is needed at all, and whether it would be opt-in per
field.

It might just be environment variables instead: variables set per project
on each install (never install-wide), which a node field reads. Most such
values are the same in dev and prod anyway, are computed by a wired node,
or are an instance's (`@instance_filled`); what is left (a test deployment
beside a real one) is the case env vars already answer everywhere else.

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

## A node's display has no limits, and every number in it is hardcoded

Reading what a node is showing works and costs nothing at the size a
local install runs at. The editor no longer polls: it opens one stream
per graph (`/events/project/{id}/displays`), the dispatcher looks at
each watched node once per 3 seconds however many editors watch it,
sends only what changed, and stops looking the moment the stream closes
(a hidden graph tab closes it). What is left is every bound a real
deployment needs, most of them missing or a literal in the middle of a
function, and the token doors are on the internet-facing surface (the
public door lets the whole `/signal-token/` prefix through, so a client
holding a `--display` token reads from anywhere).

**The one that is not about scale.** The dispatcher reads a container's
`/live` answer with `resp.json::<Value>()` and no byte cap
(`api/infra.rs`, `read_live`). The only bound is a 3 second deadline on
the whole exchange, and three seconds of container-to-runtime bandwidth is a lot
of megabytes landing in the dispatcher's memory. The dispatcher is
shared across tenants, so one tenant's buggy or hostile container can
spike the RSS of the process serving everybody. This one bites at a single
user, not at a thousand.

**The knobs, and what they are today.**

| Knob | Today | Wants |
|---|---|---|
| Bytes the dispatcher will read from a `/live` answer | unbounded | a cap, with a 502 naming the cap and the node; a `Content-Length` refusal before reading a byte |
| Deadline on that read | `Duration::from_secs(3)`, a literal in `read_live` | configurable, and probably shorter than the look interval by construction |
| How often a watched display is looked at | `LOOK_EVERY`, 3s, in `display_feeds.rs` | configurable |
| Deadline on a display's `/action` press | none at all, on purpose (the work is the container's and the wait the user's) | a cap anyway on the token-facing door, where nobody is watching a spinner and a held connection is just a held connection |
| Rate limit on `/signal-token/displays*` | none, and the dispatcher has no rate limiting anywhere | a limit per token, which is the first such limit the dispatcher would have, so it is a decision about the whole outside surface rather than about displays |

**What it should look like.** Every row above is an option with a
default that suits a local install, set where the rest of the
dispatcher's deployment knobs are set, not a literal in a handler. The
defaults are what a person running `./setup.sh` gets and never thinks
about; whoever runs a cloud install moves them.

**The token door still reads on demand.** A client on
`/signal-token/displays/...` gets the whole payload on every request and
is its own poller. Serving it from the same feeds (a stream, or a
conditional read answering 304 on `If-None-Match`) would make a hundred
outside readers of one bridge cost one container hit, as the editor's
readers already do.

**One documentation gap that comes with the cap.** A `data:` URI in an
`image` item is fine for something a container generates in memory (a
300px QR measures about 1.5 KB as a data URI, and base64's 33% on top
of that is noise). Nothing stops a node author putting a real image
there instead, and `image` items already accept a plain URL, so the
stored-file path with its expiring links is the right answer for
anything big. Once there is a cap to name, say all of that in the
node-authoring skill with the number in it.

**Open questions.** Whether the cap is per answer or per item, since one
oversized image among four good items could come back as one unreadable
line rather than a failed read; and whether a rate limit belongs per
token or per (token, node), given that one client legitimately watches
one bridge closely and has no reason to sweep every display it can see.


## A stream writes two journal rows per item

**Problem.** Every item a stream produces writes one `PortEmitted` row
and one `PulsesConsumed` row (`crates/weft-journal/src/events.rs`, the
TODO on `PulsesConsumed`). That is fine for the streams people run
today, but a stream of ten million items would write about twenty
million rows.

**Direction.** Write stream items in windows, the way a bus already
batches its appends. This belongs with the packed record format of the
current performance work (one write per step for a durable run, one
compact row per run for a fast one), which cuts the rows per item for
the same reason it cuts them per run.

[Update Notice Warning] If we touch `PulsesConsumed`, the stream
runtime (`crates/weft-engine/src/stream_runtime.rs`) or the journal's
record format, revisit this entry.

## The engine interprets the wiring: precompile its plan, or compile the wiring

**When.** After the current round of performance work (fast and durable
runs, callers straight to the worker, one journal writer per worker),
with numbers measured then.

**Problem.** Node bodies are compiled Rust, but the wiring between them
is interpreted at run time, the way Python is. The program is a data
structure (`ProjectDefinition`), and the execution loop
(`crates/weft-engine/src/execution_driver.rs`) walks it:

```
TODAY: node bodies are compiled Rust, the wiring is interpreted
───────────────────────────────────────────────────────────────────────
loop:  scan every node of the program ─► "who has all its inputs?"
       (weft_core::exec::ready::find_ready_nodes)
       └► for each ready node: build a context, start a task for it,
          wait for its message back, file its outputs in a table keyed
          by node and port NAMES (text), write its records ─► scan again

COMPILED: the wiring becomes Rust code too
───────────────────────────────────────────────────────────────────────
async fn ping(request) {
    let fired = route.run(request).await?;        // a wire is a variable
    let body  = json_object.run(fired).await?;
    reply.run(body).await
}
```

**Measured on 2026-10-08**, after the performance round (fast and durable
runs, callers straight to the worker, one journal writer per worker, the
program's facts worked out once per program, a step that ends at once run on
the drive's own task). The bench project (40 nodes, `bench/weft-project`),
its unrecorded ping (`Route` to `JsonObject` to `Reply`):

- In a test process (release build, records in memory, no network or
  database), one run costs about 73 µs of CPU, down from 172 µs before the
  round.
- In the local install under 50 callers, the worker spends about 365 µs of
  CPU per call (axum in Docker: about 50 µs). A profile of that worker,
  taken before unrecorded runs stopped keeping their history, put about
  half of its CPU in the drive loop (readiness, settling boundaries,
  building each step's input bag, applying emissions) and the rest around
  it: serving the call, the door, the run's birth, the record.
- Throughput: 23,181 unrecorded and 12,092 fast pings a second, against
  axum's 36,996 behind Docker's port relay; 0.6 ms and 0.7 ms at one caller
  against axum's 0.3 ms.

So the loop is now the largest single cost, about half of the worker's own,
and it grows with the size of the graph. The rest (the record, the door,
the call) is ordinary code that can be trimmed on its own. The probe was a
throwaway test driving `run_one_execution_observed` over `engine_test_rig`
with the bench project's definition and stand-in nodes; rebuild it to
measure again (`bench/README.md` has the worker-side profile).

**Direction, two levels.**

1. **Keep the interpreter, precompile its plan.** Everything the loop
   works out at run time (who feeds whom, which inputs are required,
   which ports are wired) becomes plain numbers decided at compile time:
   no name lookups, no scan of every node each step, and a straight
   chain runs in place instead of starting a task per node. Pausing and
   resuming do not change.
2. **Compile the wiring fully**, as in the sketch: a wire costs what a
   Rust variable costs, and the compiler, which already produces a
   native binary per program, generates it. The hard part is pausing
   and resuming. Today a paused run is rebuilt by folding its records
   back into the tables; compiled code would resume the way Restate and
   Temporal do, by running the function again from the top while every
   node that already finished hands back its recorded output instead of
   running again. Loops, streams, buses, `_should_flow` skip cascades,
   `catchErrors` and suspensions all have to be expressed in that model,
   so this is a rewrite of the engine and of how runs resume.

**Why deferred.** The language work comes first. Choose between the two
levels with numbers measured then, and before choosing the second, list
what it needs feature by feature.

[Update Notice Warning] If we touch `crates/weft-engine/src/execution_driver.rs`'s
dispatch loop or `weft_core::exec::ready`, revisit this entry.

## "At once" is a share per copy, never the project's true count

**Problem.** An entry's `callsAtOnce` is held at each worker's door, and
each copy takes its share: the limit divided by the copies alive at the
last tick, at least one (`crates/weft-engine/src/door/limits.rs`). Two
things follow. A copy whose share is full refuses a caller while another
copy has room, so under uneven routing the entry runs fewer at once than
its limit. And with more copies than the limit, every copy still gets
one, so together they run more at once than the limit allows.

**Direction.** Count what is going across copies the way the per-minute
counts are: each copy states how many of an entry's runs it has going on
its tick, and admits against the copies' total. That is exact to about a
second, like the minute counts. A run born on a dead copy has to stop
counting, which the tick's "copies alive" already tells.

**Why deferred.** Low impact for now: the share is wrong only with
several copies under uneven load, and the per-minute limits stay exact
to a second.

[Update Notice Warning] If we touch `crates/weft-engine/src/door/limits.rs`
or `/v1/door/tick`, revisit this entry.

## Every copy sends every caller's count every second

**Problem.** A worker's tick (`/v1/door/tick`, once a second) states its
counts of the minute, and the per-caller ones are keyed by entry and
caller. An entry called by many different callers in one minute makes
every copy send one count per caller, every second, and hear every other
copy's. The tick grows with the number of distinct callers, not with the
traffic that matters for the limit.

**Direction.** Send what changed since the last tick instead of the whole
minute, or keep per-caller counts on the copy the caller reached (one
caller mostly lands on one copy) and only exchange the entry-wide counts.

**Why deferred.** Only matters with thousands of distinct callers a
minute on one project.

[Update Notice Warning] If we touch `crates/weft-engine/src/door/limits.rs`
or `/v1/door/tick`, revisit this entry.

## Several workers of a project can open more Postgres connections than the database takes

**Problem.** The Postgres node keeps one pool per worker and database
(`catalog/postgres/postgres.rs`, `MOST_AT_ONCE`), held to 20 connections
so a few workers together stay under Postgres's default limit of 100.
That is a guess about how many workers there are. A project scaled to
many workers can still open more connections than the database accepts,
and the runs past it fail to connect.

**Direction.** Put a connection pooler (PgBouncer, in transaction mode)
in front of the database inside the Postgres node's own infra, so every
worker talks to the pooler and the pooler holds the few real
connections. The worker's own limit can then be generous again. The open
question is a database the user brings, which has no infra of ours to
put the pooler in.

[Update Notice Warning] If we touch `catalog/postgres/postgres.rs`'s
pool or the Postgres node's infra, revisit this entry.

## A Python process runs one script at a time

**Problem.** `ExecPython` keeps as many `python3` processes as the worker
has CPUs (`catalog/basic/exec_python/mod.rs`), and each runs one script at
a time. A script that mostly waits (an HTTP call, a `sleep`) holds its
process while it uses no CPU, so with every process busy waiting, the next
script queues for nothing.

**Direction.** Each process runs several scripts at once on threads: a
waiting script releases Python's lock, so the others go on, and scripts
that compute still share the CPUs. Cap the scripts at once per process (a
setting) and benchmark `burn` and a waiting script at a few values to
find the best. On a Python built without the lock (supported since 3.14,
not the default build), the same threads compute in parallel too. Scripts
on threads of one process share its module state more directly than
today, so the node's docs would say so.

**Why deferred.** A node's own optimisation, apart from the language
work around it.

[Update Notice Warning] If we touch `catalog/basic/exec_python/mod.rs`'s
process pool, revisit this entry.
