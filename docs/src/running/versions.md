# Versions, seeded runs and frozen examples

Every run records the code and the parameters it used. That buys you two things
while you build. A run can reuse the completed work it still agrees with, so you
are not paying for the same steps twice. And you can replay a saved example
against today's code to see how its answers changed.

## Versions

A **version** is your project's source files, named by a hash of their contents.
Every `*.weft` file (an `@include`d file is program text), `weft.toml`,
`nodes/` except the seeded `base_catalog/` (that one stands for the installed
weft's version and the catalog's content hash), `prompts/`, `scripts/`,
`sql/`, `assets/` and `examples/` are all in it,
together with the release number of the installed weft. `layouts/` is not: a
canvas drag is not a version. The same files are always the same version,
however many times you run them. Every run and every checkpoint records one.

`weft tree` lists every version and the runs under it, with head marked. Head is
the version your next checkpoint or run is recorded beneath.

![weft tree showing versions and their runs](../img/versions-tree.png)

| If you want to… | Run |
|---|---|
| record the current files without building or running | `weft checkpoint [label]` |
| go back to a version | `weft branch <version\|label\|color>` |
| see the versions, what changed in each, and their runs | `weft tree` |
| start a version with no parent | `weft checkpoint --root` or `weft run --root` |
| delete a version and everything under it | `weft prune <version>` |

`checkpoint` is also the first thing you can do in a new project: it tells the
dispatcher the project exists without building anything. When nothing changed it
prints `already at <id>`. A checkpoint also clears head's selected run, so the
next seed looks for a settled run on that version or on the nearest ancestor
that has one.

`branch` restores that version's files and moves head there. It refuses to
overwrite edits you have not checkpointed, and names the files. Pass `--discard`
to let it replace them. Naming a run's color instead of a version restores that
run's code and makes that run the next seed.

`prune` asks first, and refuses while the version is still needed. It will not
remove head, an activated project's version, a version with running work, or one
a frozen example came from, and it tells you which reason applies. Unused bakes
that belonged to a version you pruned go with it. Source files still referenced
by a registered build or surviving history stay.

## Running one group, or one node onward

You do not have to run a whole program to test one piece. Leave the graph
intact and tell `weft run` where to start and stop:

```bash
weft run --from classify='{"text":"the invoice is wrong"}' --target reply --save invoice --detach
weft run --group triage='{"text":"the invoice is wrong"}' --detach
weft run --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

| Flag | What it selects |
|---|---|
| `--from node='{"port":value}'` | Start at this node and hand it these inputs. A bare node name starts there with nothing supplied. Repeat for several starts. |
| `--emit node='{"port":value}'` | Pretend this node produced these outputs without running it, then carry on downstream. |
| `--target node-or-group` | Run this node and everything feeding it, and stop there. Repeat for several endpoints. |
| `--before node-or-group` | Run everything this node needs, but not the node itself. The mirror of `--target`. |
| `--group group='{"port":value}'` | Run one whole group or loop on its own, with these inputs. An included file uses its group alias. |

The work you select brings the other producers it needs. The upstream walk stops
at each `--from`, each `--emit`, and each trigger. Say A and B both feed C: with
`--from A`, B is pulled in; with `--from C`, the walk stops before both producers
and uses C's supplied backups or the normal closed-input behavior. Branches that
have nothing to do with the selection stay out. A cut that selects no work and
no output evidence is refused before any image is built.

`--group` is a complete selection: it cannot be combined with `--from`,
`--emit`, `--target` or `--before`. `--from group=...` instead starts at the
whole group and carries on downstream. A cut inside an ordinary group stays at
the named node, its `_should_flow` gates still apply, and a true gate dispatches
only the selected work. Loops are indivisible: select a whole loop, or move the
cut outside it. Trigger setup and infrastructure preparation follow the same
rules.

A supplied input is a backup at a named start, nothing more. The runtime waits
for real producers first. A real value wins, including a real `null`; a clean
closure with no value lets the backup through; a real error or invalid value is
still an error. Inputs at arbitrary interior nodes are not run parameters.

For a generator output, `--emit batches='{"items":["first","second"]}'` emits
those items in order and closes the port, and an empty array closes it with no
item. An ordinary list port receives its array as one value, so read the port
type before you supply it.

## Preparing and firing a trigger

```bash
weft bake
weft run --fire incoming='{"event":"the trigger wake payload"}' --detach
weft run --emit incoming='{"message":"the emitted output value"}' --detach
```

`weft bake` runs preparation and saves the resulting trigger settings without
arming any listener. A `--fire` then uses those settings and gives exactly one
trigger its wake payload. `--emit` supplies a trigger's declared outputs without
executing it, which is how you start downstream of a trigger by hand.

The payload is checked against what that trigger declares it wakes with before
anything is built or started, and the check is exact: a missing field is refused
and so is a field the trigger does not declare, each one named. `weft run --fire`
prints the shape it wanted, which is the quickest way to see what a trigger
takes ([`firesWith`](../nodes/metadata.md#fireswith)). The same trigger cannot
use both forms, triggers get their inputs through a bake, and a trigger cannot
be a `--from` start.

The bake must match the code and configuration you are running, so bake again
after a change. A closed group gate can leave a trigger unprepared; when a fire
is refused, look at the preparation events. `weft bake <project-id>` runs against
an already registered build. Builds include the whole catalog by default; if you
use `--referenced`, use it for both the bake and the run so their code identities
match.

`weft activate` prepares and arms listeners for real. Each event then starts its
own run from its one trigger, using the program that prepared that listener.

## Seeding: run only what changed

```bash
weft run --seed
```

`--seed` reuses eligible completed work from head's selected run. When head names
only a version, weft finds a settled run on that version or on the nearest
ancestor that has one. To seed from a particular older run, branch to its color
first, and checkpoint any edits you want to keep, because branching restores
that run's code.

`--seed-before node` permits reuse up to but not including that node, and
`--seed-until node` also reuses the node itself. Both require `--seed`. They
bound what may be reused; `--target` and `--before` bound what runs.

Changed implementations, changed inputs and changed dependencies invalidate the
affected work and everything downstream of it. Failed work and live handles
cannot be reused, and a loop is reused whole. The run reports what it inherited
and what it ran. A boundary you asked for does not make incompatible results
eligible: read the warning, and move the cut earlier if the old values cannot
supply it. A run that inherits everything is valid.

`--from` chooses a boundary, not a forced rerun. With `--seed`, unchanged work
inside that cut can still be reused, including identical supplied backups. Omit
`--seed` to run it all again, or use `--seed-before` / `--seed-until` to limit
reuse.

You rarely build a worker image from scratch. The unchanged standard library
shares one finished image across projects: setup prepares it and releases publish
it, so a project can pull it without compiling. Project names, ids and graph
settings do not change that image. Editing a node, adding a node, using custom
build settings, or passing `--referenced` needs an image of its own. Two
changes that do not: adding a reference to an unchanged catalog node, and
editing graph configuration, because an unreferenced node cannot change the
worker binary. Even then,
a build compiles only what no earlier build on this machine already compiled:
every worker build shares one compile cache, and a node whose files have not
changed comes out of it. Once the standard library has been compiled once, a
project with one custom node compiles that node and links. `weft clean
--build-cache` throws that cache away.

## Freezing an accepted run

When a run comes out right, save it. `--save name` writes the starting
parameters to `examples/name.json`, and `weft run name` later runs your current
program with those parameters, whether the file holds only parameters or a
frozen result too.

```bash
weft run invoice --detach
weft events <color> --full
weft freeze invoice <color> --expect reply
# After changing the program:
weft run invoice --detach
weft diff <new-color> example:invoice --full
# After accepting the new result:
weft freeze invoice <new-color> --expect reply
```

`freeze` needs a completed run. It keeps that run's starting parameters and the
outputs it observed, in `expected`. A whole seeded run keeps its original
starting inputs; a carved run keeps its cut and the inputs entering it. Results
reused in the middle do not become hidden fixed inputs.

`expected` holds output history, including finite streams and closures. Repeat
`--expect node` to focus review on particular outputs. Focus changes what the
comparison looks at, not what runs. A focused output that disappeared still
shows up as a difference. Stored media is compared by content hash.

Run an example without `--seed` when you want to see today's computation.
`weft diff` presents the changed values for a person or an assistant to judge,
and differences do not fail the command; check the execution status separately.
Run and diff leave the accepted file intact, so freeze again only after you
accept the replacement. `weft examples` lists what you have saved.

![weft diff showing changed outputs against a frozen example](../img/versions-diff.png)

## Repairing saved parameters after a graph change

When a node you removed takes a saved input with it, weft ignores that input and
warns. When a starting node or a cut endpoint is gone, that is an error, and you
move the cut explicitly:

```bash
weft run invoice --clear from --from new_classifier='{"text":"the invoice is wrong"}' --target reply --detach
weft run invoice --clear group --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

An explicit `--from`, `--target`, `--before`, `--group` or `--fire` replaces the
matching saved setting. Repeated `--from` flags become the replacement map.
`--emit` replaces the named ports and leaves your other saved emit entries
alone. `--clear from|emit|target|before|group|fire` clears a field before you
edit it, and repeats for several fields. Supplying the same port twice is an
error. Use `--save another-name` if you want the revised parameters kept
separately from the original.

## When a run waits

A frozen example keeps the human questions and answers, and the incoming caller
messages, so you can inspect them. They are not automatic replies. Compare the
new question with the recorded question and answer before you answer the
current token, and for a live connection send messages through a new connection.

`weft wake <color> <node>` resolves a pure timer wait now. A wait that expects
a value is refused by `wake`, because that value is resolved by the thing that
supplies it: a form answer, a provider event, a caller message.

## Reading the result

`weft executions` shows status. `weft logs <color>` shows failures and the lines
your nodes wrote; `weft events <color> --node <id> --full` shows the values.
Inherited history names the run it came from and is read against that run's
program; for costs, see [the journal](the-journal.md#seeded-runs). The editor can draw the same run
on the graph, inherited work included.

Completion only tells you the execution finished; the diff tells you whether it
answered the case you were testing.
