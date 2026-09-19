# The journal

Every execution writes an append-only record: one row per event, in order.

It is not a log. A log is prose a human reads when something breaks. The
journal is the **state of the execution**, in a form that can be replayed to
reconstruct it exactly.

That is what lets weft pick a program back up long after the process that
started it is gone.

## What is in it

| Group | Events |
|---|---|
| Lifecycle | execution started, node kicked, node started, completed, failed, skipped, suspended, resumed, cancelled |
| Data flow | port emitted, port closed, port type mismatch, pulses consumed, run output |
| Loops | loop instantiated, iteration launched, loop out fired, stream ended, terminated |
| Suspensions | suspension registered, suspension resolved |
| Money and logs | cost reported, log line |
| Terminals | execution completed, failed, cancelled |
| Buses | joined, left, window, closed |
| Live callers | connected, inbound, outbound, errored, disconnected |

Each row carries the execution's color, so reading an execution is one indexed
query ordered by row id.

A value a node emits is written once, on the port emitted row, however many
wires it fans out on. Which wires carried it, which ports a firing closed,
what a group boundary forwarded, and what each loop iteration received are
never written: they are worked out again from the program when the journal
is read. Group boundaries have no rows at all. So the journal is the list of
facts the engine learned from outside (a trigger payload, a node's emission,
a person's answer, a log line, a cost, a stream take, a cancellation), and
reading it means replaying those facts over the program.

## How it is used

**During a normal run, nothing reads it.** The worker holds the whole execution
in memory and writes rows as it goes. Each write is a checkpoint.

It matters at two moments. When a worker has to rebuild an execution it did
not run, on a resume or after the previous worker died, it fetches the program
the execution was started against and **folds** the rows over it in order,
reconstructing the pulse table, which nodes completed, which are suspended,
and where each loop got to, and carries on. And whenever something wants to
show a run (the editor's execution view, `weft events`), the dispatcher folds
the same way and hands out what the fold derived: the values each firing
received and emitted, the group boundaries that ran or were skipped.

## Reading it yourself

If you want to see what a run actually did:

```bash
weft executions --phase fire   # recent runs, without the setup runs an activate makes
weft logs <color>              # what the run's nodes wrote, plus every failure it recorded
weft events <color>            # every event for one execution, in order, one line each
weft follow <project>          # live, as they happen
```

If a run failed, `weft logs` names the node and the error, and that is usually
all you need. If you want the values on the wires, `weft events` prints one
line per event and takes `--node`, `--kind` and `--full` to open only the part
you want; the flags are in [the CLI](cli.md).

The editor's execution view is the same data rendered as a graph: click a node
and you see the values that firing actually received and emitted.

A failed run from last Tuesday is still readable node by node, with the real
values on the real wires, so you rarely have to make a bug happen again to
study it.

## Why debugging scales

If you are hunting a bug, the journal plus [groups](../language/groups.md) turn
it into descending a tree.

Look at the top-level boxes, find the first one whose output is already wrong,
open it, and repeat inside. Each level divides the search space, because each
boundary is a place where the value was either already wrong or still fine, so
you never have to scan the flat graph.

## What the journal costs

A ten-node execution is a few dozen rows.

Two things make that number grow.

**Buses** write one row per bus per window, one second by default, instead of
one row per message. A journaled bus's window row carries every message in
it. Joining, leaving and closing still cost a row each. An ephemeral bus
journals no payloads: it keeps them in memory for the consumers reading it,
and its window row carries only a count and a byte total per sender per
message kind, plus the window's offset range.

**A caller conversation**, an HTTP route or a socket, works exactly the same
way and by the same rules, because it is the same code deciding. One row per
second holds every message of that second in both directions, the caller's
and the program's, so a socket at fifty messages a second is one row rather
than fifty. Connecting, erroring and disconnecting cost a row each. Set
`journalEphemeral` on the trigger and only the sizes and counts are kept.

Three things are true of both, and of anything else the language grows that
carries a stream of messages:

- Content over **100 KB** is recorded trimmed, never whole. The trim keeps the
  shape and cuts the long text fields, so a row still reads as what it was
  about, and the true size travels beside it. Nothing is refused for being
  big; what a channel carries and what the journal keeps of it are separate
  questions.
- **Raw bytes are never written down**, whatever the setting says. The size is
  the whole of what is worth keeping: the content would be a third bigger as
  text and unreadable to whoever is looking at it.
- **Ephemeral means metadata only**, and the content then lives only in that
  channel's own in-memory window. Once the window rolls past, it is gone;
  there is no copy in the database to fall back on.

**Streams** write one emitted and one consumed row per item, so a stream of
ten million items writes twenty million rows. We are building the same
windowing for streams; for where that stands, go and read [the
roadmap](../appendix/roadmap.md#execution).

## Seeded runs

If you ran with `--seed`, the new run reuses part of an earlier one without
copying a single row. The new run's first row names its parent run and
which earlier execution supplied each reused node. The inherited facts
stay under their original run's color.

Readers interpret inherited history against its original program, then
combine the selected facts with the child's own events. Inherited firings
retain their input and output evidence, questions, answers, node logs and
costs, marked with the original run. Historical costs are not new charges.
Execution-wide state such as the parent's terminal status and live caller
connection is not transferred to the child.

If you are looking at a seeded run in the editor, a firing taken from the
seed is outlined in blue, because the fold marks it with the run it came
from. An input you handed in yourself is marked `provided`, and the editor
labels it `provided by hand`.

Reused history depends on its original journal. If required history has
been cleaned, replay reports the missing source run. Start a new run
without seeding, or select an available seed with `weft branch <run>`.

## Cleaning up

If you want the journal smaller, `weft clean` is the only thing that removes
rows:

```bash
weft clean                       # purge, keeping the last 30 days
weft clean <color>               # one execution
weft clean --all                 # everything
weft clean --keep-days 7
```

The same verb also reclaims build output, and those forms touch no
journal rows at all: `weft clean --images` (worker images nothing runs
any more; with `--all`, every project's, the kind node's copies, stale
`weft-infra-*` tags and old builder bases) and `weft clean
--build-cache`. For what each one removes, go and read the `weft clean`
row in [the CLI page](cli.md).

Cleaning is per subject, and naming a subject takes all of it. A color
takes that one run. `--project <id>` takes a whole project's history,
which is how you erase a project you have already removed (removing a
project deliberately leaves its runs behind). With no subject you get
the age sweep: everything older than `--keep-days`, 30 by default.

During a run the journal is append-only, and the dispatcher never edits a row.
Deleting a run also removes it from the version tree ([Versions](versions.md)),
and a version the deletion left bare (no runs, no versions under it, no
checkpoint name, not where head is) goes with it. A checkpoint you named is
never swept.

`weft rm` is the other half of this and works the other way round: with no
flags it unregisters the project and KEEPS its runs, along with everything
needed to read them back (the code each one ran, and its row in the version
tree). `weft rm --journal` is what throws those away.

## A past run that shows nothing

A run's rows say what happened, not what it meant. Every input and
output you see in the editor is worked out afterwards by replaying those
rows against the code the run ran, so a run whose code the dispatcher no
longer holds paints as a graph with all its nodes and none of its
values. The editor says so on the run itself rather than leaving you to
guess: the code is not recorded any more, here is what to do.

The code is kept for as long as any run points at it, removing a project
included, so this is rare. What gets you there is deleting the last run
that needed a version, or a journal older than the release that started
keeping them.

If you still have those files, `weft build` in the project folder puts the
code back: registering records the compiled program under its own hash,
and that hash is what the run names, so unchanged files restore exactly
what it was folded against and it reads as it did. If you do not have
them, `weft clean <color>` removes the run.

## Holes

A lifecycle write can fail because the database is unavailable or the worker
has lost permission to write. The worker stops driving that execution at its
next check. It does not deliberately continue with unsaved state. Bus-history
write failures are reported separately on the affected bus.

The failed write is logged in `weft daemon logs`. A replacement worker can only
recover what was saved, so work done after the last saved result may repeat.

An unreadable saved event prevents the worker from loading the execution.
Some events decode but cannot be applied to the program: a row naming a node
the program does not have, a loop row with no instance behind it, a malformed
pulse identifier. The execution view reports each one, and a worker refuses to
resume over any of them, because a state rebuilt from a partial journal is a
state that never existed. A journal written by an older version of weft does
not decode at all and is refused the same way.

Inspect failures with `weft logs <color>` and `weft events <color>` before
starting a new run. A new run has its own history and can repeat external
actions. `weft clean <color>` removes the old history when no longer needed.

## The execution guarantee

**At-least-once for a node whose completion never reached disk.**

A worker that dies mid-execution is replaced, and the replacement folds the
journal. A node that had finished but whose completion row was lost gets
re-run.

For an action that charges money or sends a message, repeating it can matter.
[`ctx.run`](../nodes/durable-execution.md) reuses a result once it has been
saved. If the action succeeded but saving its result failed, the action can
still repeat. Preventing a duplicate requires the receiving service to
recognize repeated requests, using the same request identifier each time.
