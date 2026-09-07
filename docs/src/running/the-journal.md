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
| Data flow | pulse emitted, pulses consumed, run output, port type mismatch |
| Loops | loop instantiated, iteration launched, boundary fired, stream ended, terminated |
| Suspensions | suspension registered, suspension resolved |
| Money and logs | cost reported, log line |
| Terminals | execution completed, failed, cancelled |
| Buses | joined, left, window, closed |
| Live callers | connected, inbound, outbound, errored, disconnected |

Each row carries the execution's color, so reading an execution is one indexed
query ordered by row id.

## How it is used

**During a normal run, nothing reads it.** The worker holds the whole execution
in memory and writes rows as it goes. Each write is a checkpoint.

It matters at exactly one moment: when a worker has to rebuild an execution it
did not run, on a resume or after the previous worker died. Then it **folds**
the rows in order, reconstructing the pulse table, which nodes completed, which
are suspended, and where each loop got to, and carries on.

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

One row per event. A ten-node execution is a few dozen rows.

Two places where that adds up, and how each is handled:

**Buses** write one row per channel per window, one second by default, rather
than one row per message. A journaled bus's window row carries every message in
it, so nothing is lost and there are simply fewer rows. An ephemeral bus writes
a rollup instead: counts and bytes per sender.

**Streams** write one emitted and one consumed row per item. Windowing stream
pulses the way buses do is designed and tracked in the repository's `TODO.md`;
until it lands, a stream of ten million items is ten million pairs of rows.

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

During a run the journal is append-only, and the dispatcher never edits a row.

## Holes

A lifecycle write can fail because the database is unavailable or the worker
has lost permission to write. The worker stops driving that execution at its
next check. It does not deliberately continue with unsaved state. Bus-history
write failures are reported separately on the affected bus.

The failed write is logged in `weft daemon logs`. A replacement worker can only
recover what was saved, so work done after the last saved result may repeat.

An unreadable saved event prevents the worker from loading the execution.
Some events can be decoded but contain invalid details, such as a malformed
pulse identifier. Those details are currently skipped during reconstruction
and reported in the execution view. The worker does not yet distinguish
damage to old, finished work from damage to values needed to resume.

Treating missing and damaged entries consistently, and refusing unsafe
resumes, is design work tracked in `TODO.md`; it is not implemented.
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
