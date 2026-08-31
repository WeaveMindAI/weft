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
weft events <color>        # every event for one execution, in order
weft logs <color>          # just the log lines
weft executions            # recent executions
weft follow <project>      # live, as they happen
```

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
weft clean --images              # also reclaim unreferenced worker images
                                 # (with --all: every project's, plus old
                                 # builder-base images; the next build
                                 # re-makes the base)
weft clean --build-cache
```

During a run the journal is append-only, and the dispatcher never edits a row.

## Holes

A journal write can fail: Postgres refuses it, or a fencing trigger rejects it
because this pod is no longer the live one. The running execution carries on,
and what is left behind is a **hole**, a missing row.

A hole costs nothing while the execution is still in memory. It bites only if
that execution later has to be rebuilt from the journal, and then the rebuild
fails rather than quietly reconstructing the wrong state.

The failed write is logged, so `weft daemon logs` is where you see it happen.
If an execution then refuses to resume, run it again: a re-run mints a new
color and reads nothing from the damaged one, and `weft clean <color>` removes
the old rows.

A row can also be present but unreadable, handled today by a separate
mechanism. Folding both into one idea is designed and tracked in the
repository's `TODO.md`.

## The execution guarantee

**At-least-once for a node whose completion never reached disk.**

A worker that dies mid-execution is replaced, and the replacement folds the
journal. A node that had finished but whose completion row was lost gets
re-run.

For most nodes that is harmless. For one that charges money or sends a message
it is not, and [`ctx.run`](../nodes/durable-execution.md) is the fix: it wraps
the side effect so a replay returns the recorded result instead of doing it
again.
