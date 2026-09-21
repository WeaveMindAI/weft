# The journal

Every run writes down what it did, event by event, as it happens. That record
is what the graph shows you, what `weft events` prints, and what a replacement
worker reads to rebuild a run that was interrupted.

```bash
weft events 9b81d0a2
weft events 9b81d0a2 --node classify --full
weft logs 9b81d0a2
```

It is append-only. Nothing edits or deletes an event once written, and
`weft clean` is the only thing that removes any.

## What is in it

One row per event: which run, what kind, when, and the event itself.

**The run's life.** It started, with the project, the entry node and a
fingerprint of the exact program shape, so a resume runs against what it
suspended on rather than whatever you have edited since. Then it completed,
failed, or was cancelled, with the reason in words and in a form the inspector
can read.

**Each node's life.** Kicked, started, and then completed, failed, skipped,
suspended, resumed or cancelled. A skip carries why in plain words, for you to
read, and a resume never reads it back.

**Values on wires.** `PortEmitted` is the only row that carries a value, and it
carries it once. The fold puts the pulses on every outgoing wire itself, which
is why replay lands on exactly the same picture as the live run did.

`PortClosed` is separate, because a node deciding to close a port is a fact it
chose, while the closures swept up when a body returns are not.

**Loops, streams and buses** get their own rows: an iteration launching, a
gather assembling, a bus participant joining, a window of messages.

**What a call cost**, from the meter, as it happens. A node can never write one
of those.

## Reading it back

The fold takes the rows and the program and rebuilds where the run had got to.

The important part: it records only the facts learned from **outside**, and
recomputes everything else with the same functions the live engine ran. So the
picture a replay rebuilds is identical to the one the live run held, rather
than a second implementation that might disagree.

That is also why a replayed run looks exactly like a live one in the editor.
Same badges, same inspector, same timestamps, which are the journal's own, so a
replay reads as when it happened.

## When a row cannot be read

A row the fold cannot apply is a hole. It is logged, listed on the run, and the
inspector shows the count rather than pretending the run is whole.

A hole in dead history is cosmetic: the replay view degrades and the run
carries on. A hole where a resume has to pick up is fatal and says so, because
rebuilding half a world and carrying on would be worse than stopping.

## The execution guarantee

**At least once.** A thing can happen twice, and here is exactly where.

A node's completion is written after the node finishes. If the worker dies in
between, the run is rebuilt without that completion and the node runs again.
Inside the body, `ctx.run` gives back a saved result instead of redoing the
work, but if the worker died between the action and the write, the action
happened and nothing recorded it.

So an external action can repeat, and weft says so rather than pretending
otherwise: a failed save names the action and tells you to look before you run
it again.

For what that means when you are writing a node, go and read
[surviving a restart](../nodes/durable-execution.md).

## Why a failed write stops the worker

If the journal is missing rows the live worker believes it wrote, every later
rebuild would reconstruct a different world: a node whose start was lost but
whose emissions landed would run again and spend twice.

So a failed write ends that worker rather than carrying on with a record
nobody can trust.

## Who may write

Every row from a worker carries which worker wrote it, and the database rejects
a write from one whose registration has been removed. That is what stops an
evicted worker still writing history for a run somebody else has taken over.

## Clearing it

```bash
weft clean                    # runs older than 30 days
weft clean 9b81d0a2           # one run
weft clean --project <id>     # that project's whole history
weft clean --all              # everything
```

A project's runs outlive the project, so `--project` is how you erase the
history of something you already removed.

## A past run that shows nothing

The journal names the program each run used, by hash. If the dispatcher no
longer holds that program, the run's rows are still there but nothing can be
drawn against them.

`weft build` puts it back: registering records the compiled program under its
own hash, which is the hash the run names, so unchanged files make the run
readable again.
