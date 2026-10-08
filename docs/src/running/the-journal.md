# The journal

Every recorded run writes down what it did, event by event. For when each
write happens, go and read [when a run waits for its
writes](#when-a-run-waits-for-its-writes).
That record is what the graph shows you, what `weft events` prints, and what a
worker reads to pick up a durable run that was interrupted, or a run that was
waiting.

```bash
weft events 9b81d0a2
weft events 9b81d0a2 --node classify --full
weft logs 9b81d0a2
```

Nothing edits an event once it is written. A run is deleted with its record
once it has been kept long enough (go and read [how long a run is
kept](#how-long-a-run-is-kept)), or sooner by `weft clean`, `weft prune` or
`weft rm`.

## What is in it

Each row holds one run's events from one write, in the order they happened:
which run, what kinds, when, and the events themselves. A worker sends the rows
of several runs in one write. A run's history is its rows, read in order.

**The run's life.** It started, with the project, the entry node and a
fingerprint of the exact program shape, so a resume runs against what it
suspended on rather than whatever you have edited since. Then it completed,
failed, or was cancelled, with the reason in words and in a form the inspector
can read.

**Each node's life.** Given its inputs (kicked), started, and then completed,
failed, skipped, suspended, resumed or cancelled. A skip carries its reason in
plain words.

**Values on wires.** `PortEmitted` is the only event that carries a value, and
it carries it once. When a run is rebuilt, weft puts the value on every
outgoing wire itself, which is why a replay lands on exactly the same picture
as the live run did.

`PortClosed` is written only when a node closes a port on purpose. The ports
that close because a body returned are worked out again on replay.

**Loops, streams and buses** get their own events: an iteration launching, a
gather assembling, a bus participant joining, a window of messages.

**What a call cost**, from the meter, as it happens. A node can never write one
of those.

## Reading it back

When weft reads a run back (the fold), it takes the rows and the program and
works out where the run had got to.

The journal holds only the facts learned from **outside**, and the fold
recomputes everything else with the same functions the live engine ran, so a
replay rebuilds exactly the picture the live run held. A replay shows the
times the journal wrote down, so you see when each thing really happened.

## When a row cannot be read

A row the fold cannot apply is a hole. It is logged, listed on the run, and the
inspector shows how many there are.

In a run that has ended, a hole only spoils the replay view. In a run that has
to resume, a hole stops the resume with an error, because a run rebuilt from
part of its record could run steps that already ran.

## The execution guarantee

A node's completion is written after the node finishes, so a step whose worker
died before that has no completion on record. For what weft does with such a
step, in a fast run and in a durable one, go and read [surviving a
restart](../nodes/durable-execution.md#when-the-worker-dies-mid-step).

## When a run waits for its writes

A worker gathers what its runs did and writes it a couple of milliseconds
later, or at once when a run is waiting on it. Each run's records always
arrive in order.

A fast run (the default) waits for its writes only where something else is
about to act on its record: when it pauses, when an answer it was waiting for
arrives, when its worker is stopping and hands it to another one, and before it
asks weft for something on its behalf (a connection or an endpoint it does not
already hold, anything on its stored files, a tag through `ctx.tag_execution`,
a stop through `ctx.stop_tagged`, a call on its own program such as starting
its infra). If a step calls a paid service and that call reports a cost, the cost is
written in the background, and the run hands over its ending only once that
cost is written. Once it has handed its ending over, the run is gone from the worker's memory straight away. The ending
is written after every event the run handed over before it, so a run is never
reported finished before the rest of its record.

A durable run also waits before each step of a node that is not
[pure](../nodes/metadata.md#features), before an answer leaves for its caller,
and for its ending. One fired by an event also waits for its start to be
written before the event counts as delivered. When the answer is the last thing the run does, its ending
goes in the same write, so the answer costs no extra wait. A durable route that
reshapes its input with pure nodes and answers with `Reply` (pure too) waits
once, for the answer and the ending together. For choosing between the two, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept).

If the database falls behind and 64 MiB of events are already waiting on a
worker, the next run with more to hand over waits until there is space, so
nothing is dropped. For what that
does to new calls, go and read [when a worker is
full](architecture.md#when-a-worker-is-full).

## Why a failed write stops the run

If a write fails (the broker refuses it, or a minute of sending it again gets
no answer), the runs whose rows were in it end as failed. A write that got no
answer is safe to send again, because the record takes a batch sent twice only
once. A journal missing rows the worker thinks it wrote would rebuild a different run:
a node whose start was lost but whose values landed would run again and spend
twice.

## How long a run is kept

An ended run is kept for a week, and then deleted with its record, its logs,
its search entry and its tags. A run that has not ended (running, parked,
waiting for a worker) is never deleted, however old. If you want another
length:

- for every run of a project, set it in `weft.toml`:

  ```toml
  [runs]
  keep_for = "30d"
  ```

- for the runs of one trigger, set its `keepRunsFor` input (`12h`, `30d`,
  `forever`);
- for one run you start by hand, `weft run --keep-for 2h`.

A trigger's setting beats the project's, and `--keep-for` beats both. A run's keep
time is fixed when it starts, so a change only reaches runs that start
afterwards. If you change a trigger's `keepRunsFor` or the project's
`keep_for`, the trigger's own runs pick up the change once you run `weft
resync`, and a run you fire with `weft run --fire` once you run `weft bake`.

## Clearing it

```bash
weft clean                    # ended runs that started more than 30 days ago
weft clean 9b81d0a2           # one run
weft clean --project <id>     # that project's whole history
weft clean --all              # everything
```

Removing a project with `weft rm` erases its runs with it.

## A past run that shows nothing

The journal names the program each run used, by hash. If the dispatcher no
longer holds that program, the run's rows are still there but nothing can be
drawn against them.

If your files are the same as when the run ran, `weft build` makes it readable
again: it registers the program under its hash, and that is the hash the run
names.
