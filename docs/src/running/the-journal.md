# The journal

Everything a run does gets written down: every value that moves between steps,
every result saved for reuse, and every failure. That record is what the graph
shows you, and what a worker reads to rebuild a run after a restart.

## Read a run

```bash
weft executions --phase fire
weft logs <color>
weft events <color>
```

The color is the run's id, from the listing. `--phase fire` hides the setup
runs that activation makes.

Start with `logs`, which gives you what steps printed and what failed. Reach
for `events` when you want the values themselves. `--node` and `--kind` narrow
it down; `--full` goes the other way and prints every value whole instead of
the truncated summary.

To watch a project as it goes, `weft follow <project>`.

In the editor, pick a run and click a step to see that firing's inputs and
outputs. For working through nested groups, read [find the mistake without
reading
everything](../thinking/sdp.md#find-the-mistake-without-reading-everything).

## What is in there

The journal is a list of rows in order, all keyed by the run's color:

| Area | Examples |
|---|---|
| Lifecycle | Started, completed, failed, skipped, suspended, cancelled |
| Data flow | Values emitted and consumed, saved `ctx.run` results |
| Loops | Iterations, boundaries, termination |
| Suspensions | Waits registered and resolved |
| What the run reported | Step logs, type errors, cost records |
| Channels | Bus windows and live-caller events |

The journal records what crossed weft's own boundaries. It does not hold a
model's private reasoning, or whatever a step was doing inside its own code.

A journaled bus stores messages in windows, a second at a time. An ephemeral
bus records counts and byte totals instead of the messages. Stream items are
recorded one by one, so a stream of a million items is a million rows. For
getting rid of them, go and read [throwing history
away](#throwing-history-away).

## The execution guarantee

A replacement worker can only reuse what actually reached the journal. If a
step did something and its completion was never saved, recovery runs it again.

`ctx.run` narrows that window by saving results one at a time: once a result
is stored, a replay reuses it instead of redoing the work. It cannot fix an
external action that succeeded and then never got recorded. That one can still
happen twice.

So for a payment or a message where a duplicate matters, send an idempotency
key the other service will recognise, built from something that does not
change on a replay, such as the color plus the step id, never a fresh random
value. For writing a step like that, read [surviving a
restart](../nodes/durable-execution.md).

## When records go missing

A write can fail if the database is unreachable, or if the worker has lost
permission to write, which happens when it has been evicted. As soon as the
worker notices, it stops driving that run. A replacement then picks up from
whatever was saved before the failure.

The failure will not show up in the journal, because the journal is the thing
that failed. So if the worker pod is still around, read its container logs:

```bash
kubectl --context kind-weft-local get pods -A -l weft.dev/role=worker -L weft.dev/project
kubectl --context kind-weft-local logs -n <namespace> <pod-name>
```

That context name is the default. If you set `WEFT_KUBE_CONTEXT` or
`WEFT_CLUSTER_NAME`, use yours instead.

Those are the container's own logs. `weft logs <color>` reads the journal
instead, and `weft daemon logs` reads the dispatcher.

If weft hits a saved event it cannot decode, it stops rebuilding that run
there. The editor still shows you the history up to that point and marks the
row it choked on. Damage inside an event weft can still read is skipped during
the rebuild and reported in the run view instead. weft does not yet reliably
tell the difference between damage to finished work, which is harmless, and
damage to the state a resume depends on, which is not.

So look at the history before you start a replacement run, because a fresh run
can repeat whatever the failed one already did.

## Throwing history away

All of these ask before deleting:

```bash
weft clean                         # runs older than 30 days
weft clean --keep-days 7           # runs older than 7 days
weft clean <color>                 # one run
weft clean --project <project-id>  # one project's whole history
weft clean --all                   # everything
```

The age sweep is not limited to the project you are standing in, so add
`--project` if you meant just this one.

Removing a project leaves its runs in the journal. `weft clean --project <id>`
is how you clear them out afterwards.

If you are short of disk rather than tired of old runs, `--images` reclaims
worker images for the project you are standing in, and `--all` spans every
project and takes the builder-base images with it. `--build-cache` prunes the
build cache, which throws away the cargo dependency cache too, so your next
build is slow. Neither touches the journal.
