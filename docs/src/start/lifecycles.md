# The three lifecycles

Three things in a weft project are alive on their own clocks. Infrastructure
and triggers start only when you (or your program) start them, and runs also
start when a trigger fires or a caller arrives. The one exception: if
infrastructure stops or breaks, the triggers that read it go down with it,
and weft turns them back on by itself once it runs again (unless the node's own health rules say
otherwise).

| | What it is | You start it with |
|---|---|---|
| **Infrastructure** | Containers your program asked for: a database, a model server, anything that has to keep running | `weft infra start` |
| **Triggers** | The listening: a webhook waiting, a timer ticking, a form open for answers | `weft activate` |
| **Runs** | One go through the graph, from start to finish | `weft run`, or a trigger firing, or a caller arriving |

Hitting **Run** never starts your database and never puts your webhooks live.

Your `hello` program has neither of the first two. It is two boxes and no
listening, so `weft run` is the whole story until you build something that
talks to the world.

## Why they are separate

**Infrastructure costs money while it runs**, so weft will not start a
container because you clicked something that did not mention one. If you run a
program whose containers are down, it refuses up front and names them:

```text
infra not running for: pg. Run `weft infra start` first.
```

**Activating reaches out to other people's systems.** A trigger that waits for
Slack messages registers a subscription with Slack during activation. A trigger
holding a socket open dials it there and then. That is a real thing happening
in somebody else's account, which is why it is a separate decision from running
your program once.

It also has a consequence you will meet: activation **freezes the trigger's
inputs**, because those values went to the provider and the provider is holding
them. Edit one afterwards and your live listener carries on with what it
registered, until you run `weft resync`.

**A run can wait as long as it needs.** It ends `completed`, `failed` or
`cancelled`, or it parks at `waiting_for_input` while a node waits on a person
or a service. Parking costs nothing: weft lets the run's worker shut down, and
a new worker picks the run back up when the answer lands, however many days
later. Once everything in the run waits, the worker first stays up a few
seconds (the trigger's `pauseAfterSecs`, 10 unless you set it), so an answer
that comes quickly carries the run on right there, before it parks.
Otherwise the worker stays up only while the run cannot pause: a caller is still
on the line, or the trigger has `recorded` off. Such a run holds the wait in its
worker instead. Without a caller, if nothing moves in the run for its
trigger's `holdSecs` (60 seconds unless you set it), the waiting node fails;
while a caller is on the line, the wait holds for as long as the caller
stays, unless `holdSecs` is `0`, which fails it at once.

## The rest

For every verb, what each status means, the difference between a run that is
waiting and one that is stuck, and which command to run after you edit
something, go and read
[starting and stopping things](../running/lifecycles.md).

Next, [talk to Tangle](tangle.md) and have it build you something with all
three.
