# The three lifecycles

Three things in a weft project are alive on their own clocks, and you start
each one yourself. None of them starts another.

| | What it is | You start it with |
|---|---|---|
| **Infrastructure** | Containers your program asked for: a database, a model server, anything that has to keep running | `weft infra start` |
| **Triggers** | The listening: a webhook waiting, a timer ticking, a form open for answers | `weft activate` |
| **Executions** | One run of the graph, start to finish | `weft run`, or a trigger firing, or a caller arriving |

Hitting **Run** never starts your database and never puts your webhooks live.
That is the whole idea and it catches everyone once.

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

**An execution is just one run.** It ends `completed`, `failed` or
`cancelled`, or it parks at **waiting for input** while a step waits on a
person or a service. Parking is free: weft shuts the machinery down and a fresh
copy picks the run back up when the answer lands, however many days later.

## The rest

For every verb, what each status means, the difference between a run that is
waiting and one that is stuck, and which command to run after you edit
something, go and read
[starting and stopping things](../running/lifecycles.md).

Next, [talk to Tangle](tangle.md) and have it build you something with all
three.
