# Starting and stopping things

Every verb for the three lifecycles, what it leaves behind, and what to run
after you change something. For why they are three separate things, go and read
[the three lifecycles](../start/lifecycles.md).

## Infrastructure

A program has infrastructure when one of its steps needs a container of its
own, which you can see in the source because the step says so:

```weft
pg = PostgresDatabase { database: "imagebot" }
```

Nothing starts it for you. Not `weft run`, not `weft build`, not activation.

| Command | What it does |
|---|---|
| `weft infra start` | Brings up whatever is down and leaves whatever is already up alone |
| `weft infra stop` | Scales to nothing and **keeps the disk**. Start it again and your rows are there |
| `weft infra terminate` | Deletes it, **disk included**, unless the step asked for the disk to be kept |
| `weft infra upgrade` | Rebuilds against your current source. The triggers it took down come back once the new infrastructure is up |
| `weft infra status` | Where each piece stands, with its address |
| `weft infra list-doors` | The addresses this project's infra answers on |
| `weft infra logs <node>` | What the containers actually printed |
| `weft infra cancel` | Stops waiting on work in flight |
| `weft infra node-stop <node>` / `node-terminate <node>` | The same, for one piece |

`weft infra status` prints one line per piece:

```text
infra for imagebot (a1b2…):
  pg [running] -> postgres://…
```

| Status | What it means |
|---|---|
| `provisioning` | Coming up |
| `running` | Every unit is ready |
| `stopped` | Nothing running, disks kept |
| `flaky` | It was healthy and has dropped below its readiness threshold |
| `failed` | The last apply failed. The message says at which stage |

A project-wide `partial` is a real steady state rather than something in
transit: some pieces are up and some are not.

### When it hangs

weft puts no time limit on a container coming up, because a model server
pulling weights can take a long time. So a bad image or a probe that never
passes looks like waiting, not like an error.

Read `weft infra logs <node>` first, because that is where a service says what
went wrong. Then `weft infra cancel`. Cancel halts between steps rather than
undoing what it did, so the piece is left partly applied and marked failed;
check `weft infra status` and either terminate it or run the verb again.

### Infra you deleted from the source

Deleting the step does not delete the container. `weft status` warns you that
live infrastructure exists whose node is gone from the source, and it keeps
running, and costing, until you stop or terminate it. The verbs stay available
for exactly that reason.

## Triggers

`weft activate` runs each trigger's setup once, then leaves the listeners
running. Setup is not bookkeeping: it registers subscriptions with providers
and dials sockets, and it fails loudly if it cannot, rather than minting a
trigger that looks live and is dead.

| What you want | Run |
|---|---|
| My program should answer the world now | `weft activate` |
| Stop it answering | `weft deactivate` |
| I edited something a trigger reads, and it is live | `weft resync` |
| Prepare a trigger without exposing it yet | `weft bake` |
| Try one trigger with an event I made up | `weft run --fire <trigger>='<json>'` |
| I clicked activate and want it back | `weft cancel-activate` |

Project status moves `registered` → `activating` → `active` →
`deactivating` → `inactive`.

### One trigger at a time

Each trigger is turned on separately, so `weft activate --trigger inbound`
turns on one and leaves the rest alone; for the other flags, go and read
[the CLI](cli.md). The project shows `active` while any of its shared triggers
listens and none is part way through turning on or off, and `weft status` lists
each trigger's own state under `triggers:`.

If you stopped infrastructure that some triggers read, those triggers came
down with it and the rest stayed on. When `weft infra start` (or the
program's own `ctx.infra(..).start()`) has the infrastructure up again, the
triggers that came down with it come back on by themselves; one you switched
off yourself since stays off. A plain `weft activate` turns back on whatever
is still off. While a route's trigger is parked, its callers are answered
`503` with a `Retry-After` at once, so a client that retries gets through once
it is back; the events a trigger picks up itself wait and run once it is
back.

A trigger that reads an instance's copy or an instance's value
(`@per_instance`, `@instance_filled`) exists once per instance, and its copies
are turned on with `--instance <id>`. For those, go and read
[programs with instances](instances.md).

### Frozen inputs

Setup snapshots the trigger's input values along with the registration, and a
fire replays that snapshot. So a trigger's inputs are whatever they were when
you last ran setup. Change one and weft tells you the bake is stale:

```text
the code changed since trigger 'inbound' was last prepared, so its bake is stale.
Prepare it again: `weft bake` does it without listening, `weft activate` does it and listens
```

### Choosing what happens to work in flight

`weft deactivate` and `weft resync` both ask. If you want nothing lost while
the triggers are down, park them:

| Mode | Work that arrives meanwhile | What a person sees |
|---|---|---|
| `park` | Nothing is lost. A provider's push (a Slack message, a new email) or a submitted form, an answer to a waiting run, and every fire a trigger makes by itself (a schedule's tick, a message on a connection it holds) wait, and run once the triggers are back, on the version they come back with. A call to a route is not held, even a webhook sent to one: it gets a `503` with a `Retry-After`, so a client that retries gets through once the triggers are back | Questions stay listed for the people who answer them, and an answer sent now waits too |
| `hibernate` | The same as `park` for the grace window (`--grace`, in minutes). After it, nothing new is taken and the triggers stop listening | Questions are hidden for the whole time; someone who already has one open can still send their answer within the window |
| `wipe` | Refused. Everything waiting is dropped and the runs waiting on the triggers are cancelled: a clean slate, and what you want while building | Questions are deleted |

A schedule parked for two hours has two hours of ticks waiting when it comes
back; the reactivate choice below decides whether they run.

With no terminal attached, or with `--json`, the default is `wipe`. Running
executions are cancelled unless you pass `--running-policy wait` with `--mode
park` or `--mode hibernate` (a `wipe` cancels everything, so it refuses
`wait`). That lets them land first, capped at a minute (`--drain-timeout`), and `weft
cancel-running` ends that wait early. The wait covers every run the triggers
started that is still going, fast ones included, even one that has not written
anything yet. A run parked on a timer or a form is not waited for: the mode
you picked decides what happens to it. Whatever is still going at the cap is
cancelled, except a `recorded: false` run that has written nothing: the cancel
cannot reach it, so it finishes on its own.

The infra verbs that take triggers down (`weft infra stop`, `terminate`,
`upgrade`) ask you the same thing, and their answer means the same. They ask
only when a trigger reading that infrastructure is on, the program's own or an
instance's. With no terminal, or with `--json`, they do not pick for you: they
stop and name the flags, for example `--mode park --running-policy wait`, and
if you passed those flags already they are used. When no trigger reading it is
on, nothing is asked, but a run may still be using that infrastructure:
`--running-policy wait` on its own lets it land before the containers go.

`weft activate` and `weft bake` make the same choice about a worker still up
from an older build: by default what it runs is cancelled and it is replaced
now; with `--running-policy wait` its executions land first. While it waits,
nothing new is sent to that worker, and once its last execution lands the
worker finishes writing down what it owes (a metered call's cost) and leaves on
its own, which takes a second or two; the cap covers both.

Reactivating a project that kept state asks you again: drain the parked work
and keep the suspensions, keep the suspensions only, or wipe everything.

### Infra has to be up first

A trigger that reads its address off one of your containers cannot be set up
until that container answers:

```text
these triggers' infra is not running: pg. Start it with `weft infra start`, then run this again.
```

Infrastructure that only a run touches does not hold activation back.

## Executions

| It starts from | Command |
|---|---|
| You | `weft run` |
| One trigger, by hand | `weft run --fire <trigger>='<json>'` |
| A trigger firing for real | Nothing. It happens because you activated |
| A caller connecting | Nothing. The project has to be active |

| Status | What it means |
|---|---|
| `running` | Work is happening now |
| `waiting_for_input` | Parked on a person or a service. No worker is up and nothing is being spent |
| `completed` | Finished |
| `failed` | A step failed (a wait the run held past its `holdSecs` and its node did not handle counts as one), or the run could get no further |
| `cancelled` | You or another run stopped it; its live caller hung up; it was a fast run and its worker went away; its worker was stopped while it could not pause (`recorded: false`, a caller still on the line with `outlivesCaller` off, or a bus between its nodes open); it reached the hour a cloud install allows one stretch of a run; or the runtime stopped it (a newer build replaced its image) |

`weft executions --status running` lists both the ones working and the ones
waiting, and the status column tells them apart.

### Waiting, and stuck

**Waiting** means somebody still has to answer. **Stuck** means nobody can:
steps are holding values that will never add up to enough to run them, and
nothing is parked on a signal, so no answer exists that would move it along.

weft ends a stuck run as failed and names every step and what each is short of:

```text
execution stuck: 2 firings hold pulses and can never fire: theirs has value,
still waiting on go; leaf has value, every wired input arrived
```

That is a bug in the shape of your graph rather than something to wait out.

### Stopping one

`weft stop <execution-id>` cancels it. If a worker is up, it stops within about fifty
milliseconds. If the run was parked, the cancellation is written down and the
listener forgets its signals, so a late answer finds nothing.

Work already done stays done, and weft cannot take back a message, an email or
a payment your program already sent.

Cancelling a run that already ended is refused rather than silently ignored, so
`weft stop` on yesterday's run tells you it has already finished.

## After you change something

`weft status` compares your source against what is running and names the verb:

```text
drift:
  infra: source has changed; `weft infra upgrade` rebuilds it
  binary: worker code has changed; the next run or `weft build` rebuilds the image
  definition: project shape has changed; the next run picks it up
  activation: the listeners fire an older program; `weft resync` re-registers them
```

| What you changed | What to run |
|---|---|
| The shape of the graph | Nothing. The next run picks it up |
| A node's Rust code | Nothing. The next run rebuilds it |
| What an infra step asks for | `weft infra upgrade`, then `weft activate` |
| Anything a trigger reads, on a live project | `weft resync` |

Until you resync, the subscription sitting at the provider is still pointed at
the program you had before.
