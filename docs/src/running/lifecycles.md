# Starting and stopping things

If you want to start, stop or change infrastructure, triggers or runs, here
is the command for each. For why they are three separate things, go and read
[the three lifecycles](../start/lifecycles.md).

## Infrastructure

A program has infrastructure when one of its nodes needs a container of its
own, which you can see in the source because the node says so:

```weft
pg = PostgresDatabase { database: "imagebot" }
```

`weft run`, `weft build` and `weft activate` never start it.

| Command | What it does |
|---|---|
| `weft infra start` | Builds, then starts what is not running. The triggers that went down with it come back on once it runs |
| `weft infra stop` | Scales to nothing and **keeps the disk**. Start it again and your rows are there |
| `weft infra terminate` | Deletes it, **disk included**, except the disks the node lists in `keepOnTerminate` |
| `weft infra upgrade` | Builds, then replaces what is running on another build. It starts nothing that is stopped and makes nothing that is missing (`weft infra start` does both) |
| `weft infra status` | Where each piece stands, with its address |
| `weft infra list-doors` | The addresses this project's infra answers on |
| `weft infra logs [<node>]` | What the containers actually printed |
| `weft infra cancel` | Calls off the infra work in flight |
| `weft infra look` | Asks the supervisor to look at the project's infra again |

If you want only some pieces, go and read [the CLI](cli.md#infrastructure)
for `--infra`.

`weft infra status` prints one line per piece:

```text
infra for imagebot (a1b2…):
  pg [running] -> postgres://…
```

| Status | What it means |
|---|---|
| `provisioning` | Coming up |
| `running` | Every unit is ready |
| `flaky` | A unit was ready, then stayed not ready for its flaky window (30 seconds unless the node sets another; for how, go and read [when a unit stops being ready](../nodes/infrastructure.md#when-a-unit-stops-being-ready)) |
| `stopping`, `terminating` | Going down |
| `stopped` | Nothing running, disks kept |
| `failed` | The last apply failed. The message says at which stage |

`weft status` lists every infra node the program declares, a piece never
started as `not started`, and under each piece the triggers that would go down
with it: `taking it down takes down 'feed', 'door'`.

### When it hangs

weft puts no time limit on a container coming up, because a model server
pulling weights can take a long time. So a bad image or a probe that never
passes looks like waiting, not like an error.

Read `weft infra logs <node>` first, because that is where a service says what
went wrong. Then `weft infra cancel`. Cancel halts between steps rather than
undoing what it did, so the piece is left partly applied and marked failed;
check `weft infra status` and either terminate it or run the verb again.

### Infra you deleted from the source

Deleting the node does not delete the container. `weft status` warns you:

```text
WARNING: live infra exists whose node the source does not declare (removed from it, or inside a part that does not compile); it keeps running (and consuming resources) until stopped/terminated via the infra verbs
```

If you deleted it from the source and have not built since, its own line under
`infra:` says so too, as `orphaned: the source declares it no more`, rather
than calling it behind.

It keeps running, and costing, until you stop or terminate it, and `weft infra
stop --infra <node>` still reaches a node the program no longer declares.

The warning says "or inside a part that does not compile" because an included
file that does not compile hides every node in it, a database included, and so
does a node whose type did not load. While a part of the program hides nodes
that way, weft takes nothing away for being missing from the source: it
terminates no copy, deletes no disk, and keeps the addresses of triggers it
cannot see. So a typo in an included file never costs you the database inside
it. Fix the file and the node is back, with its data. If you really mean to
remove it, take it out of the source with every file compiling, or stop and
terminate it by hand.

## Triggers

| What you want | Run |
|---|---|
| My program should answer the world now | `weft activate` |
| Stop it answering | `weft deactivate` |
| I changed the program, and its triggers are on | `weft resync` |
| Prepare a trigger without exposing it yet | `weft bake` |
| Try one trigger with an event I made up | `weft run --fire <trigger>='<json>'` |
| I clicked activate and want it back | `weft cancel-activate` |

`weft activate` turns on every trigger it names that is off, and leaves the
ones that are on alone. With nothing off it says so and succeeds, so a script
can run it every time. It runs each trigger's setup once, then leaves the
listeners running. Setup registers subscriptions with providers and dials
sockets. If one cannot, `weft activate` fails with the reason, and every trigger it
was turning on stays as it was.

It never starts infra and never touches a run going now. If a trigger's setup
reads infra that is not up, it refuses (a trigger whose runs alone use that
infra is turned on, with a note naming it):

```text
these triggers' infra is not running: pg. Start it with `weft infra start`, then run this again.
```

Each trigger is turned on and off separately. `weft activate --trigger inbound`
turns on one and leaves the rest alone, and `--trigger` is repeatable on every
trigger verb. `weft status` lists each trigger's own state under `triggers:`.

### Each trigger fires its own cut

When a trigger fires, its run does not run your whole program. It runs the
trigger's **cut**: the part of the program from the trigger down to what it
reaches, and back up for what those nodes need. Each trigger has its own,
recorded when it was set up, and it decides two things.

A trigger is **behind** when its cut or the code it runs changed since it was
set up. `weft status` shows it as ``on, behind the source (`weft resync` brings
it up to date)``, and a resync moves only those. If a trigger's cut no longer
works out in the source (it sits inside a loop that runs only in part), it
shows as behind too while it is on. If you deleted a trigger from the source
and it is still on, `weft status` says the program has it no more, with
`weft deactivate --trigger <name>` to take it down.

The infra a trigger depends on is what its runs reach plus any infra its
setup reads an address from (infra it reads only through [baked
outputs](../nodes/infrastructure.md#outputs-saved-with-the-infrastructure-baking)
is not). `weft status` writes it beside the trigger (`feed: on; reads pg`),
and it decides what the infra commands take down, below.

### Frozen inputs

A trigger that is on keeps the input values it was set up with until you
resync it, and shows as behind meanwhile; for why, go and read [a trigger's
inputs are frozen](../language/triggers-and-routes.md#a-triggers-inputs-are-frozen).
Any edit to the program also makes the trigger's last bake stale, so `weft
run --fire` refuses it until you bake again. `weft bake` does that without
touching what listens.

### Taking triggers down: three questions

Every command that takes triggers down answers the same three questions (the
door, what is held, the runs going), each with a default that loses nothing.
`--door-for` is part of the door's answer:

| Flag | The question | The answers |
|---|---|---|
| `--door take\|hide\|refuse` | What happens to work that arrives while they are down | `take` (the default) holds it and runs it once they are back, on the version they come back with. `hide` does the same, but a question waiting for a person is hidden from the people who answer it. `refuse` takes nothing |
| `--door-for <minutes>` | With `take` or `hide`: when the door starts refusing | After this many minutes (1 or more), new work is refused and the listeners stop; what was already held stays held. Unset, the door holds until they are back |
| `--held keep\|drop` | What happens to the work they still hold from the last time they were down, and to the paused runs the command reaches (`waiting_for_input`): for `deactivate` and `resync`, the ones these triggers started; for an infra command, every one whose cut contains the infra | `keep` (the default) keeps it for when they are back. `drop` deletes the held work and cancels the paused runs |
| `--running wait\|cancel` | What happens to the runs going right now | `wait` (the default) lets them finish on the version they started on, and the command goes on only once they have, however long that takes. `cancel` stops them at once |

The door treats three kinds of work alike: a call from outside to weft's
address (a webhook, a submitted form, a provider's push), an answer to a run
waiting on a person, and an event a listener picks up itself (a schedule's tick, a
message on a connection it holds). Listeners keep listening behind a door that
holds, so a schedule held for two hours has two hours of ticks waiting when it
comes back. Behind a door that refuses, or once its window ended, the
listeners stop, and what arrives meanwhile is never caught up. That goes for
a trigger that keeps its place in what it reads too (a poll's cursor in its
feed, the files a folder watch last saw): when it listens again, because you
switched it back on or ran `weft deactivate` on it again with `--door take`
or `hide`, it starts from its source as it finds it, so nothing that came
behind the refusing door fires.
What such a trigger missed while weft itself was down (stopped, the machine
off, `weft maintenance on`) it catches up from where it left off. A caller waiting on a route for its answer cannot be held, so
whatever the door, it is answered `503` with a `Retry-After` at once, and a
client that retries gets through once the trigger is back.

weft never asks you these: the command does what the flags say, then prints
what it did. Each event or call the door held counts as an arrival, and an
answer held for a waiting run is counted with that run:

```text
imagebot (a1b2…): took down 'feed', 'door': the door holds what arrives; 3 arrivals and 1 paused run kept; 2 runs waited for, finishing on their own version
```

`weft deactivate`, `weft resync`, `weft infra stop`, `terminate` and
`upgrade` take these flags with these defaults, and the editor's buttons and
a program's own calls answer the same questions (for the calls, go and read
[taking something down from a
run](instances.md#taking-something-down-from-a-run)). Three take triggers down
with answers you do not pick, or only some of them:

| What takes them down | Door | Held | Running |
|---|---|---|---|
| `weft rm` | `refuse` | `drop` | `cancel` |
| The health check, when infra a trigger reads breaks | `take` | `keep` | `cancel`, since those runs cannot go on without the infra |
| `weft maintenance on`, before an update ([updating weft](cli.md#updating-weft)) | `refuse`, so nothing starts on the version being replaced; a trigger that keeps its place in what it reads still catches up from it once `weft maintenance off` brings it back, since the pause was weft's and nobody refused anything | `keep` | `wait`, or `cancel` with `--running cancel` |

### Bringing them back

`weft activate` brings back what is off, and decides what happens to the work
the triggers held while they were down:

- `--held run` (the default) releases it, paced at half each trigger's own
  `callsPerMinute` and `callsAtOnce` (at least one of each), so new arrivals
  still get through beside it;
- `--held drop` deletes it and cancels the runs paused on those triggers.

It prints what it did: `imagebot (a1b2…): turned on 'door'; 12 arrivals
released, at half each trigger's limits`.

### While a command waits on running runs

If a command is waiting on runs to finish (`deactivate`, `resync` and the
infra commands under the default `--running wait`, or a `weft rm`), the
waiting happens on the install and your terminal only follows it. Ctrl+C
stops following and the work goes on. While it waits, `weft stop <run>` ends
one run now.

What Ctrl+C leaves behind depends on the command. After a `deactivate` the
triggers stay down. A resync goes on until the old version's runs have ended,
then brings its triggers back on the new one. An infra command goes on too.
If you call it off with `weft infra cancel` while it is still waiting for
runs, the infra stays up and the triggers it took down come back on; if it
was already taking the infra down, it stops where it got to, and `weft infra
status` shows what is left. A `weft rm` has already taken the
project down, but it erases nothing until you run `weft rm` again, which
picks up the wait (with `--journal`, until every run of the project has
ended) and then finishes the removal.

### Infra commands take down only what reads the infra

`weft infra stop`, `terminate` and `upgrade` take down exactly the triggers
that depend on the infra they touch, and reach exactly the runs whose cut
contains it. With `--running wait`, the infra stays up until every run
reaching it has ended; with `--running cancel`, those runs are cancelled and
the infra goes.

The triggers they took down come back on by themselves once that infra runs
again, after `weft infra start`, the end of an `upgrade`, or a program's own
`ctx.infra(..).start()`. If you ran `weft deactivate` on one of them while
the infra was down, it stays off. Meanwhile `weft status` says `off, its door set to take, taken down with its
infra (it comes back once that infra runs again)`.

The health check reaches the same triggers. A unit that turns `flaky` hands
the decision to the node's health rules, and a node that writes none gets two
built-in ones: a copy that turns `flaky` takes down the triggers whose cut
contains it, with the fixed answers above, and once it is healthy again they
come back. `weft status` then says `taken down by the health check`. If a
node's own rules never bring them back, `weft activate` does. If you write your own
rules on a node, go and read [when a unit stops being
ready](../nodes/infrastructure.md#when-a-unit-stops-being-ready).

### One build answers a project's routes

If you changed a node's Rust code or updated weft, the new build is a new
worker binary, and one binary answers all of a project's routes. So while some triggers are
still on the older build, turning one on on the new build is refused rather
than splitting the routes between two binaries. If you build with the default
(the whole catalog), a change to the graph alone keeps the same binary and
never hits this; with `--referenced`, using a node type the graph did not use
before makes a new one:

```text
one binary answers a project's routes, and these triggers are on another build than the registered one: 'door'. `weft resync --all` moves every trigger that is on to the new build; then run this again
```

A trigger that should come back by itself (taken down with its infra, or by
the health check) but cannot for the same reason waits instead. `weft status` and the
editor show it as `held back`, with the same reason and the same way through,
and a resync brings it back at once.

### Resync

`weft resync` builds, then brings every trigger that is on and behind the
build onto it. Each one goes down with the three answers above, its paused
runs move onto the new build (with `--held keep`), the runs going on the old
version finish or are cancelled (`--running`), and only then does it come back
on the new version, which takes over from the old one. A trigger that is off
is never turned on: the answer names it with the `weft activate` that would.

A resync writes down its plan before it takes any trigger down, and carries
it out on the install, so it goes on whatever happens to your terminal or to
the weft process doing it: if that process dies, the next one to take the
work up carries on where it was. Until it finishes or you give it up (even while it is stopped after a
failure, below), the resync holds the project:
no other activation of its triggers starts, a build you start is refused with
`cannot build now`, and a build that was already compiling finishes but is
not registered (`this build was not registered: the project is resyncing;
...`).

If a step fails, the resync tries again 5 seconds later, then 20 seconds after
that. If steps fail three times during the resync, it stops with the triggers it has not
brought back still down, and says why: in the answer to `weft resync`, under
`waiting on weft:` in `weft status`, and as "Resync stopped" on the editor's
action bar. If a step fails because the install's database was busy or out
of reach, it uses none of those three tries: the resync tries it again after
5, 15, 30 and then 60 seconds, and if the database still cannot take the
step, the resync stops, then tries again by itself 30 seconds later. If it stopped
because the infra those triggers read was not
running, or because an activation of them was still landing, it goes on by
itself once that infra runs or that activation has landed. Otherwise, once
you have fixed the cause, `weft resync --resume` (or the bar's Resync button)
takes it up again where it stopped, with the answers and the build it began
with. A plain `weft resync` is refused while a resync holds the project.

If you would rather give it up, `weft cancel-activate` with no `--trigger` or
`--instance` (or the bar's Give up button) ends it and names the triggers it
had not brought back yet:

```text
gave up the resync of a1b2…; these triggers it took down stay down: 'feed', 'door'. `weft activate` turns them on
```

Until they are on again or the next resync begins, `weft status` lists them
under `waiting on weft:` as `a given-up resync left these triggers down:
'feed', 'door'`. While a resync holds the project, `weft cancel-activate
--trigger` or `--instance` is refused, because the resync covers every
trigger it moves.

### Cancelling an activate

`weft cancel-activate` cancels the setup run of each trigger still turning on,
wipes every signal it registered so far, and leaves those triggers down. If
the weft process carrying out an activate dies at a point where no setup run
of it is going, weft ends the activate the same way by itself, about a minute
later.

### Instances

If a trigger reads an instance's copy or value (`@per_instance`,
`@instance_filled`), it exists once per instance; for turning those on and
off, go and read [programs with instances](instances.md).

## Runs

| It starts from | Command |
|---|---|
| You | `weft run` |
| One trigger, by hand | `weft run --fire <trigger>='<json>'` |
| A trigger firing for real | Nothing. It happens because you activated |
| A caller connecting | Nothing. The trigger has to be on |

| Status | What it means |
|---|---|
| `running` | Work is happening now |
| `waiting_for_input` | Parked on a person or a service. No worker is up and nothing is being spent |
| `completed` | Finished |
| `failed` | A node failed (a wait the run held past its `holdSecs`, which that node did not handle, counts as one), or the run could get no further |
| `cancelled` | You or another run stopped it; its live caller hung up; it was a fast run and its worker went away; its worker was stopped while it could not pause (`recorded: false`, a caller still on the line with `outlivesCaller` off, or a bus between its nodes open); it reached the hour a cloud install allows one stretch of a run; its caller's session reached the route's or socket's `maxSessionSecs`; or the runtime stopped it (a newer build replaced its image, its build's worker could not start, or a queued run was refused three times, which names who said the last no: `its worker refused it 3 times: <error>`, `the platform refused weft's calls for it 3 times: <error>`, or `its project's worker settings kept it from any worker 3 times: <error>`) |

`weft runs --status running` lists both the ones working and the ones
waiting, and the status column tells them apart.

### A run that cannot start or resume

A run whose build's worker cannot start at all is not tried again. It ends at
once, with every other run queued on that build, cancelled with the reason:

```text
<why>. Nothing will run on that build again: fix the program and build again (`weft resync` puts triggers that are on onto the new build), then `weft run --seed=<run-id>` picks this run up from what it did
```

A paused run whose record no longer fits the program it would resume on (a
resync moved it onto a build its nodes changed in) ends failed instead,
naming the same `weft run --seed` command. Either way its record is kept
like any ended run's, so the seed has something to reuse.

### Waiting, and stuck

**Waiting** means somebody still has to answer. **Stuck** means nobody can:
nodes are holding values that will never add up to enough to run them, and
nothing is parked on a signal, so no answer exists that would move it along.

weft ends a stuck run as failed and names every node and what each is short of:

```text
run stuck: 2 firings hold pulses and can never fire: theirs has value,
still waiting on go; leaf has value, every wired input arrived
```

That is a bug in the shape of your graph rather than something to wait out.

### Stopping one

`weft stop <run-id>` cancels it. If a worker is up, it stops within about fifty
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
  infra: a copy is behind the source; `weft infra upgrade` puts the change live
  binary: worker code has changed; the next run or `weft build` rebuilds the image
  definition: project shape has changed; the next run picks it up
```

A trigger that is behind says so on its own line under `triggers:`.

| What you changed | What to run |
|---|---|
| The shape of the graph, or a node's Rust code | Nothing for the runs you start by hand: the next one picks it up, and rebuilds what it must. Triggers that are on show as behind, and `weft resync` moves them (`--all` if instances have triggers on) |
| What an infra node asks for | `weft infra upgrade` |
| A trigger's inputs | `weft resync` |
