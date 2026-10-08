# Triggers and routes

A trigger is how a program starts without you. A message arriving, a schedule
coming round, a form somebody fills in, a request hitting an address.

```weft
ask = TelegramReceiveMessage { account: telegram.access }
```

Wire it like anything else. What comes out of it is the event.

Nothing listens until you run `weft activate`. Go and read
[the three lifecycles](../start/lifecycles.md) for why that is its own step.

## What a run started by a trigger covers

Everything downstream of the trigger that fired, plus whatever that work needs
upstream, stopping the walk when it reaches another trigger.

Only the trigger that actually fired gets the event. Any others in the project
close their outputs, so paths that needed them skip.

Two triggers can share the steps in the middle without their runs becoming one.

## A trigger's inputs are frozen

Whatever fed a trigger's ports when you activated is what it uses on every
firing. The nodes upstream do not run again per event.

Everything upstream of every trigger runs at activation as one program, so two
steps that do not depend on each other run at the same time. Setup that several
triggers share, like the schema all their tables live in, is written once and
wired to each of them; two copies of `create extension if not exists` racing
each other is how one of them fails on a duplicate key.

So after changing one, `weft resync`. Until then your listener carries on with
what it registered. weft tells you when a bake is stale:

```text
the code changed since trigger 'inbound' was last prepared, so its bake is
stale. Prepare it again: `weft bake` does it without listening, `weft activate`
does it and listens
```

## Answering on a URL

`Route` claims an address and starts a run when somebody calls it.

```weft
post = Route -> (body: JsonDict, photo: File) {
  path: "cards"
  method: "POST"
}

reply = Reply {
  body: { "id": saved.id }
}
```

The caller is held open while your program works, and `Reply` is what answers
them. For streaming an answer as it is produced, and for the Rust side, go and
read [talking to a live caller](../nodes/live-callers.md).

A path can capture: `cards/{id}` puts `id` where your node can read it.

**Two routes that one call could reach, where neither is more specific, is an
error.** `cards/count` beside `cards/{id}` is fine, because the first is more
specific. Two spellings that genuinely overlap are the `route-overlap` error,
because a call arriving would have no defined answer.

You know a route's address before you activate: on your machine it is
`http://127.0.0.1:14111/connect/local/<project id>/cards` (the install's
address, then `/connect/local/`, then your project's id, then the path). The
project id is the `id` under `[package]` in your `weft.toml`, so it is the
same on every install, and two projects can each serve a `cards` route without
getting in each other's way. On a cloud install it is the install's own
address, then `/connect/local/<project id>/`, then the path. `weft activate`
prints the id (`activated cards (<project id>)`), and the route's trigger
shows its full address in the editor.

If you want the fastest way in, or a frontend wants your routes at the root of
an address, call the project's own address, where your program answers with
nothing of weft's in between. On your machine that is a free port the project
gets the first time you activate it and keeps from then on
(`http://127.0.0.1:14200/cards`); if another program takes that port, the
project moves to another free one. On a cloud install it is the project's own
Cloud Run address. On your machine, if you want a port of your choosing, `weft
activate --port 8080` opens that one, and the project keeps it for later
activates. If another program or another of your projects holds that port,
`weft activate` fails and nothing is activated.
`weft activate` prints the address, and `weft status` shows it later or says
why it is unavailable. Your routes keep answering under `/connect/local/<project id>/` as well.

If a call arrives while the route's trigger is parked, hibernating within its
grace window, or still being set up, it is answered `503` with a `Retry-After`
at once, so a client that retries gets through once the trigger is on. Once
the project is switched off (wiped), its own port is closed and
`/connect/local/<project id>/<path>` answers `404`.

## How a run is kept

Your runs happen inside a worker: a copy of your compiled program, running as
a Docker container on your machine, or as a Cloud Run service on a cloud
install. A worker
keeps the runs it is working on in its memory. A trigger has inputs that
decide what happens to those runs: whether they survive their worker dying,
whether they are written down, how long they are kept once they end, and how
long a wait holds when the run cannot pause. You write them in the trigger's braces like
any other input:

```weft
pay = Route -> (body: JsonDict) {
  path: "pay"
  method: "POST"
  durable: true
}
```

| Input | Default | Change it when |
|---|---|---|
| `durable` | off | a run has to carry on after its worker dies, instead of ending |
| `recorded` | on | a page asks a route for something every few seconds (turn it off) |
| `outlivesCaller` | off | a run answers its caller early and keeps working (only on a trigger that holds a caller on the line, like `Route` and `Socket`) |
| `keepRunsFor` | the project's, else a week | you want its runs kept longer or shorter once they end (`12h`, `30d`, `forever`). For the project-wide default, go and read [how long a run is kept](../running/the-journal.md#how-long-a-run-is-kept) |
| `holdSecs` | 60 | a run that cannot pause should wait longer for an answer (up to 30 days), or not at all (`0`). See [when a run cannot pause](#when-a-run-cannot-pause) |

**By default (`durable` off), a run is fast: it only waits for the database
when it has to.** That is when it pauses (a timer, a form), and in a few
other places (for the full list, go and read [when a run waits for
its writes](../running/the-journal.md#when-a-run-waits-for-its-writes)). The
rest of the time its steps live in the worker's memory, and its record (the
journal that `weft events` and the editor read) is written a moment after
each step. If its worker dies, the run ends
cancelled and is not run again. For why, and for what happens when the
platform shuts a worker down on purpose, go and read [what happens when
something dies](../running/architecture.md#what-happens-when-something-dies).

**If a run must not end when its worker dies (it moves money, say), turn
`durable` on.** Before each step starts, everything the run did so far is
written to the database, and a route's answer is written before it is sent.
The exception is a step of a [pure](../nodes/metadata.md#features) node
(`Text`, `JsonObject`, `Switch`, `Reply`), which does nothing outside the run
but answer its caller and handle the run's own files, and starts without
waiting.

If the worker dies mid-run, another worker picks the run up where it stopped:
every finished step stays finished, and a step that was in the middle of its
work is failed with a message saying it may have partly happened, so you know
which step to check. The exception is a step of a pure node that takes no
stream and had not passed anything on yet: it simply runs again. If a caller
started the run, its connection was on the old worker and is gone: the run
carries on in the new worker with nobody on the line, so a step that answers
the caller fails, saying no live caller is attached. If the platform stops the
worker instead, a run that can pause gets 5 seconds for the steps already
running to finish, and only a step still running after that is failed. A run
that cannot pause ([below](#when-a-run-cannot-pause)) keeps running on the
stopping worker for as long as the platform lets it, and if the worker goes
before the run ends, that is the same as the worker dying. If you want to know
what `catchErrors` does with that failure, or what happens to a step that was
waiting on an answer or reading a stream, go and read [surviving a
restart](../nodes/durable-execution.md#when-the-worker-dies-mid-step).

Even with `durable` off, a run waiting on a timer or a form, with nothing else
of it running, survives its worker, because its whole record is written before
it pauses. A run that cannot pause is the exception, below.

### When a run cannot pause

Three kinds of run cannot pause: one whose caller is still on the line while
its trigger leaves `outlivesCaller` off (the caller cannot follow it), one with
`recorded` off (there is no record to pick it back up from), and one with a
bus between its nodes open (a bus lives in its worker's memory alone).

**When such a run reaches a timer or a form, the wait holds in the node's call
instead.** The wait is registered as usual, the run stays on its worker, and
the answer carries it on right there. The hold clock only runs while the run
is quiet, meaning every step still running is waiting, on an answer or on a
bus. Anything that moves (a bus message, a step finishing) starts it over. If
the run stays quiet for `holdSecs` (60 seconds unless the trigger says
otherwise), the call that was waiting fails, saying it gave up, and the wait is
withdrawn, so a form is no longer offered. The node can handle that error like
any other, or send it to `error` with `catchErrors`. With `holdSecs: 0`, such a
wait fails at once.

If the run becomes able to pause while a wait holds (the last bus between its
nodes closes), the wait pauses after all, the way it would have from the
start. A run whose caller leaves while `outlivesCaller` is off is cancelled,
as always. And a fast run holding a wait still ends with its worker.

**If a page asks a route for a status every few seconds, turn `recorded`
off**, or `weft executions` fills with hundreds of runs. weft keeps no history
of such a run, not even in its worker's memory. If it fails, `weft executions`
lists it with the step that failed and why, and nothing of what ran before. If
it reports a cost, or asks weft for something on its behalf that its worker
does not already hold (a stored file, a new connection or infrastructure
address, a task such as starting or stopping its infra, a stop through
`ctx.stop_tagged`, a wait on a timer or a form), it is listed with its costs
and how it ended. Otherwise it leaves nothing. If you want to check that a
trigger with `recorded` off is being called at all, `weft status` shows how
many runs each trigger started in the last minute or two, and how many of them
failed.

An unrecorded run cannot pause, so a timer or a form in it holds
([when a run cannot pause](#when-a-run-cannot-pause)). If a node tags it with
`ctx.tag_execution` (a label you later find or stop runs by), the node fails:
an unrecorded run has no record a tag could point at. The compiler also
refuses `durable: true` with `recorded: false`.

**On a cloud install, one stretch of a run lasts an hour at most.** Cloud Run
cuts a request at 60 minutes, so a run stops itself 59 minutes after a worker
starts it. It ends cancelled with a message saying so, instead of being cut
off mid-step with no ending written. A run that pauses whole (every branch
waiting on a timer or a form) starts a fresh hour when it picks back up; one
branch waiting while another runs does not. So if a job can take longer than
an hour, split it with a pause. On your machine no run is cut.

A run that answers a caller lives on the worker the caller reached. While the
caller is on the line, the run stops at 59 minutes too, with a message telling
you to have the client reconnect. Once the caller has gone, a route with
`outlivesCaller` on keeps its run going, and on a cloud install the run moves:
it starts no new step on that worker, and once the steps it was running end, it
carries on under a call weft holds open for it, with a fresh hour. Moving works
from the run's record, so the compiler refuses `outlivesCaller: true` with
`recorded: false`. A run with a bus open cannot move, since a bus lives in its worker's memory
alone, so it keeps running on that worker for as long as the worker stays up.

## Answering on a socket

`Socket` is the same idea for a WebSocket: the caller connects, your program
runs, and the two talk until one of them stops.

A client that can send headers (a server, a script, an app) opens its socket at
the route's address and is checked like any other call. A browser can't put a
credential on a socket's opening request, so on a route that checks callers it
asks first with a plain `GET`, which carries the credential, and gets back
`{"url": "...", "protocol": "websocket"}`; it opens its socket at that `url`
within a couple of minutes.

**On a cloud install, one connection lasts an hour at most.** Google cuts any
request to weft at 60 minutes, and an open socket is one long request, so a
socket (or a streaming answer) that is still open then is closed. A connection
can also drop sooner for ordinary reasons: a phone changing networks, a laptop
going to sleep. When it does, the route's `outlivesCaller` decides what
happens to the run: off (the default), the run stops; on, it carries on and
what it sends goes nowhere.

So if you want a conversation to last longer than one connection, keep what it
needs outside the run, and have the client reconnect:

- the client picks a session id once (or the program hands it one in its
  first message) and sends it on every connection, as a query parameter or in
  the first message;
- the program keeps the conversation's state in its own storage, keyed by that
  id (a table in the project's Postgres, a file), and reads it back when a
  connection arrives with an id it knows;
- the client reconnects whenever its socket closes, with the same id.

Each connection is then its own run, and losing one, at the hour or before,
loses nothing the conversation needs. If your client is a web page, the
connect library's `openLiveSocket` does the client's half: it keeps the
session id, sends it as the `session` query parameter on every connection
(your program reads it off the trigger's `query` port), and reconnects until
the page closes the socket, your program closes it with code `4000`, which
means the conversation is over (a run that simply ends closes with `1000`,
and that ends only its connection), or, when the page passes credentials,
the route refuses them. A browser is told nothing about a socket refused at
its opening (it sees only that it closed), so a socket opened without
credentials keeps trying, at most every 15 seconds.

## Which triggers need a public address

Only the ones a provider **pushes** to.

| The trigger | Needs a public address? |
|---|---|
| A timer or a schedule | No |
| Something weft polls | No |
| A socket weft dials out to | No |
| A form somebody fills in | No |
| A provider pushing events at you | **Yes** |
| A `Route` or `Socket` that strangers call | **Yes** |

`./setup.sh --public-url` opens one. Go and read
[a public address](../build/public-address.md).

## What a trigger cannot do

| | Why |
|---|---|
| Be wired from another trigger | A trigger's inputs are frozen at setup, so another trigger's output could never reach it |
| Have an infra node downstream | Provisioning happens before any event exists |
| Sit inside a loop | It registers once for the project, not once per item |

All three are compile errors, named `trigger-into-trigger`,
`trigger-into-infra` and `trigger-in-loop`.

## Firing one by hand

While you are building, you do not want to expose anything:

```bash
weft bake
weft run --fire inbound='{"chatId":"123","text":"hello"}'
```

`weft bake` prepares every trigger's settings and starts no listeners.
`--fire` then runs exactly one, with an event you typed.

The payload is checked against what that trigger declared it fires with, both
ways: a missing field is refused, and so is one you invented.

A run you fire this way follows the trigger's `durable` and `keepRunsFor`, and
it is always recorded, even when the trigger has `recorded` off, so you can
find it in `weft executions`. For the flags that change how one run is kept, go and read [the `weft
run` flags](../running/cli.md#run-something).

For writing a trigger of your own, go and read
[writing a trigger](../nodes/triggers.md).
