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
upstream. Any other trigger in the project closes its outputs, so paths that
needed it skip.

## A trigger's inputs are frozen

Whatever fed a trigger's ports when you activated is what it uses on every
firing. The nodes upstream do not run again per event.

Everything upstream of every trigger runs at activation as one program, so two
nodes that do not depend on each other run at the same time. Setup that several
triggers share, like the schema all their tables live in, is written once and
wired to each of them; two copies of `create extension if not exists` racing
each other is how one of them fails on a duplicate key.

If you want something done once before the triggers listen, like creating the
table they write to, wire it in front of them. A gate is enough, since any
value but `false` lets the trigger through:

```weft
pg = PostgresDatabase {}

schema = PostgresExecuteQuery {
  account: pg.access
  query: "create table if not exists cards (id text primary key, title text)"
}

add = Route { path: "cards", method: "POST" }
add._should_flow = schema.rows

list = Route { path: "cards", method: "GET" }
list._should_flow = schema.rows
```

`schema` runs once, at `weft activate`, before either route listens, and if it
fails the activation is cancelled. A request never runs it again.

So after changing anything in front of a trigger, `weft resync`. Until then
`weft status` shows the trigger as behind, and your listener carries on with
what it registered. weft tells you when a bake is stale:

```text
the code changed since trigger 'inbound' was last prepared, so its bake is
stale. Prepare it again: `weft bake` does it and changes nothing that listens;
if the trigger is off, `weft activate` does it and turns it on; if it is on,
`weft resync` brings it onto the new build and prepares it there
```

## Answering on a URL

`Route` claims an address and starts a run when somebody calls it.

```weft
type Card = { title: String, color?: String }

post = Route -> (body: Card) {
  path: "cards"
  method: "POST"
}

answer = JsonObject {
  title: post.body.title
  status: "saved"
}

reply = Reply { body: answer.object }
```

The caller is held open while your program works, and `Reply` is what answers
them. For streaming an answer as it is produced, and for the Rust side, go and
read [talking to a live caller](../nodes/live-callers.md).

The request's body comes out whole on `body`. Left undeclared, it is a
`JsonDict`. If you declare a type on it, as `Card` above, a call whose body
does not fit is answered `400` naming what is wrong, and no node after `post`
runs. A body carrying a key that `Card` does not declare is answered `400`
too. If callers may
send more than you read, end the type with `..`; for what that does, go and
read [what fits into what](types.md#what-fits-into-what). If callers send JSON
of any shape, a list or a bare number as well as an object, declare
`-> (body: Json)`. A request with no body, like a `GET`, closes `body`,
whatever the route's `dataType`. A json body that is `null` is a value: it
comes out on a `body` whose type takes `Null` (`Json` does), and closes
`body` on any other, the way a `null` counts as nothing on a port that does
not take it.

If you want part of the path as a value, capture it with `{name}` and declare
an output of the same name:

```weft
card = Route -> (id: String) {
  path: "cards/{id}"
  method: "GET"
}
```

A call to `cards/42` puts `"42"` on `card.id`. Every other output you add to a
`Route` has to be a capture of its path, typed `String`, or the build stops
with `route-output-not-a-capture`. A capture can't share a name with one of
`Route`'s own outputs (`body`, `method`, `path`, `query`, `headers`, `caller`,
`fired`), so `cards/{body}` is the `route-capture-shadows-output` error,
asking you to rename the capture.

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

If a call arrives while the route's trigger is down or still being set up, it
does not wait for the trigger: it is answered `503` at once, with a `Retry-After` of 5 seconds,
so a client that retries gets through once the trigger is back on. The
exception is a trigger taken down with `weft deactivate --door refuse --held
drop`, which takes its route away with it, so a call is answered `404`,
the same as a call to a path no route serves. For what a take-down does with
other work, like a webhook or a form, go and read [starting and stopping
things](../running/lifecycles.md).

## How a run is kept

Your runs happen inside a worker: a copy of your compiled program, running as
a process on your machine, or as a Cloud Run service on a cloud install. A
worker
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
| `journalEphemeral` | off | what streams through the route should not be written to the run's record, only counted. For what that keeps, go and read [two modes](streams-and-buses.md#two-modes) |
| `journalWindowMs` | 1000 | you want the record's rows to cover less or more time, from 100 to 60000 ms. Same page |
| `holdSecs` | 60 | a run that cannot pause should wait longer for an answer (up to 30 days), or not at all (`0`). See [when a run cannot pause](#when-a-run-cannot-pause) |

**By default (`durable` off), a run is fast: it only waits for the database
when it has to.** That is when it pauses (a timer, a form), and in a few
other places (for the full list, go and read [when a run waits for
its writes](../running/the-journal.md#when-a-run-waits-for-its-writes)). The
rest of the time its work lives in the worker's memory, and its record (the
journal that `weft events` and the editor read) is written a moment behind
it, as it goes. If its worker dies, the run ends
cancelled and is not run again. For why, and for what happens when the
platform shuts a worker down on purpose, go and read [what happens when
something dies](../running/architecture.md#what-happens-when-something-dies).

**If a run must not end when its worker dies (it moves money, say), turn
`durable` on.** Before each node starts, everything the run did so far is
written to the database, and a route's answer is written before it is sent.
The exception is a [pure](../nodes/metadata.md#features) node
(`Text`, `JsonObject`, `Switch`, `Reply`), which does nothing outside the run
but answer its caller and handle the run's own files, and starts without
waiting.

If the worker dies mid-run, another worker picks the run up where it stopped:
every finished node stays finished, and a node that was in the middle of its
work is failed with a message saying it may have partly happened, so you know
which node to check. The exception is a pure node that takes no
stream and had not passed anything on yet: it simply runs again. If a caller
started the run, its connection was on the old worker and is gone: the run
carries on in the new worker with nobody on the line, so a node that answers
the caller fails, saying no live caller is attached. If the platform stops the
worker instead, a run that can pause gets 5 seconds for the nodes already
running to finish, and only a node still running after that is failed. A run
that cannot pause ([below](#when-a-run-cannot-pause)) keeps running on the
stopping worker for as long as the platform lets it, and if the worker goes
before the run ends, that is the same as the worker dying. If you want to know
what a wired `_error` does with that failure, or what happens to a node that was
waiting on an answer or reading a stream, go and read [surviving a
restart](../nodes/durable-execution.md#when-the-worker-dies-mid-run).

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
is quiet, meaning every node still running is waiting, on an answer or on a
bus. Anything that moves (a bus message, a node finishing) starts it over. If
the run stays quiet for `holdSecs` (60 seconds unless the trigger says
otherwise), the call that was waiting fails, saying it gave up, and the wait is
withdrawn, so a form is no longer offered. The node's code can handle that
error like any other, and if it does not, the program can: it comes out on the
node's `_error` when that is wired ([when a node
fails](how-a-program-runs.md#when-a-node-fails)). With `holdSecs: 0`, such a
wait fails at once.

If the run becomes able to pause while a wait holds (the last bus between its
nodes closes), the wait pauses after all, the way it would have from the
start. A run whose caller leaves while `outlivesCaller` is off is cancelled,
as always. And a fast run holding a wait still ends with its worker.

**If a page asks a route for a status every few seconds, turn `recorded`
off**, or `weft runs` fills with hundreds of runs. weft keeps no history
of such a run, not even in its worker's memory. If it fails, `weft runs`
lists it with the node that failed and why, and nothing of what ran before. If
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
`ctx.tag_run` (a label you later find or stop runs by), the node fails:
an unrecorded run has no record a tag could point at. The compiler also
refuses `durable: true` with `recorded: false`.

**On a cloud install, one stretch of a run lasts an hour at most.** Cloud Run
cuts a request at 60 minutes, so a run stops itself 59 minutes after it
started on its worker. It ends cancelled with a message saying so, instead
of being cut off while a node is still running, with no ending written. A run
that pauses whole (every branch
waiting on a timer or a form) starts a fresh hour when it picks back up; one
branch waiting while another runs does not. So if a job can take longer than
an hour, split it with a pause. On your machine no run is cut.

A run that answers a caller lives on the worker the caller reached. While the
caller is on the line, the run stops at 59 minutes too, with a message telling
you to have the client reconnect. Once the caller has gone, a route with
`outlivesCaller` on keeps its run going, and on a cloud install the run moves:
it starts no new node on that worker, and once the nodes it was running end, it
carries on under a call weft holds open for it, with a fresh hour. Moving works
from the run's record, so the compiler refuses `outlivesCaller: true` with
`recorded: false`. A run with a bus open cannot move, since a bus lives in its worker's memory
alone, so it keeps running on that worker for as long as the worker stays up.

### When a run ends before it answers

Every answer a route's run gives, good or bad, carries a `Weft-Run-Id` header
holding the run's id.

If a run ends before it has finished answering its caller, the caller gets
one of these statuses instead, and this JSON body:

```json
{"error": "Something went wrong", "runId": "9b81d0a2-..."}
```

| Status | When |
|---|---|
| `500` | the program failed, got stuck, or ended without answering |
| `504` | a time limit ran out (the four limits are listed below the table) |
| `503` | weft, a person, or another run (`ctx.stop_tagged`) stopped the run, weft could not keep it going, or it carries on without this caller (it paused on a wait, or moved to another worker) |

A `504` means one of four limits ran out: on a cloud install, the run's
hour; the route's or socket's `maxSessionSecs` (the longest one connection
may stay open, off unless you set it); a wait that held for its whole
`holdSecs`; or, on a cloud install, the hour a worker's connection to weft
may last, when it runs out while every run on that worker only waits (for
why, go and read
[the broker](../running/architecture.md#the-broker-and-who-may-talk-to-what)).

The caller never sees why the run ended: the reason can name your nodes, your
files and a provider's words, and those are yours to read. The whole reason
stays on the run's record, and you open it with `weft logs <run id>` or in the
graph. The id is safe to hand out, because opening a run takes your own
credential. If you want a caller to get a message of your own instead, wire
`_error` on the nodes that may fail and gate a `Reply` of yours on it; the run
then no longer fails ([when a node
fails](how-a-program-runs.md#when-a-node-fails)).

A `503` here carries no `Retry-After`, because calling again would start a
second run beside the one that may still be going. If the answer had already
started, its status was already sent, so the error comes at the end of the
body instead, in the answer's own format:

| The answer is | It ends with |
|---|---|
| JSON lines | one more line, `{"error": "Something went wrong", "runId": "...", "status": 504}`, where `status` is the one the call would have got |
| Server-sent events | an `error` event whose data is `{"error": "Something went wrong", "runId": "..."}` |
| Anything else | a line `[error] Something went wrong (run <id>)` |

A socket closes with `1011` where a call would get `500` or `504`, and with
`1013` where it would get `503`, and the close reason is that same
`Something went wrong (run <id>)`.

A `Stream` answers this way too when the bus it reads ended failed: a `500` if
nothing was sent yet, the marker above if it was, `1011` on a socket. Its
`done` output then stays absent.

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

## Watching a folder

If you want a run each time a file lands in a folder on your machine (a scan,
a download, an export from another program), use `FolderWatch`:

```weft
invoice = FolderWatch {
  folder: "/home/me/Invoices"
  pattern: "*.pdf"
  events: ["created"]
}
```

Each run gets the file's absolute path on `invoice.path`, what happened to it
on `invoice.event` (`created`, `changed` or `removed`), and its size on
`invoice.sizeBytes`, which is absent for a removed file. The worker runs on
the same machine, so a node downstream opens the path itself.

`folder` has to be an absolute path to a folder that exists when you run
`weft activate`. `recursive: true` watches the folders under it too, however
deep. `pattern` matches the file's name only (`*.{pdf,docx}` for either).
If you leave `events` out, all three fire.

A save or a slow copy that writes one file many times starts one run, sent
once the file has been quiet for a second. To decide what happened, weft
compares the file with what it last saw rather than trusting the operating
system's event, because systems report a save differently: as a write, as a
rename over the old file, or as a removal followed by a creation.

weft keeps what it last saw in its database, so a file that arrives, changes
or goes while weft is stopped, while it is in maintenance for an update, or
while your machine is off still fires once the trigger is back up, the same
as if weft had been watching. While you have the trigger down behind a door
that takes or hides, weft keeps watching, and what fires waits for the
trigger to be back on. Behind a door that refuses, or once a door's
`--door-for` window ended, nothing is watched: what happened in the folder
meanwhile never fires, and turning the trigger back on starts it as if for
the first time. A poll keeps its place in its feed by the same rule; for
the rule itself, go and read [taking triggers
down](../running/lifecycles.md#taking-triggers-down-three-questions).

The first time the trigger starts, the files already in the folder fire
nothing. Changing `folder`, `pattern` or `recursive` counts as a first time;
changing `events` does not. If weft stops just after a file's run started and
before it wrote that down, that file fires once more when weft is back. If a
file's run could not be handed over at all, weft offers it again the next
time that file changes or the trigger starts.

It only works on a local install, since that is where your folders are. On a
cloud install, `weft activate` refuses it, with a message that starts `a
folder watch needs the install to run on the machine the folder is on`. On WSL, a folder under
`/mnt/<drive>` is refused too, because Windows tells Linux nothing about the
changes made there: move it under `~/`.

## Slow callers and messages that cannot be made

Picture a live price page streaming from a `Route`, and a phone on a bad
connection reading it. Your program writes faster than the phone reads, so
messages pile up for it. Up to 256 wait; past that, the route's
`whenCallerIsSlow` decides what gives:

| `whenCallerIsSlow` | What happens when 256 are waiting | Set it when |
|---|---|---|
| `block` (the default) | Your program's next send waits for room, so a slow caller slows the program down and gets every message | The caller has to read the whole thing: a model's answer, a conversation |
| `drop_oldest` | The oldest waiting message is thrown away to make room, so the caller always gets the latest | Only the newest value matters: live video, a price feed |
| `drop_newest` | The new message is thrown away, and what was already waiting goes out in order | The messages already waiting matter more than the ones arriving while the caller is behind |

```weft
db = PostgresDatabase { database: "shop" }

prices = Route -> (symbol: String) {
  path: "prices/{symbol}"
  method: "GET"
  whenCallerIsSlow: "drop_oldest"
}
latest = PostgresWatchQuery(symbol: String) {
  account: db.access
  symbol: prices.symbol
  query: "SELECT price FROM quote WHERE symbol = $symbol"
  channel: "quote_changed"
}
feed = Stream { format: "ndjson", bus: latest.row }
```

The other setting, `whenChunkFails`, is for a message your program cannot turn
into the route's `dataType` at all: a value that is not a `String` on a `text`
route, a file that cannot be read.

| `whenChunkFails` | What happens | Set it when |
|---|---|---|
| `surface` (the default) | The node sending it fails and the caller is told, the way [a run that ends before it answers](#when-a-run-ends-before-it-answers) tells them | Every message counts |
| `drop_chunk` | That message is skipped, the skip goes on the run's record, and the answer goes on | One missing message is fine, like a frame of a live feed |

A `Socket` has both settings and they mean the same, for every message a
`Reply` or a `Stream` sends on it; under `surface` the socket closes with
`1011`. A run that fails, or a bus that ends failed, is told to the caller
whatever `whenChunkFails` says.

If a caller says part of a stream never arrived, the run's record shows
which. The inspector strikes each lost message through and says why, and
`weft events` prints them on the caller's rows as `dropped=<offsets> (<why>)`,
an offset being the message's place in the conversation. The messages still
waiting when the caller left are named the same way, whatever
`whenCallerIsSlow` says. A message skipped under `drop_chunk` shows as
an error row saying `a chunk was dropped and the exchange went on`, then why.

## Which triggers need a public address

Only the ones a provider **pushes** to.

| The trigger | Needs a public address? |
|---|---|
| A timer or a schedule | No |
| Something weft polls | No |
| A socket weft dials out to | No |
| A form somebody fills in | No |
| A folder on your machine | No |
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
find it in `weft runs`. For the flags that change how one run is kept, go and read [the `weft
run` flags](../running/cli.md#run-something).

For writing a trigger of your own, go and read
[writing a trigger](../nodes/triggers.md).
