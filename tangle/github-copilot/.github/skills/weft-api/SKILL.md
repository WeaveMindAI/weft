---
name: weft-api
description: "Read when the program needs to answer calls from outside (an HTTP route, a websocket, anything a page or a service will call), and before briefing a frontend on what it will call: the URL known before activation, how a request's parts and body reach the graph, pictures in and out as links, streaming an answer as it is produced, holding a conversation open, who may call, and how to try it."
---


# Building an API

Everything here is in the `api` package of the catalog. You read each
node's `metadata.json` before wiring it; when a name below drifts from it,
the metadata wins. A [route] is one `Route` or `Socket` node: one trigger,
and one call is one fresh execution carrying the request on its ports. An
[answer] is any node that writes back to that caller: the runtime hands the
caller's connection to every node in the run, so a node of your own is one
as readily as a catalog node. The `api` package ships three: `Reply` sends
a body, `Stream` sends a bus as it flows, `Close` ends the exchange. A
[gate] is an auth access node wired into a [route]'s `auth`; the package
ships one per way of proving who is calling, and the listing names them. A
[stored-file value] is a few hundred bytes saying where a file sits in
storage, the only way bytes travel a wire.

## The URL is known before anything runs

A [route] answers at `<dispatcher base>/connect/<tenant>/<path>`, and every
piece is fixed by the install, not minted at activation: on a local install
the base is `http://127.0.0.1:9999` and the tenant is `local`, so
`hello = Route { path: "hello" }` answers at
`http://127.0.0.1:9999/connect/local/hello`, and a socket at the same
address with `ws://`. You write those URLs into the frontend-builder's
[the brief] the moment the routes are shaped, while the graph is still being
built; `weft activate` prints the same URLs afterwards and only turns them on.

## The shape

The run ends when it has sent its [answer]. You declare the body keys you
want on the trigger's arrow, never a body blob, and each arrives on its own
port.

```weft
hello = Route -> (name: String) { path: "hello", method: "POST" }
answer = Reply { status: 201 }
answer.body = hello.name
```

A [route]'s fixed output ports carry the request as the gateway saw it: the
method, the path as called, the captures, the query, the headers, and who
the [gate] let in. Its metadata lists them.

What you DECLARE on the arrow is how the body reaches the graph, and the
rule depends on the body shape the [route] is set to (`dataType` in the
source, "Body shape" in the editor):

- an object body: each declared name is a top-level key of it. There is no
  port for the whole object, so you declare every key you read, and a key
  that is itself an object arrives as one value (`meta: JsonDict`). An
  undeclared key is dropped.
- a text body: the whole body arrives as a `String` on the ONE port you
  declare.
- a binary body: it is stored, and the [stored-file value] arrives on the
  one port you declare.

Names can collide, and the order is fixed: a fixed port beats a body key of
the same name, and a declared port named like a path capture reads the
capture rather than the body.

```weft
card = Route -> (id: String) { path: "cards/{id}", method: "POST" }
```

The first thing the program sends commits the status line. A `Reply` on a
branch with `status: 404` works. A `Reply` after a `Stream` fails loud. A
program that never sends anything holds the caller while it runs, then the
caller gets `500` with the body `the run ended without answering`. If you
catch yourself wiring a branch that reaches no [answer], stop and write:
"Wait. Every branch answers." Then end that branch on a `Reply` or `Close`.

Two routes of one account can share a call as long as one of them spells out
what the other captures: `chat/general` beside `chat/{room}` is fine, and the
literal takes that one call while the capture takes the rest. What `weft
activate` refuses, naming both, is the pair where neither is the more specific
(`chat/{room}` beside `chat/{name}`, or `a/{x}/c` beside `a/b/{y}`, which both
answer to `a/b/c`). Give one of them a literal the other captures, or distinct
methods.

## Request-response

The graph decides the status. A Python node reading `params` and `query`
returns the body and the status together:

```weft
user = Route { path: "users/{id}", method: "GET" }
lookup = ExecPython(params: Dict[String, String], query: Dict[String, String]) -> (body: JsonDict, status: Number) {
  code: "uid = params['id']\nif uid == '42':\n    return {'body': {'id': uid, 'verbose': query.get('verbose')}, 'status': 200}\nreturn {'body': {'error': 'no user ' + uid}, 'status': 404}"
}
lookup.params = user.params
lookup.query = user.query
found = Reply
found.body = lookup.body
found.status = lookup.status
```

A `GET` route has no body: declare nothing on the arrow and read the fixed
ports. A `text` route: `say = Route -> (line: String) { path: "say", dataType:
"text" }`, and the `Reply` body must be a `String`.

A run that stops without a body ends through the closing [answer] node,
which takes no data from upstream: its `_should_flow` gate is the whole
wiring. So the shape for "do this, and if it did not work, stop here" is a
`Switch` on the outcome, the failing case gating a close with its reason
and status already written on it, the passing case gating the rest.

```weft
sweep = Route { path: "sweep/{what}", method: "DELETE" }
clear = ExecPython(params: Dict[String, String]) -> (removed: Number) {
  code: "return {'removed': 3 if params['what'] == 'cards' else 0}"
}
clear.params = sweep.params
outcome = Switch {
  value: clear.removed
  cases: [
    { "kind": "gt", "value": 0, "port": "some" },
    { "kind": "otherwise", "port": "none" }
  ]
}
report = Reply
report.body = clear.removed
report._should_flow = outcome.some
nothing = Close { status: 404, reason: "nothing to sweep" }
nothing._should_flow = outcome.none
```

The gate takes any port of any type; the value is never read, only a
`false` says no, so wire a Boolean into it only when its `false` should mean
"do not close". A branch that skipped closes its ports, so the `Close` behind
it skips too and the other branch's [answer] stands.

The gateway answers on its own before any run starts: `405` for a served path
called with the wrong method (naming the verbs it serves), `404` for an
unknown path, `401` for a caller the [gate] refused.

If you want a webhook receiver that says `200` at once and does the slow
part after, wire the `Reply` early in the graph, then the rest, and set
`outlivesCaller: true` so the caller hanging up does not cancel the run.

If you want a long job polled later, one route answers `202` with an id and
writes the job's state to the project's Postgres, and a second route reads
it back. Two routes and a table, no new node.

## Pictures in and out

A value a node emits is at most 100 KB; above that the node fails, naming
the port. A picture a caller sends inside a JSON body lands on a port
declared as its kind, and the [route] stores it on the way in:

```weft
upload = Route -> (photo: Image, caption: String) { path: "cards", method: "POST" }
```

The body key `photo` holds a `data:image/png;base64,...` URL (the media type
comes from it) or bare base64 (the media type comes from the bytes' own
signature); bytes that are not what the port declares fail the run by name.
Multipart bodies are not read: the caller sends JSON with a data URL. The
file lands at execution scope, walled to that run and swept when it ends: a
later request cannot read it even if it was kept. If you want to serve a picture from a
later request, wire it through `KeepFile { scope: "project" }`, which copies
it into the project's storage, then write the whole [stored-file value] it
emits into a jsonb column, exactly as it arrived.

Two columns, and only one of them is right:

- the whole value in a jsonb column: correct, and every answer builds a fresh
  link out of it.
- a `url` you pulled out of that value and put in a text column: dead by
  morning, because the link is minted per answer and expires in minutes.

So never unwrap the value, never pull a field out of it, never strip its
`url` before writing it. `Reply` and `Stream` recognise a stored file by its
whole shape, and a value you edited comes back to the caller as a raw storage
key instead of a picture.

Sending that picture back needs nothing else. An [answer] node walks the
whole body and turns every stored file it finds, however deep (in an object,
in a list of rows), into `{ url, mimeType, filename, sizeBytes }`, so a list
route serving rows with pictures is three nodes and no loop. The `url` is an
address this install answers on, and a frontend puts it straight in an
`<img>`. `Cast` the value to `Image` or `File` only when you want the file
as a typed value on a wire, to hand it to a node that takes a picture.

If you catch yourself putting base64 on a wire or reading a multipart body,
stop and write: "Wait. Files are links." Then declare the port `Image` or
`File`.

## Streaming

`Stream { bus, format }` pipes a bus to the caller, one chunk per message,
until the bus closes. Any node that opens a bus can feed it, and the node's
own ports say which output is a `Bus`; a model streaming its answer is the
one you will reach for most:

```weft
ask = Route -> (prompt: String) { path: "chat", method: "POST" }
prov = OpenRouterProvider { model: "openai/gpt-4.1-nano" }
live = LlmStream
live.provider = prov.provider
live.prompt = ask.prompt
out = Stream { format: "sse" }
out.bus = live.stream
```

`format` has two good answers and one for the rare case. Pick between the
first two on what the reader is, and neither is a fallback for the other:

- `ndjson`, one JSON value per line (`application/x-ndjson`). Every payload
  arrives exactly as it left, whitespace included, and a reader is one
  `JSON.parse` per line. Reach for it first for anything whose text matters,
  which includes model output, because a delta usually begins with a space.
- `sse`, server-sent events (`text/event-stream`). Reach for it when the
  reader is a browser's built-in `EventSource`, or a client that already
  speaks SSE. The grammar is `data: <line>` per line of the payload and a
  blank line per message, and a reader must strip exactly ONE space after the
  colon and no more. A hand-rolled parser that does `.trim()` instead eats the
  leading space of every delta and fuses words together, which looks fine
  until you diff it. If you write the reader yourself, say so in the
  frontend's brief.
- `raw`, the payloads as they are, no separator. For a body that is already
  its own format (bytes, one long document). Read what the route section says
  about a ceiling before you pick it.

`first` sends one value ahead of the bus, framed exactly like the messages
behind it: the id, the row just created, the session token, then the feed.
`status` and `headers` ride the first chunk. The response ends when the bus
closes; a bus that closed before the `Stream` node ran still streams whole.

A run behind a `Route` lives as long as its caller: when they hang up, the
run is cancelled and whatever it was still doing stops. That is
`outlivesCaller` on the trigger, and it is the lever for what a disconnect
MEANS: off (the default) ties the run to its caller; on, the run finishes on
its own, and only what it would have SENT the caller goes nowhere. Its
writes still land.

**The longer a node holds the caller open, the more of the run the caller
can end.** A node answers in one shot or holds the connection and writes to
it over time; that is the node's own choice, made through the caller the
runtime hands every node, and any node can be written either way. Whatever
holds it open, the caller's chance to leave lasts as long as the holding
does, and on a caller-tied run their leaving cancels the whole run, not just
the sending: every node still to fire never fires.

That is how a run loses a write. Work placed after the answering node is
work the caller can cancel by closing a tab, and the row it would have
written keeps whatever it started with, for ever. Nothing reports it. Your
own client never shows it, because you let it finish.

So a run with a write behind its answer owns itself (`outlivesCaller:
true`), and then you name what ends it, as below. Prove it by hanging up in
the middle:

```bash
curl -N -L <url>/chat -d '{"prompt":"..."}' &   # start it
sleep 1 && kill %1                              # leave before it finishes
weft executions                                 # the run, and what it wrote
```

If you catch yourself putting a write behind the node that answers a
caller-tied route, stop and write: "Wait. The caller can end this
mid-write." Then turn the flag on and name the ending.

This also changes the order you work in, because a connection held open is
the one thing you cannot try before activating: Trying it, below, says why
and gives the commands for a real client.

### Answering early and carrying on

"Take this, say yes at once, do the slow part after" is an ordinary shape and
you build it like this: answer, then gate the rest of the work on the
answer's `done`, and set `outlivesCaller: true` so the caller leaving does not
kill the work you promised to do.

```weft
door = Route -> (text: String) { path: "ingest", method: "POST", outlivesCaller: true }
ack = Reply { status: 202 }
ack.body = door.text
slow = ExecPython(text: String) -> (done: Boolean) { code: "return {'done': True}" }
slow.text = door.text
slow._should_flow = ack.done
```

**Turning that on is you taking the ending into your own hands.** With it
off, the caller leaving is what ends the run, and that is the protection you
just switched off: nothing else is counting. So the work behind the answer
has to end by itself, every branch of it, and you check that before you
write the flag.

Work that ENDS on its own is fine and needs nothing more: a call returns,
a query answers, a script finishes. Work that does NOT is where this bites:
anything watching, subscribing or looping until something changes never
closes by itself, and with the caller no longer able to end it, nothing
would.

Nothing refuses you for writing that, and it should not: a loop after the
answer is often exactly the program. What it means is that the ending is
now yours, so you name it in the graph before you ship, as a ceiling on the
route with `maxSessionSecs`, or a `TagRun` and `StopTagged` pair so a newer
run of the same thing stops the older one. If you cannot name the thing
that ends it, you have written a leak, and the way you find out is `weft
executions`: drive it, close the caller, and see whether the count comes
back down.

A caller leaves two ways and only one of them says so. Closing a tab sends a
goodbye and the run ends in milliseconds. VANISHING (a lid closed, a network
gone) sends nothing at all, and on a feed that is quiet between changes
(a watched table nobody is touching) nothing arriving looks exactly like
nothing happening.

What tells them apart is that a machine still there ACKNOWLEDGES what it is
sent, by itself, whatever the person is doing. So while a feed is quiet the
worker writes a byte the reader ignores, and a caller who is gone stops
acknowledging it. That is what ends the run, and `callerSilenceSecs` on the
route is how long that silence may last before it counts as gone (thirty
seconds unless you say otherwise). A caller who is still there resets it
constantly, so a feed running for hours is never touched: it bounds silence,
not the call. You raise it only for a client whose link genuinely goes quiet
for longer, a device that sleeps its radio between messages.

This holds whatever the answer is framed as, `raw` included: a feed with
nothing to write is watched by the connection itself asking the caller
whether it is there, which costs the payload nothing.

All of that rests on the run being the caller's. Turn on `outlivesCaller`
and it is not: the caller going away stops mattering, so it stops ending the
run, and what the run does next is yours to bound. Nothing refuses you for
that, because a loop after the answer is often exactly what you meant. It
just means the ending is now yours to name. `maxSessionSecs` is a ceiling on
the whole connection if you want a hard one; there is no default for it on
purpose, since it ends a connection on the clock and would cut a healthy
feed short.

**A run that holds its caller's connection open is one run per open tab.**
A socket always holds one; so does a route whose answer streams, or whose
answering node holds the connection itself.
A person who reloads three times leaves three runs behind, each still holding
its own caller. So tag the run by whatever names the connection (the room,
the board, the person) and stop the older ones, right after the trigger and
before any work.

First check there IS such a value. The tag has to differ per caller, which
means a capture in the path, or a field of the body, or who the gate said is
calling. A route like `live/count` with no captures has none: the path is the
same literal for everybody, so tagging on it makes every new viewer stop all
the others, and you have broken the feature you were protecting. If nothing on
the route names one caller apart from another, that route wants no tagging at
all.

```weft
sock = Socket -> (room: String, inbound: Generator[JsonDict]) { path: "chat/{room}" }

claim = TagRun { room: sock.room }

stop = StopTagged {
  _should_flow: claim.done
  room: sock.room
}
```

Everything downstream hangs off `stop.done`. Those two nodes are the whole
wiring; the rules behind them (why the newer run wins, `includeSelf`, how a
tag is cleaned) are in the `weft-language` skill.

When a route has no such value and you leave it untagged, that is the right
call, and it leaves you owing one check: a page anyone can open, holding a
feed open per tab, is the shape that piles runs up. So after you have driven
it, list what is actually running and confirm the count comes back down
when you close the page. A standing pile of runs for one route is a finding,
not a surprise, and the count is one command you never have to guess at.

## Sockets

```weft
type Said = { text: String }

sock = Socket -> (inbound: Generator[Said]) { path: "chat/{room}" }
turn = Loop(msg: Generator[Said]) -> (results: List[Boolean | Null]) {
  parallel: false
  over: ["msg"]
  echo = JsonObject { echo: self.msg.text }
  say = Reply
  say.body = echo.object
  self.results = say.done
}
turn.msg = sock.inbound
```

Two things in that worth naming. The item type is a RECORD, not
`JsonDict`, because `.text` has to be readable and an unnamed object has no
keys to read (see narrowing, in the `weft-language` skill). And the body is
built by wiring a value onto the key it belongs under, which is what the
graph does instead of a script whose whole body is `return {...}`.

What a socket adds over a [route] is `inbound`: a `Generator` carrying one
item per message the caller sends, ended when the caller disconnects. You
declare its item type on the arrow to match the body shape (a record when
you will read keys off it, `Generator[String]` for plain text,
`Generator[File]` for uploads); an undeclared `inbound` wired anywhere is a
compile error. A `Loop` over it runs
once per message, in order.

The [answer] nodes mean something different here, because the exchange does
not end with one answer: sending is one message and the socket stays open,
and only the closing node ends it. Each says so in its own metadata, and
what a [route] accepts but a socket refuses (a status line, headers) is
refused loudly rather than ignored.

**One connection is one run, so a socket is not finished until it carries a `TagRun` and a `StopTagged`; the wiring is under Streaming above.** A bus lives inside one execution, so a message
on one socket cannot reach another socket through a bus. A chat room today:
each run writes to a table in the project's Postgres, and a trigger reads it.
Two callers seeing each other live is not expressible yet: you say so and
build the table shape.

## Auth

A [route] is open unless a [gate] is wired into its `auth`. The user stores
the [gate]'s connection through its connect flow, like any other; the
gateway checks every caller before a run starts, and what the check
established comes out on the trigger's `caller` port.

The `api` package ships one [gate] per way of proving who is calling: a
shared key, a signed token from an identity provider, and a signature over
the request body. Which you want depends on who calls: a token from an
identity provider is the frontend door (anything publishing a JWKS), a
shared key suits a service you control, a body signature suits a webhook
whose sender signs it.

Each [gate]'s own metadata names the exact header a caller must present and
the shape it puts on `caller`, in its description; the fields its connection
holds are in its `service` block, which `--compact` strips, so you read the
file itself. Both before wiring one, and before telling a caller how to
authenticate.

Finer rules (this key may only read, this user owns that room) are never the
[gate]'s job: they are a branch in the graph on `caller`.

## When you need a custom node

Two readers of one socket (inbound is broadcast to `ctx` readers), a reply
assembled from many chunks with logic between them, a response mixing writes
and a computed head: a node of your own reaching `ctx.http_caller()` /
`ctx.ws_caller()`, described in the docs page "Talking to a live caller".
You reach for `ctx` only when the graph cannot say it.

## Trying it

```bash
weft activate                      # prints the live URL (/connect/<tenant>/<path>)
curl -L -X POST "<url>/hello" -H 'content-type: application/json' -d '{"name":"ada"}'
curl -L -i "<url>/users/42?verbose=1"
curl -L -N "<url>/feed"            # a Stream route: -N shows each frame as it lands
websocat "<url as ws://>/chat/room7"
weft follow <project>              # one execution per request, live
```

Always `curl -L`. The live URL answers a `307` that points the caller at the
pod serving the run, so a first call without `-L` comes back a redirect and
looks like total failure. The body says so, and `-L` follows it.

A connection held open is the one thing `--fire` below cannot show you:
firing serves one request and records one answer, while a node holding the
caller writes to it over time, and time is what firing has none of. So prove
that against a real client: `weft activate`, then `curl -L -N`, and watch
the frames land. `-N` turns off curl's buffering; without it everything
appears at once at the end and tells you nothing about timing.

To try a route BEFORE activating (no listeners armed, no URL, no waiting on
a resync), `--fire` runs the whole thing. Give it the request envelope the
trigger declares in its `firesWith` metadata (for a Route: `method`,
`path`, and optionally `params`, `query`, `headers`, `caller`), plus a
`body` key holding what the caller would have sent. The envelope is checked
exactly, so a key the trigger does not declare is refused by name, same as a
missing one.

```bash
weft bake
weft run --fire 'hello={"method":"POST","path":"hello","body":{"name":"ada"}}'
```

`body` is one of the fields the `Route` declares it can wake with, marked
optional because only a fired run carries one: a real caller's body stays on
the open connection and the node reads it from there, so a live firing wakes
with the request line alone. It is a declared field like every other, held to
its type and refused on any trigger that does not name it. A
stand-in caller serves that body and records what the program answers, so
the `Reply` or `Close` at the end runs for real, and its status, headers
and body land in the journal exactly as a real exchange would; `weft
follow` shows them. Every node runs its own ordinary code, so this is not
a different path from production.

A `Socket` cannot be fired this way: its shape is a conversation over
time, and there is nothing honest to invent for the caller's next message.
Firing one fails saying so; use `weft activate` for a real client.

`--emit`, which hands a node its output values directly and never runs the
node's own body, still exists for supplying values without running code,
but firing is now the faster way to watch a route work end to end.

A request answered `500 the run ended without answering` shows in `weft
follow` as a run that reached no [answer]. The fix is in the graph, never in
a retry.
