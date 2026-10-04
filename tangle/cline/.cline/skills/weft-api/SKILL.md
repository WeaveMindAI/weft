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

A [route] answers at `<install>/connect/local/<path>`, and every piece is
fixed by the install, not minted at activation. On this machine the install
is `http://127.0.0.1:14111`, so `hello = Route { path: "hello" }` answers at
`http://127.0.0.1:14111/connect/local/hello`, and a socket at the same
address with `ws://`. On a cloud install it is the target's `url` in
`weft.toml` (`https://weft-role-dispatcher-123456789.us-central1.run.app/connect/local/hello`,
and `wss://`). You write those URLs into the frontend-builder's
[the brief] the moment the routes are shaped, while the graph is still being
built; `weft activate` prints the same URLs afterwards and only turns them on.

`127.0.0.1:14111` answers only on this machine. When the install has a public
address (a tunnel), `weft activate` and `weft token mint` print URLs on that
address instead, and both reach the same dispatcher. A frontend that runs
anywhere else (a hosted site, a phone, a browser on another machine) uses the
public one; a server on this same machine may use either.

A program that is an API can have a domain of its own on a cloud install.
If the user wants one and has agreed to its price, the deployer runs `weft
domain add api.shop.com --for api --on prod` (with `--accept-cost` for the
install's first domain), which serves the project's routes at the root of that domain,
so `hello` answers at `https://api.shop.com/hello` as well as at its
`/connect/local/` address. A domain costs money, because it needs a load balancer, so
design for the free address and offer a domain only when the user wants a
name people see; read weft-deploying before you offer one.

## The shape

The run ends when it has sent its [answer]. You declare the body keys you
want on the trigger's arrow, never a body blob, and each arrives on its own
port.

```weft
hello = Route -> (name: String) { path: "hello", method: "POST" }
answer = Reply { status: 201, body: hello.name }
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
capture on every body shape. It never counts as the text or binary body's one
port either, so a bytes or text route still reads its captures: on
`session/preview/{view}`, `-> (view: String)` is the capture, and the body
lands on whatever else you declare.

```weft
card = Route -> (id: String) { path: "cards/{id}", method: "POST" }
```

The first thing the program sends commits the status line. A `Reply` on a
branch with `status: 404` works. A `Reply` or a second `Stream` after a
`Stream` is a compile error, because the head already went out: end a
streamed answer with a `Close`. A
program that never sends anything holds the caller while it runs. When that
run ends, it is recorded as failed, and the caller gets `500` with the body
`execution failed: the run ended without answering: every path to its Reply
(or Stream, or Close) was skipped, usually because a value it waited on never
arrived`. If you
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
  params: user.params
  query: user.query
  code: "uid = params['id']\nif uid == '42':\n    return {'body': {'id': uid, 'verbose': query.get('verbose')}, 'status': 200}\nreturn {'body': {'error': 'no user ' + uid}, 'status': 404}"
}
found = Reply { body: lookup.body, status: lookup.status }
```

A `GET` route has no body: declare nothing on the arrow and read the fixed
ports. A `text` route: `say = Route -> (line: String) { path: "say", dataType:
"text" }`, and the `Reply` body must be a `String`.

If the answer has a different shape from what came in, the [answer] node says
which with `answerAs`: `json`, `text` or `bytes`, and empty follows the
route's `dataType`. A `GET` that answers a picture is the usual case: it takes
no body in, so the route keeps its default, and the [answer] node (a `Reply`,
say) sets `answerAs: "bytes"` with a [stored-file value] as the body. Sent that way over HTTP, the file goes out
with its own content type and an inline `content-disposition` carrying its
filename, unless `headers` sets them.

A run that stops without a body ends through the closing [answer] node,
which takes no data from upstream: its `_should_flow` gate is the whole
wiring. So the shape for "do this, and if it did not work, stop here" is a
`Switch` on the outcome, the failing case gating a close with its reason
and status already written on it, the passing case gating the rest.

```weft
sweep = Route { path: "sweep/{what}", method: "DELETE" }
clear = ExecPython(params: Dict[String, String]) -> (removed: Number) {
  params: sweep.params
  code: "return {'removed': 3 if params['what'] == 'cards' else 0}"
}
outcome = Switch {
  value: clear.removed
  cases: [
    { "kind": "gt", "value": 0, "port": "some" },
    { "kind": "otherwise", "port": "none" }
  ]
}
report = Reply { body: clear.removed, _should_flow: outcome.some }
nothing = Close { status: 404, reason: "nothing to sweep", _should_flow: outcome.none }
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

A slow job (anything that takes minutes: a render, a build, a batch of model
calls) never sits in one call the caller waits on. Answer early (a `202`
with an id), run the work after the answer with `outlivesCaller: true`, and
show progress: on the infra node's display when the work runs in a container
(the `weft-node-authoring` skill has the display), or as a status the caller
polls. For the polled status, the route that answered writes the job's state
to the project's Postgres, the work updates it as it goes, and a second route
reads it back. Two routes and a table, no new node.

If a page asks a route for a status every few seconds, set `recorded: false`
on the `Route` so each call does not leave a run behind. A run that succeeds or
is cancelled then leaves nothing but what it cost, and one that fails is written
down whole afterwards, so you still find it in `weft executions` and inspect it
like any other. An unrecorded run keeps its steps in the worker's memory, which
means it cannot wait (a timer or a form fails at the call) and is lost if the
worker dies mid-call. With `outlivesCaller` on too, it keeps running after the
caller leaves, still unable to wait.

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
emits into a jsonb column, exactly as it arrived. If it should not live for
ever, give `KeepFile` a `ttl_days` too: the copy then expires that many days
after anybody last read it.

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
route serving rows with pictures is three nodes and no loop. The `url` is
built on the address the caller used: a call to `127.0.0.1:14111` gets a
loopback link, a call through the tunnel gets a tunnel link. A run no request
started (`weft run --fire`) gets the install's internet address when it has
one, loopback otherwise. A frontend puts the `url` straight in an `<img>`. `Cast` the value to `Image` or `File` only when you want the file
as a typed value on a wire, to hand it to a node that takes a picture.

If you want to serve a picture that is redrawn often (a live preview), the
shape is one place that always names the latest copy, which every render
overwrites and the serving route reads and answers as bytes. A render's own
file is at execution scope and swept when its run ends, so a link to it dies
with that run: each render is kept in the project scope first. One way to
build it, with a table: each render goes through
`KeepFile { scope: "project", ttl_days: 1 }`, and its whole value goes over
one row per preview (`INSERT INTO preview (view, picture) VALUES ($view, $picture)
ON CONFLICT (view) DO UPDATE SET picture = EXCLUDED.picture`). No storage node
rewrites a picture in place, so the row is what stays put: the old copies
expire a day after their last read, and the one on screen is read on every
call, which resets its clock. The route that serves it, with `db` the
program's `PostgresDatabase`:

```weft
preview = Route -> (view: String) { path: "session/preview/{view}", method: "GET" }
latest = PostgresExecuteQuery(view: String) -> (picture: JsonDict) {
  account: db.access
  view: preview.view
  query: "SELECT picture FROM preview WHERE view = $view"
}
show = Reply { answerAs: "bytes", body: latest.picture }
```

An `<img>` can point straight at that route. A json answer works for a
project-scope file too, through the `url` the [answer] builds.

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
live = LlmStream { provider: prov.provider, prompt: ask.prompt }
out = Stream { format: "sse", bus: live.stream }
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

The status line went out with the first chunk, so a failure after that
cannot change it. It arrives in the body's own framing, as the last thing the
reader gets: a final `{"error":"..."}` line on `ndjson`, an `event: error` on
`sse`, a `[error] ...` line on anything else. Tell the frontend's reader to
look for it.

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
curl -N <url>/chat -d '{"prompt":"..."}' &   # start it
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
ack = Reply { status: 202, body: door.text }
slow = ExecPython(text: String) -> (done: Boolean) {
  text: door.text
  _should_flow: ack.done
  code: "return {'done': True}"
}
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
  say = Reply { body: echo.object }
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

**On a cloud install a connection lasts an hour at most**: Google cuts any
request at 60 minutes, and a socket or a streaming answer is one long
request. Connections also drop sooner (a phone changing networks), and the
trigger's `outlivesCaller` decides whether the run stops or carries on alone.
So when you build something meant to stay open longer than a few minutes (a
chat page left open, a live dashboard), build its reconnect from the start:
the client sends a session id on every connection (the `session` query
parameter, read off the trigger's `query` port; the connect library's
`openLiveSocket` sends it and reconnects on its own) and reconnects whenever
its socket closes, and the program keeps the conversation's state in the
project's storage keyed by that id and reads it back when a connection
arrives. Each connection is then its own run, and losing one loses nothing.
To end the conversation for good, the program closes the socket with code
`4000`; a run that simply ends closes with `1000`, and the client comes back.
Tell the frontend builder so in its brief.

A browser cannot put a credential on a socket's opening request. On a socket
whose route checks callers, a browser asks with a plain `GET` first (which
carries the credential) and gets `{"url": ..., "protocol": "websocket"}`
back, then opens its socket at that `url`; anything else opens its socket at
the route's own address.

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

Every [route] and socket counts calls per caller: by default one caller gets
60 calls a minute, and the next one is answered `429` with a `Retry-After`
before any run starts. A caller is who the [gate] let in, or the address on
an open route. `callsPerMinutePerCaller` on the trigger changes it, and `0`
means no limit. A page that polls a route every second from one server is
one caller at 60 a minute, so on a polled route raise it, next to
`recorded: false`. `callsPerMinute` (everybody together) and `callsAtOnce`
are the other two limits.

In a program with instances (separate copies of part of it, the
`weft-instances` skill), a [route] is always shared: one that reads a
per-instance container or value, or sits inside or reads from a group that
receives one, is a compile error. Keep the [route] outside that group and
send its work in through the group's inputs. The caller picks the
instance per call, with `Weft-Instance: <id>` on a gated [route] (only a
server that already checked the caller may send it; an open route refuses
the header), or with an instance token in `Weft-Instance-Token`.

## When you need a custom node

Two readers of one socket (inbound is broadcast to `ctx` readers), a reply
assembled from many chunks with logic between them, a response mixing writes
and a computed head: a node of your own reaching `ctx.http_caller()` /
`ctx.ws_caller()`, described in the docs page "Talking to a live caller".
You reach for `ctx` only when the graph cannot say it.

## Trying it

```bash
weft activate                      # prints the live URL (/connect/local/<path>)
curl -X POST "<url>/hello" -H 'content-type: application/json' -d '{"name":"ada"}'
curl -i "<url>/users/42?verbose=1"
curl -N "<url>/feed"               # a Stream route: -N shows each frame as it lands
websocat "<url as ws://>/chat/room7"
weft follow <project>              # one execution per request, live
```

A connection held open is the one thing `--fire` below cannot show you:
firing serves one request and records one answer, while a node holding the
caller writes to it over time, and time is what firing has none of. So prove
that against a real client: `weft activate`, then `curl -N`, and watch
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

A request answered `500 execution failed: the run ended without answering`
is a failed run that reached no [answer], with that error, in `weft follow`
and in `weft executions`, even on a `recorded: false` route (a failed run is
always written down whole). The fix is in the graph, never in a retry.
