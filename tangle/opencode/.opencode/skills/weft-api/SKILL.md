---
name: weft-api
description: "Read when the program needs to answer calls from outside (an HTTP route, a websocket, anything a page or a service will call), and before briefing a frontend on what it will call: the URL known before activation, how a request's parts and body reach the graph, pictures in and out as links, streaming an answer as it is produced, holding a conversation open, who may call, and how to try it."
---


# Building an API

Everything here is in the `api` package of the catalog. You read each
node's `metadata.json` before wiring it; when a name below drifts from it,
the metadata wins. A [route] is one `Route` or `Socket` node: one trigger,
and one call is one fresh run carrying the request on its ports. An
[answer] is any node that writes back to that caller: the runtime hands the
caller's connection to every node in the run, so a node of your own is one
as readily as a catalog node. The `api` package ships three: `Reply` sends
a body, `Stream` sends a bus as it flows, `Close` ends the exchange. A
[gate] is an auth access node wired into a [route]'s `auth`; the package
ships one per way of proving who is calling, and the listing names them. A
[stored-file value] is a few hundred bytes saying where a file sits in
storage, the only way bytes travel a wire.

## The URL is known before anything runs

A [route] answers at `<install>/connect/local/<project id>/<path>`, and every part of that address is fixed before the route is ever activated: the install, and the project id, which is the `id` under `[package]` in the project's `weft.toml` (the same on every install, so two projects can serve the same path). On this machine the install
is `http://127.0.0.1:14111`, so `hello = Route { path: "hello" }` answers at
`http://127.0.0.1:14111/connect/local/<project id>/hello`, and a socket at the same
address with `ws://`. On a cloud install it is the target's `url` in
`weft.toml` (`https://weft-role-dispatcher-123456789.us-central1.run.app/connect/local/<project id>/hello`,
and `wss://`). You write those URLs into the frontend-builder's
[the brief] the moment the routes are shaped, while the graph is still being
built; `weft activate` only turns them on.

Each project also has an address of its own where the program answers its routes at the root with nothing of weft's in between, which is the fastest way in: on this machine a free port it gets the first time it is activated (14200 or above) and keeps, moving to another free one if a program took it (`http://127.0.0.1:14200/hello`; `weft activate --port <n>` opens a chosen one and keeps it; if another program or project holds it, the command fails and nothing is activated), on a cloud install the project's own Cloud Run address. `weft activate` prints it and `weft status` shows it, or says why it is unavailable. While a route's trigger is not on (taken down, or still coming on), a call is answered `503` with `Retry-After: 5` and `This service is unavailable right now; try again in a moment.`, so tell the frontend to retry a `503` that carries `Retry-After`, and never one without it. When you brief a frontend, give it the `/connect/local/<project id>/` address: it has the same shape on every install, so the frontend's code runs unchanged on a cloud.

Both local addresses, the project's own port and `127.0.0.1:14111`, answer only on this machine. When the install has a public address (a tunnel), `weft daemon status` prints it, and each route answers there at `<public address>/connect/local/<project id>/<path>`, reaching the same program. A frontend that runs
anywhere else (a hosted site, a phone, a browser on another machine) uses the
public one; a server on this same machine may use either.

A program that is an API can have a domain of its own on a cloud install.
If the user wants one and has agreed to its price, the deployer runs `weft
domain add api.shop.com --for api --on prod` (with `--accept-cost` for the
install's first domain), which serves the project's routes at the root of that domain,
so `hello` answers at `https://api.shop.com/hello` as well as at its
`/connect/local/<project id>/` address. A domain costs money, because it needs a load balancer, so
design for the free address and offer a domain only when the user wants a
name people see; read weft-deploying before you offer one.

## The shape

The run ends when it has sent its [answer]. The body arrives whole on the
[route]'s `body` output, and you declare its shape on the arrow, then read
the keys off it.

```weft
hello = Route -> (body: { name: String }) { path: "hello", method: "POST" }
answer = Reply { status: 201, body: hello.body.name }
```

A [route]'s fixed output ports carry the request as the gateway saw it: the
method, the path as called, the query, the headers, and who the [gate] let
in. Its metadata lists them. The headers carrying a credential
(`authorization`, `proxy-authorization`, `cookie`, `weft-instance-token`, and
the ones the [gate] reads) are never on `headers`: they come out on
`credentials`, each a `Secret[String]`, which the run's record holds only as a
marker. Hand them to a node that takes a secret (`HttpRequest`'s `headers`
sends them revealed, and so does its `body` when what you wire into it declares
where the secret is, like `{ token: Secret[String] }`); a plain `String` port refuses them at build. A path capture has no fixed port: you declare an
output of its name (below).

What you declare on `body` depends on the body shape the [route] is set to
(`dataType` in the source, "Body shape" in the editor):

- `json` (the default): a record type, `JsonDict` to take any object, or
  `Json` for a body of any shape (a list, a bare value). A
  record is checked as the request arrives: a body that does not fit is
  answered `400` naming the field, and nothing downstream runs. A record
  refuses keys it does not declare; write `..` before its closing brace
  (`{ name: String, .. }`) and they are dropped instead. A key the caller
  may leave out is `nick?: String`, and reading it when it is missing closes
  that wire. A record's fields keep whatever spelling you give them, so if
  the caller's body spells a key in snake_case, spell the field the same way
  (`{ user_id: String }`). If your own frontend sends the body, name its keys
  camelCase, like every field weft writes.
- `text`: the whole body as a `String` (`-> (body: String)`).
- `bytes`: the body is stored, and its [stored-file value] arrives on `body`
  (`-> (body: File)`, or `Image`, which is checked against the bytes).

Declare the capture as an output, and read it as `card.id`:

```weft
card = Route -> (id: String) { path: "cards/{id}", method: "GET" }
```

Every output you add to a `Route` must be a capture of its path, typed
`String`; the compiler refuses anything else with
`route-output-not-a-capture`. A capture may not share a name with one of
`Route`'s own outputs (`body`, `method`, `path`, `query`, `headers`,
`credentials`, `caller`, `fired`): `cards/{body}` is `route-capture-shadows-output`, asking
you to rename the capture.

The first thing the program sends commits the status line. A `Reply` on a
branch with `status: 404` works. A `Reply` or a second `Stream` after a
`Stream` is a compile error, because the head already went out: end a
streamed answer with a `Close`. A
program that never sends anything holds the caller while it runs. When that
run ends, it is recorded as failed with `the run ended without answering:
every path to its Reply (or Stream, or Close) was skipped, usually because a
value it waited on never arrived`, and the caller gets a `500` (the body below). If you
catch yourself wiring a branch that reaches no [answer], stop and write:
"Wait. Every branch answers." Then end that branch on a `Reply` or `Close`.

When a run ends before it answers, the caller never sees the failure's own
message: the body is `{"error": "Something went wrong", "runId": "<run id>"}`,
and the status tells it why:

| Status | When |
|---|---|
| `500` | the program failed, got stuck, or ended without answering |
| `504` | a time limit ran out: the run's hour on a cloud install, the route's `maxSessionSecs`, a wait held for its whole `holdSecs`, or, on a cloud install, the worker stopping just before Cloud Run cuts its connection to weft (which it does after an hour), because every run on it was only waiting |
| `503` | weft or a person stopped the run (`weft stop`, `ctx.stop_tagged`, a take-down with `--running cancel`), or the run carries on without this caller (it paused, or moved to another worker) |

That `503` carries no `Retry-After`, unlike the one a route sends while its trigger is not on:
calling again would start a second run beside the one that may still be
going. If the answer had already started streaming, its status line is
gone, so the marker comes in the body's own framing: a JSON-lines answer
ends with `{"error": "Something went wrong", "runId": "...", "status": 504}`
(the status the answer would have had), server-sent events get an `event:
error` whose data is `{"error":"Something went wrong","runId":"..."}`, and
anything else a line `[error] Something went wrong (run <id>)`. A socket
closes with `1011` for a failure or a time limit, and `1013` when the run was
stopped or carries on without this caller, with the reason `Something went
wrong (run <id>)`. Every answer of a route's run, success or failure,
carries the header `Weft-Run-Id: <run id>`. The full message is on the run
(`weft logs <run id>`, `weft runs`, the graph), and opening a run needs the
owner's credential, so the id is safe to hand out.

Two routes of one account can share a call as long as one of them spells out
what the other captures: `chat/general` beside `chat/{room}` is fine, and the
literal takes that one call while the capture takes the rest. What `weft
activate` refuses, naming both, is the pair where neither is the more specific
(`chat/{room}` beside `chat/{name}`, or `a/{x}/c` beside `a/b/{y}`, which both
answer to `a/b/c`). Give one of them a literal the other captures, or distinct
methods.

## Request-response

The graph decides the status. A Python node reading the `id` capture and
`query` returns the body and the status together:

```weft
user = Route -> (id: String) { path: "users/{id}", method: "GET" }
lookup = ExecPython(uid: String, query: Dict[String, String]) -> (body: JsonDict, status: Number) {
  uid: user.id
  query: user.query
  code: "if uid == '42':\n    return {'body': {'id': uid, 'verbose': query.get('verbose')}, 'status': 200}\nreturn {'body': {'error': 'no user ' + uid}, 'status': 404}"
}
found = Reply { body: lookup.body, status: lookup.status }
```

A `GET` route has no body: declare nothing on the arrow and read the fixed
ports. A `text` route: `say = Route -> (body: String) { path: "say", dataType:
"text" }`, and the `Reply` body must be a `String`. A request that carries no
body closes `body`, whatever the `dataType`, and a json body that is `null` comes out on a `body` whose type takes `Null`
(`Json` does) and closes it on any other.

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
sweep = Route -> (what: String) { path: "sweep/{what}", method: "DELETE" }
clear = ExecPython(what: String) -> (removed: Number) {
  what: sweep.what
  code: "return {'removed': 3 if what == 'cards' else 0}"
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
polls. On a cloud install one stretch of a run lasts an hour at most, so a
job that can run longer is split with a pause (a timer), which gives it a
fresh hour.

If the caller polls for a status, you need no table: the run's own record
has it. The answering route hands back its run's id (`GetRunId`), and a
second route reads one node of that run with `GetNodeState { runId, node }`,
which answers where it stands (its status, an outside job's last answer
while it waits, or its error). A table in the project's Postgres is
for a status the program itself decides (a step count, a percentage it
works out): the answering route writes the row, the work updates it, and
the second route reads it back.

If a page asks a route for a status every few seconds, set `recorded: false`
on the `Route` so each call does not leave a run behind. weft keeps no history
of such a run: one that fails is listed in `weft runs` with the node that
failed and why, one that reported a cost, or asked weft for something its
worker does not already hold (a stored file, a new connection or infrastructure
address, starting its infra, a stop by tag, a wait on a timer or a form), is
listed with its costs and how it ended, and any other leaves nothing. An
unrecorded run keeps no record to pause on, and it cannot be tagged.

A run that cannot pause (an unrecorded one, or one whose caller is still on
the line with `outlivesCaller` off) still waits on a timer or a form: the wait
holds the worker and the call open, and the answer carries the run on. If
nothing moves in the run for the route's `holdSecs` (60 by default, `0` to
never wait), the waiting node fails saying it gave up, and the caller gets a 504 with the plain failure body and the run id (the reason stays on the run, for you).

A route also has two settings for what a streamed answer leaves on the run's record: `journalEphemeral` (on keeps only counts and sizes, never what was said; off by default) and `journalWindowMs` (messages sent within one window share one record row; 100 to 60000, 1000 by default; shorter shows them in the inspector sooner, longer writes fewer rows for a fast stream).

A route also has `durable`, off by default, and it cannot be combined with `recorded: false`. With it off, a run whose worker dies ends cancelled. With it on, another worker carries the run on from its finished nodes, but the caller's connection died with the old worker, so a node that answers the caller after that fails and the caller has to call again. For the details, go and read How a run is kept in the `weft-language` skill.

## Pictures in and out

A value a node emits weighs at most 100 KB as JSON; above that the node
fails, naming the port. A picture a caller sends inside a JSON body lands on a field
declared as its kind, and the [route] stores it on the way in:

```weft
upload = Route -> (body: { photo: Image, caption: String }) { path: "cards", method: "POST" }
```

The body key `photo` holds a `data:image/png;base64,...` URL (the media type
comes from it) or bare base64 (the media type comes from the bytes' own
signature); bytes that are not what the field declares are answered `400` naming the field, and nothing past the route runs.
Multipart bodies are not read: the caller sends JSON with a data URL. A file
too big for a JSON body (a video, a long recording) goes through an upload
slot. The program mints one with `UploadSlot` and hands its `slot` string
(`weft-upload:...`) to the page; the page uploads the file with it, then
calls the route with that string in a field the route declares as a file
(`{ "video": "weft-upload:..." }`), and the route receives the stored file.
A slot takes one file, and an upload no route received within its `minutes`
is deleted. For the page's side, go and read the `weft-frontend` skill. A
file sent in the body lands at run scope, walled to that run and swept when it ends: a
later request cannot read it even if it was kept. If you want to serve a picture from a
later request, wire it through `KeepFile { scope: "project" }`, which copies
it into the project's storage, then write the whole [stored-file value] it
emits into a jsonb column, exactly as it arrived. If it should not live for
ever, give `KeepFile` a `ttlDays` too: the copy then expires that many days
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
file is at run scope and swept when its run ends, so a link to it dies
with that run: each render is kept in the project scope first. One way to
build it, with a table: each render goes through
`KeepFile { scope: "project", ttlDays: 1 }`, and its whole value goes over
one row per preview (`INSERT INTO preview (view, picture) VALUES ($view, $picture)
ON CONFLICT (view) DO UPDATE SET picture = EXCLUDED.picture`). No storage node
rewrites a picture in place, so the row is what stays put: the old copies
expire a day after their last read, and the one on screen is read on every
call, which resets its clock. The route that serves it, with `db` the
program's `PostgresDatabase`:

```weft
preview = Route -> (view: String) { path: "session/preview/{view}", method: "GET" }
latest = PostgresExecuteQuery(view: String) -> (row: { picture: JsonDict }) {
  account: db.access
  view: preview.view
  query: "SELECT picture FROM preview WHERE view = $view"
}
show = Reply { answerAs: "bytes", body: latest.row.picture }
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
ask = Route -> (body: { prompt: String }) { path: "chat", method: "POST" }
router = OpenRouterAccess
prov = OpenRouterModel { access: router.access, model: "openai/gpt-4.1-nano" }
live = LlmStream { provider: prov.provider, prompt: ask.body.prompt }
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
A producer that fails before its first piece has opened no bus, so the
`Stream` never starts, and a backup gated on its `_error` can still answer
(`fallback = Reply { body: "...", _should_flow: live._error }`).

The status line went out with the first chunk, so a failure after that
cannot change it, and neither can a bus that ends failed (the producer
broke off). It arrives in the body's own framing, as the last thing the
reader gets: a final `{"error":"Something went wrong","runId":"...","status":...}`
line on `ndjson`, an `event: error` on `sse`, a `[error] Something went
wrong (run <id>)` line on anything else. Tell the frontend's reader to look
for it.

Two settings on the `Route` decide what happens to ONE message, and you pick
both on what the caller is reading:

- `whenCallerIsSlow`, for a caller reading slower than the program writes,
  once 256 messages wait for it. `block` (the default) makes the program's
  next send wait, so the caller gets everything: keep it for a model's answer
  or anything read whole. `drop_oldest` throws the oldest waiting message
  away, so the caller always has the latest: a price feed, live video.
  `drop_newest` throws the new one away and sends what was waiting, in order.
- `whenChunkFails`, for a message the `Stream` cannot make in the route's
  `dataType` (a value that is not a `String` on a `text` route, a file it
  cannot read). `surface` (the default) fails the answer: an error status if
  nothing went out yet, the marker above if it did; `drop_chunk` skips that message, notes it on the run's record and
  goes on. A run that fails, or a bus that ends failed, is told to the caller
  under both.

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

A message the caller never received is on the run's record: struck through
in the inspector, and `dropped=<offsets> (<why>)` on the caller's lines in
`weft events`, which also names what was still waiting when the caller left.
When the frontend reports a gap, that is where you look (the `weft-debugging`
skill reads it).

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
timeout 1 curl -N <url>/chat -d '{"prompt":"..."}'   # leave a second in, before it finishes
weft runs --since 1m                                   # the run, and what it wrote
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
door = Route -> (body: { text: String }) { path: "ingest", method: "POST", outlivesCaller: true }
ack = Reply { status: 202, body: door.body.text }
slow = ExecPython(text: String) -> (done: Boolean) {
  text: door.body.text
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
runs`: drive it, close the caller, and see whether the count comes
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
feed = Route -> (room: String) { path: "live/{room}", method: "GET" }

claim = TagRun { room: feed.room }

stop = StopTagged {
  _should_flow: claim.done
  room: feed.room
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
you will read keys off it, `Generator[Json]` for messages of any shape,
`Generator[String]` for plain text,
`Generator[File]` for uploads); an undeclared `inbound` wired anywhere is a
compile error. A `Loop` over it runs
once per message, in order.

The [answer] nodes mean something different here, because the exchange does
not end with one answer: sending is one message and the socket stays open,
and only the closing node ends it. Each says so in its own metadata, and
what a [route] accepts but a socket refuses (a status line, headers) is
refused loudly rather than ignored.
`whenCallerIsSlow` and `whenChunkFails` sit on the `Socket` too and mean the
same as under Streaming, for every message a `Reply` or a `Stream` sends;
under `surface` the socket closes with `1011`, and a `Reply` whose message was
skipped leaves its `done` absent.

**One connection is one run, so a socket whose gate names the person is not finished until it carries a `TagRun` and a `StopTagged`, wired as under Streaming above.** What a socket can tag on is who the [gate] let in. A path capture like `{room}` names a room shared by everyone in it, not one person, so it is the wrong tag here. The `caller` port takes the type you declare: declare it on the arrow as an open record holding the claim that names the person, and wire that claim into both nodes. Here `UserTokens` is an access node of the project's own whose `verify` is `oidc` (weft ships no identity-token gate: its service declares the fields `issuer`, `jwksUrl` and `audience`, and `"verify": { "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }`).

```weft
gate = UserTokens
sock = Socket -> (inbound: Generator[Said], caller: { sub: String, .. }) {
  path: "chat/{room}"
  auth: gate.access
}
claim = TagRun { person: sock.caller.sub }
stop = StopTagged {
  _should_flow: claim.done
  person: sock.caller.sub
}
```

The `..` matters: a token carries more claims than `sub`, and a record without it refuses every claim it does not declare. A socket with no gate has nothing that tells one caller from another, and wants no tagging.

A bus lives inside one run, so a message
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
weft activate                      # prints <url>, the project's own address (on a cloud install, its Cloud Run address)
curl -X POST "<url>/hello" -H 'content-type: application/json' -d '{"name":"ada"}'
curl -i "<url>/users/42?verbose=1"
curl -N "<url>/feed"               # a Stream route: -N shows each frame as it lands
websocat "<url as ws://>/chat/room7"
weft follow <project>              # one run per request, live
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
`path`, and optionally `params`, `query`, `headers` (never a credential
header, nor `credentials`: the fire is refused naming it), `caller`), plus a
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

A request answered `500` with `Something went wrong` is a failed run: read
its `Weft-Run-Id` header or the body's `runId`, then `weft logs <run id>`.
When it says `the run ended without answering`, the run reached no [answer];
that error is in `weft follow` and in `weft runs`, even on a `recorded: false` route (a failed run
always leaves its failure on record). The fix is in the graph, never in a retry.
