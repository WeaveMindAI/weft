# Building an API

You make a program into an API with two trigger nodes and three answering
nodes, all in `catalog/api`, and no line of Rust. `Route` serves HTTP and
`Socket` serves WebSocket; each starts a fresh execution per request and puts
that request on its ports. `Reply`, `Stream` and `Close` answer whichever caller
the run belongs to.

Underneath, the engine knows nothing about any of these nodes. It holds the
connection, lets a node read the request and set the response head, and hands
the trigger its wake payload. The catalog nodes are one user of that handle; a
node you write in Rust reaches the same one through `ctx`
([Talking to a live caller](../nodes/live-callers.md)).

## One route, one run

```weft
hello = Route -> (name: String) { path: "hello", method: "POST" }
answer = Reply { status: 201 }
answer.body = hello.name
```

`path` is a pattern and `method` is optional. `path: "users/{id}"` captures that
segment under `id`, and an empty `method` serves every verb.

Two routes may share a call as long as one of them spells out what the other
captures. `chat/general` beside `chat/{room}` is fine: `chat/general` takes that
one call and `chat/{room}` takes the rest. What is refused when you activate,
naming both, is a pair where neither is more specific, because then the shared
call has two equal claims: `chat/{room}` beside `chat/{name}`, or `a/{x}/c`
beside `a/b/{y}`, which both answer to `a/b/c`.

## The request

Six ports are always there, whatever else you declare: `method`, `path` (as
called, no leading slash), `params` (the captures), `query`, `headers` (lowercase
names), and `caller`, which is who the auth gate let in, or null on an open
route.

The body comes out on the ports you declare after the arrow, and how depends on
the route's `dataType`:

- On a `json` route, each name you declare is one top-level key. There is no port
  for the whole object, so declare every key you mean to read. A key that is
  itself an object is `meta: JsonDict`. An undeclared key is dropped.
- On a `text` route, the whole body arrives as a `String` on the one port you
  declare.
- On a `bytes` route, the body is stored as a file and the port delivers the
  stored-file value, so declare it `File`.

A body key named like one of the fixed ports loses to the fixed port.

To get a capture on its own port, declare a port with the capture's name:
`Route -> (id: String) { path: "cards/{id}" }` puts the `{id}` segment on `id`,
typed, and a body key called `id` loses to it, because the capture is part of the
request like the fixed ports. `params` still carries every capture either way.

### Files in and out

Nothing on a wire is ever base64. A value a node emits is at most 100 KB, and a
node that goes over fails naming the port. Bytes live in storage as a
stored-file value: a few hundred bytes that say where the file is.

If a caller sends a picture in a JSON body, declare the port as the file kind
you expect, and the route stores it on the way in:

```weft
upload = Route -> (photo: Image, caption: String) { path: "cards", method: "POST" }
```

The body key may hold a `data:image/png;base64,...` URL, whose media type comes
from the URL, or bare base64, whose media type comes from the bytes' own
signature. Bytes that are not what the port declares fail the run by name. The
file lands at execution scope, walled to that run and swept when it ends, so a
later request cannot read it even if the run kept the reference. To serve it from
a later request, wire it through `KeepFile { scope: "project" }`, which copies it
into the project's storage, and keep the stored-file value that node emits in
your database, in a jsonb column.

Sending a file back needs nothing extra. `Reply` and `Stream` walk the whole
answer and link every stored file they find, at any depth, so a route serving a
list of rows with pictures is three nodes and no loop. `Cast` a stored-file value
read out of the database to `Image` (or `File`) only when you want it as a typed
value on a wire, to hand to a node that takes a picture.

On the way out, a stored file anywhere in a `Reply` or `Stream` body, nested in
an object or in a list of rows, goes out as a link:

```json
{ "url": "http://127.0.0.1:9998/public/files/<token>", "mimeType": "image/png", "filename": "photo", "sizeBytes": 48211 }
```

That `url` is what a browser puts in an `<img>`. Its address is the install's,
never the caller's: the public tunnel's when the install runs one, otherwise the
install's own base, which locally is `http://127.0.0.1:9998`. Multipart bodies
are not read; send JSON with a data URL.

The link is minted for the answer it goes out in and expires minutes later, so
never write it into a table. Keep the file itself, with `KeepFile` and a scope
that outlives the run, and store the value that node emits. Each answer then
mints a fresh link from it.

`Reply` is the response: `status` (200 by default), `headers` (an object of
strings), and `body`, whose shape follows the route's `dataType`. The first
thing your program sends commits the status line, so a `Reply { status: 404 }`
on a branch works, and a `Reply` after a `Stream` fails loudly with "response
head already sent".

A run that stops without sending a body answers through `Close`. A bare one is a
bodiless `204`. With a `reason`, that sentence is the body, under the `status`
you set: `Close { status: 404, reason: "nothing to sweep" }`.

## Branching on the request

Let the graph decide the status. A Python node reads `params` and returns both
the body and a number, which wire straight into the reply:

```weft
user = Route { path: "users/{id}", method: "GET" }
lookup = ExecPython(params: Dict[String, String]) -> (body: JsonDict, status: Number) {
  code: "uid = params['id']\nif uid == '42':\n    return {'body': {'id': uid}, 'status': 200}\nreturn {'body': {'error': 'no user ' + uid}, 'status': 404}"
}
lookup.params = user.params
found = Reply
found.body = lookup.body
found.status = lookup.status
```

### Do this, and if it did not work, stop here

`Close` takes no value from upstream, so its `_should_flow` gate is the whole
wiring, and that gate accepts any port of any type: the value is never read, and
only a `false` says no. So put a `Switch` on the outcome, let the failing case
gate a `Close` that already has its reason and status written on it, and let the
passing case gate the rest:

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

A branch that skipped closes its ports, so the `Close` behind it skips too and
the other branch's answer stands.

## Answer first, keep working

A webhook receiver wants to say `200` at once and do the slow part afterwards.
Send the `Reply` early, then let the rest of the graph run, and set
`outlivesCaller: true` on the route so the caller hanging up does not cancel the
run. With it off, the run is tied to the caller and a disconnect cancels it,
which is what a request-response API wants.

## Long job, poll later

Answer `202` with an id from one route, store the state in the project's
Postgres, and read it back from a second route. No new node is needed: two routes
and a database.

## Streaming

`Stream` pipes a bus to the caller, one chunk per message, until the bus closes.
The canonical producer is an LLM stream:

```weft
ask = Route -> (prompt: String) { path: "chat", method: "POST" }
prov = OpenRouterProvider { model: "openai/gpt-4.1-nano" }
live = LlmStream
live.provider = prov.provider
live.prompt = ask.prompt
out = Stream { format: "sse" }
out.bus = live.stream
```

`format` decides the framing on an HTTP body: `sse` gives `text/event-stream`,
one `data:` line per line of the payload and a blank line per message, which is
what a browser's `EventSource` and LLM-style clients read; `ndjson` gives one
JSON value per line, for a script; `raw` sends the payloads as they are. The head
goes out with the first chunk, and the response ends when the bus closes. A bus
that already closed still streams whole, because `Stream` reads from the earliest
message retained.

![An answer streaming into a client as it is generated](../img/api-stream.gif)

## Sockets

```weft
sock = Socket -> (inbound: Generator[JsonDict]) { path: "chat/{room}" }
turn = Loop(msg: Generator[JsonDict]) -> (results: List[Boolean | Null]) {
  parallel: false
  over: ["msg"]
  echo = ExecPython(msg: JsonDict) -> (body: JsonDict) { code: "return {'body': {'echo': msg['text']}}" }
  echo.msg = self.msg
  say = Reply
  say.body = echo.body
  self.results = say.done
}
turn.msg = sock.inbound
```

`inbound` is a `Generator`: one item per message the caller sends, ended when the
caller disconnects, so a `Loop` over it runs its body once per message in
lock-step. Declare its item type to match the socket's `dataType`
(`Generator[JsonDict]`, `Generator[String]`, `Generator[File]`); an `inbound`
wired anywhere without a declared type is a compile error.

Behind a socket, `Reply` is one message and the socket stays open (a `status` or
`headers` there is refused), `Stream` is one message per bus message, and
`Close { code, reason }` sends the close frame.

**One connection is one run.** A bus lives inside one execution, so two sockets
cannot see each other's messages through a bus. A chat room today is a table, the
project's Postgres, that each run writes to and a trigger that reads it; a room
where one socket's message reaches another socket live is not expressible yet.

## Auth

A route is open unless you wire an auth access node into its `auth` input. Three
ship, each a connection you store the way the editor stores any other:

| Node | The connection holds | The caller presents | `caller` |
|---|---|---|---|
| `ApiKeyAuth` | `keys`, comma-separated | `X-Api-Key: <key>` or `Authorization: Bearer <key>` | `{"key": <index of the matched key>}` |
| `JwtAuth` | `issuer`, `jwks_url`, optional `audience` | `Authorization: Bearer <jwt>` | the token's claims |
| `HmacAuth` | `signing_secret` | `X-Timestamp: <unix seconds>`, `X-Signature: <hex hmac-sha256 of "<timestamp>.<body>">` | `{}` |

The dispatcher checks the caller before a run starts, so a refusal is a `401` and
the program never sees it. What the check established comes out on the trigger's
`caller` port, so finer rules, like this key may only read or this user owns that
room, are just a branch in the graph. The dispatcher never holds the key or the
secret: it hands the request to the broker, which holds the connection's material,
does the comparison, and answers with the identity it established.

A scheme these three do not cover is a fourth access node: a `metadata.json` with
a `service` recipe whose `verify` block names the scheme and whose paste fields
hold the material under the names the scheme reads (`keys`, `signing_secret`,
`public_key`, `audience`; an `oidc` scheme names its addresses as templates over
the fields). No Rust.

## When the catalog is not enough

Two nodes reading one socket (inbound is broadcast to `ctx` readers, each with
its own cursor), a reply assembled from many chunks with logic between them, a
response that mixes writes with a computed head: reach for
`ctx.http_caller()` / `ctx.ws_caller()` in a node of your own, described in
[Talking to a live caller](../nodes/live-callers.md). Reach for the catalog
nodes first, and `ctx` when the graph cannot say it.

## Trying it locally

```bash
weft activate                      # registers the routes; it prints no URL
curl -X POST "http://127.0.0.1:9999/connect/local/hello" -H 'content-type: application/json' -d '{"name":"ada"}'
websocat "ws://127.0.0.1:9999/connect/local/chat/room7"
weft follow <project>              # one execution per request, live
```

![A curl request to a route and its JSON reply](../img/api-curl.png)

`activate` prints `activated <name> (<id>)` and nothing else (see
[Triggers](triggers.md#activation)); there is no
URL to hand you: the live URL is `<dispatcher base>/connect/<tenant>/<path>`, and
every piece is fixed by the install rather than minted at activation. Locally the
base is `http://127.0.0.1:9999` and the tenant is `local`, so a frontend can be
written against `http://127.0.0.1:9999/connect/local/hello` before the program
ever runs. That address is reachable from this machine only for now: the public
tunnel (`--public-url`) does not forward `/connect/`, so a route called through it
answers `404`.

A browser page on another origin can call it, because the redirect the dispatcher
answers with and the gateway it lands on both carry open CORS headers. A route's
auth is per route, so the caller's origin says nothing about whether it may call.
That redirect is a signed pointer at the worker chosen to serve the call, and
nothing has run yet when it goes out: the execution is born when the caller
arrives at that worker, so a client that ignores redirects burns no run and leaves
nothing behind.

A route called with the wrong method answers `405` naming the verbs it serves, and
an unknown path answers `404`. A program that never replies holds the caller while
it runs, then answers `500` with the body `the run ended without answering`: the
run shows in `weft follow` with no `Reply` reached, and the fix is in the graph.
