# Building an API

A program becomes an API with two triggers and three answering nodes, all in
`catalog/api`, none needing a line of Rust. `Route` (HTTP) and `Socket`
(WebSocket) start a fresh execution per request or connection and put the
request on their ports. `Reply`, `Stream` and `Close` answer whichever caller
the run has.

The rule underneath them is the one every weft mechanism follows: the engine
knows nothing about these nodes. It holds a connection, it lets a node read
the request and set the response head, and it delivers the trigger's wake
payload. The catalog nodes are one use of that; a node you write in Rust
reaches the same handle through `ctx` ([Talking to a live caller](../nodes/live-callers.md)).

## One route, one run

```weft
hello = Route -> (name: String) { path: "hello", method: "POST" }
answer = Reply { status: 201 }
answer.body = hello.name
```

A `Route` is a pattern and, optionally, a method. `path: "users/{id}"` captures
the segment under `id`; `method` empty serves every method.

Two routes can share a call as long as one of them spells out what the other
captures: `chat/general` beside `chat/{room}` is fine, and `chat/general` takes
that one call while `chat/{room}` takes the rest. What is refused when you
activate, naming both, is the pair where neither is the more specific, because
then the shared call has two equal claims: `chat/{room}` beside `chat/{name}`,
or `a/{x}/c` beside `a/b/{y}`, which both answer to `a/b/c`.

The request comes out on six fixed ports: `method`, `path` (as called, no
leading slash), `params` (the captures), `query`, `headers` (lowercase names),
and `caller` (who the auth gate let in; null on an open route). The body comes
out on the ports you declare after the arrow: on a `json` route each declared
name is a top-level key, and there is no port for the whole object: declare
every key you read (a key that is itself an object is `meta: JsonDict`). A `text`
route delivers the whole body as a `String` on the one port you declare; a
`bytes` route stores it as a file and delivers the stored-file value (declare
the port `File`). An undeclared key is dropped; a body key named like a fixed
port loses to the fixed port.

If you want a capture on its own port, declare a port with the capture's
name: `Route -> (id: String) { path: "cards/{id}" }` puts the `{id}` segment
on `id`, typed, and a body key called `id` loses to it (the capture is part of
the request, like the fixed ports). `params` still carries every capture.

### Pictures in and out

Nothing on a wire is ever base64. A value a node emits is at most 100 KB
(the node fails, naming the port, above that), and bytes live in storage as
a stored-file value: a few hundred bytes that say where the file is.

If a caller sends a picture in a JSON body, declare the port as the file kind
you expect and the route stores it on the way in:

```weft
upload = Route -> (photo: Image, caption: String) { path: "cards", method: "POST" }
```

The body key `photo` holds a `data:image/png;base64,...` URL (the media type
comes from it) or bare base64 (the media type comes from the bytes' own
signature). Bytes that are not what the port declares fail the run by name
(`port 'photo': the caller's bytes are not what the port declares`). The
file lands at execution scope, walled to that run and swept when it ends; a
later request cannot read it even if it was kept. To serve it from a later
request, wire it through `KeepFile { scope: "project" }`, which copies it into
the project's storage, and keep the stored-file value it emits in your
database (a jsonb column).

Sending it back to a caller needs nothing else: `Reply` and `Stream` walk the
whole answer and link every stored file they find, at any depth, so a route
serving a list of rows with pictures is three nodes and no loop. `Cast` a
stored-file value read out of the database to `Image` (or `File`) only when
you want the file as a typed value on a wire, to hand it to a node that takes
a picture.

On the way out, a stored file anywhere in a `Reply` or `Stream` body (nested
in an object, in a list of rows) goes out as a link:

```json
{ "url": "http://127.0.0.1:9998/public/files/<token>", "mimeType": "image/png", "filename": "photo", "sizeBytes": 48211 }
```

The `url` is what a browser puts in an `<img>`. Its address is the install's,
never the caller's: the public tunnel's when the install has one running, else
the install's own base, which locally is `http://127.0.0.1:9998`. Multipart
bodies are not read; send JSON with a data URL.

That link is minted for the answer it goes out in and expires minutes later,
so never write it into a table. Keep the file itself (`KeepFile` with a scope
that outlives the run) and store the value that node emits; each answer mints
a fresh link from it. A stored `url` serves dead pictures by morning.

`Reply` is the response: `status` (default 200), `headers` (an object of
strings), and `body`, whose shape follows the route's `dataType`. The first
thing your program sends commits the status line, so a `Reply` with
`status: 404` on a branch works, and a `Reply` after a `Stream` fails loud
("response head already sent").

A run that stops without a body answers through `Close`: a bare one is a
bodiless `204`, and with a `reason` that sentence is the body under the
`status` you set (`Close { status: 404, reason: "nothing to sweep" }`).

### Branching on the request

Let the graph decide the status. A Python node reading `params` and
returning both the body and a number wires straight into the reply:

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
wiring, and that gate takes any port of any type (the value is never read,
only a `false` says no). The shape is a `Switch` on the outcome: the failing
case gates a `Close` with the reason and status already written on it, the
passing case gates the rest.

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

A branch that skipped closes its ports, so the `Close` behind it skips too
and the other branch's answer stands.

## Answer first, keep working

A webhook receiver wants to say `200` at once and do the slow part after.
`Reply` early, then the rest of the graph; set `outlivesCaller: true` on the
route so the caller hanging up does not cancel the run. Off, the run is tied
to the caller and a disconnect cancels it, which is what a request-response
API wants.

## Long job, poll later

Answer `202` with an id from one route, store the state in the project's
Postgres, and read it back from a second route. No new node: two routes and a
database.

## Streaming

`Stream` pipes a bus to the caller, one chunk per message, until the bus
closes. The canonical producer is an LLM stream:

```weft
ask = Route -> (prompt: String) { path: "chat", method: "POST" }
prov = OpenRouterProvider { model: "openai/gpt-4.1-nano" }
live = LlmStream
live.provider = prov.provider
live.prompt = ask.prompt
out = Stream { format: "sse" }
out.bus = live.stream
```

`format` decides the framing on an HTTP body: `sse` (`text/event-stream`, one
`data:` line per line of the payload and a blank line per message) for a
browser's `EventSource` and LLM-style clients, `ndjson` (one JSON value per
line) for a script, `raw` for the payloads as they are. The head goes out with
the first chunk; the response ends when the bus closes. A bus that already
closed still streams whole: `Stream` reads from the earliest message retained.

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

`inbound` is a `Generator`: one item per message the caller sends, ended when
the caller disconnects, so a `Loop` over it runs its body once per message in
lock-step. Declare its item type to match the socket's `dataType`
(`Generator[JsonDict]`, `Generator[String]`, `Generator[File]`); an undeclared
`inbound` wired anywhere is a compile error. Behind a socket, `Reply` is one
message and the socket stays open (a `status` or `headers` there is refused),
`Stream` is one message per bus message, and `Close { code, reason }` sends
the close frame.

**One connection is one run.** A bus lives inside one execution, so two
sockets cannot see each other's messages through a bus. A chat room today is
a table (the project's Postgres) each run writes to and a trigger that reads
it; a room where one socket's message reaches another socket live is not
expressible yet.

## Auth

A route is open unless you wire an auth access node into its `auth` input.
Three ship, each a connection you store as the editor stores any other:

| Node | The connection holds | The caller presents | `caller` |
|---|---|---|---|
| `ApiKeyAuth` | `keys`, comma-separated | `X-Api-Key: <key>` or `Authorization: Bearer <key>` | `{"key": <index of the matched key>}` |
| `JwtAuth` | `issuer`, `jwks_url`, optional `audience` | `Authorization: Bearer <jwt>` | the token's claims |
| `HmacAuth` | `signing_secret` | `X-Timestamp: <unix seconds>`, `X-Signature: <hex hmac-sha256 of "<timestamp>.<body>">` | `{}` |

The dispatcher checks the caller before a run starts: a refusal is a `401` and
the program never sees it. What the check established comes out on the
trigger's `caller` port, so finer rules (this key may only read, this user
owns that room) are a branch in the graph. The dispatcher does not hold the
key or the secret: it hands the request to the broker, which holds the
connection's material and does the comparison, and answers with the identity
it established.

A scheme these three do not cover is a fourth access node: a `metadata.json`
with a `service` recipe whose `verify` block names the scheme and whose paste
fields hold the material by the names the scheme reads (`keys`,
`signing_secret`, `public_key`, `audience`; an `oidc` scheme names its
addresses as templates over the fields). No Rust.

## When you need a custom node

Two nodes reading one socket (inbound is broadcast to `ctx` readers, each
with its own cursor), a reply assembled from many chunks with logic between
them, a response that mixes writes and a computed head: reach for
`ctx.http_caller()` / `ctx.ws_caller()` in a node of your own, described in
[Talking to a live caller](../nodes/live-callers.md). The catalog nodes first;
`ctx` when the graph cannot say it.

## Trying it

```bash
weft activate                      # registers the routes; it prints no URL
curl -X POST "http://127.0.0.1:9999/connect/local/hello" -H 'content-type: application/json' -d '{"name":"ada"}'
websocat "ws://127.0.0.1:9999/connect/local/chat/room7"
weft follow <project>              # one execution per request, live
```

`activate` prints `activated <name> (<id>)` and nothing else, because there is
no URL to hand you: the live URL is `<dispatcher base>/connect/<tenant>/<path>`,
every piece fixed by the install rather than minted at activation. Locally the
base is `http://127.0.0.1:9999` and the tenant is `local`, so a frontend can be
written against `http://127.0.0.1:9999/connect/local/hello` before the program
ever runs. That address is reachable from this machine only for now: the public
tunnel (`--public-url`) does not forward `/connect/`, so a route called through
it answers 404. A browser page on another origin can call it: the redirect the dispatcher
answers with and the gateway it lands on both carry open CORS headers (a
route's auth is per route, so the caller's origin says nothing about whether it
may call). That redirect is a signed pointer at the worker chosen to serve the
call, and nothing has run yet when it goes out: the execution is born when the
caller arrives at that worker, so a client that ignores redirects burns no run
and leaves nothing behind. A route with the wrong method
answers `405` naming the verbs it serves, an unknown path `404`. A program that
never replies holds the caller while it runs, then answers `500` with the body
`the run ended without answering`: the run shows in `weft follow` with no
`Reply` reached, and the fix is in the graph.
