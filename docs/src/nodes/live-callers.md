# Talking to a live caller

[Suspension](durable-execution.md) is a **disconnected** wait: the worker
parks and dies.

When someone hits a `Route` (HTTP) or `Socket` (WebSocket) trigger, the
dispatcher routes that held connection to one worker, which stays alive on
the open socket for the life of the request. Any node downstream of the
trigger can talk back over it.

The catalog answers most of it without Rust: `Reply`, `Stream` and `Close`
([Building an API](../language/building-an-api.md)). This page is the handle
those nodes are built on, for a node of your own: two readers of one socket,
a body assembled from many chunks with logic between them, anything the
graph cannot say.

A live caller is not durable: the connection is pinned to that worker and
dies with it. Work that has to survive a restart goes through
[suspension](durable-execution.md) instead.

## Getting the handle

```rust
let http = ctx.http_caller().await?;   // fails loud if this run has no HTTP caller
let ws = ctx.ws_caller().await?;
let either = ctx.live_caller().await?; // CallerHandle::Http(_) | CallerHandle::Websocket(_)
```

Those are the one-call forms. Each folds the whole chain (a caller is
present, it is the right protocol, the connection barrier passed) and fails
loudly naming the trigger to wire it under.

For a node that branches without waiting:

```rust
ctx.caller()             // Option<CallerHandle>, an enum over the two protocols
ctx.caller_request()?    // Arc<LiveRequest>: what the caller sent to open the exchange
ctx.is_api_call()
ctx.is_websocket()
ctx.caller_data_type()   // the declared shape: Json, Text, Bytes
```

`is_api_call` and `is_websocket` are separate questions because there are three
answers, not two: HTTP, WebSocket, or nobody on the line at all.

`CallerHandle` is protocol-typed, so the type is honest about what each side
can do. An HTTP caller has no `send`; a WebSocket caller has no `respond`.

## The request

Both protocols carry what the caller sent to open the exchange, as the
gateway matched and gated it:

```rust
let req = ctx.caller_request()?;   // also handle.request()
req.method                          // "POST"
req.path                            // "chat/room7", as called, no tenant, no leading slash
req.params                          // the route's {name} captures
req.query                           // the query string, parsed
req.headers                         // Vec<(name, value)>; req.header("content-type") reads one
req.caller                          // Some(identity) when the route has an auth, else None
```

On HTTP the body sits beside it: `http.request_parts()?` is `{ request, body }`,
the body decoded per the trigger's `dataType` (`Json`, `Text`, `Bytes`). An
empty JSON body decodes to `null`, which is what a bodiless GET carries.

## HTTP

```rust
http.write(chunk).await?                 // stream a chunk
http.write_with(head, chunk).await?      // the first chunk, with the status and headers
http.respond(body).await?                // the final body, 200
http.respond_with(head, body).await?     // the final body under this head
http.close().await?                      // end the response (204 if nothing went out)
http.close_with(head).await?             // end it bare under this head (a 404 with nothing to say)
http.wire_started()                      // has anything gone out yet?
```

The status line and headers are the program's to set, and they are set by
the FIRST outbound item. `ResponseHead { status, headers }` rides `write_with`,
`respond_with` or `close_with`; a plain `write` or `respond` first commits a
`200` with a content type matching the chunk's shape. A head given after the
first item is `HeadAlreadySent`, an error, never a silent drop.
`respond`, `close` and their `_with` forms are terminal: the first one wins
and a second errors loudly.

Nothing reaches the caller before your first item. The worker holds the
response until then. If the run ends and the program never wrote, the
caller gets a `500` whose body says `the run ended without answering`; if
it wrote but never closed, the body simply ends. A socket the run leaves
open gets a normal close frame.

## WebSocket

```rust
ws.send(chunk).await?
ws.recv_next().await?       // Some(msg), or None when the stream ends
ws.receive().await?         // the typed-error form of the same read
ws.request(chunk).await?    // send, then await one reply
ws.close().await?           // a normal-closure frame (1000)
ws.close_with(CloseReason { code: 4001, reason: "done".into() }).await?
```

A head handed to a WebSocket connection is dropped: its only head was the
upgrade.

Both protocols share `is_connected()` and one `ensure_connected().await?`
barrier, which waits for the caller's socket to actually attach before you
talk into it.

A read is **unbounded**. A node may wait minutes or hours for the caller's
next message; only a disconnect or the trigger's session cap ends the wait.

### The loop

```rust
use weft::caller::{InboundMessage, OutboundChunk};

async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let ws = ctx.ws_caller().await?;

    while let Some(msg) = ws.recv_next().await? {
        let v = match msg {
            InboundMessage::Json(v) => v,
            InboundMessage::Text(s) => Value::String(s),
            InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
        };
        ws.send(OutboundChunk::Json(json!({ "echo": v }))).await?;
    }

    let _ = ws.close().await;
    ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
}
```

`recv_next` yields `Some(msg)` per message and `Ok(None)` when the stream ends
for good: the caller disconnected, the session timed out, or it expired. A
consumer that fell behind comes back as an `Err` instead, because that gap is
resumable and must not be read as the end, so the language does the
end-of-stream classification for you and a real failure propagates through
`?`. When you need to distinguish the exact outcome, `receive()` returns the
typed error so you can match every case.

## Two readers, no race

Inbound on a WebSocket is **broadcast** and forward-only, the same model as a
bus. `ws.receive()` reads messages arriving after you got the handle, with the
position pinned at the moment you obtain it, so a reader that attaches after a
message was sent still sees it.

Every reader has its own position, so a responder and an observer can both run
off one socket. This is the one thing the catalog's `Socket` trigger does not
give you: its `inbound` stream has one consumer.

### Reading history

Mint a positioned cursor, the same concept as a bus:

```rust
ws.cursor_from_start()        // everything still retained in RAM
ws.cursor_at(offset)
ws.cursor_including_last()    // forward, plus the single most recent message
ws.now_offset()
ws.retained_floor()
```

Offsets are absolute over the connection's whole life, so a saved offset keeps
naming the same message as the retention window moves.

A cursor reads the in-RAM window only. When its offset has been trimmed out,
the read returns `FellBehind { oldest_resident }` and the cursor is moved
there, so the next read resumes at the earliest message still retained.

## Lifetime: tied to the caller, or surviving it

One field on the trigger, `outlivesCaller`, is the whole lifetime axis.

**Off (the default).** The run is tied to the caller, so a disconnect cancels
it and a node that hits a durable `await_signal` holds the worker briefly and
then the run is killed. This is what a request-response API wants.

How a caller who leaves is noticed depends on whether anything can be written
to them. If the program is still holding the response head (nothing has gone
out yet), the connection itself carries the news: the handler is dropped, the
body's receiver goes with it, and the run ends there. Once the head is out, a
write is the only way to find out, so on a framed stream (`sse`, `ndjson`) the
worker writes a filler the format ignores every heartbeat, and a hang-up a
proxy in the middle would otherwise hide behind a quiet feed is found by that
write.

A `raw` body has no filler, because every byte of it is the program's. That
feed is watched anyway, by the connection itself: while it is quiet the
machine asks the caller whether it is still there, in a packet carrying no
payload at all, so the program's byte stream is untouched. A caller who
answers resets the clock; one that has vanished does not, and the connection
fails. `callerSilenceSecs` on the trigger is how long that silence may last,
thirty seconds unless the author says otherwise.

Both halves matter, and they cover different disappearances. The filler puts
bytes on the wire, which is what catches a caller that vanishes with data in
flight. The machine's own questions catch one that vanishes while everything
is quiet, which is the only thing a feed with nothing to write can rely on.

One shape is beyond both, and it is the only thing refused: a stream on a run
that MAY OUTLIVE ITS CALLER. Noticing the caller left changes nothing there,
because the run does not end with them, and a stream ends only when its bus
closes, so nothing would ever end it. That fails before the first chunk,
naming both ways out: `maxSessionSecs` on the trigger, or turning off the
setting that lets the run outlive its caller. The refusal lives in the
connection itself, so a node that builds its own framing meets it too, and
the compiler catches the catalog's `Stream` node before anything runs.

`maxSessionSecs` is the only deadline weft puts on a live exchange, and it is
off unless the author sets it. There is deliberately no default: it ends a
connection on the clock whatever the caller is doing, which is right for the
one shape above and wrong everywhere else. So weft says what is missing
instead of picking a number behind your back.

**On.** The run may suspend and resume later without the caller, becoming a
background job. A disconnect does not kill it, and further sends go into the
void.

## Testing a node that talks to a caller

The fake rig attaches a scripted caller:

```rust
let conn = FakeCallerConnection::connected(config);   // CallerRuntimeConfig for the protocol
conn.set_handshake(request);                          // the LiveRequest
conn.set_http_body(InboundMessage::Json(json!({ "text": "hi" })));
rig.attach_caller(conn.clone());
let outcome = rig.run(&MyNode, json!({})).await.ok()?;
conn.heads();          // every ResponseHead handed over
conn.chunks();         // every chunk, streamed and final
conn.close_reason();   // the close frame, if the node closed
```

`catalog/api/*/tests.rs` are worked examples, one per shipped node.

## Worked examples

Two ctx-driven nodes live in the end-to-end test fixtures, so they run on
every pass of the suite:

- `crates/weft-e2e/fixtures/web_trigger/nodes/http_responder`
- `crates/weft-e2e/fixtures/live_chat/nodes/ws_echo`
