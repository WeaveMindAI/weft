# Talking to a live caller

[Suspension](durable-execution.md) is a **disconnected** wait: the worker
parks and dies.

When someone hits an `ApiEndpoint` (HTTP) or `LiveSocket` (WebSocket) trigger,
the dispatcher routes that held connection to one worker, which stays alive on
the open socket for the life of the request. Any node downstream of the trigger
can talk back over it.

A live caller is not durable: the connection is pinned to that worker and dies
with it. Work that has to survive a restart goes through
[suspension](durable-execution.md) instead.

## Getting the handle

```rust
let http = ctx.http_caller().await?;   // fails loud if this run has no HTTP caller
let ws = ctx.ws_caller().await?;
```

Those are the one-call forms for a node that only makes sense on one protocol.
Each folds the whole chain (a caller is present, it is the right protocol, the
connection barrier passed) and fails loudly naming the trigger to wire it
under.

For a node that branches:

```rust
ctx.caller()             // Option<CallerHandle>, an enum over the two protocols
ctx.is_api_call()
ctx.is_websocket()
ctx.caller_data_type()   // the declared shape: Json, Text, Bytes
```

`is_api_call` and `is_websocket` are separate questions because there are three
answers, not two: HTTP, WebSocket, or nobody on the line at all.

`CallerHandle` is protocol-typed, so the type is honest about what each side
can do. An HTTP caller has no `send`; a WebSocket caller has no `respond`.

## HTTP

```rust
http.request_parts()?           // the inbound request
http.write(chunk).await?        // stream a chunk
http.respond(body).await?       // send the final body
http.close().await?
```

`respond` and `close` are terminal. The first one wins and a second errors
loudly.

## WebSocket

```rust
ws.send(chunk).await?
ws.recv_next().await?       // Some(msg), or None when the stream ends
ws.receive().await?         // the typed-error form of the same read
ws.request(chunk).await?    // send, then await one reply
ws.close().await?
```

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
off one socket.

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

One field on the trigger, `canSuspend`, is the whole lifetime axis.

**Off (the default).** The run is tied to the caller, so a disconnect cancels
it and a node that hits a durable `await_signal` holds the worker briefly and
then the run is killed. This is what a request-response API wants.

**On.** The run may suspend and resume later without the caller, becoming a
background job. A disconnect does not kill it, and further sends go into the
void.

## Worked examples

Two live in the end-to-end test fixtures, so they run on every pass of the
suite:

- `crates/weft-e2e/fixtures/web_trigger/nodes/http_responder`
- `crates/weft-e2e/fixtures/live_chat/nodes/ws_echo`
