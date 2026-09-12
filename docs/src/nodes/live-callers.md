# Talking to a live caller

If a program starts from an HTTP request or a WebSocket connection, its
nodes can answer the caller through `ctx`. They share the connection
attached to that execution.

The connection belongs to a worker. If that worker dies, the socket is
lost. To let the work outlive its caller, enable `canSuspend` on the trigger
and give the caller another way to retrieve the result. For saving progress
across restarts, read [Surviving a restart](durable-execution.md).

## Answer an HTTP request

Inside a node's `run` method:

```rust
use weft::caller::OutboundChunk;

let http = ctx.http_caller().await?;
http.respond(OutboundChunk::Json(
    serde_json::json!({ "message": "Hello from weft" }),
)).await?;
```

Wire this node downstream of an `ApiEndpoint` trigger.
`http_caller()` checks the protocol and waits for the caller to attach;
it returns an error if the execution has no suitable caller.

| Method | Use it to |
|---|---|
| `request_parts()?` | Read the incoming request |
| `write(chunk).await?` | Stream part of the response |
| `respond(body).await?` | Send the final body and finish the response |
| `close().await?` | Finish without another body |

`respond` and `close` end the response. A second attempt to end it
returns an error, including one from another node sharing that caller.

For a complete node, read the
[HTTP responder fixture](https://github.com/WeavemindAI/weft/tree/mvp/crates/weft-e2e/fixtures/web_trigger/nodes/http_responder).

## Read and reply over WebSocket

Wire a node downstream of `LiveSocket` and get its connection with
`ctx.ws_caller().await?`. This body fragment echoes JSON and text messages;
for binary messages it returns their byte count:

```rust
use weft::caller::{InboundMessage, OutboundChunk};
use serde_json::{json, Value};

let ws = ctx.ws_caller().await?;

while let Some(message) = ws.recv_next().await? {
    let value = match message {
        InboundMessage::Json(value) => value,
        InboundMessage::Text(text) => Value::String(text),
        InboundMessage::Bytes(bytes) => json!({ "bytes": bytes.len() }),
    };
    ws.send(OutboundChunk::Json(json!({ "echo": value }))).await?;
}
```

`recv_next` waits for a message without a per-read deadline. It returns
`None` when the stream ends, including disconnection or the configured
session limit. Unexpected failures return an error and propagate through
`?`. Cancellation can also stop the node while it waits.

A custom trigger can set `LiveConnectionConfig::max_session_secs` to cap
the session's total duration. Zero disables that cap. The catalog triggers'
shared input reader leaves it at zero; it is not a source field you can
set on those nodes.

| Method | Use it to |
|---|---|
| `send(chunk).await?` | Send a message |
| `recv_next().await?` | Read a message or finish a loop when the stream ends |
| `receive().await` | Read while handling specific `CallerError` variants yourself |
| `request(chunk).await` | Send, then read the next inbound message |
| `close().await?` | Close the connection |

`request` does not match replies to requests. It returns the next
message at this reader's position, which might be an unrelated event.
If the protocol uses request IDs, your node needs to match them.

## More than one reader

WebSocket messages are broadcast. Separate handles obtained from `ctx`
have independent read positions, so a responder and an observer can both
read the same messages.

The handle's starting position is fixed at connection attachment.
Messages that arrive before your node begins reading are still available
if the in-memory window retains them. Cloning an existing handle shares
its read position; create a new cursor when you want an independent reader.

| Method | Starting position |
|---|---|
| `ws.cursor()` | After the messages already received |
| `ws.cursor_from_start()` | Earliest message still in memory |
| `ws.cursor_at(offset)` | Specified absolute offset |
| `ws.cursor_including_last()` | Most recent message, then future messages |

`now_offset()` gives the next offset after the latest message;
`retained_floor()` gives the earliest one still in memory.

If your reader falls behind that window, the read returns
`CallerError::FellBehind { oldest_resident }` and advances its position
to that oldest retained message. Use `receive()` if you want to handle
that error and continue. The next read can resume there, but the missing
messages are no longer available to the cursor. Cursors do not fetch
older messages from the journal.

## Lifetime: tied to the caller, or surviving it

The trigger's `canSuspend` field decides whether the execution may
outlive its connection.

With the default `false`, disconnection cancels the execution. If all
remaining work becomes suspended while the caller is still connected,
the runtime keeps the connection open for `defaultHoldSecs`, currently
60 seconds by default. If no signal lets it continue before that hold
expires, it cancels the execution. One branch reaching a wait does not
cancel other branches that are still working.

With `canSuspend: true`, disconnection allows the work to continue.
A durable wait can suspend the execution and resume it later without a
caller. Further sends after disconnection deliver nothing, so arrange
another way to collect the result.

## Nodes that support either protocol

Use `ctx.caller()` to get an optional `CallerHandle`, whose variants
are HTTP and WebSocket. You can also ask `ctx.is_api_call()`,
`ctx.is_websocket()`, or `ctx.caller_data_type()`.

The handle exposes `is_connected()` and `ensure_connected().await?`.
If your node supports only one protocol, prefer `http_caller()` or
`ws_caller()`; those methods perform the connection check for you.
