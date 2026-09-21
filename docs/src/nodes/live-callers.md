# Talking to a live caller

Most runs have nobody waiting. A run started by an HTTP request or a WebSocket
does, and that caller is holding a connection open while your program works.

Your node gets at them through the ctx.

```rust
let caller = ctx.http_caller().await?;
caller.respond(OutboundChunk::json(json!({ "ok": true }))).await?;
```

## Finding out whether there is one

| Call | What you get |
|---|---|
| `ctx.is_api_call()` | Whether an HTTP caller is attached |
| `ctx.is_websocket()` | Whether a WebSocket one is |
| `ctx.caller()` | The caller as a handle, or `None` |
| `ctx.caller_request()` | What they sent to open the exchange |
| `ctx.caller_data_type()` | Whether this connection speaks bytes or JSON |

Those are two separate questions rather than one enum, because a node can
branch three ways: HTTP, WebSocket, or nobody.

`ctx.caller_request()` does not wait for the connection, because the handshake
is known the moment the run starts. It gives you the method, the path, the
route parameters, the query, the headers and whatever identity the gate
established.

## Getting the handle

| Call | Gives you | Fails when |
|---|---|---|
| `ctx.http_caller().await?` | An HTTP caller, attached and connected | There is none, or it is a WebSocket |
| `ctx.ws_caller().await?` | A WebSocket caller | There is none, or it is HTTP |
| `ctx.live_caller().await?` | Whichever is connected | There is none |

Each of those folds together "is anybody there", "is it the right protocol" and
"wait for the connection" into one call. Use the specific one when your node
only makes sense behind a route, and `live_caller` when it works either way.

## Answering over HTTP

```rust
caller.respond(OutboundChunk::json(json!({ "answer": text }))).await?;
```

| Call | What it does |
|---|---|
| `respond(body)` | Send the whole answer and finish |
| `respond_with(head, body)` | The same, choosing the status and headers |
| `write(chunk)` | Send a piece, keeping the connection open |
| `write_with(head, chunk)` | The first piece, setting the head |
| `close()` / `close_with(head)` | Finish |

Streaming is `write` repeatedly, then `close`. That is how an answer appears in
somebody's browser a word at a time while your model is still writing it.

The head can only be sent once, and sending a body after the exchange ended is
an error rather than a silent nothing.

## Talking over a WebSocket

```rust
let ws = ctx.ws_caller().await?;

while let Some(msg) = ws.recv_next().await? {
    let reply = handle(msg)?;
    ws.send(OutboundChunk::json(reply)).await?;
}
```

| Call | What it does |
|---|---|
| `send(msg)` | Send one message |
| `receive()` | Wait for the next one |
| `recv_next()` | The same, with `None` when the caller has gone |
| `request(msg)` | Send, and wait for the reply to that |
| `close()` / `close_with(reason)` | End it |

There is also a cursor, so a node joining late can read from a chosen point
rather than only from now: `cursor()`, `cursor_from_start()`,
`cursor_at(offset)` and `cursor_including_last()`.

## A caller is not durable

This is the thing to hold on to. A caller is a socket in one worker. It is not
written down and it cannot be rebuilt.

So a run holding one keeps its worker alive, and a node in that run cannot
park for a week the way a human step does. If your program needs both, do the
long wait in a different run and answer the caller with something it can come
back for.

If the caller goes away, that is not your node failing. It reads as the run
being cancelled, which your node can notice with `ctx.is_cancelled()` and stop
doing expensive work.

## In a test

```rust
rig.attach_caller(conn);
```

The fake rig gives you a caller to talk to, so a route node's behaviour is
testable without a cluster or a real socket.
