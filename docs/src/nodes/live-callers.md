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
is known the moment the run starts. It gives you `method`, `path`, `params` (the
path's captures, like `room` in `/chat/{room}`), `query`, `headers`,
`credentials` (the headers carrying a credential, each a `Secret` you read
with `ctx.reveal`; they are never in `headers`), `caller` (whatever identity
weft established for the caller) and `base_url` (the address the caller
reached you at). With no caller attached it fails the run,
so if your node also runs without one, check `ctx.caller()` first.

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
| `drop_or_fail(made)` | Decide what happens to a piece you could not make, per the route's `whenChunkFails` |
| `close()` / `close_with(head)` | Finish |
| `fail(message)` | Finish as failed: a `500` if nothing went out yet, the answer format's error marker if it did |

Streaming is `write` repeatedly, then `close`. That is how an answer appears in
somebody's browser a word at a time while your model is still writing it.

If making one piece can fail (a value the route's `dataType` does not carry, a
file you cannot read), hand the attempt to `drop_or_fail` before you write it:

```rust
if let Some(chunk) = caller.drop_or_fail(make_chunk(&item).await)? {
    caller.write(chunk).await?;
}
```

You get `Some(chunk)` back when it was made. When it was not, the route's
`whenChunkFails` decides: on `surface` you get the error, and returning it
fails your node and tells the caller; on `drop_chunk` you get `None`, the skip
goes on the run's record, and you carry on with the next piece. Only a failure
of the work itself is ever skipped: a cancel, a pause, or a bad setting or
input always comes back as the error. For what
each setting is for, go and read [slow callers and messages that cannot be
made](../language/triggers-and-routes.md#slow-callers-and-messages-that-cannot-be-made).

If what you are streaming broke off halfway, end with `fail` rather than
`close`, so the caller does not take a cut answer for a whole one. `fail`
always tells the caller, whatever the route's `whenChunkFails` says. The caller
gets only `Something went wrong` and the run's id. Your `message` goes on the
run's record, which you open with `weft logs <run id>`. For the exact status
and body the caller receives, go and read [when a run ends before it
answers](../language/triggers-and-routes.md#when-a-run-ends-before-it-answers).

The head can only be sent once, and sending a body after the exchange ended is
an error rather than a silent nothing.

If you want the caller to receive a secret's value while the run's record keeps
its marker, send an `OutboundChunk::Revealed`. `sent` is what the caller gets,
and `kept` is what the record holds:

```rust
let revealed = ctx.reveal_in(value.clone(), &ty).await?;
caller.respond(OutboundChunk::Revealed { sent: Box::new(OutboundChunk::json(revealed)), kept: value }).await?;
```

`Reply` and `Stream` do this for you wherever their input's type says a secret
is, and fail on a secret anywhere else, since their caller would receive the
marker.

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
| `drop_or_fail(made)` | The same as on HTTP, per the socket's `whenChunkFails` |
| `receive()` | Wait for the next one |
| `recv_next()` | The same, with `None` when the caller has gone |
| `request(msg)` | Send, and wait for the reply to that |
| `close()` / `close_with(reason)` | End it |
| `fail(message)` | End it as failed, with a `1011` close |

There is also a cursor, so a node joining late can read from a chosen point
rather than only from now: `cursor()`, `cursor_from_start()`,
`cursor_at(offset)` and `cursor_including_last()`.

## A caller is not durable

A caller is a socket in one worker. It is not written down and it cannot be
rebuilt.

So while the caller is on the line, on a `Route` or `Socket` that leaves its
`outlivesCaller` setting off, the run keeps its worker, and `ctx.await_signal`
waits inside your call instead of pausing the run (go and read [when the run
cannot pause](durable-execution.md#when-the-run-cannot-pause)). With
`outlivesCaller` on, the run can pause. If it pauses before your node has
answered, weft ends the caller's request with an error saying no answer will
come (on a socket, a `1013` close), and the run carries on without them. For that setting, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept).

If the caller goes away, that is not your node failing. It reads as the run
being cancelled, which your node can notice with `ctx.is_cancelled()` and stop
doing expensive work. The exception is a route or socket with `outlivesCaller` on: its run carries
on, and what your node sends to the caller who left is dropped
without an error. The run's record names the messages that were still
waiting to go out when the caller left, so you can see exactly what they
never received; what your node sends after that is not on the record.

If you want the record to hold only the sizes of what streams through, or to
group messages into rows differently, the route has `journalEphemeral` and
`journalWindowMs` (go and read [two
modes](../language/streams-and-buses.md#two-modes)).

## In a test

```rust
rig.attach_caller(conn);
```

The fake rig gives you a caller to talk to, so a route node's behaviour is
testable without an install or a real socket.
