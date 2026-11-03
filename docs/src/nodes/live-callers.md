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
| `ctx.caller_data_type()` | Whether this connection speaks `json`, `text` or `bytes` (the trigger's `dataType`) |

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

## Reading what the caller sent

If you want the request's body as the value your program declared for it,
ask for it by type and weft does the rest:

```rust
let http = ctx.http_caller().await?;
let taken = http.body_as(&ctx, &declared, "body", None).await?;
let body = match taken {
    Ok(body) => body,                        // Option<Value>: None when no body came (every GET)
    Err(why) => return http.refuse(why).await, // a 400 telling the caller what was wrong
};
```

`body_as` reads the body in the trigger's `dataType` and holds it to
`declared`. A picture the caller sent in a file-typed place, as a `data:` URL
or bare base64, is stored (run scope) and the stored file takes its place; an
upload slot (`weft-upload:...`) in such a place is received. A stored-file or
secret marker the caller wrote in themselves is refused, since only weft makes
those. The last argument stands in when the connection carries no body: pass
the body a fired run carries (one started without a request), or `None`.

There are two kinds of failure, kept apart. The inner `Err(why)` is the
caller's mistake, worded for the caller: a body in the wrong shape, a field
that does not fit, a picture that is not one. `refuse(why)` answers it with a
`400`, `{"error": why}` on a `json` route and the text itself on the others.
The outer `?` is your node's own failure, like storage refusing a write.

On a WebSocket, `recv_as(&ctx, &declared, name)` does the same for each
message, and gives `None` once the caller has gone. There `refuse(why)` sends
the reason back and the session goes on:

```rust
let ws = ctx.ws_caller().await?;
let mut turn = 0;
while let Some(taken) = ws.recv_as(&ctx, &item, &format!("message-{turn}")).await? {
    turn += 1;
    match taken {
        Ok(msg) => handle(msg).await?,
        Err(why) => ws.refuse(why).await?,
    }
}
```

`name` is what a refusal and a stored file are called.

If you want the fields of what you took on your node's same-named output
ports, `ctx.fan_declared(&value)` builds that output (go and read
[values and emission](values-and-emission.md)).

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

### Sending one of your node's inputs

If what you answer with is the value of one of your node's inputs, let weft
turn it into the chunk:

```rust
let chunk = OutboundChunk::of_input(&ctx, "body", value, data_type).await?;
http.respond_with(ResponseHead::new(status).with_headers(headers), chunk).await?;
```

`of_input` reads the input's declared type and, per `data_type`: on `json`
every file the type places goes out as a link (see [files going
out](#files-going-out)); on `text` the value must be a string; on `bytes` it
must be a stored file, and its content is what gets sent. Every secret the
type places is revealed for the caller while the run's record keeps its
marker. A secret where the type declares none fails the node, since the caller
would receive the marker instead of the value.

The head has a few builders:

| Call | What it gives you |
|---|---|
| `ResponseHead::new(status)` | A status and no headers yet |
| `.with_header(name, value)` / `.with_headers(map)` | Headers added, one at a time or a whole `BTreeMap<String, String>` (a `headers` input, say, once `ctx.reveal_input` has revealed it) |
| `.with_file(&stored)` | For a stored file answered as bytes: its type as `content-type`, its name as an inline `content-disposition`, unless the head already set them |
| `ResponseHead::redirect(status, location)?` | A redirect: `301`, `302`, `303`, `307` or `308`, and `200` becomes `302`. Any other status is refused |

If you hold a value that is not one of your inputs and want a secret in it
revealed, send an `OutboundChunk::Revealed` yourself. `sent` is what the
caller gets, and `kept` is what the record holds:

```rust
let revealed = ctx.reveal_in(value.clone(), &ty).await?;
caller.respond(OutboundChunk::Revealed { sent: Box::new(OutboundChunk::json(revealed)), kept: value }).await?;
```

### Streaming a bus

If you want every message of a bus sent to the caller as it comes, hand the
bus over:

```rust
let head = ResponseHead::new(200);
let whole = http.stream_bus(&ctx, &bus, head, Format::Sse, data_type, Some(("first", first))).await?;
```

It reads from the earliest message the bus still holds, so a producer that
started before your node, or already finished, loses nothing. The head goes
out with the first chunk, and the response ends when the bus closes. A bus
that ended failed is never ended as a whole answer: the response fails, as
with `fail`, and `stream_bus` answers `false` (your node does not fail;
`true` means the whole bus went out). Each message goes through `drop_or_fail`. `first` is one value
sent ahead of the bus and framed like its messages, the value of the input
it names, whose type says where its files and secrets are. The run is held
while it streams, since an answer cannot be picked up from the middle.

`Format` says how messages are framed. It reads straight from an input that
holds `"raw"`, `"ndjson"` or `"sse"` (`ctx.inputs.get::<Format>("format")?`):

| `Format` | The caller gets | Content type | While the bus is quiet |
|---|---|---|---|
| `Raw` | Each payload as it is, per `data_type`, no separator | None added | Nothing |
| `Ndjson` | One JSON value per line | `application/x-ndjson` | An empty line |
| `Sse` | `data:` lines per event, a blank line after each | `text/event-stream` | `: keepalive` |

The filler written while the bus is quiet is ignored by every reader of the
format, and it is how a caller who hung up without a word is found. A head
that already names a content type keeps it.

### Files going out

A file can never go to a caller as bytes inside JSON, so it goes as a link:
`{ "url", "mimeType", "filename", "sizeBytes" }`. A browser can put that `url`
straight into an `<img>` or a `<video>`. Asked again soon, the same file gets
the same link, and the link lives as long as the file's `linkSecs` says, else
fifteen minutes. A file the install cannot link fails the node.

What counts as a file is decided by the value's **declared type**, never by
finding a marker in raw JSON. `of_input` and `stream_bus` do this for you.
If you build a JSON answer
yourself, `caller::link_files(&ctx, &value, &ty)` gives you the value with
every file linked. For the walk underneath, go and read [whole values full of
files](storage.md#whole-values-full-of-files).

A bus declares no type for its messages, so `stream_bus` reads each message's
type off the message itself (`WeftType::infer`): a stored file in it is a file,
a secret is a `Secret`. From there a message goes out exactly like an input of
that type: every file as a link, every secret revealed, with the run's record
keeping the secret's marker. If your own node reads a bus and hands its
messages outside weft, read them with `next_message` and
`WirePayload::into_typed_json`, which gives you the same type to link and
reveal by.

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
| `recv_as(&ctx, &declared, name)` | The next one as the value you declared (see [reading what the caller sent](#reading-what-the-caller-sent)) |
| `refuse(why)` | Tell the caller what was wrong with a message; the session goes on |
| `stream_bus(&ctx, &bus, data_type, first)` | One socket message per bus message until the bus closes, the socket staying open. A bus that ended failed ends the session as failed, and the call answers `false` |
| `request(msg)` | Send, and wait for the reply to that |
| `close()` / `close_with(reason)` | End it |
| `fail(message)` | End it as failed, with a `1011` close |

There is also a cursor, so a node joining late can read from a chosen point
rather than only from now: `cursor()`, `cursor_from_start()`,
`cursor_at(offset)` and `cursor_including_last()`.

## A caller is not durable

A caller is a socket in one worker. It is not written down and it cannot be
rebuilt.

So while the caller is on the line, the run keeps its worker, whatever the
route's `outlivesCaller` setting says, and `state.wait` waits inside your call
instead of pausing the run (go and read [when the run cannot
pause](durable-execution.md#when-the-run-cannot-pause)). The trigger's
`holdSecs` does not count down meanwhile, so a wait does not cut a caller who
is still there, unless `holdSecs` is `0`, which fails the wait at once. `outlivesCaller` only decides what the caller leaving does: off, the
run is cancelled; on, it carries on without them, and pauses once everything
in it waits on something from outside. For that setting, and for what happens
when a connection reaches a cloud install's one-hour limit, go and read [how a
run is kept](../language/triggers-and-routes.md#how-a-run-is-kept).

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
