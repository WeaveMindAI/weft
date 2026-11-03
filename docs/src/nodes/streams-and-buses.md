# Streams and buses in Rust

For which of the two you want, go and read
[streams and buses](../language/streams-and-buses.md). This is the Rust on both
sides.

## Producing a stream

Declare a `Generator[T]` output, then emit as many times as you like:

```rust
for chunk in response_chunks {
    ctx.pulse_downstream(NodeOutput::new().set("chunk", chunk)).await?;
}
```

Each emission is one item, checked against the element type. When your body
returns, the port closes and that is the end of the stream. To end it early,
`ctx.close_port("chunk")`. After a close, nothing can follow.

`pulse_downstream` sends and carries on. If you are producing faster than the
consumer reads, use `yield_downstream(output, &mut state)`, which waits until
each item is taken. The run can pause in that wait, so it takes your node's
state. After a pause the body runs again from the top, and it has to come back
to the yield it paused in, with the same item: that yield sends nothing again
and waits for the item to be taken. So keep in the state how far the node got,
the way the producer in [a stream producer that
pauses](durable-execution.md#a-stream-producer-that-pauses) does.

If you do neither and run far ahead, you hit the cap at 4096 un-taken items and
the emit fails, telling you the three ways out: yield instead of pulse, let the
consumer catch up, or `ctx.set_max_buffered_items("chunk", n)`.

## Consuming a stream

Read the port like any input, then pull:

```rust
let rows: Generator<Row> = ctx.inputs.get("rows")?;
let mut state = ctx.keep(()).await?;

while let Some(row) = rows.next(&mut state).await? {
    // one row at a time
}
```

Your body starts when the first item arrives, so you are running while the
producer is still producing.

`next(&mut state)` waits for the next item and gives you `None` once, at a
clean end. The run can pause while it waits, which is why it takes your
node's state (go and read [surviving a restart](durable-execution.md)): weft
saves how many items you took, and a resumed body carries on after them.
`try_next(&state)` is the same without waiting, answering `Item`, `Empty` or
`Finished`. `drain(&mut state)` takes the lot and waits for the end; the run
can pause there too, and a resumed body's `drain` starts from the items it
had already taken. If your node pulls a stream and also waits, yields or
reads at another line, or needs an item again after a pause, go and read
[reading a bus or a stream across a
pause](durable-execution.md#reading-a-bus-or-a-stream-across-a-pause).
`end()` tells you whether the producer finished or failed, or `None` while it
is still open.

If your node's work cannot be picked up from the middle (a session with a
provider, audio gathered into one file), pass `&mut holding` from `ctx.hold()`
to these calls (and to bus reads and `wait_for`) instead of your state: while
`holding` exists, no wait of your node pauses. For when to keep a state and when to hold, go and read
[holding the run instead of keeping a
state](durable-execution.md#holding-the-run-instead-of-keeping-a-state).

The `?` matters. If the producer failed, that is where you find out, rather
than getting a clean end you would mistake for an empty stream. `drain` errors
without handing back the partial list, for the same reason.

Reading the port twice gives you two handles over one stream, sharing a
position, not two copies.

## Hosting a bus

```rust
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
bus.send("text", json!({ "text": "hello" })).node_err("sending the greeting")?;
```

`open_bus` does the whole producer move: makes the bus, emits the marker on
that port, and registers your name.

If the work that makes your pieces can fail (a provider's call, a handshake, a
first query), open the bus on the first piece instead of before the work.
Until the marker goes out, your node has opened no stream. If it fails then, a
program that wired your `_error` still gets the failure there, and the port
closes as if nothing was coming, so the readers never start. `ctx.pending_bus` holds the
options until then:

```rust
let mut bus = ctx.pending_bus("stream", BusOptions::default(), "llm");
let mut chunks = client.stream(&prompt).await.node_err("starting the stream")?;
while let Some(chunk) = chunks.next().await {
    bus.open().await?.send("delta", json!(chunk)).node_err("sending a delta")?;
}
```

The first `open()` makes the bus and emits its marker, and every later one
hands back the same bus. If you never call it, the port closes like any port
you never mentioned. `bus.opened()` tells you whether it is open yet without
opening it. `LlmStream` works this way.

## Joining one

```rust
let mut state = ctx.keep(()).await?;
let bus = ctx.join_bus("channel", "translator")?;
bus.wait_for("host", &mut state).await?;

let mut cursor = bus.cursor()?;
while let Some(msg) = cursor.next(&mut state).await? {
    // ...
}
```

`wait_for("host", &mut state)` waits until the host has joined the bus. If
the run pauses later, the body run again gets past it at once, because the
list of who joined comes back with the bus.

`join_bus` resolves the bus on that input and registers your name. A bus you
joined ends with your body, as one you made does, so a reader that dies
mid-conversation never leaves the producer waiting on it.

For an observer that must **not** end the bus when it is done (a debug tap, or
a node streaming the bus out to a caller), use `ctx.bus_from_input("channel")`
instead.

If your node has a slow warmup, call `join_bus` only once it is ready, because
registering is what everybody else's `wait_for` waits on. Each reader has its
own cursor, starting where it joined, so reading takes nothing away from the
others.

Every read and every `wait_for` is a place the run can pause, so each takes
your node's state (or the `Holding` from `ctx.hold()`, if the run must not
pause while your node reads). weft saves how far your reader on each bus read
and the name you hold there, and a body that runs again after a pause gets
them back.

A name that joined and left since still counts for `wait_for`, and the wait
fails the node if the bus closed first. You never have to decide whether a name will ever turn up: the
engine watches the run, and when a wait can no longer be satisfied it ends the
bus, failed and saying the run was stuck, and every waiting cursor wakes.

## One reader per bus

Your body can open only one reader on each bus, because weft saves one
position per bus for your node. `cursor()`, `cursor_from_start()` and `cursor_at(offset)`
return a `Result`, and opening a second reader on a bus your body already
opened one on, still open or dropped, fails the node:

```text
node 'translator' opened a second reader on a bus at <line>, which it already
opened one on at <line>: a node keeps one reader per bus, so that it can come
back where it stopped after a pause. Read every message through that one reader, filtering by sender or
kind in the node's code
```

If you want only some of the messages, filter that one reader: match on the
entry in your loop, or hand `with_filter` a function that keeps the ones you
want. If your node sends on a bus it also reads and can pause in a `wait`,
drop its own messages with `with_filter`: weft saves the reader's place past
what the filter turns down, so it never comes back behind its own replies.

In a body run again after a pause, whichever of the three you call, the reader
starts where the old one stopped. The offset you ask for only counts the first
time.

## How a bus ends

weft ends every bus your firing made or joined when your body returns, and
says how: **finished** if the body returned `Ok`, **failed** with the error if
it returned one, panicked, or was cancelled. A pause ends nothing: the bus
stays open through it, and your body gets it back when it runs again. A task
you spawned does not keep a bus open: the bus ends with the body, whoever
still holds a handle.

Dropping a handle ends nothing. If you are done sending before your body is
done working, `bus.close()` ends the bus now, and always as finished. To end
it failed, return the error from your body.

A reader finds out once its cursor runs out. `next` returns `None` at the
end either way, so ask `bus.end()`:

```rust
while let Some(msg) = cursor.next(&mut state).await? {
    // ...
}
if let Some(StreamEnd::Failed { error }) = bus.end() {
    weft::node_bail!("the stream broke off: {error}");
}
```

`end()` is `None` while the bus is open. What a failed end means is up to your
node. If it answers a caller from a bus, end the answer with `fail` (go and
read [talking to a live caller](live-callers.md)).

## Choosing options

```rust
use weft::bus::{BusOptions, BusPayloadKind};

BusOptions {
    journal: ctx.journal_policy()?,
    window: Some(256),
    payload: BusPayloadKind::Bytes,
    ..BusOptions::default()
}
```

| Field | Default | What it decides |
|---|---|---|
| `journal` | `JournalPolicy::default()`: journaled, one-second window | What the record keeps of this bus and how often it writes a row. `ctx.journal_policy()?` hands over what your node's users set, `JournalPolicy::ephemeral()` keeps payloads out whatever they set (what `ElevenLabsSpeak` does) |
| `window` | `None`, which is 64 messages | How far back a reader can reach |
| `payload` | `BusPayloadKind::Json` | JSON or bytes. Frozen at creation; sending the other shape is refused |
| `meta` | `Value::Null` | Details every reader can read off the marker: a sample rate, dimensions. Keep it small, it rides the marker |

If you want your node's users to choose between the two modes and pick the
window, set `features.journalSettings` in its `metadata.json` (go and read
[features](metadata.md#features)) and pass `ctx.journal_policy()?` as above.
For what each mode keeps and what the window does, go and read [two
modes](../language/streams-and-buses.md#two-modes).

## What a send tells you

Failures are values, never silent drops: the bus was closed, you never
registered, you sent the wrong payload shape, or the record is degraded.

## A pause keeps a bus, a crash ends it

When a run pauses with a bus open, the bus comes back in the next worker, with
the same id, at the offset where it stopped, and your reader on it starts
where it stopped. A journaled bus comes back with its last JSON messages, as many as
its window holds, but only those after the newest one over 100 KB: its record
keeps a message that large trimmed, so that one and every message before it
are lost. An ephemeral bus, or one that carries bytes, comes back empty.
A reader that was waiting on its cursor had read everything, so it loses
nothing (for an example, go and read [a conversation that pauses while nobody
talks](durable-execution.md#a-conversation-that-pauses-while-nobody-talks)).
A reader that paused in a `wait` with messages still unread carries
on where it stopped if the bus still holds those messages when it comes back;
if it does not (they did not come back with the bus, or the bus's window
dropped them while the reader waited), its first read on that cursor fails
instead of skipping them (for how to write a reader that stops to wait, go and
read [a bus reader that stops to ask
somebody](durable-execution.md#a-bus-reader-that-stops-to-ask-somebody)).
Messages its reader's filter turns down right after
where it stopped (its own replies, say) do not count: its place is saved past
them. A reader on a bus your node had not read before the pause starts like
any new one.

A node holds one name per bus, the one its state carries over a pause, so a
second `register` under another name is refused.

A worker that dies is different: its buses can end failed, and each reader
then finds that in `bus.end()`. For when, go and read [when the worker dies
mid-run](durable-execution.md#when-the-worker-dies-mid-run). Anything that
has to survive a crash goes in [storage](storage.md), or out as an ordinary
output of a node that finishes.
