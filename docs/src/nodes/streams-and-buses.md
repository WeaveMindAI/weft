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
consumer reads, use `yield_downstream`, which waits until each item is taken.

If you do neither and run far ahead, you hit the cap at 4096 un-taken items and
the emit fails, telling you the three ways out: yield instead of pulse, let the
consumer catch up, or `ctx.set_max_buffered_items("chunk", n)`.

## Consuming a stream

Read the port like any input, then pull:

```rust
let rows: Generator<Row> = ctx.inputs.get("rows")?;

while let Some(row) = rows.next().await? {
    // one row at a time
}
```

Your body starts when the first item arrives, so you are running while the
producer is still producing.

`next()` waits for the next item and gives you `None` once, at a clean end.
`try_next()` is the same without waiting, answering `Item`, `Empty` or
`Finished`. `drain()` takes the lot and waits for the end. `end()` tells you
whether the producer finished or failed, or `None` while it is still open.

The `?` matters. If the producer failed, that is where you find out, rather
than getting a clean end you would mistake for an empty stream. `drain` errors
without handing back the partial list, for the same reason.

Reading the port twice gives you two handles over one stream, sharing a
position, not two copies.

**Your node cannot durably suspend.** A stream cannot be replayed, so
`ctx.await_signal` in a stream consumer is refused. Do the waiting upstream or
downstream.

## Hosting a bus

```rust
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
bus.send(MessageKind::Json, json!({ "text": "hello" }))?;
```

`open_bus` does the whole producer move: makes the bus, emits the marker on
that port, and registers your name. The guard closes the bus when it drops, so
every way out of your function, including a panic, ends the channel instead of
leaving readers parked forever.

## Joining one

```rust
let bus = ctx.join_bus("channel", "translator")?;
bus.wait_for("host")?;

let mut cursor = bus.cursor();
while let Some(msg) = cursor.next().await {
    // ...
}
```

`join_bus` resolves the bus on that input and registers your name, with the
same close-on-drop guard.

For an observer that must **not** close the bus, a debug tap that comes and
goes, use `ctx.bus_from_input("channel")` instead.

Registering is the "I am here and ready" moment. A node with a slow warmup
should hold the bus and register when it is genuinely ready, because that is
what everybody else's `wait_for` is waiting on.

Each reader has its own cursor. Reading does not consume, so ten readers all
see everything.

`wait_for(name)` parks until that name is live, or returns an error if the bus
closed first. You never have to decide whether a name will ever turn up: the
engine watches the run, and when a wait can no longer be satisfied it closes
the bus and every waiting cursor wakes.

## Choosing options

```rust
BusOptions::default()
    .ephemeral(true)
    .window(256)
    .payload(BusPayload::Bytes)
```

| Option | Default | What it decides |
|---|---|---|
| `ephemeral` | `false` | Whether payloads are recorded, or only counts |
| `window` | 64 messages | How far back a reader can reach |
| `payload` | JSON | JSON or bytes. Frozen at creation; sending the other shape is refused |
| `journal_window` | 1 second | How often the record is written |
| `meta` | nothing | Details every reader can read off the marker: a sample rate, dimensions. Keep it small, it rides the marker |

Ephemeral is for a firehose. Video frames should not stall because a reader is
slow, and should not end up in permanent storage either. A slow reader on an
ephemeral bus silently skips to the oldest message still held, because there is
no backpressure: the camera does not wait.

On a journaled bus, a send is refused if the record cannot be written, before
anything is appended. On an ephemeral one it never is, because its record is
notes rather than the data.

In both, who joined and who left is always kept, so nothing a reader needs can
hide in a gap.

## What a send tells you

Failures are values, never silent drops: the bus was closed, you never
registered, you sent the wrong payload shape, or the record is degraded.

## The one thing to remember

A bus lives exactly as long as its worker. It is not rebuilt when a worker
restarts, and a marker resolved afterwards fails saying the bus is unknown.

It is for nodes that are alive together right now. Anything that has to survive
goes in [storage](storage.md), or through a step that ends.
