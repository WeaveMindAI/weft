# Streams and buses in Rust

To send items while your node is still working, emit them on a
`Generator[T]` output. For choosing between streams and buses when connecting
nodes, read [Live channels](../language/live-channels.md).

## Producing a stream

A `Generator[T]` output accepts repeated emissions. Emit items with the call
you already use, keep your state in ordinary local variables, and the stream
ends when your body returns.

```rust
// metadata.json: { "name": "rows", "type": "Generator[Row]" }

for row in read_rows(&file) {
    if keep(&row) {
        ctx.yield_downstream(
            NodeOutput::new().set("rows",
                serde_json::to_value(row).node_err("encoding the row")?)
        ).await?;
    }
}
// body returns: the engine closes the stream
```

If your body returns an error instead, the stream closes as **failed**, and
the consumer's pull gets your error rather than a clean end.

### Yield or pulse

```rust
ctx.yield_downstream(output).await?;    // returns once the item was pulled
ctx.pulse_downstream(output).await?;    // returns immediately, item buffers
```

`yield_downstream` is lock step. Your body waits on each item until the
consumer takes it, so a producer yielding one item at a time keeps at most
one untaken item per connection. Taking an item does not confirm that the
consumer processed it successfully.

`pulse_downstream` lets the producer continue while items wait for the
consumer. Each connection buffers up to 4096 untaken items by default;
exceeding that limit fails the producer.

If your producer needs to buffer more than 4096 items, raise its limit:

```rust
ctx.set_max_buffered_items("rows", 100_000)?;
```

Call this on a `Generator` output before or between emissions. The limit
must be greater than zero and applies to subsequent emissions.

### Which one to use

Ask what should happen if the consumer stops early.

With `pulse_downstream`, queued items are dropped when the consumer finishes.
Use it when the consumer may abandon the remaining items; the producer can
still continue doing work.

A yielded item whose consumer finishes without taking it **fails your body**,
which is what you want when your producer must know its items landed.

### Ending early

To end the stream before your body returns, call
`ctx.close_port("rows").await?`, even if you have emitted no items. For closing
other outputs, read [Explicit closure](values-and-emission.md#explicit-closure).

## Consuming a stream

The consumer reads the stream from the input bag. It runs once, after all
its wired inputs are ready. The stream supplies either its first item or
its closure; the consumer pulls subsequent items itself.

```rust
let rows = ctx.inputs.get::<Generator<Row>>("rows")?;
while let Some(row) = rows.next().await? {
    // one item at a time
}
```

On the handle:

| Call | Answers |
|---|---|
| `next()` | Waits for the next item; returns `Some(item)`, `None` when the stream ends cleanly, or an error if it fails |
| `try_next()` | Returns `TryNext::Item(item)`, `TryNext::Empty` while open with no buffered item, or `TryNext::Finished`; returns an error if the stream fails |
| `drain()` | the whole stream as a `Vec`, erroring on a failed end rather than handing back a truncated list |
| `end()` | `None` while open; otherwise `Finished` or `Failed(error)`, even if buffered items remain to be read |

A pull can also become impossible to satisfy, when every remaining node is
waiting on one of the others. The engine's stuck check spots that and fails the
stream, so it reaches you through `?` like any producer failure rather than
hanging.

An **empty stream still runs your node**. A producer that closes without
yielding delivers a stream whose first `next()` answers `None`, so your
post-loop code runs the same over zero rows as over one.

To wait until an ordinary port's consumer starts, read
[Waiting for the value to be taken](values-and-emission.md#waiting-for-the-value-to-be-taken).

## Buses

One node creates the channel and emits its marker; others resolve the marker
and exchange messages.

```rust
// Producer. The returned guard closes the bus when dropped.
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
bus.wait_for("guest").await.node_err("waiting for guest")?;
bus.send("msg", json!("hello")).node_err("sending on the bus")?;
drop(bus);   // the close IS the end-of-stream signal

// Consumer that participates: registers, and closes on exit.
let bus = ctx.join_bus("channel", "guest")?;
let mut cursor = bus.cursor();
while let Some((from, value)) =
    cursor.next_json("msg").await.node_err("reading the bus")? {
    // ...
}

// Observer that must NOT close the bus (a debug tap).
let bus = ctx.bus_from_input("channel")?;
```

`open_bus` creates the bus, registers the host and emits its handle.
The host waits for the guest to register before sending the message.
Without that wait, the message could be sent before the guest is listening.

Both `open_bus` and `join_bus` return a guard that closes the whole bus when
dropped. Use `bus_from_input` for an observer whose departure should not end
the conversation.

### Where reading starts

A registered participant's `cursor()` starts at its own join event, so it
can read messages sent after joining even if it creates the cursor later.
An observer's `cursor()` starts at the current end of the bus.

Use `cursor_from_start()` to read from the earliest retained entry, or
`cursor_at(offset)` for a specific absolute position. Neither retrieves
evicted messages from the journal. In both journaled and ephemeral mode,
a reader behind the retained window skips to the oldest entry still in
memory.

### `BusOptions`

Declare these when creating the bus. Consumers read its settings through
the resolved handle.

| Option | Meaning |
|---|---|
| `payload` | JSON values (the default) for chat-shaped traffic, or raw bytes for media frames, raw end to end with no base64 between nodes |
| `meta` | creator-declared stream metadata, such as sample rate and encoding, read via `bus.meta()` |
| `ephemeral` | keeps payloads out of the journal; metadata is still recorded |
| `window` | target number of retained messages, 64 by default; journaled messages awaiting persistence can exceed it |
| `journal_window` | Groups messages into journal rows over this interval; defaults to one second |

Choose JSON or bytes when creating the bus; sending the other kind returns
an error. `journal_window` controls how messages are grouped for storage.
For the contents of those journal rows, read
[The journal](../running/the-journal.md#what-is-in-there).

## Keep bus work on your own task

Keep bus reads and waits directly in your node body. Do not move a bus handle or
cursor into a `tokio::spawn`ed background task.

The engine detects stuck conversations by tracking whether each node is
waiting or working. A background task using the same handle reports its
waits as belonging to your node. The engine can then mistake a waiting
background task for a waiting node and close a conversation while the node
is still working.

If you need concurrent work, model it as another node and exchange with it over
the bus.
