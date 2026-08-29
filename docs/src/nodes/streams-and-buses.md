# Streams and buses in Rust

The graph-level story is in [Live channels](../language/live-channels.md).
This page is the node author's side.

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

An open connection, a paging cursor, a decoder's state: all of it is just
locals across the loop, because the producer body stays alive for the whole
stream.

If your body returns an error instead, the stream closes as **failed**, and
the consumer's pull gets your error rather than a clean end.

### Yield or pulse

```rust
ctx.yield_downstream(output).await?;    // returns once the item was pulled
ctx.pulse_downstream(output).await?;    // returns immediately, item buffers
```

`yield_downstream` is lock step. Your body waits on each item until the
consumer takes it, so the buffer never grows past one and you always know your
items landed.

`pulse_downstream` runs ahead. Items buffer on the edge, bounded at 4096 by
default, and an emission past the bound **fails your node loudly** rather than
growing until the pod runs out of memory.

A producer that deliberately runs far ahead raises its own bound:

```rust
ctx.set_max_buffered_items("rows", 100_000)?;
```

Only legal on a `Generator` output, refused for 0, and it applies to the
emissions that follow the call. Before or between emissions both work.

### Which one to use

Ask what should happen if the consumer stops early.

Fire-and-forget items are **dropped** harmlessly when the consumer finishes,
which is usually what you want for a stream the consumer is allowed to abandon.

A yielded item whose consumer finishes without taking it **fails your body**,
which is what you want when your producer must know its items landed.

Getting this backwards produces a confusing failure at the end of a run that
otherwise worked.

### Ending early

`ctx.close_port("rows").await?` ends the stream, legal after any number of
yields. See [Explicit closure](values-and-emission.md#explicit-closure).

## Consuming a stream

The consumer reads the stream from the input bag like any other input. Its
node fires once, on the first item, and pulls the rest itself.

```rust
let rows = ctx.inputs.get::<Generator<Row>>("rows")?;
while let Some(row) = rows.next().await? {
    // one item at a time
}
```

On the handle:

| Call | Answers |
|---|---|
| `next()` | waiting take: `Some(item)`, `None` on a clean end, the producer's error on a failed one |
| `try_next()` | no wait; distinguishes "nothing buffered yet" from "finished" |
| `drain()` | the whole stream as a `Vec`, erroring on a failed end rather than handing back a truncated list |
| `end()` | the end marker, once the producer's side ended |

A pull can also become impossible to satisfy, when every remaining node is
waiting on one of the others. The engine's stuck check spots that and fails the
stream, so it reaches you through `?` like any producer failure rather than
hanging.

An **empty stream still runs your node**. A producer that closes without
yielding delivers a stream whose first `next()` answers `None`, so your
post-loop code runs the same over zero rows as over one.

`yield_downstream` also works on an ordinary port, where it waits for the
consumer to be dispatched:
[Waiting for the value to be taken](values-and-emission.md#waiting-for-the-value-to-be-taken).

## Buses

One node creates the channel and emits its marker; others resolve the marker
and exchange messages.

```rust
// Producer. The returned guard closes the bus when dropped.
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
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

The producer ritual (create, emit the marker, register a name, close on
**every** exit path) is one call, and so is the consuming side. A bus left open
parks its readers forever, so the API closes it for you.

### `BusOptions`

Declared at creation, read back by every consumer off the handle or the
marker.

| Option | Meaning |
|---|---|
| `payload` | `Json` (default) for chat-shaped traffic, or `Bytes` for media frames, raw end to end with no base64 between nodes |
| `meta` | creator-declared stream metadata, such as sample rate and encoding, read via `bus.meta()` |
| `ephemeral` | keeps payloads out of the journal entirely; a consumer that falls behind resumes at the oldest frame still in the window |
| `window` | how many frames the bus keeps for a consumer that falls behind, 64 by default |
| `journal_window` | how coarsely the trail is recorded: one row per bus per window, one second by default |

`payload` is frozen at creation and the wrong shape is refused loudly.
`journal_window` affects only how the trail is stored, never what travels the
bus: what a window row contains is in
[The journal](../running/the-journal.md#what-the-journal-costs).

## Keep bus work on your own task

Nothing enforces this one, so you have to hold it yourself. Do all of a bus's
reads and waits directly in your node body, and never move a bus handle or
cursor into a `tokio::spawn`ed background task.

The engine decides "every node is stuck, close the buses" by tracking whether
each node execution is waiting or working, and it assumes those waits happen on
the node's own task. A wait on a task you spawned is invisible to that
accounting, so the engine can wrongly tear down a live conversation, or hang.

If you need concurrent work, model it as another node and exchange with it over
the bus.
