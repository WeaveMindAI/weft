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
that port, and registers your name, which makes your node a member of the bus
until your body returns ([how a bus ends](#how-a-bus-ends)).

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

`join_bus` resolves the bus on that input and registers your name, which makes
your node a member, exactly as the node that made the bus is one.

If your node only reads the bus (a debug tap, or a node streaming the bus out
to a caller), use `ctx.bus_from_input("channel")` instead. The handle can read
straight away. It registers nothing, so your node is no member, and once it
reads or waits on the bus it no longer keeps it open ([how a bus
ends](#how-a-bus-ends)). To send on it, call
`bus.register("name")` first, which is what
`join_bus` does for you.

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

## Joining the same bus on every run

If your node runs many times and sends on the same bus each time (each turn of
a loop sending on a session's stream, say), join it every time:

```rust
let bus = ctx.join_bus("stream", "turn")?;
bus.send("event", json!(event)).node_err("sending an event")?;
```

When your body returns, your name leaves and the bus stays open for the node
that is still on it, the one that made it. That node, or a `CloseBus` the
program puts after the last firing, ends it (for the pattern, go and read [a
bus that outlives the loop that sends on
it](../language/streams-and-buses.md#a-bus-that-outlives-the-loop-that-sends-on-it)).

If your body pauses and runs again from the top, calling `join_bus` with the
same name gives you the name back, and nobody on the bus sees you leave.
Running again from the top also repeats every `send` your body made before it
paused, so keep in your state how far you got and skip what already went out,
the way [a stream producer that
pauses](durable-execution.md#a-stream-producer-that-pauses) does. A name is
held by one member at a time, so two firings on the bus at once need two
names.

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

Nobody owns a bus. If your node registered a name on it (with `open_bus`,
`pending_bus`, `join_bus`, or `register` on any handle), it is a member until
your body returns. Then, however the body returns (`Ok`, an error, a panic, a
cancel), your name leaves the bus, and that is all that happens to it: the
others on it carry on.

The bus ends once nobody is on it and no node can still join it: finished if
its last member's body returned `Ok` (or if nobody ever joined), failed with
its error if it returned one, panicked or was cancelled. For the whole rule,
including where a node can still join from, go and read [how a bus
ends](../language/streams-and-buses.md#how-a-bus-ends).

So if your node opens a bus and returns at once, the nodes it handed the bus to
still get to join it, so there is no need to `wait_for` them before you
return. A bus you made with
`create_bus` that nobody ever registers on ends on its own too, finished, once
its marker is nowhere a node could still join from.

If your node joins a bus, join it first: call `join_bus` before you read it or
wait on it. Once your node reads or waits without having joined, it no longer
keeps the bus open, so if nobody else is on it the bus can end before you join,
and `join_bus` is refused with `bus is closed; cannot register`.

A pause is not leaving: your name stays on the bus through it, so a bus whose
members are all paused stays open, and your body gets its name back when it
runs again. Dropping a handle is not leaving either, and nor is a task you
spawned still holding one: your names leave when the body returns, and that
task cannot register on a bus after.

If you are done before the others are, `bus.close()` ends the bus now, for
everyone: finished while anybody is still on it. Any handle can do it, so a node that is no member can end
it too, which is all `CloseBus` does. If nobody is on the bus by then, it ends
the way its last member left, failed if that member failed. If the bus already
ended, the end it had stands.

A reader finds out from its cursor. `next` returns `None` once the bus ended
finished, and fails with `the bus ended failed: <why>` once it ended failed, so
the `?` on your read is where your node fails with it instead of taking a
broken stream for a whole one. If your node handles the failure itself instead
of passing it on, match on that error; `bus.end()` says how the bus ended, and
is `None` while it is open. A node answering a caller from a bus ends the
answer with `fail` (go and read [talking to a live caller](live-callers.md)).

If your node stays on a bus while other nodes use it and has nothing to read
on it (it holds a session for the run, and a `CloseBus` downstream says when
the run is done with it), wait for the end instead of opening a reader:

```rust
let bus = ctx.open_bus("stream", BusOptions::default(), "agent").await?;
bus.until_end(&mut state).await?;
```

`until_end` returns once the bus ended finished, and fails with `the bus ended
failed: <why>` once it ended failed, the same as a read. The run can pause
while it waits, so it takes your node's state. It is not a reader: it keeps no
place on the bus, so however many messages go by while your node is paused,
the body run again just waits on, and your node can still open its one reader
on that bus.

If your node waits on a bus it is the last member of (`until_end`, a read, a
`wait_for` on a name that never joined), it keeps waiting: it is still on the
bus, so the bus is open, and a node handed the marker later could still join.
It does not hang for good. Once nothing else in the run can move, weft ends the
bus failed, saying the run was stuck, and your wait fails with that.

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

If the bus ended while your node was paused (another node closed it, say),
your body run again still gets it back from `open_bus`, `pending_bus` or
`join_bus`, ended, with your name back, and nothing new goes
on the bus. Your reader then comes to the end, so a body that reads until the
bus ends returns. Registering on an ended bus is refused only for a name your
node never held there.

A worker that dies is different: its buses can end failed, and each reader's
next read then fails with `the bus ended failed: the worker holding this bus
went away, so messages it carried may be lost`. For when, go and read [when the worker dies
mid-run](durable-execution.md#when-the-worker-dies-mid-run). Anything that
has to survive a crash goes in [storage](storage.md), or out as an ordinary
output of a node that finishes.
