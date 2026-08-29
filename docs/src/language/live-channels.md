# Live channels: streams and buses

Most wires in a weft program carry one value once. Two port types carry
something that keeps arriving, and they answer different questions.

| | `Generator[T]` | `Bus` |
|---|---|---|
| Direction | one way, producer to consumer | any participant to any participant |
| Readers | exactly one | any number |
| Ends | when the producer's body returns | when the creator closes it |
| Typed payload | yes, every item checked against `T` | no, the channel declares its shape |
| Crosses a group boundary | no | yes |

A stream is a sequence someone is producing. A bus is a conversation between
things that are alive at the same time.

## Streams

A `Generator[T]` port accepts being emitted into repeatedly. Each emission is
one item.

```weft
rows = ReadCsv() -> (rows: Generator[Row])
rows.path = @asset("data/big.csv", Blob)

summarise = SummariseRows
summarise.rows = rows.rows
```

The consumer fires **once**, on the first item, and pulls the rest itself at
its own pace.

Or a `Loop` names the port in `over` and pulls one item per iteration:

```weft
each = Loop(rows: Generator[Row]) -> (kept: List[Row | Null]) {
  over: ["rows"]
  ...
}
each.rows = reader.rows
```

### Why a stream instead of a list

The visible difference is when the second stage starts.

With `List[Row]`, the producer builds the whole list, emits it, and only then
does anything downstream begin. With `Generator[Row]`, the consumer starts on
item one while the producer is still working on item four hundred.

For a ten-row query nobody cares. For a query that takes a minute to page
through, or an LLM streaming tokens, the first result lands in seconds instead
of after the whole thing finishes.

If the whole collection exists up front, use `List[T]`. If the items appear
over time, use `Generator[T]`.

### The rules, and why each exists

- **Exactly one producer, exactly one consumer.** A stream has one taker.
  Broadcasting is a bus's job.
- **A stream cannot cross a group boundary**, sit inside a container, or be
  carried between loop iterations. A stream is a live handle, and those three
  operations would all mean holding it somewhere its producer cannot reach.
- **A `Generator` input must be required.** An unwired stream has no meaning.
- **No literal.** The value is minted at run time.

### Backpressure

A producer that emits without waiting runs ahead of its consumer, and the
un-taken items buffer on the edge. That buffer is bounded, 4096 items by
default, and an emission past the bound **fails the producer loudly** rather
than growing until the pod runs out of memory. A producer that means to run far
ahead raises its own bound. A producer that
yields in lock step waits for each item to be taken, so its buffer never grows
past one.

### The early-termination edge

A lock-step producer is parked holding an item until the consumer takes it. If
the consumer stops early, that item can never be taken, and the producer fails
loudly, failing the execution.

So when the **consumer** decides how much of the stream to use, the producer
must emit fire-and-forget (leftovers are dropped) or be the side that decides
when to stop. The same rule appears in [Loops](loops.md).

### An empty stream still runs the consumer

A producer that closes without yielding anything delivers a stream whose first
pull answers "finished". The consumer still fires, its loop runs zero times,
and whatever comes after the loop runs normally.

## Buses

A bus is an in-process channel between nodes that are alive at the same time.
One node creates it and emits a marker on a `Bus`-typed output; downstream
nodes resolve that marker and exchange messages.

```weft
host = ConversationHost() -> (channel: Bus)

guest = ConversationGuest
guest.channel = host.channel

observer = ConversationTap
observer.channel = host.channel
```

Three nodes on one channel, all three talking, which is what a stream cannot
do.

### What a bus carries

The creator declares the channel's shape once, and every participant reads it
back off the handle:

- **payload**: `Json` for chat-shaped traffic, or `Bytes` for media frames,
  which travel raw end to end with no base64 in between. Frozen at creation.
- **meta**: whatever a consumer needs to know before the first message, such as
  an audio stream's sample rate and encoding, instead of every message
  repeating it.
- **ephemeral**: keeps payloads out of the journal entirely, for bytes that are
  transient by nature.
- **window**: how many frames the bus keeps for a consumer that falls behind,
  64 by default. Raise it in the node that creates the bus.
- **journal_window**: how coarsely the trail is recorded, one row per bus per
  window, one second by default. What travels the bus is untouched.

### Reading a bus

Every reader has its own position, so a responder and an observer both see
every message. Positions are absolute over the channel's whole life, so a saved
one keeps naming the same message as the window moves.

When a bus is closed, every reader's pull ends cleanly. Closing is therefore
the end-of-stream signal, and a producer that forgets to close leaves its
readers parked forever, which is why the node-side API closes on every exit
path for you.

### Buses and the graph

A bus is the answer to "these two things need to talk while both are running",
which the pulse model does not express on its own: a pulse is one delivery in
one direction.

The canonical shape is a parallel loop that launches N agents, gathers their
bus markers as `List[Bus | Null]`, and hands that list to a coordinator that
talks to all of them while they work. See
[a loop is a launcher, not an owner](loops.md#a-loop-is-a-launcher-not-an-owner).

## Which one do I want

Ask what happens when a second reader appears.

If a second reader would be a bug (each item must be handled once), it is a
stream. If a second reader is fine or desirable (everyone should see the
message), it is a bus.

Ask who decides when it ends.

If the producer decides, by finishing, it is a stream. If the conversation
ends when the participants are done, it is a bus.

The Rust side of both is in
[Streams and buses in Rust](../nodes/streams-and-buses.md).
