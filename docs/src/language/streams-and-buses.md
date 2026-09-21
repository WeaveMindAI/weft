# Streams and buses

Most wires carry one value. Two things carry more than one, and they are for
different jobs.

A **stream** is one node handing items to one other node, in order, until it
ends. A model writing its answer a word at a time.

A **bus** is several nodes that are all running at once talking to each other.
A transcriber, a translator and a display, each doing its part while the audio
keeps arriving.

## Streams

Write the type on the port and you have one:

```weft
rows = PostgresStreamQuery -> (rows: Generator[JsonDict]) { ... }
grade = GradeEachRow { rows: rows.rows }
```

Items travel as ordinary pulses, each checked against the element type. The
consumer starts when the first item arrives and pulls the rest as it goes, so
it is running while the producer is still producing.

**A closure on a stream is its end, not a skip.** This is the one place where a
closed wire does not make the consumer skip. It runs, sees the stream is over,
and acts on it. A stream that carried nothing is an empty stream, which is a
result, not an absence: zero rows has to behave like one row, not like a branch
that never happened.

If the producer failed, the consumer finds out through an error rather than a
clean end, so nothing mistakes a broken stream for an empty one.

### What a stream will not do

| | Why |
|---|---|
| Feed two consumers | Two readers on one stream is a question with no answer: do both see every item, or do they race? Fanning out is what a bus is for |
| Cross a group boundary | It is a live edge between two running nodes, so both ends have to be in the same scope. A loop takes one through its own `over` and nowhere else |
| Be optional, or have a default | There is no such thing as an empty stream sitting unwired. A stream input is required |
| Sit inside a list, dict, record or union | It is a port type, not a value |
| Be a loop's `carry` | A carried value goes through the journal as JSON between iterations, and a stream is a live thing |
| Be stored, cast, or turned into text | Same reason |

A producer that runs far ahead of its consumer will fill the buffer: 4096
un-taken items, and then it fails and tells you the three ways out, which are
to yield instead of pulse so each item waits to be taken, to let the consumer
catch up, or to raise the cap.

## Buses

A bus is a channel any number of nodes join. One node makes it and emits a
marker, and anything that gets the marker can join.

```weft
mic = MicrophoneCapture -> (audio: Bus)
transcribe = Transcribe { audio: mic.audio }
translate = Translate { audio: mic.audio }
```

The marker is an ordinary value. It crosses group boundaries, goes into loop
bodies, sits inside records, and travels like anything else. Only the live
channel is special, and it lives in the worker rather than on the wire.

Every participant registers a name, which is the stamp on everything it sends
and what other nodes wait for. Registering is the "I am here and ready" moment,
so a node with a slow warmup holds the bus and registers when it is genuinely
ready rather than when it starts.

Each reader has its own position. Reading does not consume, so ten readers all
see everything.

### Two modes

| | Journaled, the default | Ephemeral |
|---|---|---|
| What the record keeps | Every message | Counts per sender, and nothing else |
| A reader that falls behind | Can reach back as far as the window | Skips to the oldest message still held |
| If the record cannot be written | The send is refused before anything is appended | Nothing is refused |

Ephemeral is for a firehose. Video frames should not stall because something
downstream is slow, and they should not end up in permanent storage either.

The window is 64 messages either way. What is always kept, in both modes, is
who joined and who left, so nothing a reader needs can disappear into a gap.

The record exists so you can read the conversation in the inspector afterwards.
It is not what rebuilds a bus, because nothing rebuilds a bus.

### A bus lives exactly as long as its worker

It is memory in a process. If that worker goes away, the bus goes with it, and
a marker resolved afterwards fails saying the bus is unknown.

That is fine for what it is for, which is nodes that are alive together right
now. It is why a run that used a bus cannot be partly reused by a seeded run:
weft leaves the producer and its readers out rather than handing you a marker
pointing at nothing.

A durable wait is a different thing and the two coexist. A node can park on a
question for a week while a bus somewhere else stays up.

### What a bus does not check

Payloads. A stream checks every item against its element type; a bus carries
whatever you send. You pick JSON or bytes when you make it and that is frozen,
but what is inside is between the nodes talking.

## Which one

| You want | Use |
|---|---|
| One node's output, arriving in pieces | A stream |
| Several live nodes talking to each other | A bus |
| To fan one thing out to several readers | A bus |
| Ordering and type checking on every item | A stream |
| To survive the worker restarting | Neither. Use storage, or a step that ends |

For the Rust on both sides, go and read
[streams and buses in Rust](../nodes/streams-and-buses.md).
