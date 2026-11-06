# Streams and buses

Most wires carry one value. Two things carry more than one, and they are for
different jobs.

A **stream** is one node handing items to one other node, in order, until it
ends: the numbers `Range` counts out, or the messages a `Socket` hands on as
its caller sends them.

A **bus** is several nodes that are all running at once talking to each other:
a model writing its answer, a `Stream` passing the words to the caller, and
a voice reading them aloud as they come.

## Streams

A stream is a port typed `Generator[T]`. `Range` hands out numbers that way, and
a loop over it takes one per iteration:

```weft
numbers = Range { to: 1000 }
double = Loop(n: Generator[Number]) -> (doubled: List[Number | Null]) {
  over: ["n"]
  times = ExecPython(n: Number) -> (out: Number) {
    n: self.n
    code: "return {'out': n * 2}"
  }
  self.doubled = times.out
}
double.n = numbers.values
```

The `| Null` is required, because an iteration that sends nothing leaves a
null in its place in the list. For the other kinds of loop port, go and read
[the four kinds of port](loops.md#the-four-kinds-of-port).

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
ask = Route -> (body: { question: String }) { path: "ask", method: "POST" }
answer = LlmStream {
  prompt: ask.body.question
  provider: model.provider
}
toCaller = Stream { bus: answer.stream }
voice = ElevenLabsSpeak {
  account: eleven.access
  text: answer.stream
  voice: "JBFqnCBsd6RMkjVDRZzb"
}
```

`LlmStream` makes the bus and sends the model's answer on it as it is
written. `Stream` passes it to the caller, and at the same time
`ElevenLabsSpeak` turns the same words into audio on its own `audio` bus (its
`voice` is an ElevenLabs voice id). `model` and `eleven` are the model and
the ElevenLabs connection, set up elsewhere in the program.

The marker is an ordinary value. It crosses group boundaries, goes into loop
bodies, sits inside records, and travels like anything else. Only the live
channel is special, and it lives in the worker rather than on the wire.

Every participant registers a name, which is the stamp on everything it sends
and what other nodes wait for. Registering is the "I am here and ready" moment,
so a node with a slow warmup registers when it is genuinely ready rather than
when it starts. Registering is also what makes a node a member of the bus,
which decides when the bus ends (below).

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

A node that streams lets you pick the mode with two settings:
`journalEphemeral` (on keeps payloads out of the record, off by default) and
`journalWindowMs` (a whole number from 100 to 60000, 1000 by default). Every
message sent within one window lands in the same row of the record. A shorter
window shows messages in the inspector sooner and gives a row per message on a
slow stream. A longer one writes fewer rows for a fast stream (audio frames,
tokens), and the inspector trails the live stream by up to that long. A row
also closes early once it holds 2 MB, and at once when the bus ends or the run
pauses. `Route` and `Socket` have both settings,
and so do `LlmStream`, `ElevenLabsTranscribe` and `PostgresWatchQuery`. Other
nodes have them only if their author turned them on.

The window is 64 messages either way. What is always kept, in both modes, is
who joined and who left, so nothing a reader needs can disappear into a gap.

The record exists so you can read the conversation in the inspector
afterwards, and so a paused run gets its buses back: the worker that picks the
run up opens each one again from it (below).

### How a bus ends

Picture a reader at the end of a model's answer. Did the model finish, or did
it stop halfway through a sentence? A bus says which.

Nobody owns a bus. Every node that registered a name on it, the one that made
it and every one that joined it, is a member until it is done, and a member
paused on a wait is still on it. When a member is done, its name leaves and
nothing else happens: the others carry on.

Once nobody is on it, the bus ends as soon as no node can still join it. A node
can join only through the bus's marker, so the bus stays open while its marker
is still somewhere a node could get it:

- on a wire, on its way to a node that has not started yet, including one still
  waiting on its other inputs;
- in a loop that has not ended: in the value every iteration gets, a value
  carried to the next iteration, the list the loop goes over, or a list the
  loop is gathering, which it hands on to the node after it when it ends;
- in the input of a node that is running, or paused on a wait, and has not used
  the bus yet. A node uses a bus by registering on it, reading it, or calling
  `wait_for` or `until_end` on it, so a node that only passes the marker on
  keeps the bus open until it returns and hands it on;
- with the node that made it, until it sent the marker out.

A node that was skipped never gets the marker, so it keeps nothing open. A
node that only reads, like `Debug` or `Stream`, registers nothing, so it is no
member. Once it starts reading it stops keeping the bus open, otherwise a
reader waiting for the end would hold the bus open forever.

When the bus ends, it ends one of two ways:

- **finished**, when its last member's work completed, or when nobody ever
  joined it;
- **failed**, with the reason, when its last member failed or was cancelled.

So a stream whose one producer dies halfway ends failed, and its readers know
it is incomplete. A member that fails while others are still on the bus just
leaves, because one of them may be how the conversation recovers.

Any node can also end a bus on purpose: a member done talking before it is
done working, or a `CloseBus`. If somebody is still on the bus, it ends
finished. If nobody is, it ends the way its last member left, so a stream
whose only producer failed still reads as failed when a `CloseBus` runs after. weft ends
a bus failed itself when the run got stuck, when the run ended while the bus
was still open, and when a node was still running as its worker died.

Each reader decides what a failed end means for it. `Stream` fails the answer
to its caller ([what the caller
sees](triggers-and-routes.md#when-a-run-ends-before-it-answers)), even on a
route whose `whenChunkFails` is `drop_chunk`, because that setting skips one
message and a failed bus has no next one to go on to. `Debug` logs
every message that came, then fails naming the reason, and the bus's panel in the inspector ends with
`* the bus failed here: <reason>` instead of `* the bus closed here`.

If a producer fails before its first piece, it may not have opened its bus
yet. For what the program can do then, go and read [when a node
fails](how-a-program-runs.md#when-a-node-fails).

### A bus that outlives the loop that sends on it

Picture a node holding a session with a coding agent for the whole run. It
opens a bus and waits until the bus ends, and a loop asks the agent one thing
per turn, each turn joining the bus to put what the agent does on it. Every
turn is done long before the session is, and its name leaves the bus then, but
the session's node is still on it, so the bus stays open for the next turn
(for how a node that joins on every turn is written, go and read [joining the
same bus on every run](../nodes/streams-and-buses.md#joining-the-same-bus-on-every-run)).
What ends the bus is a `CloseBus` after the loop:

```weft
end = CloseBus { bus: session.stream }
end._should_flow = turns.replies
```

Here `session` is the node that opened the bus and `turns` is the loop.
`turns.replies` comes out once the last turn has written its reply, and only a
literal `false` on `_should_flow` stops a node, so `end` runs after the last
reply. Make sure whatever sends on the bus inside a turn feeds `replies`:
work that does not feed it can still be running when the list comes out. `end`
ends the bus as finished, so every reader comes to the end, and the session's
node, which reads until then, returns. If the session failed and nothing was on
the bus after it (no turn still on it, and none joining later), nobody is on the
bus when `end` runs, so `end` ends it the way the session left it, failed, and
the failure still shows.

If you leave `CloseBus` out, the run fails instead of finishing. Once the last
turn is done, the session's node is the last member, waiting on a bus that
only its own leaving could end, and nothing else in the run moves, so weft
finds the run stuck and ends the bus failed. The session's node then fails
with `node failed: the bus ended failed: the run was stuck: every node on this
bus waited on another, and nothing could end the wait`, and so does the run: a
node that already opened a bus cannot hand its failure to `_error`.

### A bus pauses with its run

A bus is memory in the worker running the run, and it does not stop the run
from pausing. A node can wait a week on a question while a bus between two
other nodes stays open: once every node still running waits, the run pauses,
buses and all, and the worker lets go. Nobody on the bus sees anybody leave.

When the run picks up, the new worker opens every bus that was open again,
with the same id, so every marker still names it, and its messages carry on
numbering from where they stopped. A journaled bus comes back with its last
JSON messages, as many as its window holds, so a reader can still reach back;
the record keeps a message over 100 KB trimmed, so that message and every one
before it are left out. An
ephemeral bus, or one that carries bytes, comes back empty. A reader that was
waiting for the next message had read everything and loses nothing. One that
stopped to wait on something else carries on where it stopped, and if the
messages it had not read did not come back, its first read fails, saying
so. A node that must not stop reading at all holds the run
instead (`ctx.hold()`), which keeps the run from pausing while it reads.

A bus that had already ended comes back too, ended the way it ended, and
keeps the same last messages an open one would. A node handed its marker
after the pause reads those messages and comes to the same end, finished or
failed, that it would have reached before the pause.

A worker that dies is different, because the last messages it carried may
never have reached the record. If a durable run carries on in another worker
and any node was still running when the first one went, each bus that was
open ends failed, with `the worker holding this bus went away, so messages it
carried may be lost`.

A seeded run cannot reuse a node that emitted a bus, since that bus belonged
to the earlier run: weft leaves the producer and its readers out rather than
handing you a marker pointing at nothing.

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
| To survive its worker dying | Neither. Use storage, or a node that ends |

For the Rust on both sides, go and read
[streams and buses in Rust](../nodes/streams-and-buses.md).
