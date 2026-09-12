# Live channels: streams and buses

Use a stream when one step produces items for another to work through. Use a
bus when several running steps need to talk to each other.

| | `Generator[T]` | `Bus` |
|---|---|---|
| Data | Items of type `T`, in order | JSON messages or byte frames |
| Readers | One | Several, each with its own position |
| Direction | Producer to consumer | Anyone to anyone |
| End | The producer closes it or finishes | A participant closes it |
| Across a group boundary | Only through a loop's `over` | Yes |

## Process items as they arrive

`Range` produces numbers one at a time, and this loop doubles each one as it
turns up rather than waiting for the whole list:

```weft
numbers = Range { to: 5 }
double = Loop(values: Generator[Number]) -> (results: List[Number | Null]) {
  over: ["values"]
  step = ExecPython(n: Number) -> (out: Number) {
    n: self.values
    code: "return {'out': n * 2}"
  }
  self.results = step.out
}
double.values = numbers.values
show = Debug { data: double.results }
```

A step can also read a stream directly, inside its own code. It runs once and
pulls items as they come. An empty stream still lets it run and see that
nothing arrived.

If you already have the whole collection, use `List[T]`. A stream is for when
producing and processing should overlap.

## Where a stream can go

One producer, one consumer, and that is it. A connected stream input has to be
required with no default, and you cannot write a stream down as a value or put
one inside a list or a record.

Producer and consumer have to be in the same scope. An ordinary group will not
pass a stream through its boundary. A loop can take one through an input named
in `over`, as above, but cannot carry it between iterations or emit one.

For the loop rules, read
[Looping over a stream](loops.md#looping-over-a-stream).

## When the consumer falls behind

A producer either waits for each item to be taken or lets items pile up in a
buffer, which holds 4096 untaken items per connection by default. Go past that
and the producer fails. Waiting for an item to be taken only means the consumer
received it, not that it finished with it.

### Stopping early can fail the run

`Range` waits for each number to be taken. Stop its consumer while a number is
still waiting and that delivery fails, which fails the run. This catches people
using `max_iters` or `self.done` to end a loop before the range runs out, and
the fix is to make the range produce only what you need.

A producer that does not wait behaves differently: leftovers are dropped when
the consumer finishes, and it carries on working. Which of the two you get is
the step author's choice, in
[Yield or pulse](../nodes/streams-and-buses.md#yield-or-pulse).

## Let running steps talk

A bus lets one step send a message while another is still working, so a
coordinator can go back and forth with several agents instead of waiting for
one final answer from each.

One step creates the bus and emits the handle on a `Bus` output. Other steps
receive that handle down ordinary arrows and can then send and receive on it.
A handle can cross a group boundary, and a loop can gather handles into
`List[Bus | Null]`.

The catalog's `LlmStream` uses one for its text updates. For two custom steps
talking to each other, see the
[bus chat fixture](https://github.com/WeaveMindAI/weft/tree/mvp/crates/weft-e2e/fixtures/bus_chat).

### Wait for the other one to join

Handing a step the handle does not mean it is listening yet. If a message has
to be received, wait for that participant to join before sending it.

Each reader has its own position, and the bus keeps roughly the last 64
messages. A reader that falls behind that window skips whatever is no longer
held, and it will not go and fetch them from the journal afterwards. If every
item must be seen exactly once, that is a stream, not a bus.

### Ending it

`open_bus` and `join_bus` hand back a guard that closes the whole bus when it
drops, including when the step exits with an error. Closing tells the readers
nothing more is coming. For an observer that should leave without closing it,
use `bus_from_input`.

Whether messages carry JSON or bytes, and what gets journaled, is the step
author's choice. For that and the reading APIs, read
[Streams and buses in Rust](../nodes/streams-and-buses.md).
