# Surviving a restart

A node can park for a week waiting for somebody to answer, and cost nothing
while it waits. The trick is that the worker goes away and a fresh one picks
the run back up.

Which means **your body runs again from the top.**

Read that twice, because everything on this page follows from it. When the
answer arrives, a new worker replays your function from line one. It does not
resume inside your `await`. It starts over.

## What is remembered and what is not

| | On a replay |
|---|---|
| `ctx.await_signal` that was answered | Returns the answer straight away, without waiting |
| `ctx.run("name", ...)` that finished | Returns what it returned last time, without running the closure |
| Anything else in your body | **Runs again** |
| Anything you emitted | Runs again, which is why emitting before an await is refused |

So the shape is: do the work, wrap anything expensive or irreversible in
`ctx.run`, wait, then emit.

```rust
let quote = ctx.run("fetch quote", || async {
    let body = ctx.http().get(url).send().await.node_err("fetching the quote")?;
    Ok(json!(body.text().await.node_err("reading the quote")?))
}).await?;

let answer = ctx.await_signal(Form::approval("Send this?")).await?;

ctx.pulse_downstream(NodeOutput::new().set("approved", answer)).await
```

Without `ctx.run`, that fetch happens again every time somebody takes a day to
answer.

## Two things weft refuses

**Emitting, then awaiting.** A replay would emit again, and the value already
went downstream.

```text
node 'review' called await_signal after emitting or closing an output port; a
node that touches a port then durably suspends would touch it again on replay.
Emit and close after all awaits, or (for a co-alive node) stay warm with
bus.recv() instead of await_signal.
```

**Awaiting in a node that reads a stream.** A replay cannot re-pull items that
were already pulled.

```text
node 'grade' has a Generator input and called await_signal; a stream consumer's
body cannot durably suspend. Do the waiting upstream or downstream of the
stream consumer.
```

## ctx.run is keyed on order, not on the name

The name is for reading logs. What the runtime matches on is **which call this
was**, counting every `ctx.run` and every `ctx.await_signal` in your body.

So two `ctx.run` calls with the same name in a stable order are fine, and two
with perfect names in an order that changes between replays are not:

```text
ctx.run('fetch quote') call_index mismatch (counter=2, journal=1). This means
the node body's call order changed between replays. Wrap any non-deterministic
logic in ctx.run.
```

If your body can take a different path on a replay, whatever decides that path
goes inside a `ctx.run` so the decision is remembered rather than remade.

## The guarantee, exactly

**At least once.** A thing can happen twice, and here is precisely when.

`ctx.run` writes its result down after the closure finishes. If the worker dies
in between, the action happened and nothing recorded it, so the replay does it
again. weft cannot close that window from the inside, and it says so rather
than pretending:

```text
could not save result of 'charge the card' for node 'billing': <error>. The
action may already have happened; inspect this run before repeating it.
```

The same applies one level up. A crash between a node emitting and its consumer
starting re-delivers the value, so the consumer runs again.

What this means for you: if the thing you are doing matters and the service on
the other end has an idempotency key, use it. That is what it is for.

Two things are deliberately **not** wrapped, because they are already safe to
repeat: `ctx.tag_execution` keeps its place in the order, and `ctx.stop_tagged`
finds its targets already gone.

## What a resume actually does

The new worker fetches your program by hash, folds the journal back into a
picture of where the run had got to, and re-dispatches every node that is
ready. Each body starts from the top with its answered awaits and finished
`ctx.run`s loaded, so they replay in order and everything else runs.

A step whose completion was safely written down does not run again at all.

## When the worker stays

A run holding a live caller, or a bus with nodes still talking on it, keeps its
worker up. Parking there is an ordinary `await` in the same process, not a
death and a rebuild, so nothing replays.

That is why the refusal above suggests a bus for a node that has to stay warm:
if two nodes are alive together and talking, they do not need durability
between them.
