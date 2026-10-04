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

## Waiting on a job a service runs

If your node starts something slow on a provider (a video render, a dubbing
job, a site crawl) and the provider hands back a status address, do not loop
and sleep in your body: that holds a worker for the whole job. Submit the job
inside `ctx.run`, then wait on the status address with `PollEndpoint`:

```rust
use weft::signal::{PollEndpoint, Predicate};

let submitted = ctx.run("submit", || async {
    post_json(&http, &submit_url, &payload, "submitting the render").await
}).await?;
let id = submitted["request_id"].as_str().node_err("the submit answered no request_id")?;

let status = ctx.await_signal(PollEndpoint {
    url: format!("{API}/requests/{id}/status"),
    interval_secs: 5,
    access: Some(weft::primitive::AccessRef::from(&account)),
    filters: vec![
        Predicate::neq("status", "IN_QUEUE"),
        Predicate::neq("status", "IN_PROGRESS"),
    ],
    ..Default::default()
}).await?;
```

The run parks and the worker goes away. Weft checks the address once straight
away (a job that is already done resumes at once), then every `interval_secs`,
and an interval under 5 seconds is refused. The first answer that passes every filter is what
`await_signal` returns, and the checking stops. With no filters, the first
successful answer is the one. An error status or a failed request is not an
answer: it is tried again at the next check and shown on the node. `access`
signs each check with that connection.

If the finished answer is heavy (a crawl's status that embeds its first page
of results), list the fields your node reads next in `carry`, like
`carry: vec!["status".into(), "error".into()]`. The filters still read the
whole answer, but only those fields are handed back, so megabytes of data
never sit in the run's history; read the rest yourself once the wait is over.

When that answer comes, your body replays from the top, so the submit has to
be inside `ctx.run`. Without it the replay would start a second job, and pay
for it.

Write the filters as "not one of the in-flight statuses" rather than "one of
the finished ones", the way the example does. A status you did not expect then
ends the wait, and your code can fail on it, instead of waiting for ever.

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

## When the worker dies mid-step

If the worker goes away while a body is running (a crash, a lost machine),
nobody can tell how much of the body's work already happened, so the next
worker fails the step and says why:

```text
the worker running 'billing' went away while it was running; it was not run
again, because it may have partly happened. Re-run from here once you have
checked what it did.
```

If the node sets `catchErrors` and you wired its `error` output, this failure
goes there like any other, and that branch carries on.

A step's start is written down in the background, as the step begins (go and
read [the journal](../running/the-journal.md#a-run-does-not-wait-for-its-writes)).
If the worker goes away in the moment before that write lands, nothing says the
step ever started, and the next worker runs it as a step that never did. That
moment is one write to the database long.

If the body was waiting on `ctx.await_signal` when the worker went away, it is
not failed: when its answer comes, it replays from the top, and its `ctx.run`
calls give back their saved results instead of doing the work again.

If a step emitted a value before the crash and the step reading it had not
started yet, that value is still delivered and the reading step runs.

## When ctx.run cannot save its result

`ctx.run` writes its result down after the closure finishes. If that write
fails, the action has happened but nothing recorded it, and the step fails
with:

```text
could not save result of 'charge the card' for node 'billing': <error>. The
action may already have happened; inspect this run before repeating it.
```

Two things are deliberately **not** wrapped, because they are already safe to
repeat on a replay: `ctx.tag_execution` keeps its place in the order, and
`ctx.stop_tagged` finds its targets already gone.

## What a resume actually does

The new worker fetches your program by hash, reads the journal to see where the
run had got to, and carries on from there: a step that was running is failed,
a step that was waiting replays, and every step whose inputs are ready starts.

## When the worker stays

A run holding a live caller, or a bus with nodes still talking on it, keeps its
worker up. Parking there is an ordinary `await` in the same process, not a
death and a rebuild, so nothing replays.

That is why the refusal above suggests a bus for a node that has to stay warm:
if two nodes are alive together and talking, they do not need durability
between them.
