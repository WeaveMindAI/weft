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
| `ctx.await_signal` that was given up | Fails the same way again, without waiting |
| `ctx.run("name", ...)` that finished | Returns what it returned last time, without running the closure |
| An emit or close on a port this firing already sent on or closed | Does nothing: the first value stands, and nothing downstream fires twice |
| Anything else in your body | **Runs again** |

So the shape is: do the work, wrap anything expensive or irreversible in
`ctx.run`, wait, then emit. If you want a node downstream to start before the
wait (an image node sending `stats` at once and `image` when the provider is
done), emit that port first, then wait, then emit the rest.

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
use weft::signal::{CancelCall, PollEndpoint, Predicate};

let submitted = ctx.run("submit", || async {
    post_json(&http, &submit_url, &payload, "submitting the render").await
}).await?;
let id = submitted["request_id"].as_str().node_err("the submit answered no request_id")?;

let status = ctx.await_signal_with_cancel(
    PollEndpoint {
        url: format!("{API}/requests/{id}/status"),
        interval_secs: 5,
        access: Some(weft::primitive::AccessRef::from(&account)),
        filters: vec![
            Predicate::neq("status", "IN_QUEUE"),
            Predicate::neq("status", "IN_PROGRESS"),
        ],
        ..Default::default()
    },
    CancelCall::put(format!("{API}/requests/{id}/cancel")),
).await?;
```

The run parks and the worker goes away. Weft checks the address once straight
away (a job that is already done resumes at once), then every `interval_secs`,
and an interval under 5 seconds is refused. The first answer that passes every
filter is what the wait returns, and the checking stops. With no filters, the
first successful answer is the one. An error status or a failed request is not
an answer: it is tried again at the next check and shown on the node. `access`
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

### If the run is cancelled while the job runs

Cancelling the run takes the wait down, but the provider hears nothing and
goes on working, and charging, for an answer nobody will read. If you want the
provider told, wait with `ctx.await_signal_with_cancel` and give it, as its
second argument, one request to send if the run is cancelled, usually to the
provider's own cancel address for the job.
Whatever cancelled the run (a person, another run's `stop_tagged`, a
take-down that cancels the runs still going), weft sends that request once, signed
with the same `access` as the checks. It is never sent when the answer
arrives, or when the run ends any other way.

`CancelCall::post`, `put`, `patch` and `delete` take the address. If the
provider's cancel wants a body, add `.with_body(json!({ .. }))`.
If the address is empty, or is not `http` or `https`, the wait fails at once,
before anything parks, with `invalid signal spec: the cancel call's url must not be empty` or
`invalid signal spec: the cancel call's url must be http(s): got '<url>'`.

If the provider has no way to cancel a job, wait with plain
`ctx.await_signal`.

If the provider refuses the call, or does not answer within 60 seconds, weft
does not try again, and the job may still be running. The runtime's log
(`weft daemon logs` on your machine) says `a cancelled run's last call to its
outside service failed`, with the address and what the provider answered.

If you want to check the call in a test, read it off the wait:

```rust
assert_eq!(
    rig.awaited_signals()[0].on_cancel,
    Some(CancelCall::put("https://queue.example/requests/req-1/cancel")),
);
```

### Watching it while it waits

If you want to show how far the job has got (a progress bar for a render,
say), weft keeps the last answer a check got while the node waits, exactly as
the provider sent it. A failed check does not replace it, and an answer over
64 KiB is kept only as its size. You can read it:

- in the editor, on the waiting node's panel, while you follow the run;
- in the terminal, with `weft node-state <run> <node>`, which prints
  `render: waiting` and then `last answer: {"status":"IN_QUEUE"}`;
- in a program, with the `GetNodeState` node, which takes a run's id (a run
  gets its own id from the `GetRunId` node);
- in your own node's code, with `ctx.run_by_id(run).node_state(node)`.

## When the run cannot pause

Some runs cannot let go of their worker: a route's run while its caller is on
the line (and the route does not outlive it), an unrecorded run, a run with a
bus open. There, `ctx.await_signal` does not suspend. It holds in your call,
on the same worker, and returns the answer when it comes, without replaying
anything. If the run stays quiet for its trigger's `holdSecs`, the call fails
with `WeftError::WaitGaveUp`, saying why. That is an outcome like any
other: if your node can do something sensible without the answer,
match it and carry on.

```rust
let approved = match ctx.await_signal(Form::approval("Send this?")).await {
    Ok(answer) => answer,
    Err(WeftError::WaitGaveUp(_)) => json!(false),
    Err(other) => return Err(other),
};
```

For what holds and when the clock runs, go and read [when a run cannot
pause](../language/triggers-and-routes.md#when-a-run-cannot-pause). To test
it, `rig.signal_given_up()` makes the next wait fail this way.

## Two things weft refuses

**Yielding on a stream, then awaiting.** A replay keeps an ordinary port's
first value, but a stream's items went out live and cannot be sent again.

```text
node 'narrate' called await_signal after yielding on or closing a stream
output; a pause replays the body from the top, and a stream's items went out
live and cannot be sent again. Wait before the stream starts, or (for a
co-alive node) stay warm with bus.recv() instead of await_signal.
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

## When the worker dies mid-run

What happens depends on the trigger's `durable` setting. For what that setting
does, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept).

**A fast run (the default) ends cancelled with its worker**, and is not run
again (go and read [what happens when something
dies](../running/architecture.md#what-happens-when-something-dies)).

**A durable run carries on in another worker.** The same goes for a durable
run whose worker is alive but cannot reach the broker to write its record for
over a minute: the worker stops its nodes and hands the run back (for how, go
and read [when a write of the record
fails](../running/the-journal.md#when-a-write-of-the-record-fails)). Each node's
start (except a pure node's, below) is on record before its body runs, so the
next worker knows exactly which nodes were running when the first one went
away. weft cannot tell how much of such a
node's work already happened, so the next worker fails it and says why:

```text
the worker running 'billing' went away while it was running; it was not run
again, because it may have partly happened. Re-run from here once you have
checked what it did.
```

A node that was reading a stream says `went away while it was reading a
stream` instead: the items it had read are gone from the stream, so the
message tells you to run the whole run again to start the stream over.

If you want to run that node again once you have checked, `weft run --from
<node>=<json>` starts a run there with the inputs you hand it. If you wired
the node's `_error` output, this failure goes there like any other, and that
branch carries on; otherwise the run fails.

If the node that was running is a pure one that does not read a
stream, and had not passed a value on yet, the next worker runs it again instead of failing it. A
pure node does nothing outside the run (for what that means, go and read the `pure` flag in
[metadata.json](metadata.md#features)), so running it twice changes nothing. One that had passed a value on
is failed like any other, because running it again would pass that value a
second time.

A run parked whole on `ctx.await_signal` survives its worker even when it is
fast, unless a caller is still on the line and its trigger leaves
`outlivesCaller` off, or a bus between its nodes is open (for why, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept)). When its answer comes, the waiting node replays from the top, and its
`ctx.run` calls give back their saved results. A fast run with one branch
waiting while another still ran ends cancelled like any other fast run.

In a durable run, if a node emitted a value before the crash and the node
reading it had not started yet, that value is still delivered and the reading
node runs.

## When ctx.run cannot save its result

`ctx.run` writes its result down after the closure finishes. If that write
fails, the action has happened but nothing recorded it, and the node fails
with:

```text
could not save result of 'charge the card' for node 'billing': <error>. The
action may already have happened; inspect this run before repeating it.
```

Two things are deliberately **not** wrapped, because they are already safe to
repeat on a replay: `ctx.tag_run` keeps its place in the order, and
`ctx.stop_tagged` finds its targets already gone.

## What a resume actually does

The new worker fetches your program by hash, reads the journal to see where the
run had got to, and carries on from there: a node that was running is failed,
a node that was waiting replays, and every node whose inputs are ready starts.

## When the worker stays

A run holding a live caller, or a bus with nodes still talking on it, keeps its
worker up. Parking there is an ordinary `await` in the same process, not a
death and a rebuild, so nothing replays.

That is why the refusal above suggests a bus for a node that has to stay warm:
if two nodes are alive together and talking, they do not need durability
between them.
