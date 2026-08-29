# Surviving a restart

A node body can touch the outside world in a way that survives its worker dying
and a fresh worker picking the execution back up hours or days later. Four
things it can reach for, and one of them is doing nothing special.

| Primitive | For |
|---|---|
| `ctx.register_signal(kind)` | a trigger declaring a persistent endpoint, cron, or feed |
| `ctx.await_signal(kind)` | a mid-flow wait for a human or an external event |
| `ctx.run("name", closure)` | non-deterministic or side-effecting work between waits |
| nothing | pure logic, branching, computing from journaled values |

## `ctx.await_signal`

Parks **this firing** until the signal fires. The worker exits while parked. A
fresh worker spawns when the fire arrives.

```rust
use weft::signal::Form;

async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let answer = ctx.await_signal(Form {
        form_type: "human-query".into(),
        schema: my_form_schema(),
        title: Some("Approve?".into()),
        description: None,
        consumer_kind: Some("human_in_the_loop".into()),
    }).await?;

    ctx.pulse_downstream(NodeOutput::new().set("answer", answer)).await
}
```

Other firings of the same execution keep going. Only this one parks.

## The thing to understand: the body re-runs from the top

When the fire arrives, the next worker **re-runs the whole body from the
first line**. The `await_signal` call that parked last time returns instantly
with the journaled value, and execution continues past it.

So a body with two waits runs three times across its life:

1. First dispatch. Hits wait 0, suspends.
2. Approval fires. Body re-runs from the top. Wait 0 returns its value. Logic
   runs. Wait 1 suspends.
3. Confirmation fires. Body re-runs from the top. Both waits return their
   values. The body completes.

Everything **between** the waits ran three times.

## `ctx.run`: making something happen once

Anything between waits that is non-deterministic or has a side effect must be
wrapped.

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    // Minted ONCE. Every replay returns the same token.
    let idem = ctx.run("idem", || async {
        Ok(json!(uuid::Uuid::new_v4().to_string()))
    }).await?;

    let approval = ctx.await_signal(approval_spec()).await?;

    // Charged ONCE. Every replay returns the recorded response.
    let http = ctx.http();
    let receipt = ctx.run("call_billing", || async {
        let resp = http.post("https://api.billing/charge")
            .json(&json!({ "idem": idem, "approved_by": approval["who"] }))
            .send().await.node_err("charging the card")?
            .json::<serde_json::Value>().await.node_err("reading the receipt")?;
        Ok(resp)
    }).await?;

    ctx.pulse_downstream(NodeOutput::new().set("receipt", receipt)).await
}
```

The closure runs at most once for this node in this execution, counting each
loop iteration as its own firing, and every later replay returns the journaled
value without invoking it.

The `name` is only for traceability in the journal. The runtime keys on
call-site **order**, so two `ctx.run` calls may share a name and you may rename
any call freely.

### What needs wrapping

| Safe between waits | Wrap in `ctx.run` |
|---|---|
| pure logic | `rand::random()`, `Uuid::new_v4()`, `Instant::now()` |
| branching on values from waits or runs | network calls, database writes, file I/O |
| reading `ctx.inputs` | environment reads that might change |
| | anything that could differ between two runs of the same code |

## The replay rule

**The sequence of `ctx.await_signal` and `ctx.run` calls must be identical
across every replay.**

The runtime checks each call against the journaled sequence, so a mismatch
fails the node loudly rather than desyncing quietly.

So do not make the *number* or *order* of those calls depend on anything that
could change. Branching between them is fine as long as both arms make the same
calls, or neither does.

### Emitting before a wait is refused

Emitting on, or closing, an output port **before** an `await_signal` is refused
outright, because the resume replays from the top and would touch the port
twice.

Emit after all your waits. Or, for a node that wants to stay warm and
interactive, use a [bus](streams-and-buses.md) instead of suspending.

## `ctx.register_signal`

The trigger form, covered in [Writing a trigger](writing-triggers.md).
`register_signal` declares a persistent entry point that spawns a fresh
execution per event; `await_signal` parks the current firing. Which one you get
is the method you called, never a flag on the kind.

## Worker lifetime

A worker pod dies whenever every live firing is parked.

A suspended firing holds no worker, so ten thousand pending approvals cost no
compute, just journal rows. When a fire arrives, a fresh worker spawns, folds
the journal, and re-runs every node that has a fire to deliver, with each
prior wait and run returning instantly from the record.

## Why this matters even without a wait

Weft's crash guarantee is **at-least-once** for a node whose completion never
reached disk. The mechanism is in
[The journal](../running/the-journal.md#the-execution-guarantee).

So a body with no `await_signal` anywhere in it is still not exempt: if its
worker dies mid-node, the replacement runs it again from the top. The rule is
simply **if this node's work must not happen twice, wrap it**.

## A loop with a wait inside a node

A `loop` in your body containing an `await_signal` means the node is
orchestrating, which is the graph's job, so reach for a weft `Loop` and let
each iteration fire a node that does one thing.

If a genuine constraint forces the shape on you anyway, here is what you are
signing up for. A resume re-runs the **whole body** from the top, so your loop
restarts at iteration zero. Past `await_signal` and `ctx.run` calls replay
instantly, but every other call runs for real again, once per replay. So an
unwrapped paid API call inside that loop is charged again on every human
response: wrap every side-effecting call in `ctx.run`.
