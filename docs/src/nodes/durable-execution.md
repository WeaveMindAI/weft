# Surviving a restart

If a node waits for a person to answer tomorrow, its Rust stack does not
sit there until tomorrow. weft records the wait. When the answer arrives,
it runs the body again from the top and supplies the saved answer at the
same call.

That changes how you write the work around a wait. A network request on
line two can run again even though the user is answering the question on
line ten.

## `ctx.await_signal`

Use `ctx.await_signal(kind)` to wait during an existing execution.
For example, inside a node body that has built a `Form` named `form`
and declares an `answer` output:

```rust
let answer = ctx.await_signal(form).await?;
ctx.pulse_downstream(NodeOutput::new().set("answer", answer)).await?;
```

If no answer has arrived, the call suspends this firing. Other firings
can continue. When the body runs again, that call returns the recorded
answer and the code continues below it.

For a complete form-building implementation, read
[HumanQuery](https://github.com/WeavemindAI/weft/blob/mvp/catalog/human/query/mod.rs).
To use a human step in a program, follow
[A person in the loop](../start/a-person-in-the-loop.md).

## The body re-runs from the top

Suppose a body waits for approval, does some work, then waits for confirmation.
When each answer arrives after its wait has been registered, the sequence is:

1. The first dispatch reaches the approval wait and suspends.
2. The approval arrives. The body starts again, reads that saved answer,
   does the work, and suspends at confirmation.
3. The confirmation arrives. The body starts again, reads both answers,
   and finishes.

The code before approval ran three times. The work between the waits ran
twice. A worker crash can add more attempts.

## `ctx.run`: reusing a saved result

If a result must stay the same when the body re-runs, put the work in
`ctx.run`. Here is a node-body fragment that saves a generated identifier:

```rust
let request_id = ctx.run("request_id", || async {
    Ok(serde_json::json!(uuid::Uuid::new_v4().to_string()))
}).await?;
```

This example needs the `uuid` dependency with its `v4` feature in the
node's `deps.toml`. On the first attempt the closure generates the value
and weft saves it. Later attempts return that saved value without invoking
the closure.

The same method accepts a closure that makes an HTTP request or writes to
a database. It returns a JSON value, which can contain the result you need
afterwards.

There is a failure window to account for: a service can accept your request
just before the worker crashes, leaving weft without a saved result.
The next attempt sends the request again. If repeating the action would be
harmful, the receiving service needs to recognize repeated requests and
return the original result. Save its request identifier before making the
call, and use the service's documented duplicate-prevention mechanism.
An arbitrary identifier in the request does nothing unless the service
honors it.

| Work in the body | How to handle it |
|---|---|
| Calculations from inputs or saved results | Let them run again |
| A random value or current time that must remain stable | Save it with `ctx.run` |
| An external request or persistent write | Save its result with `ctx.run`; account for an interrupted attempt |
| An environment value that affects later decisions | Save it if it could change between attempts |

A saved result does not preserve files on the worker's local disk.
Recreate temporary files after a restart, or use weft storage and save
the stored-file reference.

## Keep the replayed path stable

weft matches `ctx.run` and `ctx.await_signal` calls by their position in
the firing's recorded sequence. The `ctx.run` name helps you read the
journal; it is not the lookup key.

If a branch depends on a saved answer, it can safely choose different work
for different answers. Replaying that firing takes the same branch because
it gets the same answer. A branch based on a fresh random number could
instead reach a different call in the same position.

The runtime rejects mismatched call positions or kinds. It cannot detect
every changed intention: two different `ctx.run` calls in the same
position still look like a run call. Keep the decisions leading to them
stable.

### Emitting before a wait is refused

Finish your durable waits before emitting or closing any output.
`await_signal` rejects a firing that has already mentioned an output,
because replaying it would repeat the emission. It also rejects nodes
with generator inputs, whose earlier stream reads cannot be replayed.

If the node needs to exchange messages while it stays alive, use a
[bus](streams-and-buses.md#buses) and wait for messages on that bus.

## Worker lifetime

Suspending a firing does not immediately stop its worker. Other work can
keep the worker busy, and a live bus or caller can require it to stay
available. A suspended firing can resume in that same process.

When there is no work keeping it alive, the worker can exit after its
idle period, currently 30 seconds. The suspended execution remains in the
journal, and a later answer can bring up a worker to continue it.
Pending approvals therefore do not each need a waiting worker, although
the runtime's shared services still run.

For callers that remain connected, read [Live callers](live-callers.md).

## Restarts without a wait

The same concern applies to a node with no `await_signal`. If its worker
dies before completion is recorded, weft can run the body again. Use
`ctx.run` for results you need to preserve and handle repeated external
actions as described above.

For what the journal records and how execution resumes, read
[The execution guarantee](../running/the-journal.md#the-execution-guarantee).

## A loop with a wait inside a node

A Rust loop containing `await_signal` starts again at iteration zero on
replay. Earlier waits and saved runs return their recorded results;
unwrapped work repeats. The loop's decisions must follow the same saved
values each time.

If each iteration is a separate piece of your program, a
[weft loop](../language/loops.md) makes that structure visible and lets you
inspect its iterations in the graph.

A trigger has a different entry method: `ctx.register_signal` registers
a signal that starts executions. For that API, read
[Writing a trigger](writing-triggers.md).
