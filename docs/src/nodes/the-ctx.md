# The ctx

One object, handed to every node body, carrying everything a node needs from
the outside world.

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>
```

## Why one object

Left to themselves, two nodes calling two APIs end up with two HTTP clients,
two retry policies, two ideas about where a token lives, and two different
bugs. So authentication, storage, buses, journaling, suspension and
cancellation are built once and reached through this object.

`ctx.client(&access)` hands back an HTTP client already signed in to the
service you named, so a node whose connection declares AWS SigV4 as JSON gets
every request signed without a line of node code.

Where the dividing line runs, and how to argue that it is in the wrong place:
[the commandments of plumbing](../thinking/plumbing.md).

## Identity

Plain fields, always present.

```rust
ctx.execution_id
ctx.project_id
ctx.node_id
ctx.node_type
ctx.node_label      // Option<String>
ctx.color           // the execution's id
ctx.frames          // the loop iteration stack
```

## Values in

```rust
ctx.inputs      // everything wired, configured, or defaulted
ctx.wake        // a trigger fire's event payload
```

Both are `ValueBag`s with the same accessors. Full treatment in
[Reading inputs, emitting outputs](values-and-emission.md).

## Values out

```rust
ctx.pulse_downstream(NodeOutput::new().set("port", value)).await
ctx.yield_downstream(output).await     // waits until the value was taken
ctx.close_port("port").await?        // explicitly emit nothing
ctx.fan_declared(&value)               // fan a JSON object onto same-named ports
ctx.output_type("port")                // the port's resolved type
```

## Calling a third party

```rust
let conn = ctx.open(&access).await?;    // resolve + lease for this firing
conn.client()                            // signed in, and measured if a meter exists
conn.credential()?                       // the raw string, when there is one
conn.value("imap_host")?                 // a stored value by name
conn.socket(url).await?                  // the service's realtime API

ctx.client(&access).await?               // sugar: open, hand back the client
ctx.http()                               // a plain client, for unauthenticated calls
```

The node names a service and nothing else. Whether calls are measured, and
whose money pays, are decided elsewhere and are invisible here.
[Using a connection](../connections/using-a-connection.md).

## Files

```rust
let storage = ctx.storage(StorageScope::Project);
storage.put(...).await?;
storage.get(...).await?;
storage.presign(...).await?;
storage.externalize(&value, &ty, policy).await?;
storage.internalize(&response, &ty, None).await?;
```

The scope decides where the file lives **and how long**. [Storage](storage.md).

## Pausing

```rust
ctx.await_signal(Form { .. }).await?     // park this firing; the worker exits
ctx.register_signal(ApiEndpoint { .. }).await?   // a trigger's registration
ctx.run("name", || async { ... }).await? // run once, replay the result forever
```

[Surviving a restart](durable-execution.md).

## Talking to a live caller

```rust
ctx.http_caller().await?      // fails loud if this run has no HTTP caller
ctx.ws_caller().await?
ctx.caller()                  // Option<CallerHandle>, the protocol-typed form
ctx.is_api_call()
ctx.is_websocket()
```

[Talking to a live caller](live-callers.md).

## Talking to other nodes

```rust
ctx.open_bus("channel", BusOptions::default(), "host").await?
ctx.join_bus("channel", "guest")?
ctx.bus_from_input("channel")?
ctx.set_max_buffered_items("rows", 100_000)?
```

[Streams and buses in Rust](streams-and-buses.md).

## Infrastructure

```rust
let api = ctx.endpoint("api").await?;   // resolves, then waits until it answers
api.url();
api.host_and_port()?;
api.call(EndpointMethod::Get, "/outputs", None).await?;
```

[Infrastructure nodes](infrastructure.md).

## Stopping

```rust
ctx.is_cancelled()
ctx.cancellation()      // Arc<CancellationFlag>
```

Ordinary async Rust is cancellable with no code at all. You need these only
for subprocesses, blocking CPU work, and resources needing explicit cleanup.
[Cancellation](cancellation.md).

## Steering other runs

```rust
ctx.tag_execution(["user_7"]).await?;                 // label this run
ctx.stop_tagged("user_7", StopSelf::Keep).await?;     // stop the others carrying it
ctx.stop_tagged("exp_3", StopSelf::Include).await?;   // stop them all, me too
```

A run can label itself and stop every other run of the project carrying a
label, including runs parked on a person or a timer. This is how three
messages from one sender end with only the latest one answered. For the
ordering rule and what the journal says afterwards, go and read
[Stopping other runs](steering-executions.md).

## Logging and errors

```rust
ctx.log(LogLevel::Info, "message").await?;

// on any non-weft Result or Option:
something().node_err("doing the thing")?;

// for a bad condition you detected yourself:
weft::node_bail!("bridge rejected: {reason}");

// the expression form, for map_err / ok_or_else closures:
Err(weft::node_error(format!("no timestamp in {body}")))
```

Those are the only error doors: the input accessors stamp their own errors,
every ctx handle already returns `WeftResult`, and node code never names a
`WeftError` variant. Worked examples of each are in
[Values and emission](values-and-emission.md#errors).

## What the ctx will not give you

There is **no way to ask whose credential you are using**, whether the call
was billed, or what it cost. A node that could ask could branch on it, and
then the same node would behave differently for different users.

There is **no way to construct a client for a connection yourself**. A
hand-rolled client is invisible to the cost trail and will not carry the
routing a runtime-supplied credential needs.

There is **no way to write to the journal directly**. The journal records what
happened; it is not a log you post to. `ctx.log` is the log.

There is **no lifecycle phase to inspect**. A trigger writes two bodies and
the engine calls the right one.

Every one of those is missing so that a node cannot behave one way on the
author's machine and another way in production.
