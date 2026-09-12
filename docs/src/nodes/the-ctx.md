# The ctx

Inside a node, `ctx` is your access to the graph's inputs and the services
weft provides. The runtime passes this `ExecutionContext` to `run`
and to a trigger's `setup_trigger`.

## Read inputs and emit results

```rust
let text: String = ctx.inputs.get("text")?;
ctx.pulse_downstream(NodeOutput::new().set("value", text)).await?;
```

`ctx.inputs` contains the firing's wired, literal and defaulted inputs.
A firing trigger also reads its event from `ctx.wake`.

| To… | Use |
|---|---|
| Read a required value | `ctx.inputs.get("name")?` |
| Read an optional value | `ctx.inputs.opt("name")?` |
| Emit values | `ctx.pulse_downstream(output).await?` |
| Emit and wait until the consumer takes the value | `ctx.yield_downstream(output).await?` |
| Close an output | `ctx.close_port("port").await?` |
| Build outputs from the declared keys of a JSON object | `ctx.fan_declared(&value)` |
| Read a resolved output type | `ctx.output_type("port")` |

For defaults, dynamic inputs and emission rules, read
[Reading inputs, emitting outputs](values-and-emission.md).

## Open an account connection

```rust
let account: Access = ctx.inputs.get("account")?;
let conn = ctx.open(&account).await?;
let client = conn.client();
```

The returned client applies the service's authentication and routing.
Where a meter supports the request, it also records cost information.
For the client alone, use `ctx.client(&account).await?`.
For an unauthenticated request, use `ctx.http()`.

The opened connection exposes `identity()` and `owner()`, along with
`value(name)` and, for single-string credentials, `credential()`.
Keep credential values out of node outputs and logs. A manually constructed
client bypasses the connection's routing and metering.

For sockets, longer credential windows and permission requirements, read
[Using a connection in a node](../connections/using-a-connection.md).

## Store files

```rust
let storage = ctx.storage(StorageScope::Execution);
```

Use the returned handle to `put`, `get` or `presign` files.
The scope determines where new files live and when they are deleted.
Execution files can also receive a keep policy.
For complete calls and file conversion helpers, read [Storage](storage.md).

## Wait for an event or reuse a result

| To… | Use |
|---|---|
| Register a trigger's event source | `ctx.register_signal(spec).await?` |
| Suspend this firing until an event arrives | `ctx.await_signal(spec).await?` |
| Reuse an operation's recorded result on replay | `ctx.run("name", closure).await?` |

A durable wait resumes by replaying the node body. `ctx.run` returns a
saved result when there is one; an external action can still repeat if the
worker dies before saving it. Read
[Surviving a restart](durable-execution.md) before putting side effects
around waits.

## Talk to a caller or another node

For an HTTP caller, use `ctx.http_caller().await?`. For a WebSocket
caller, use `ctx.ws_caller().await?`. Both fail when the run has no
caller of that protocol.

If the node supports either protocol, `ctx.caller()` returns an optional
`CallerHandle`. You can also ask `ctx.is_api_call()`,
`ctx.is_websocket()` and `ctx.caller_data_type()`.
For replying and reading messages, read
[Talking to a live caller](live-callers.md).

For a conversation between nodes:

```rust
let host = ctx.open_bus("channel", BusOptions::default(), "host").await?;
let guest = ctx.join_bus("channel", "guest")?;
```

These are the producer and consumer calls, used in their respective nodes.
Their guards close the bus when dropped. An observer that should leave it
open uses `ctx.bus_from_input("channel")?`.

For the complete exchange, including when a participant starts receiving,
read [Streams and buses in Rust](streams-and-buses.md). That chapter also
covers generator inputs and `ctx.set_max_buffered_items`.

## Reach infrastructure

`ctx.endpoint("api").await?` resolves an endpoint declared by this node.
The returned handle offers `url()`, `host_and_port()` and `call(...)`.

For endpoint readiness and passing access to another node, read
[Infrastructure nodes](infrastructure.md#talking-to-your-infrastructure).

## Cancel or stop other runs

`ctx.is_cancelled()` checks the execution's cancellation flag.
`ctx.cancellation()` returns the shared flag for blocking work or
best-effort cleanup. For subprocess handling and the limits of aborting
an external request, read [Cancellation](cancellation.md).

To label this run and request stops for matching runs in the same project:

```rust
ctx.tag_execution(["user_7"]).await?;
ctx.stop_tagged("user_7", StopSelf::Keep).await?;
```

`StopSelf::Keep` uses tag-registration order to protect newer runs.
For the ordering and when a queued stop takes effect, read
[Stopping other runs](steering-executions.md).

## Log what happened

```rust
ctx.log(LogLevel::Info, "Started rendering the image").await?;
```

Use `.node_err("reading the response")?` to attach context to another
library's error. Use `weft::node_bail!` or `weft::node_error` for a
failure the node detects itself. For examples, read
[Errors](values-and-emission.md#errors).

## Identify the current firing

| Field | Identifies |
|---|---|
| `execution_id` | The execution, as a string |
| `color` | The same execution, as a UUID |
| `project_id` | Its project |
| `node_id` | This node instance |
| `node_type` | Its catalog type |
| `node_label` | Its optional display label |
| `frames` | The loop iteration stack |

### State shared inside a worker

A Rust `static` is shared by node firings in the same worker process.
That can hold a connection pool or a cache, but another worker will have
its own copy and a restart loses it.

Use a database or file storage for data that must survive or be shared
between workers. For data private to one execution,
include its identity in any cache key and arrange cleanup when that data
is no longer needed.
