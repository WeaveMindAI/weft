# Writing a trigger

A trigger has two jobs: register what it is waiting for, then handle each
event that arrives. Put those jobs in `setup_trigger` and `run`.
The engine calls them at the appropriate point.

Set this metadata feature:

```json
"features": { "isTrigger": true }
```

For example, the catalog's `Cron` node registers a timer in
`setup_trigger`. Add the `use` declaration at module scope and the method
inside `impl Node`. Cron's metadata declares `cron` and `timezone` string
inputs with defaults.

```rust
use weft::signal::{Timer, TimerSpec};

async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let expression: String = ctx.inputs.get("cron")?;
    let timezone: String = ctx.inputs.get("timezone")?;
    ctx.register_signal(Timer {
        spec: TimerSpec::Cron { expression, timezone },
    }).await
}
```

When the timer fires, the listener supplies `scheduledTime` and
`actualTime`. The node's `run` method forwards those fields to its
declared outputs:

```rust
async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let scheduled: serde_json::Value = ctx.wake.get("scheduledTime")?;
    let actual: serde_json::Value = ctx.wake.get("actualTime")?;
    ctx.pulse_downstream(
        NodeOutput::new()
            .set("scheduledTime", scheduled)
            .set("actualTime", actual),
    ).await
}
```

You can read the complete
[Cron node](https://github.com/WeavemindAI/weft/tree/mvp/catalog/triggers/cron)
for its imports and metadata.

## Inputs and event data

At fire time, `ctx.inputs` contains the input values saved when the
trigger registered. Upstream nodes do not run again for every event.
If someone changes an input, reactivate the project to register the new
value. For how that affects a program, read
[Two phases](../language/triggers.md#two-phases).

`ctx.wake` contains this event's fields. Read required fields with
`get`, as the timer does above. To forward all matching fields to your
declared outputs:

```rust
let data = ctx.wake.record()?;
ctx.pulse_downstream(ctx.fan_declared(&data)).await?;
```

`record()` fails if the event is not a keyed record. `fan_declared`
selects the fields matching the node's declared outputs; it does not
create new ports from arbitrary event keys.

A trigger body can be retried after a worker failure. If it performs an
external action, follow the rules in [Surviving a restart](durable-execution.md).

## Choose a signal kind

Construct a kind from `weft::signal` and pass it to
`ctx.register_signal`. The listener maintains the subscription, so your
trigger body does not need its own background task.

| Event source | Signal kind |
|---|---|
| A schedule or a specified time | `Timer` |
| A submitted form | `Form` |
| A service's SSE feed | `SseSubscribe` |
| A URL checked periodically | `PollEndpoint` |
| An outbound WebSocket connection | `SocketListen` |
| An outbound TCP or TLS connection | `StreamListen` |
| An incoming HTTP request | `ApiEndpoint` |
| An incoming WebSocket connection | `LiveSocket` |
| Events from a connected provider | `ProviderEvents` |

`SocketListen` connects to another service. `LiveSocket` accepts a
connection from a caller. Choose by which side starts the connection.

For the kinds' fields, read their
[Rust definitions](https://github.com/WeavemindAI/weft/tree/mvp/crates/weft-core/src/signal).
A form can also be used with `await_signal` to pause an existing execution.

### Polling and outbound streams

`PollEndpoint` can return each response or use a delta rule to emit new
items. It supports JSON and RSS/Atom feed parsing.

`StreamListen` describes an opening dialogue and how to split the byte
stream into messages. Its frames can interpolate connection values, so
credentials need not be written into the dialogue. The
[ReceiveEmail node](https://github.com/WeavemindAI/weft/tree/mvp/catalog/email/receive_email)
uses it to watch IMAP events; its fired body uses an email library to fetch
the messages.

### Incoming callers

`ApiEndpoint` and `LiveSocket` share `LiveConnectionConfig`.
Inside a trigger's `setup_trigger`, build it from the declared inputs:

```rust
use weft::NodeErrExt;
use weft::signal::{ApiEndpoint, LiveConnectionConfig};

let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?)
    .node_err("reading endpoint settings")?;
ctx.register_signal(ApiEndpoint { common }).await?;
```

Use the catalog's
[API endpoint node](https://github.com/WeavemindAI/weft/tree/mvp/catalog/live/api_endpoint)
for the accompanying input declarations. For reading the caller's messages
and sending replies, read [Talking to a live caller](live-callers.md).

## Reacting to provider events

If a service has an event recipe, register `ProviderEvents` with the
connection, topic, and filters. This fragment belongs in `setup_trigger`
for a node with an `account` access input and a `channel` string input:

```rust
use weft::Access;
use weft::signal::{Predicate, PredicateOp, ProviderEvents};

let account: Access = ctx.inputs.get("account")?;
let channel: String = ctx.inputs.get("channel")?;
let events = ProviderEvents::new(&account, "messages", vec![
    Predicate {
        field: "type".into(),
        op: PredicateOp::Eq,
        value: Some("message".into()),
    },
    Predicate {
        field: "channel".into(),
        op: PredicateOp::Eq,
        value: Some(channel.into()),
    },
]);
ctx.register_signal(events).await?;
```

Here `messages`, `type`, and `channel` are names declared by the
service's event recipe, as in the Slack recipe. Filters apply to those
named fields before an execution starts. If the recipe needs values for
its subscription request, such as a file ID to watch, pass them with
`.with_params(...)`.

The runtime chooses the recipe's available transport and handles the
subscription. Your node does not need separate webhook and socket
implementations. If the connection cannot support the subscription, or
a push-only provider needs a public address that the runtime lacks,
registration fails. For setup and those errors, read
[Events from a service](../connections/events.md).

By default, the subscription covers the connected account. Use
`.app_wide()` only for a node whose job is to receive events across the
accounts that installed its app. That scope requires the recipe's
outbound socket transport. Give these different jobs distinct node types
so the graph makes clear whose events can start work.
