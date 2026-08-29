# Writing a trigger

A trigger node writes **two bodies** and never inspects any phase. The engine
calls the right one.

```rust
use weft::signal::{ApiEndpoint, LiveConnectionConfig};

#[async_trait]
impl Node for MyTriggerNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let common = LiveConnectionConfig::from_node_fields(ctx.inputs.object()?);
        ctx.register_signal(ApiEndpoint { common }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Runs once per external fire.
        let value: serde_json::Value = ctx.wake.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
```

If you set `features.isTrigger: true` in the metadata, the engine calls
`setup_trigger` at registration time instead of `run`.

## The two value sources at fire time

**`ctx.inputs`** is a snapshot of what the trigger's inputs held **when it
registered**, saved alongside the registration. Nothing upstream runs again
when the trigger fires:
[a trigger's inputs are frozen at activation](../language/triggers.md#two-phases).

**`ctx.wake`** is this fire's event payload, as a bag of named fields: the
HTTP body, the SSE event JSON, the form submission, the timer info.

A trigger that forwards the whole payload reads it at once:

```rust
let data = serde_json::Value::Object(ctx.wake.object()?.clone());
ctx.pulse_downstream(ctx.fan_declared(&data)).await
```

`ctx.wake.object()` fails loudly when the fire delivered no keyed record, so a
broken delivery can never pass as an empty one.

## The signal kinds

Each is a struct in `weft::signal`. You construct one and pass it to
`register_signal`.

Two families, pointing opposite ways. `SocketListen` and `LiveSocket` sound
alike and are easy to swap by mistake: `SocketListen` dials out to a service,
`LiveSocket` is what an outside caller dials into.

### Outbound event sources

The listener reaches out to something and fires a fresh execution per event.

| Kind | What it does | Use for |
|---|---|---|
| `SseSubscribe { url, event_name }` | holds a one-way Server-Sent-Events stream, fires per matching event. Receive only. | a service pushing an SSE feed |
| `PollEndpoint { url, interval_secs, method?, body?, format?, delta? }` | hits a URL on a timer, fires with the body, or with `delta` once per new item. `method: Post` plus `body` polls a query endpoint; `format: Feed` parses RSS and Atom into `{ "items": [...] }`. No held connection. | a "give me what's new" endpoint: a bot's getUpdates loop, a database query, a feed |
| `SocketListen { url, minted, handshake?, heartbeat?, heartbeat_secs }` | holds a bidirectional WebSocket alive, sends an optional handshake on open and an optional heartbeat on a schedule, fires per inbound frame | a gateway that needs login and keepalive or it drops you. The op-code protocol is yours, expressed as the literal `handshake` and `heartbeat` frames. |
| `StreamListen { address, framing, script, replies?, heartbeat?, fire }` | holds a raw TCP or TLS pipe for services speaking neither HTTP nor WebSocket | any wire protocol: IMAP, MQTT, Redis, XMPP |

`StreamListen` is the kind that makes "no per-service engine code" literal. It
runs a declared connect dialogue (send
a frame, wait for a matching line), cuts the byte stream by a declared framing
(delimiter, length prefix, or varint prefix), and fires every unit matching the
`fire` pattern. Text frames interpolate `{placeholders}` from the attached
connection, so credentials ride the dialogue without sitting in the spec.

The watch is the trigger. The fired body then talks the protocol properly
itself, with a real library, where code is unrestricted.
`catalog/email/receive_email` is the worked example, watching a mailbox over
IMAP IDLE.

### Inbound live-caller endpoints

An outside caller dials in and holds the connection; nodes talk back through
[`ctx.caller()`](live-callers.md).

| Kind | For |
|---|---|
| `ApiEndpoint { common }` | an HTTP endpoint people call; a node replies once or streams |
| `LiveSocket { common }` | an inbound WebSocket; a node holds a two-way conversation |

Both share `LiveConnectionConfig`, built from the node's merged values with
`LiveConnectionConfig::from_node_fields(ctx.inputs.object()?)`.

The wire protocol is the **kind**, not a config field. The runtime derives it
from which struct you passed, which is why there is no `protocol:` knob to set
wrong.

### Always present

`Timer { spec }` for cron, after, and at. `Form { .. }` for a human
submission, normally used with `await_signal` rather than here.

## Reacting to provider events

A trigger that fires when something happens at a connected service registers
**one** kind, whatever the service is and however its events travel.

```rust
async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
    let account: Access = ctx.inputs.get("account")?;
    ctx.register_signal(ProviderEvents::new(&account, "messages", vec![
        Predicate { field: "type".into(), op: PredicateOp::Eq,
                    value: Some("message".into()) },
        Predicate { field: "channel".into(), op: PredicateOp::Eq,
                    value: Some(channel) },
    ]))
    .await
}
```

The parts, and where each one's knowledge lives:

- **The connection** says whose events. Whether weft holds an outbound line to
  the service or takes its pushes at a public address is decided by the runtime
  from what the connection can do, and your code is the same either way.
- **The topic** (`"messages"`) names one of the event topologies the service's
  recipe declares. One service may declare several; `slack` declares
  `messages`, `reactions`, `interactions` and `files`.
- **The filters** are predicates over the topic's **named** fields, evaluated
  before anything fires, so a non-matching event costs no execution. Translate
  the node's plain config inputs into predicates here; anything the grammar
  cannot express runs as ordinary code in `run`, after the fire.
- Topics whose subscribe call needs node-supplied values, such as the Drive
  file to watch, pass them with `.with_params(...)`.

Everything mechanical is the runtime's: holding the socket, acknowledging
frames, verifying push signatures, and subscribing, renewing and stopping
provider-side watch channels.

Registration fails **loudly** when the trigger cannot be served: the connection
lacks a value the dial-out transport needs, or the install has no public
address for a push-only service. The error names the fix, and
[Events from a service](../connections/events.md) is the user-facing side of
it.

## The scope decision

Which **scope** a trigger subscribes at is a node split:
[one node, one process](what-a-node-is.md#one-node-one-process).
`ProviderEvents::app_wide()` is how the app-owner node states which one it
is.

## A trigger that just fires

The simplest kinds take their fields directly:

```rust
use weft::signal::SseSubscribe;

ctx.register_signal(SseSubscribe {
    url: events_url,
    event_name: "message.received".into(),
}).await?;
```

`register_signal` returns once the dispatcher acknowledges. Any public URL is
derived from the signal's own path, so nodes never get one handed back and
never have to store one.
