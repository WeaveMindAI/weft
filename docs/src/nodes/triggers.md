# Writing a trigger

A trigger starts runs from outside. It has two bodies instead of one.

```rust
#[async_trait]
impl Node for CronNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let spec: String = ctx.inputs.get("schedule")?;
        ctx.register_signal(Timer::cron(spec)).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        ctx.pulse_downstream(ctx.fan_declared(&ctx.wake.record()?)).await
    }
}
```

`setup_trigger` runs once, when somebody activates the project. `run` fires
every time the thing actually happens.

In the metadata:

```json
"features": { "isTrigger": true },
"firesWith": { "chatId": "String", "text": "String", "caller?": "JsonDict" }
```

## Setup

Read your inputs, decide what should wake you, and register it. That is all.

You never listen for anything yourself. No socket, no polling loop, no webhook
handler. You say what should wake you and the runtime does the holding, the
renewing and the checking that the event is real.

Two things to know about it.

**Setup reaches out.** A trigger that waits for Slack messages registers a
subscription with Slack right there. If that cannot be done, activation fails
on the spot naming what is missing, rather than leaving a trigger that looks
armed and is dead.

**Your inputs are frozen at that moment.** Whatever they were is snapshotted
alongside the registration and replayed onto `ctx.inputs` on every firing. The
nodes upstream do not run again per event. When somebody changes one, they have
to run `weft resync`, and that is why.

Register once per node per setup. A second call is a loud error rather than a
trigger that quietly never fires.

## Firing

`run` gets the event on `ctx.wake`, a bag with the same methods as
`ctx.inputs`. Your declared inputs are there too, replaying the snapshot.

```rust
let chat_id: String = ctx.wake.get("chatId")?;
```

`ctx.fan_declared(&ctx.wake.record()?)` is the common shape: take the whole
payload and fan its keys onto your same-named output ports, skipping anything
you did not declare.

## firesWith

Say exactly what the event carries. The `?` goes on the **name**:

```json
"firesWith": { "chatId": "String", "caller?": "JsonDict" }
```

It is checked both ways before your body runs. A payload missing a field you
declared without `?` is refused, and so is one carrying a field you never
declared, at the top level and nested. `weft run --fire` checks a payload you
typed by hand against it too.

One honest caveat: nothing enforces at build time that a trigger has a
`firesWith`, or that a non-trigger does not. A repo test holds the shipped
catalog to both. A node you write is held to neither, and a typo in one of the
type strings is not caught when the metadata loads, so it reaches a real firing
before it fails.

## What can wake you

| Kind | What it is |
|---|---|
| `Timer` | A delay, a date, or a cron schedule |
| `Form` | A person filling something in |
| `Route` | An inbound HTTP request |
| `Socket` | An inbound WebSocket |
| `SocketListen` | A socket weft dials out to and holds |
| `StreamListen` | A TCP or TLS stream |
| `SseSubscribe` | A server-sent-event stream weft subscribes to |
| `PollEndpoint` | An address weft checks on a timer |
| `ProviderEvents` | A service's own events, by topic, through its recipe |

`ProviderEvents` is the one to reach for when the service has a recipe, because
it takes care of both roads: a provider that pushes to you, and one you have to
dial. Go and read [events from a service](../connections/events.md).

## Two things a trigger cannot do

It cannot be wired from another trigger, and nothing downstream of a trigger
can be an infra node. Both are refused at compile time, because a trigger's
inputs are frozen at setup and provisioning happens before any event exists.

It also cannot sit inside a loop. It registers once for the project, so a
per-iteration one is not a thing.

## Testing one

```rust
rig.run_setup_trigger(&CronNode, json!({ "schedule": "0 9 * * *" })).await.ok()?;
assert_eq!(rig.registered_signals().len(), 1);

let outcome = rig.wake(json!({ "firedAt": 1700000000 }))
    .run(&CronNode, json!({})).await.ok()?;
```

`fake` is the top tier for a trigger. The live rig drives a plain `run` body,
so a live test on a trigger is refused.
