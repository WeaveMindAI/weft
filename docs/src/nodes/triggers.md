# Writing a trigger

A trigger starts runs from outside. It has two bodies instead of one.

```rust
#[async_trait]
impl Node for CronNode {
    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let expression: String = ctx.inputs.get("cron")?;
        let timezone: String = ctx.inputs.get("timezone")?;
        ctx.register_signal(Timer { spec: TimerSpec::Cron { expression, timezone } }).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

The metadata does not load if a node that is not a trigger declares
`firesWith`, or if a type in its `firesWith` is not a weft type. A trigger may
leave `firesWith` out, but then nothing checks what it fires with, and `weft
run --fire` cannot tell you what to send.

## What can wake you

| Kind | What it is |
|---|---|
| `Timer` | A delay, a date, or a cron schedule. A delay counts from when it was asked for: the activation for a trigger, the `state.wait` call for a node that waits |
| `Form` | A person filling something in |
| `Route` | An inbound HTTP request |
| `Socket` | An inbound WebSocket |
| `SocketListen` | A socket weft dials out to and holds |
| `StreamListen` | A TCP or TLS stream |
| `SseSubscribe` | A server-sent-event stream weft subscribes to |
| `PollEndpoint` | An address weft checks on a timer. For waiting on the status address of a job your node started, go and read [waiting on a job a service runs](durable-execution.md#waiting-on-a-job-a-service-runs) |
| `ProviderEvents` | A service's own events, by topic, through its recipe |

If your node registers a `SocketListen` or `StreamListen` with a connection,
the text frames it sends (and a stream's address) can name the connection's
stored values as `{name}`, and the value goes in as it is. When the value is
something a person typed into a line protocol, like a password in an IMAP
`LOGIN`, write `{name|quoted}` instead: weft wraps it in `"` and escapes `\`
and `"`, so `a1 LOGIN {user|quoted} {password|quoted}` signs in whatever the
password holds. A value with a line break in it cannot be quoted and is
refused rather than sent as a second command.

`ProviderEvents` is the one to reach for when the service has a recipe, because
it takes care of both roads: a provider that pushes to you, and one you have to
dial. Go and read [events from a service](../connections/events.md).

## Asking a person

If your node needs somebody to answer something, build a `Form` from whatever
you have in hand and wait on it with `state.wait` (or register it, if your node
is a trigger). The fields are plain data, so a node can ask a question it only
learned about while it ran:

```rust
use weft::signal::{ChoiceOption, Form, FormField};

let options = vec![ChoiceOption::new("left", "Go left"), ChoiceOption::text("Right")];
let form = Form::for_person("agent-question", Some("The agent is stuck".into()), None, vec![
    FormField::display("said", "What it said", json!(said)),
    FormField::choice("pick", "Which way?", options).required(),
    FormField::text("other", "Or tell it something else").placeholder("Your answer").answering_for("pick"),
    FormField::decision("allow", "Let it run this command?").approve_label("Allow").reject_label("Deny"),
]);
let schema = form.schema.clone();
let submission = state.wait(|s| &mut s.submission, form).await?;
let answers = schema.answers(&submission)?;
let allowed = answers.decision("allow")?;
let picked: Option<String> = answers.value("pick")?;
```

The person answers it in the browser extension (for setting that up, go and
read [putting a person in the loop](../build/a-person-in-the-loop.md)).

| Field | What the person sees | What you read back |
|---|---|---|
| `display(key, label, value)` | The value, as text, or as JSON for a list or a record | Nothing |
| `display_image(key, label, image)` | The image, stored or at a URL | Nothing |
| `chat_history(key, label, conversation)` | A conversation, message by message, from a file holding a `ChatHistory` (the file a model node keeps its conversation in), so the person sees what led to the question | Nothing |
| `decision(key, label)` | Approve and reject buttons, their texts set with `.approve_label` and `.reject_label` | `answers.decision(key)`, `true` for approve |
| `choice(key, label, options)` | The options, to pick one | `answers.value::<String>(key)`: the picked option's value, an empty string when none was picked |
| `choices(key, label, options)` | The options, to pick any of them | `answers.value::<Vec<String>>(key)`: the picked options' values, an empty list when none was picked |
| `text(key, label)`, `long_text(key, label)` | A one-line box, or a bigger one, with `.placeholder(text)` shown while it is empty | `answers.value::<String>(key)`, an empty string when left blank |

Each option is a `ChoiceOption`. The person sees its label, and the answer
carries its value, so if you want an id back, write `ChoiceOption::new(id,
label)`, and two options may then show the same text. `ChoiceOption::text(t)`
shows `t` and answers `t`, and `.described(text)` adds a line under the label.

If you need an answer to a field, mark it `.required()`. The extension keeps
its Submit button off until the field has one, and `schema.answers` refuses a
submission where it is blank (nothing, an empty string, or an empty list),
naming the field. When a box is a second way of answering a field, like the
"or tell it something else" box above, mark the box `.answering_for(key)`, and
text in it counts as an answer to that field.

`answers.value` gives `None` only when the submission carried nothing for the
field at all. The extension sends every field it shows, so what you get from
it is the value as the table says, blank included, unless the field is
required. `answers.value` fails naming the field when what came back does not
read as the type you asked for, and `schema.answers` fails naming the field
when a decision came back as anything other than `true`, `false`, `"approve"`
or `"reject"`. A form with two fields under one key, a required field that only
shows something, or a box answering for a field that is not there is refused
when it is registered.

If your node's fields come from its own settings instead, the way HumanQuery's
`fields` list does, declare the kinds in your metadata's
[`portsFromConfig`](metadata.md#portsfromconfig), each with a `render`, and
`ConfigForm` does the rest. `ConfigForm::of(self.manifest(), &ctx.inputs)?`
reads the entries, `.fields(&prefill)` builds them (a field that shows a value
takes it from `prefill` by its key), and `.outputs(&answers)` puts each answer
on the port its entry added. A decision sets only the port of the side the
person chose, `{key}Approved` or `{key}Rejected`, and the other one closes.

## A trigger that polls a list

If the service only offers "list the latest items" (a mailbox, a database
query), register a `PollEndpoint` with a `delta`, and weft fires one run per
new item instead of one per poll:

```rust
use weft::access::client::url;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollPaging};

ctx.register_signal(PollEndpoint {
    url: url(API, &["messages"])?.query("maxResults", 500).into(),
    interval_secs: interval,
    delta: Some(PollDelta {
        items: "messages".into(),
        cursor_field: Some("id".into()),
        mode: DeltaMode::Set,
        cursor_param: None,
        paging: Some(PollPaging::query("nextPageToken", "pageToken")),
    }),
    access: Some(weft::primitive::AccessRef::from(&account)),
    ..Default::default()
})
.await
```

`items` is the dotted path to the list in the answer. `cursor_field` is the
field that tells items apart: with `DeltaMode::Max` an item is new when that
field is above the highest one seen so far, which needs a field that only
grows in the order items show up; with `DeltaMode::Set` it is new when weft
has not seen its id recently, for a list whose ids are opaque. The first poll
after activation only records what is there and fires nothing, so activation
means "from now on". Each run gets the item on `ctx.wake` as `item`.

If the list comes in pages, newest first, say how to ask for the next one in
`paging`, so a burst bigger than one page is not cut to its newest page:

- `PollPaging::query(next, param)`: the answer says the next page's cursor at
  the dotted path `next`, and the next request sends it as the query
  parameter `param`;
- `PollPaging::body(next, field)`: the next request sends it as the field
  `field` of the JSON body, for a `PollMethod::Post` poll whose `body` is an
  object.

Each poll then follows the pages until one holds an item it has already
seen, or the answer names no next page (a cursor that is missing, empty or
not a string), and fires every new item it read. The first poll after
activation reads the first page alone. If more than 50 pages of new items
arrived since the last poll, the poll fails and fires nothing, rather than
firing only part of the burst: the log says `paged poll ran past its page
limit`, and the fix is polling more often (`interval_secs`) or asking for
bigger pages. Set `paging: None` for a list that fits one answer.

`paging` needs a `cursor_field`, a JSON answer (not `PollFormat::Feed`), and
no `cursor_param` beside it, since that already says where the next request
starts. Activation refuses each of those, saying which.

## Two things a trigger cannot do

It cannot be wired from another trigger, and nothing downstream of a trigger
can be an infra node. Both are refused at compile time, because a trigger's
inputs are frozen at setup and provisioning happens before any event exists.

It also cannot sit inside a loop. It registers once for the project, so a
per-iteration one is not a thing.

## Testing one

```rust
rig.run_setup_trigger(&CronNode, json!({ "cron": "0 0 9 * * *" })).await.ok()?;
assert_eq!(rig.registered_signals().len(), 1);

let outcome = rig.wake(json!({ "scheduledTime": "2026-01-05T09:00:00Z", "actualTime": "2026-01-05T09:00:01Z" }))
    .run(&CronNode, json!({})).await.ok()?;
```

`fake` is the top tier for a trigger. The live rig drives a plain `run` body,
so a live test on a trigger is refused.
