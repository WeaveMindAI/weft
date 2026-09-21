---
name: weft-node-authoring
description: "Read before dispatching a node-smith (the brief and the review) and when an expert writes a node by hand: the node authoring manual, the test tiers, and the shapes half-done work takes. The node-smith reads this same file as its manual."
---

# Writing a custom node

Tangle reads the dispatch protocol and [the review] checklist. The `node-smith` subagent, and an expert taking the hand, read the manual that follows them.

Three terms hold across the file:

- [the contract] is the node's typed interface, and Tangle designs it: one job in a sentence; every input port (name, type, required or optional, and `accepts` only when a wire would be a mistake) and every output port (name, type); the service it wraps, if any; anything the surrounding program depends on (a form schema, a trigger registration, infra). Every value the node takes from the graph is its own input port. A `List` or `JsonDict` input whose elements would come from separate wires is the wrong shape: a list literal cannot hold a wire, so it forces a Python node whose whole body is `return {'params': [a, b]}`. An open-ended set of values (a query's parameters, a template's holes) is declared with `canAddInputPorts` and read with `ctx.inputs.custom()`. `PostgresExecuteQuery` is the pattern to copy, and any node's own `features` block (`weft describe-nodes --node <Type> --compact`) says whether it carries the flag, so you confirm there rather than trusting a name you remember.
- [the report] is what the node-smith hands back, and the only thing Tangle sees of its work.
- [the tiers] are the three test tiers a node carries: `basic` (no external world at all), `fake` (a stubbed client), `live` (the real service, real credentials, real money). `weft test-node <Type>` runs `basic` and `fake`, fast and free. Nobody but the user runs `live`: the node-smith writes those tests, names the service and declares any fixtures the test cannot self-provide, and the user runs them later, with consent, through `/weft-live-test`.

## The dispatch protocol (Tangle)

A node is missing only after the catalog says so: `weft describe-nodes --list` for the whole vocabulary, then `weft describe-nodes --node <Type> --compact` on anything close. If the catalog already holds a node that does the job, you go straight to the weft code. Otherwise:

1. **Design [the contract].** The node-smith implements it and never invents it. One job per node, and the permissions it needs are fixed by that job: when the permission would depend on WHICH input arrives (a post that needs one scope for a person and another for a company page), that is two nodes, each declaring its own `requiresScopes`, never one node with the rule in prose. Every value a node emits on a port is at most 100 KB (the wire limit, see the manual's "The wire limit"); a contract whose output could be bigger (page text, a long listing, a file's bytes) says how the node bounds it, or hands the bytes to storage and emits the file value.
2. **Dispatch one node-smith per package.** [the brief] is [the contract] plus the project context the node-smith cannot see (what [stage] this node feeds, what the upstream types are). Nodes that share a package (one service's access node and the nodes that use it, `nodes/linkedin/`) go to ONE node-smith with every contract in the brief, because two smiths writing into one package trip over each other's half-written files and one of them ends up creating a `package.toml` over the other's folder. Packages that share nothing go out in parallel, one node-smith each; nodes that depend on each other's types go out in sequence.
3. **Run [the review]** on [the report] against the checklist below. A report that fails goes back as a redispatch whose brief carries the previous attempt's folder, the specific finding (never "do better"), and what to keep. You never fix the node-smith's node yourself unless the fix is one line and obvious, because the next dispatch needs to know the pattern anyway. If you catch yourself editing the node-smith's `mod.rs` or `tests.rs`, stop and write: "Wait. Redispatch." Then write the finding into the brief.
4. **Wire it.** With the node green and in the catalog, read its `metadata.json` once more as delivered, and write the weft code.

A redispatch that comes back with the same finding gets the finding restated in one sentence and nothing else. The third identical failure comes back to the user as "this contract is not landing, here is what I suspect", because by then the blocker is probably real. If [the report] is blocked, you judge the blocker: a real impossibility comes back to the user as "this cannot be done honestly, here is the closest shape"; a soft blocker (missing docs, rig limits) goes back out with what you know.

### [the review] checklist

You never trust a report you can re-verify for the cost of one command. If you catch yourself accepting the quoted test output in [the report] as the verdict, stop and write: "Wait. My run is the verdict." Then run the command.

**Re-verify first, always:**

- Re-run `weft test-node <Type>` yourself. A report that claimed green and runs red is redispatched with the dishonesty named as the finding.
- Diff the delivered `metadata.json` against the port list in [the report]. A port renamed or dropped between report and file is redispatched.
- Read every test and ask: how would this test fail? A test with no answer (runs the node, ignores the result, asserts nothing about the outputs) is not a test, whatever its name says.
- `weft validate --file src/main.weft < src/main.weft` still passes with the node in the catalog, and `weft describe-nodes --node <Type> --compact` succeeds. The folder sits beside the module that uses it under `src/`, or under `nodes/` when shared; never inside `nodes/base_catalog/`.

**Then check [the contract] and the body:**

- No port renamed, added, or dropped; the one job is still the one job; no assembled `List` or `JsonDict` input.
- A skim of `mod.rs`: no fallbacks, no swallowed errors, no retry loops, no orchestration inside the body; failures are loud.
- **Every loop and every long wait watches `ctx.cancellation()`.** Stopping is cooperative: nothing kills a node mid-call, so a loop with no cancellation arm is a node that cannot be stopped by anything, and the only sign is a run that never ends.
- The `live` tests are written, with the service named and fixtures declared, and [the report] names them as not run.

**The half-arsing catalog.** Each of these fails [the review] and goes back as a redispatch with the specific finding:

- **smoke-only**: one test that runs the node once and asserts nothing.
- **happy-path-only**: the error paths (the loud `node_bail!` failures) are never exercised.
- **no closure test**: nothing covers an optional input arriving closed.
- **weakened assertions**: the test checks that an output exists, not that it holds the expected value.
- **swallowed in the test**: patterns like `if let Err(_) = ... {}` that pass on failure.
- **coverage gap against the contract**: count the tests against the ports: every port in [the contract] needs a test that fails if its behavior breaks, and a port with none is the finding.
- **live tests missing or hollow**: [the contract] names a service but there is no `NodeTest::live` entry for it, or the entry declares no service and no fixtures.
- **stale versions**: an API or dependency version taken from memory or an old example instead of the service's current docs, or one the service no longer serves; the rule is under deps.toml.
- **empty rig**: `tests()` returns an empty vec, or `tests.rs` does not exist, and [the report] did not say so. An access node is the one exception: its body is the `access_node!` macro, there is nothing of the smith's to test, and it ships with no `tests.rs` at all.
- **flaky-dismissed**: an intermittently failing test waved off as flaky instead of chased to its race. A race in the node is the node's bug; a test made tolerant of it (a retry, a sleep, a longer timeout) fails [the review] on both counts.
- **body smells**: `.ok()` discarding an error, a default value standing in for a missing input, a retry loop, orchestration inside the node.
- **a dead end in an image**: a state an infra container can sit in (a dead pairing, a lost credential, a revoked session) with no button on its display that leaves it, so the user's only way out is restarting or terminating the infra. Every such state gets an action, offered in every state; the rule is under Infra node.
- **a marker in an outbound payload**: a `__weft_<kind>__` wrapper handed to a provider, a bridge, a form spec or a live item, instead of the plain URL, `data:` URL or `{ url, mimeType, filename }` that consumer reads; the rule is under A file input.
- **silent failure in an image**: a service inside an infra image that fails a step without writing a line to its log, or answers the node with a success when the thing asked for did not fully happen; the two rules are under Infra node.

## The manual

A node does one thing: calls an API, transcribes audio, writes a row. It
never orchestrates (looping, retrying, branching, waiting for a person are
the graph's job, and the engine gives journaling, resumability and
cancellation for free) and never does plumbing (transport, credentials,
acknowledgement protocols, subscriptions, retry bookkeeping are the
language's).

A new node goes in this project's `nodes/` folder and is usable by its
`type` name as soon as it is there; the build compiles its Rust directly.
Its body may only `use` the `weft` crate, the crates its package declares
in `deps.toml`, and code inside its own package; a sibling package's code is
never on its path.

## Anatomy

A node is a directory (folder snake_case, `"type"` PascalCase, struct
`<Type>Node`):

```
nodes/my_thing/
  metadata.json    the declared surface: ports, config, presentation
  mod.rs           the Rust body, a Node trait impl
  deps.toml        optional: extra cargo crates, OS packages, build env
  tests.rs         optional: the node's own tests
```

A package is a directory with `package.toml` (`[package] name`, shared
`[dependencies]`); members are auto-detected as immediate subdirs holding a
`metadata.json`; shared `.rs` files at the package root are reached by
members as `use super::<file>;`. A package root may hold a partial
`metadata.json` of defaults every member inherits (key-by-key, member wins;
`type`/`label`/`description` are never inherited). Never place any of this
under `nodes/base_catalog/`: `weft catalog update` wipes it.

Each package compiles as its own crate, so a project's node sees its own
package's shared files and nothing under `nodes/base_catalog/`: you cannot
`use` a stdlib helper such as `elevenlabs.rs`. If you need one function from
a stdlib helper, copy it into your package's own shared file and say so in
[the report]. If you need a whole capability, report it as a ctx feature the
language is missing.

`weft`, `tokio`, `serde`, `serde_json`, `async-trait`, `anyhow` and `tracing`
are always available without declaring them; anything else, `uuid` included,
goes in `deps.toml`.

## metadata.json

Unknown keys are a loud parse error. Top level:

| Key | Meaning |
|---|---|
| `type`, `label`, `description` | identity, required |
| `tags`, `icon`, `color` | search and presentation |
| `inputs` | one list for wired data and design-time config |
| `outputs` | output ports |
| `types` | named type declarations, e.g. `"ChatHistory": "List[ChatMessage]"` |
| `features` | flags: `isTrigger`, `canAddInputPorts` (an open-ended set of values arrives as ports the author declares inline; the body reads `ctx.inputs.custom()`), `canAddOutputPorts`, `optionalCustomInputs`, `customInputType`, `oneOfRequired`, `showDebugPreview`, `liveEndpoint` (the endpoint serving this infra node's display, see [The display](#the-display)), `castPorts`, `hidden` |
| `portsFromConfig` | ports derived from a config list: `{ "field", "matchInput", "specs": [{kind, keyField, catchAll?, addsInputs, addsOutputs}] }` |
| `firesWith` | trigger only: EVERY field a firing can carry, name to weft type, `?` on the name for sometimes-present (`{"scheduledTime": "String", "caller?": "JsonDict"}`). Checked exactly: a firing missing a required field is refused, and so is one carrying a field you did not name |
| `display` | inline render: `{ "kind": "media" \| "link", "output" \| "input": "<port>" }` |
| `validate` | declarative rules: `{ "when": {...}, "then": {message, level: "structural"\|"runtime", field} }` |
| `requires_infra`, `images`, `publishes` | infra nodes |
| `service` | access nodes only, the connection recipe |
| `accessApps` | project-shipped OAuth apps |

Input entry: `name`, `type`, `required`, `accepts`, `widget`, `default`,
`label`, `placeholder`, `description`, `requiresScopes`, `requiresValues`.
Output entry: `name`, `type`, `description` (an output has no optionality).

`required` is written only as `"required": true`, on an input the node cannot
run without. You leave the key off every other input: absent already means
optional, so `"required": false` says nothing and reads as though you meant
something by it.

`accepts` is the list of drivers the port takes, `["literal", "wire"]` when
absent, and absent is right for almost every port. You write `["wire"]` only
for a port that needs a real node (a provider, a history, an `Access` handle
a consumer reads). Restricting a port a program could plausibly fill with a
written value is a review finding. Two port kinds never carry the list: a
`Bus`/`Generator` port is wire-only (the loader forces it), and a
compiler-read port (the `portsFromConfig` list, the access picker) takes an
inline typed value only by a fixed rule.

A minimal, real example (the catalog's `Text`):

```json
{
  "type": "Text",
  "label": "Text",
  "description": "Emit a literal string. Useful for prompts, labels, and config values.",
  "tags": ["literal", "string"],
  "icon": "Type",
  "color": "#64748b",
  "inputs": [
    { "name": "value", "type": "String", "widget": { "kind": "textarea" },
      "required": true, "label": "Value" }
  ],
  "outputs": [
    { "name": "value", "type": "String" }
  ],
  "requires_infra": false
}
```

## mod.rs

```rust
//! One doc line saying what the node does.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct MyThingNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MyThingNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> { tests::tests() }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: String = ctx.inputs.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("out", value)).await
    }
}
```

The `NodeManifest` derive embeds the sibling `metadata.json` at compile time
(a missing or malformed file is a compile error). Ports are camelCase in
JSON, and `ctx.inputs` is keyed by them. Config and wired inputs are one
bag: `ctx.inputs.get::<T>("name")` returns the value however it arrived
(wire, braces literal, assignment literal, or the declared `default`).

You fail loudly, always: `WeftResult<()>`; `ctx.inputs.get(...)?` stamps its
own errors; `node_bail!("message")` for conditions the node detects;
`.node_err("context")?` wraps an external error with context. A node body
never names a `WeftError` variant and never falls back to a default: the
failure is recorded in the journal where the user reads it. If you catch
yourself writing a fallback value, a `.ok()`, or a retry, stop and write:
"Wait. Fail loudly." Then bail with the cause.

You emit only through `ctx.pulse_downstream(NodeOutput::new().set(port, value))`;
ports you did not emit are closed, which is the skip signal downstream. For
user-added output ports use `ctx.fan_declared(...)`. Work that must not
happen twice across a restart goes through `ctx.run(...)`, which replays the
recorded result.

**Stopping is cooperative, and a node that ignores it cannot be stopped.**
Nothing kills a node mid-call: a cancelled execution (the caller left, a
person pressed stop, a newer run stopped this one) only trips a flag, and
the node is what acts on it. So anything that does not return promptly by
itself watches that flag and returns when it trips:

```rust
let cancel = ctx.cancellation();
loop {
    // ... one pass of the work ...
    tokio::select! {
        _ = tokio::time::sleep(interval) => {}
        _ = cancel.cancelled() => return Ok(()),
    }
}
```

Every loop that waits, every long external call, every read that could block
for minutes. A node that loops without it keeps running after everything
that asked for it is gone, and the only sign is a run that never ends. If
you catch yourself writing a loop with no cancellation arm, stop and write:
"Wait. Nothing else can stop this." Then add the arm.

Any node can stop other runs of its project from inside its own body, and
two ctx calls are the whole of it (`TagRun` and `StopTagged` are thin
wrappers over exactly these, with no privilege yours lacks):
`ctx.tag_execution([tag, ...]).await?`
puts tags on this run; `ctx.stop_tagged(tag, StopSelf::Keep).await?` stops
every older run of the project carrying the tag, waiting ones included (a
run parked on a person or a timer never wakes); `StopSelf::Include` stops
this run too. Tag first, then stop: a stop only reaches runs that put the
tag on before this one did, so when two runs race, the later one survives.
Both calls are safe to re-run after a crash (a repeated tag keeps its place
in the order, a repeated stop finds its targets already ended), so neither
goes through `ctx.run`. A tag is `[A-Za-z0-9_-]{1,64}`; the ctx refuses
anything else before writing. In the `fake` tier nothing is stopped:
`rig.execution_tags()` and `rig.stops()` record what the node asked for, so
you assert on those.

## The special shapes

**Access node**: the whole body is `weft::access_node!(MyServiceAccessNode);`
plus a `service` recipe in metadata (acquisition fields with `secret: true`,
auth steps, a test URL, an identity template). The macro reads the `account`
input and pulses it on `access`. It has no `tests.rs`: the macro is the whole
body, so there is nothing of yours to test, and the review does not ask for
one. Credentials live sealed in the runtime's access store, never in the
project. The compiler synthesizes the runtime
"no connection picked" rule from the `service` block; a hand-written one is
a finding. `"connection_optional": true` inside the service block is
reserved for a node that genuinely runs unconnected. That node is the one
access shape the macro cannot serve (`access_node!` on a
`connection_optional` service fails loudly at run time): it writes its own
body and reads the pick with `ctx.inputs.access("<picker input>")?`, which
returns `None` when nothing is picked.

**Infra node**: `"requires_infra": true`, plus `images` (dirs with a
Dockerfile the CLI builds) and `publishes` (the service name it hands out).
Implement `async fn provision_infra(&self, ctx, input) -> WeftResult<InfraSpec>`
returning the desired-state spec; the engine applies it, then calls `run`.
A container that serves `/live` (named by `features.liveEndpoint`) has a
**display**, and "The display" below is its whole contract: the shape, the
four item types, the `/action` envelope and a worked example. Every state
the container can sit in has
a button that leaves it, offered in every state: the WhatsApp bridge's
"Disconnect phone" drops the pairing and shows a fresh QR code whether
the bridge is paired, stuck, or half way through pairing; the Postgres
node's "Reset password" mints a new one over the database's own socket.
You walk the container's states and ask what a user does from the graph to
leave each; a state whose only exit is restarting or terminating the
infra fails [the review].
An endpoint is `Expose::ClusterInternal` unless you say otherwise, and only
weft nodes can reach it. `Expose::SameNetwork` on an endpoint makes it
reachable from the machine the runtime runs on, so a client that is not a weft
node can speak the service's own protocol to it: a frontend needing the
program's database for its sign-in tables, a `psql` session, a dashboard. It
means that machine on a local install (the port binds to loopback) or the
cluster's own subnet in a deployed one, never the internet.

The endpoint saying so IS the door. Nothing opens or closes one after the
fact, and nothing in a project's source can reach past what your spec
declared, so reading your node tells anybody what is reachable.
`weft infra list-doors` prints the addresses, because the port is the
cluster's to allocate and is the one part not in the source.

Rarely does every user of your node want that, so give them the choice rather
than making it for them. `provision_infra` runs with your inputs already
computed, so you branch on one like any other decision, off by default:

````rust
let reachable: bool = input.get("reachable")?;
...
expose: if reachable { Expose::SameNetwork } else { Expose::ClusterInternal },
````

One rule comes with it, and a node that breaks it fails [the review].
**An endpoint that hands out a credential is never `SameNetwork`**, whatever
guards it: `PostgresDatabase` can open `sql`, where reaching Postgres still
costs a password, and leaves `credential`, the little server that mints that
password, cluster-internal for ever.

Then one judgement call, which is yours and not a rule. A reachable endpoint
carries the connection your node publishes, the same one the program's own
nodes use. Where the service can express a NARROWER identity and the wider one
could destroy what the program depends on, minting a second one is worth it (a
database role with its own schema, a broker user scoped to its own topics),
because what comes through is usually somebody's frontend written fast. Where
the service has no such notion there is nothing to mint and one connection is
the right answer. Either way it is another input, not a decision you make for
the author: only they know what they are letting in.

Anything that runs inside the image obeys two rules, without exception:
every failure writes one line with its cause to stdout or stderr (so `weft
infra logs <node>` shows it; a library logger set to silent is no log, and
an install-time optional dependency is a silent skip waiting to happen, so
pin it), and every answer to a node is an error unless the thing asked for
fully happened (a message id for a message that will never show, a partial
result with nothing said about the gap, a skipped step: each is an error,
and the node reading the answer fails on it). A silent failure fails
[the review] outright.

**Trigger**: `"features": { "isTrigger": true }` and implement
`async fn setup_trigger(&self, ctx)`, called instead of `run` at activation,
where the node registers its wake signal. Polling triggers carry an
`intervalSecs` config; activation starts from now, history never replays.
Declare `firesWith` too: EVERY field the wake payload can carry, with `?` on
the ones that only sometimes arrive. The engine checks a real firing (and a
hand-typed `weft run --fire`) against it before your `run` even starts, and
the check is EXACT: a payload missing a field you marked required is refused,
and so is one carrying a field you never named. Name only the fields you fan
onto ports and the trigger dies the first time the provider sends anything
else. When the payload comes from a connection's events, the list is already
written down: `events.<topic>.fields` in the service's recipe is exactly what
a firing can carry, so copy those names. Skip it only when there is truly no
payload shape to name (a trigger that reads its own connection rather than
the wake payload, or one whose fields are per-instance config the author
typed in) and say why in the report.

A trigger's display is NOT yours to write: the signal KIND serves it, inside
weft. "The display" below says what that means for you.

**The wire limit**: a value emitted on a port is at most 100 KB, checked on
every emission (and on every yielded item of a stream), and a bigger one
fails the node right there: `port 'results' of 'search' carries 107 KB; a
wire carries at most 100 KB`. Nothing downstream can trim it, because the
check is on what YOU emit, so the node bounds its own output: a cap input
where the size comes from the outside world (`WebSearch`'s `maxTextChars`
cuts each page's text at the provider), and storage for anything that is
bytes rather than a value: `ctx.storage(scope).put(..)` and emit the file
value, which weighs a few hundred bytes whatever the file does. A node
whose output CAN exceed the limit on a bad day (a long article, a big
listing) and does neither is a node that fails on that day.

**A file input**: the value on an `Image` / `Audio` / `Video` / `Blob` port
is the stored-file marker, and inside the running node it also carries a
`url` minted for this firing (an hour), so a body that only speaks URLs (a
Python snippet, a provider's API) reads it straight off. A Rust node reads
bytes through `ctx.storage(...).get_bytes(&handle)`. The link never leaves
the node: everything you emit, park, or memoize is stripped back to the
stored form, and the journal never holds one. The marker never leaves weft
either: what goes to a provider, a bridge, a form a browser renders, or a
live item is the plain thing that consumer reads (a URL string, a `data:`
URL, a plain `{ url, mimeType, filename }`), through
`ctx.storage(scope).externalize` for a typed value or `public_link` /
`presign` for one file. A marker wrapped around a link in an outbound
payload renders as nothing.

**A bus**: a live channel one node opens and others read, for anything that
has to move while the run is still going (rows kept live, a model's deltas,
audio frames). A `Bus` port is wire-only, and what travels it is a marker,
not the data.

If you want to produce one, `ctx.open_bus(port, BusOptions::default(), "watch")`
is the whole ritual in one call: it creates the bus, pulses its marker on that
output port, and registers you on it under that name. Then
`bus.send("<kind>", json!(value))`, or `send_bytes` on a `Bytes` bus. The
options, all declared at creation and read back by every consumer: `payload`
(`Json` by default, `Bytes` for media frames, wrong shape refused loud), `meta`
for facts a reader needs about the stream (sample rate, model name),
`ephemeral` to keep payloads out of the journal, `window` for how far a slow
reader may fall behind (64 frames).

A bus must be closed, because a reader parked on one that never closes waits
for ever. The handle `open_bus` gives you closes it when it drops, on every
exit path including the failing one, so what you have to get right is not
holding it past the work: a node that runs until the run ends waits on
`ctx.cancellation()` and returns, and that drop is what the reader downstream
sees as the end of the feed.

If you want to read a bus the graph handed you, the choice is what you must not
miss:

| Call | Reach for it when |
|---|---|
| `ctx.join_bus(port, name)?`, then `bus.cursor()` | you take part in the exchange and want what is sent from now on |
| `ctx.bus_from_input(port)?`, then `bus.cursor_from_start()` | you must not miss what was sent before you attached, or the producer may already have finished |

`join_bus` closes on drop like `open_bus`; `bus_from_input` never closes, which
is what an observer wants, because ending a feed the producer still owns is a
bug. Read with `cursor.next().await` for every entry, or
`cursor.next_json("<kind>").await` for one kind's payloads. `None` means the
bus closed.

Do every bus read and wait on your own task, and never move a handle or a
cursor into a `tokio::spawn`. The engine works out "everyone is stuck, close
the buses" by watching what each node execution waits on, a wait on a task you
spawned is invisible to it, and it then tears down a live conversation or
hangs. If you need concurrency, that is another node exchanging over the bus.

Its tests go on the `fake` tier, because all of this is async. The two rig
calls face opposite ways, so read them once before writing the suite:

- `rig.seed_bus(opts)?` mints a bus and answers `(writer, marker)`, for driving
  a node's bus INPUT: put the marker on the input port,
  `writer.register("<name>")`, `send`, then `writer.close()` so the node sees
  the end.
- `rig.bus(&outcome.outputs["<port>"])?` resolves a bus the node EMITTED, for
  reading back what it sent.

For the rest (streams, `yield_downstream` against `pulse_downstream`, what the
journal keeps), go and read `docs/src/nodes/streams-and-buses.md`.

## The display

A **display** is what a node shows on its body in the editor while it runs:
the WhatsApp bridge's QR code and then the phone that scanned it, the
password the Postgres node minted, the address a webhook trigger listens on.
The same feed also reaches a website or an app somebody built on the program,
through a token their operator minted, so write it for a stranger's screen
as much as for the graph.

One shape, whoever produces it:

```json
{ "items": [
  { "type": "image", "label": "Scan with WhatsApp", "data": "data:image/png;base64,iVBOR..." },
  { "type": "text",  "label": "Phone", "data": "not paired",
    "action": { "label": "Disconnect phone", "actionKind": "unpair",
                "confirm": "Detach the paired phone and show a new QR code?" } },
  { "type": "secret",   "label": "Password", "data": "hunter2" },
  { "type": "progress", "label": "Restore",  "data": 0.4 }
] }
```

| `type` | `data` is | drawn as |
|---|---|---|
| `text` | a string | a copyable box |
| `image` | anything an `<img src>` takes: a `data:` URI, a URL | the picture, inline |
| `progress` | a number from 0 to 1 | a bar |
| `secret` | a string | masked behind `••••` until the reader clicks the eye; copy hands over the real value either way |

`label` is required on every item. `action` is optional and at most one per
item: `label` is the button's text, `actionKind` is the name you answer to,
`confirm` (optional) is asked before the press, and `payload` (optional)
rides along with it.

### If the node is an infra node, you write it

Serve it from the container, and name the endpoint that serves it in
`metadata.json`:

```json
"features": { "liveEndpoint": "api" }
```

`"api"` is the name of one of the endpoints your `provision_infra` spec
publishes. Naming it is what opts the node in; leave it out and the node has
no display. Two routes on that endpoint, both called by weft itself:

- `GET /live` returns the object above. Weft asks about every three seconds
  while somebody is looking. Answer from current state, never from a cache:
  a QR code expires in under a minute. An answer that is not
  `{ "items": [...] }` comes back to the reader as an error naming what you
  sent, so do not answer `{}` or a bare array.
- `POST /action` receives a press as `{ "action": "<actionKind>", "payload": ... }`
  and answers `{ "result": { ... } }`. Put a refusal in
  `{ "result": { "error": "why" } }`; the reader sees that text as it is.
  Weft reads `/live` again right after the press, so whatever it changed
  shows at once.

Node.js, matching the shape above:

```js
app.get('/live', (_req, res) => {
  const state = bridge.getState();
  const items = [];
  if (state.status === 'qr_pending' && bridge.getQr()) {
    items.push({ type: 'image', label: 'Scan with WhatsApp', data: bridge.getQr() });
  }
  items.push({
    type: 'text', label: 'Phone',
    data: state.status === 'connected' ? state.phoneNumber : 'not paired',
    action: { label: 'Disconnect phone', actionKind: 'unpair',
              confirm: 'Detach the paired phone and show a new QR code?' },
  });
  res.json({ items });
});

app.post('/action', async (req, res) => {
  const { action, payload } = req.body;
  if (action !== 'unpair') return res.json({ result: { error: `no action '${action}'` } });
  await bridge.unpair(payload);
  res.json({ result: { ok: true } });
});
```

**Every state the container can sit in has a button that leaves it, offered
in every state.** The bridge's "Disconnect phone" drops the pairing and shows
a fresh QR code whether the bridge is paired, stuck, or half way through
pairing; the Postgres node's "Reset password" mints a new one over the
database's own socket. Walk the container's states and ask what a user does
from the graph to leave each. A state whose only exit is restarting or
terminating the infra is a dead end, and shipping one fails [the review].

### If the node is a trigger, you write nothing

The signal **kind** serves the display, from weft's listener, because the
kind is what mints the address and the auth at activation. Every node
declaring that kind gets the same panel, and a node cannot add to it. A
trigger's panel is also read-only, where a container's may carry buttons:
nothing about a registration is the reader's to change from there.

So a trigger of yours showing nothing useful is a change in weft itself,
`crates/weft-listener/src/kinds/<kind>.rs`, where the kind implements
`KindHandler::live` and returns the same items. That is not node work: say so
in your report and stop, rather than reaching for it from the node.

## deps.toml

```toml
[dependencies]
regex = "1"

[system.runtime.apt]
debian_12 = ["libpq5"]
```

Sections: `[dependencies]` (cargo), `[build-dependencies]`, `[system.build]`
/ `[system.runtime]` (OS packages per manager: `apt`, `apk`, `yum`, `brew`,
keyed by distro like `debian_12`, `ubuntu_24_04`, `alpine_3_19`, or
`default`), `[build.env]`.

You declare the version you actually built and tested against, current and
maintained: the latest stable or LTS, never a bleeding-edge major when a
stable one works, never a deprecated one. A version copied from an old
example unchecked, or an unpinned `*`, is a review finding; [the report]
names each dependency's version and where it came from.

## Tests

`tests.rs` exports `pub fn tests() -> Vec<NodeTest>`. The two cheap tiers are
NOT the same shape, and mixing them up is how a suite starts panicking:

- `NodeTest::basic(name, f)` takes a plain `fn() -> WeftResult<()>`: pure
  logic, no ctx, no rig, nothing async. The runner is already inside a tokio
  runtime, so building one in here (`Runtime::new().block_on(...)`) panics
  with "Cannot start a runtime from within a runtime". Anything async belongs
  on the other tier.
- `NodeTest::fake(name, f)` takes an `async fn(rig: FakeRig) -> WeftResult<()>`
  driving the node with `rig.run(&MyThingNode, json!({...})).await` and
  asserting on the result. Everything touching a ctx, a caller, a signal or a
  bus goes here. One that never finishes fails by name after 30 seconds
  instead of stalling the suite.

`NodeTest::live(...)` is the `live` tier. A test name states its assertion
("a_matching_case_takes_its_branch", not "test_switch"). You run
`weft test-node <type-or-package>` until green; the `live` tier asks first
and you never run it. You write the tests in the same change as the node.

## After writing the node

The catalog walk picks the folder up automatically; no registration exists.
Check it landed: `weft describe-nodes --node MyThing --compact` must
succeed, or re-run `weft validate`, which compiles against `nodes/` fresh.
Then use the type in `src/main.weft` like any catalog node. A custom type
name must not collide with an existing one (loud error, no shadowing).
