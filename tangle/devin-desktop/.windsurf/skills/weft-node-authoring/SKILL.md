---
name: weft-node-authoring
description: "Read before dispatching a node-smith (the brief and the review) and when an expert writes a node by hand: the node authoring manual, the test tiers, and the shapes half-done work takes. The node-smith reads this same file as its manual."
---

# Writing a custom node

Tangle reads the dispatch protocol and [the review] checklist. The `node-smith` subagent, and an expert taking the hand, read the manual that follows them.

Three terms hold across the file:

- [the contract] is the node's typed interface, and Tangle designs it: one job in a sentence; every input port (name, type, required or optional, and `accepts` only when a wire would be a mistake) and every output port (name, type); the service it wraps, if any; anything the surrounding program depends on (a form schema, a trigger registration, infra). Every value the node takes from the graph is its own input port. A `List` or `JsonDict` input whose elements would come from separate wires is the wrong shape: a list literal cannot hold a wire, so every program would need a `List` node in front of it just to feed the port. (A port that genuinely takes a list of things, like `LlmInference`'s `tools`, is fine: the program builds it with the `List` node, never with Python.) An open-ended set of values (a query's parameters, a template's holes) is declared with `canAddInputPorts` and read with `ctx.inputs.custom()`. `PostgresExecuteQuery` is the pattern to copy, and any node's own `features` block (`weft describe-nodes --node <Type> --compact`) says whether it carries the flag, so you confirm there rather than trusting a name you remember.
- [the report] is what the node-smith hands back, and the only thing Tangle sees of its work.
- [the tiers] are the three test tiers a node carries: `basic` (no external world at all), `fake` (a stubbed client), `live` (the real service, real credentials, real money). `weft test-node <Type>` runs `basic` and `fake`, fast and free. Nobody but the user runs `live`: the node-smith writes those tests, names the service and declares any fixtures the test cannot self-provide, and the user runs them later, with consent, through `/weft-live-test`. Live tests do not cover infra nodes for now (a live test needs a service with a stored connection): an infra node is tested inside a real program, with the carving and fake-value tools of the `weft-sdp` skill (`--from`, `--emit`, `--target`).

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
- **body smells**: `.ok()` discarding an error, a default value standing in for a missing input, a retry loop, orchestration inside the node, a database client or a process opened fresh on every run where `ctx.shared` would hold one.
- **an infra output left unbaked**: an infra node's output that holds as long as the infra does (an address, a connection, a handle) with no `"baked": true`, so every run that reads it runs the node to make it again.
- **a dead end in an image**: a state an infra container can sit in (a dead pairing, a lost credential, a revoked session) with no button on its display that leaves it, so the user's only way out is restarting or terminating the infra. Every such state gets an action, offered in every state; the rule is under Infra node.
- **a marker in an outbound payload**: a `__weft_<kind>__` wrapper handed to a provider, a bridge, a form spec or a live item, instead of the plain URL, `data:` URL or `{ url, mimeType, filename }` that consumer reads; the rule is under A file input.
- **silent failure in an image**: a service inside an infra image that fails a step without writing a line to its log, or answers the node with a success when the thing asked for did not fully happen; the two rules are under Infra node.

## The manual

A node does one thing: calls an API, transcribes audio, writes a row. It never orchestrates: looping, retrying, branching and waiting for a person are the graph's job. It never does plumbing (transport, credentials,
acknowledgement protocols, subscriptions, retry bookkeeping are the
language's).

A new node goes beside the module that uses it under `src/`
(`src/billing/stripe/` next to `src/billing/charge.weft`), or under
`nodes/` when several parts of the program share it, and is usable by its
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
| `features` | flags: `catchErrors` (the node reaches outside; weft adds the `error` output and catches the body's failures onto it), `pure` (the body does nothing outside its run beyond answering its caller and the run's own files: no network, connection, `ctx.run`, wait, tag, or storage beyond this run; a durable run starts it without waiting for the database and runs it again after a crash if it had sent nothing on and reads no stream. `Text`, `Switch` and `Reply` are pure, `HttpRequest` is not; an outside ctx call on a pure node fails, naming the flag), `isTrigger`, `canAddInputPorts` (an open-ended set of values arrives as ports the author declares inline; the body reads `ctx.inputs.custom()`), `canAddOutputPorts`, `optionalCustomInputs`, `customInputType`, `oneOfRequired`, `showDebugPreview`, `liveEndpoint` (the endpoint serving this infra node's display, see [The display](#the-display)), `castPorts`, `hidden`, `answersCaller` (`"whole"`, `"stream"` or `"end"`: this node answers a live caller the way `Reply`, `Stream` or `Close` does; any custom node that answers the caller declares it, so the compiler counts it), `liveConnection` (`"http"` or `"websocket"`: this trigger holds a caller, as `Route` and `Socket` do) |
| `portsFromConfig` | ports derived from a config list: `{ "field", "matchInput", "specs": [{kind, keyField, catchAll?, addsInputs, addsOutputs}] }` |
| `firesWith` | trigger only: EVERY field a firing can carry, name to weft type, `?` on the name for sometimes-present (`{"scheduledTime": "String", "caller?": "JsonDict"}`). Checked exactly: a firing missing a required field is refused, and so is one carrying a field you did not name |
| `display` | inline render: `{ "kind": "media" \| "link", "output" \| "input": "<port>" }` |
| `validate` | declarative rules: `{ "when": {...}, "then": {message, level: "structural"\|"runtime", field} }` |
| `requires_infra`, `images`, `publishes` | infra nodes |
| `service` | access nodes only, the connection recipe |
| `accessApps` | project-shipped OAuth apps |

Input entry: `name`, `type`, `required`, `requiredWhenWired`, `accepts`,
`widget`, `default`, `label`, `placeholder`, `description`, `requiresScopes`,
`requiresValues`.

If an unwired input means something of its own (no `instance`: the program's
own copy), mark it `requiredWhenWired`, not `required`: unwired, the node runs;
wired, a wire that delivers nothing skips the node instead of falling back to
that meaning. If an optional input needs one more permission when it is used,
leave it out of `requiresScopes`, which every program using the node is asked
for. In `run`, when the input is used and before `ctx.client(&account)`, copy
`account.required_permissions()` (it already holds the declared ones), push the
extra one, and pass the list to `account.with_required_permissions`. Opening
then refuses a connection that lacks it, naming the permission.

A `select` or `multiselect` widget's `options` are a closed list: a program
writing anything else fails with `literal-not-an-option`. Keep it closed when
your code matches on the value. When the list is only the ones you know of
and a provider can add more (model ids, voices), add `"free_text": true` to
the widget, and the options become suggestions.
A `code` widget's `language` is one of `python`, `javascript`, `sql`, `json`;
any other word fails to load. `json` also fits a `JsonDict` input
(edited as JSON text); the others edit a `String`. A `number` widget with a whole `step`
(`"step": 1`) takes whole numbers only: the compiler checks a written value
and the runtime a wired one, so your body can cast it to an integer.
Output entry: `name`, `type`, `description` (an output has no `required`), and on an infra node `baked` (see Infra node).

A `validate` rule's `when` is a closed set of conditions, combined with
`all`, `any` and `not`: the input and config checks (`input_satisfied`,
`input_wired`, `output_wired`, `config_present`, `config_equals`, ...), plus
the graph checks below. A rule never names another node's type: it selects
nodes by a feature they declare, with `with: {feature: value}`.

- `run_reaches` (`direction: "downstream"` or `"upstream"`, `with`): the run
  this node is part of holds a node with that feature. `Route` checks
  `{"answersCaller": true}` downstream; `Reply` checks
  `{"liveConnection": true}` upstream.
- `downstream_of` (`with`): a node with that feature runs before this one
  along the wires (how `Reply` refuses to follow `{"answersCaller":
  ["stream"]}`).
- `per_instance`: this node exists once per instance: marked
  `@per_instance`, with an `@instance_filled` field, or reading one of
  those. A group that receives a per-instance value on any input counts for
  everything inside it and everything reading its ports. A node that cannot
  be copied per instance (a trigger serving one public address) refuses it
  with a rule, and the rule can be conditional (`all` with a config check).
- `input_names` (`port`, `names`): every name written in that input (a
  String, each String of a list, or each key of an object) is something of
  this program. `names: {"node": {}}` is a node, narrowed with optional
  `role: "infra"` or `"trigger"` and `per_instance: true/false`;
  `names: {"field": {}}` is a field written `node.field`, narrowed with
  optional `instance_filled: true/false`. Wrap it in `not` to refuse a bad
  name. A wired name is not known yet, so the rule does not fire on it.

`{with}`, `{per_instance_reason}` and `{names}` in the message name what the
check found (`{per_instance_reason}` says why, path included: "it reads
'bridge'", "it sits inside group 'work', which receives 'bridge'"). Copy a real rule from `Route`, `Reply` or `StartInfra`
before writing your own.

`required` is written only as `"required": true`, on an input the node cannot
run without. You leave the key off every other input: absent already means
optional, so `"required": false` says nothing and reads as though you meant
something by it.

`accepts` is the list of drivers the port takes, `["literal", "wire"]` when
absent, and absent is right for almost every port. You write `["wire"]` only
for a port that needs a real node (a provider, a stored file, an `Access` handle
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

A node that reaches outside (a service, a database, a file, a model) can fail
in a way a program wants to handle, so it lets the program choose: it sets
`"features": { "catchErrors": true }` in its metadata and writes no error
handling at all. Weft gives it an `error` output and does the catching:

```json
"features": { "catchErrors": true }
```

With `error` wired, a failure of the body becomes its message on `error`
and every output the body had not emitted closes; unwired, the failure fails
the run. A bad setting, input or type always fails the run. The body stays
loud: it returns its errors with `?` and `node_bail!` exactly as any node
does. Weft owns `error`: declaring it in the metadata is refused, and so is a
body emitting on it. A node that only shapes values (a cast, a switch, a
template) has nothing outside to fail on and does not set the flag.
`ctx.is_output_wired(port)` tells a body whether anything reads a port. A
`fake` test of the caught path calls `rig.wire_output("error")` first; a rig
wires nothing by default, so without it the failure fails the test run.

**Calling a service.** `weft::access::client` already holds what every API
node repeats, so you never write it again: `get_json` / `post_json` (send,
refuse a non-success status quoting the provider's own words, parse),
`json_call` for a request you prepared yourself, `checked_send` when the
answer is not JSON, `require_ok_flag` for a service that answers 200 and says
`ok: false` in the body, `cursor_paged` with a `CursorPaging` (its
`past_cap_hint` tells the user what to narrow when the list runs past the page
cap) for a cursor-paged list, and `required_str` for a field you cannot do
without. To hand a stored file to a provider, `ctx.storage(scope).external_url(&file)`
gives its public link or a `data:` URL (`external_file` adds the mime type
and filename); never write that fallback yourself. To emit a struct, use
`NodeOutput::new().set_serialized(port, &value)?`.

**Waiting on a provider's job** (a render, a dub, a crawl) is
`ctx.await_signal(PollEndpoint { .. })`, never a sleep loop in the body,
which holds a worker for the whole job. Submit inside `ctx.run`, then wait:

```rust
use weft::signal::{PollEndpoint, Predicate};

let submitted = ctx.run("submit", || async {
    post_json(&http, &submit_url, &payload, "submitting the render").await
}).await?;
let id = submitted["request_id"].as_str().node_err("the submit answered no request_id")?;
let status = ctx.await_signal(PollEndpoint {
    url: format!("{API}/requests/{id}/status"),
    interval_secs: 5,
    access: Some(weft::primitive::AccessRef::from(&account)),
    filters: vec![Predicate::neq("status", "IN_QUEUE"), Predicate::neq("status", "IN_PROGRESS")],
    ..Default::default()
}).await?;
```

The worker goes away while it waits. The first check runs at once, then
every `interval_secs` (under 5 is refused); the first answer passing every
filter is what `await_signal` returns, and checking stops. A failed check is
tried again at the next one. The body then replays from the top, which is why
the submit sits in `ctx.run`: without it the replay starts and pays for a
second job. Filter on "not in flight" as above, so an unknown status ends the
wait and your code can fail on it. `delta` is refused here. Read the
stdlib's `nodes/base_catalog/ai/fal/fal.rs` (`run_queued`) for a whole worked case.

You emit only through `ctx.pulse_downstream(NodeOutput::new().set(port, value))`;
ports you did not emit are closed, which is the skip signal downstream. For
user-added output ports use `ctx.fan_declared(...)`.

**Share what is slow to build**: a client pool, a process. `ctx.shared(&access, |opened| async move { .. })` builds it the first time a run asks and hands every later run on that worker the same one; it is built again when the connection's values change, or after nobody used it for the project's idle window (300 seconds by default, `weft workers set --shared-idle-seconds`). The closure returns a `WeftResult` of the thing and you get a handle you use as the thing. `ctx.shared("name", |()| async { .. })` is the same when nothing is built from a connection, and `.with_limit(n)` before the `.await` lets at most `n` runs of a worker hold it at once (a pool kept under a database's connection limit). A database client opened on every run pays the connection each time; for a worked pool, go and read `nodes/base_catalog/postgres/postgres.rs`.

If the worker dies while your body runs, a fast run (the default) ends cancelled, and a durable run fails the step: its start was on record before the body began, so the next worker knows it was running and fails it rather than start it again. With `catchErrors` and `error` wired, that failure goes to `error` like any other. A node marked `pure` is the exception: a step of it that had sent nothing on and read no stream runs again, since it did nothing anyone could see. The one other body weft starts again on purpose is one parked on `ctx.await_signal`, which replays from the top when its answer comes, so the work before the wait goes through `ctx.run(...)`, which
gives back the recorded result. In a run that cannot pause (a route's caller on the line, `recorded: false`, a bus open), `ctx.await_signal` holds in your call instead and returns the answer without a replay; if the run stays quiet for its trigger's `holdSecs`, the call fails with `WeftError::WaitGaveUp`, which your node may match and handle (test it with `rig.signal_given_up()`).

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
Both calls are safe to repeat when the body replays after a wait (a
repeated tag keeps its place in the order, a repeated stop finds its targets
already ended), so neither goes through `ctx.run`. Pass any non-empty string
as a tag, a chat id or an email included: the ctx keeps a valid tag
(`[A-Za-z0-9_-]{1,64}`) as it is and turns anything else into one (other
characters become `_`, plus a short fingerprint of the original), the same
way in both calls, so never clean a tag yourself. Only an empty tag fails. In the `fake` tier nothing is stopped:
`rig.execution_tags()` and `rig.stops()` record what the node asked for, so
you assert on those.

A program with instances (separate running copies of part of it, each under
its own id) reaches them from a node through the ctx.
`ctx.instance()` is which instance this run is for (`Option<&InstanceId>`, `None` for a run
for no instance, `.as_str()` for the id). The rest name an instance with `.instance(id)`
and end in one call:

| Call | Answers |
|---|---|
| `ctx.infra(node).instance(id).start()` | `()` once the instance's container runs (the run parks between looks); fails with its `failure` |
| `.stop(spec, stop_self)` / `.terminate(spec, stop_self)` | `()`; `spec` is a `DeactivateSpec`, `stop_self` a `StopSelf` |
| `.status()` | `Option<InfraCopy>`: `{ node, instance, status, failure }`, `None` when there is no container |
| `ctx.infra(node).copies()` | `Vec<InfraCopy>`, the shared container and each instance's |
| `ctx.triggers().instance(id).activate()` / `.deactivate(spec, stop_self)` | `()`; `.only([..])` narrows to named triggers |
| `ctx.values().instance(id).get()` | `InstanceValues`: step -> field -> value, what was given for the instance's `@instance_filled` fields |
| `ctx.values().instance(id).set(step, field, value).clear(step, field).apply()` | `Vec<String>`, the instance's triggers set up again; each value is checked against its node first, all or none |
| `ctx.values().instance(id).forget()` | `Vec<String>`, as `apply()` |
| `ctx.connections().instance(id).list()` / `.forget()` | `Vec<GrantSummary>` / `u64` forgotten |
| `ctx.costs().instance(id).service(s).node(n).paid_by(p).since(unix).list()` | `Vec<CostRecord>`: `{ run, instance, node, service, model, amount_usd: Option<f64>, paid_by, at_unix }` |
| `ctx.runs().instance(id).status(s).older_than(d).clean(running, stop_self)` | what was cleaned |
| `ctx.tokens().mint_for_instance(id, expires_in)` / `.instance(id).revoke()` | `MintedInstanceToken { id, token, expires_at_unix }` / `()` |

The types are in `weft::program` (`InfraCopy`, `CostRecord`, `PaidBy`,
`MintedInstanceToken`). Every call is journaled, so none goes through
`ctx.run`. The `instances` package already wraps most of them (`CurrentInstance`,
`StartInfra`, `ListInstanceInfra`, `InstanceCosts`, `SetInstanceValues`, ...), so check it before
writing one. In the `fake` tier, `rig.instance("user-42")` makes the run a run
for that instance, `rig.answer_program_call("weft.infra.status", json!(..))`
queues the answer to one call by its journal name (`weft.infra.copies`,
`weft.costs.list`, ...; several queue in order), and `rig.program_calls()`
records what the node asked for.

## The special shapes

**Access node**: the whole body is `weft::access_node!(MyServiceAccessNode);`
plus a `service` recipe in metadata (acquisition fields with `secret: true`,
auth steps, a test URL, an identity template). The macro reads the `account`
input and pulses it on `access`. It has no `tests.rs`: the macro is the whole
body, so there is nothing of yours to test, and the review does not ask for
one. The test URL must pass whatever permissions the person ticked: when it
needs a permission of its own to learn who the account is, declare that
permission with `"always": true` and it is asked for on every connection
(Google's `openid` and `userinfo.email`; the service docs' "Permissions"
section has the rules). Credentials live sealed in the runtime's access store,
never in the project. The compiler synthesizes the runtime
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

**Bake every output that holds as long as the infra does** (an address, a
connection, a handle): `"baked": true` on the output in `metadata.json`. A baked output is made when the infra is applied, and a run that reads nothing from your node but baked outputs uses the saved values instead of running it, so a route that queries a database never calls the database's side container. A run that also reads an output that moves on its own (a status, a phone number a bridge pairs later) runs your node, and a baked output your `run` sends nothing on then carries the saved value. weft bakes again every time the infra is applied, and on `weft infra rebake <node>`. In between, the container itself tells weft when a value changes (a reset button, a rotated password): it `POST`s `{"connection": {"password": "..."}}` to `$WEFT_VALUES_URL`, which weft sets on every container of the unit (`connection` changes values inside the connection your node published, `outputs` changes baked outputs by name). Only what your node already handed weft can change. The push answers `200` once weft has written the value, or `422` with a body naming the key or output it could not change. If the push came from a button and failed, answer the press with `{ "result": { "error": "..." } }` telling the person to run `weft infra rebake <node>`. The value itself never goes in the press's answer. The Postgres node's reset is the worked example (`nodes/base_catalog/postgres/database/images/credential/bootstrap.py`). Changing what `run` emits on a baked output is not noticed on its own: run `weft infra upgrade`, which rebuilds the infra against your current source and bakes again. A step that fails using a connection an infra node made says to run `weft infra rebake` on that node; your node's own error says only what the driver said.

Every field of the `InfraSpec` (the machine and its GPU, containers, probes, disks,
endpoints), long jobs, kept files and the rig calls for all of it are in
[Infra node reference](#infra-node-reference) below.
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
An endpoint is `Expose::Project` unless you say otherwise, and only
weft nodes can reach it. `Expose::SameNetwork` on an endpoint makes it
reachable from the machine the runtime runs on, so a client that is not a weft
node can speak the service's own protocol to it: a frontend needing the
program's database for its sign-in tables, a `psql` session, a dashboard. It
means that machine on a local install (the port binds to loopback) or the
install's private network on a cloud install, never the internet.

The endpoint saying so IS the door. Nothing opens or closes one after the
fact, and nothing in a project's source can reach past what your spec
declared, so reading your node tells anybody what is reachable.
`weft infra list-doors` prints the addresses, because the port is the
install's to allocate and is the one part not in the source.

Rarely does every user of your node want that, so give them the choice rather
than making it for them. `provision_infra` runs with your inputs already
computed, so you branch on one like any other decision, off by default:

````rust
let reachable: bool = input.get("reachable")?;
...
expose: if reachable { Expose::SameNetwork } else { Expose::Project },
````

One rule comes with it, and a node that breaks it fails [the review].
**An endpoint that hands out a credential is never `SameNetwork`**, whatever
guards it: `PostgresDatabase` can open `sql`, where reaching Postgres still
costs a password, and leaves `credential`, the little server that mints that
password, project-only for ever.

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

### Infra node reference

Every type below is in `weft::infra`. The spec exists only in Rust, as what
`provision_infra` returns; `metadata.json` carries just `requires_infra` and
`images`. A whole spec for one container on a GPU, from the e2e fixture
`infra_gpu`:

```rust
use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, Expose, Gpu, Image, InfraSpec, MachineShape, Probe, Protocol, Unit,
};
use weft::{InfraProvisionContext, ValueBag, WeftResult};

async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag) -> WeftResult<InfraSpec> {
    Ok(InfraSpec {
        units: vec![Unit {
            name: "probe".into(),
            containers: vec![Container::new("app", Image::Local { name: "gpu_probe".into() })
                .with_ports(vec![ContainerPort { name: "http".into(), port: 8080, protocol: Protocol::Tcp }])
                .with_readiness(Probe::http("/health", 8080).with_initial_delay(1))],
            machine: MachineShape { gpu: Some(Gpu { kind: "nvidia-l4".into(), count: 1 }), ..Default::default() },
            ..Default::default()
        }],
        endpoints: vec![Endpoint {
            name: "api".into(),
            target: EndpointTarget::Unit { unit: "probe".into(), container: "app".into(), port: "http".into() },
            expose: Expose::Project,
        }],
        ..Default::default()
    })
}
```

**The machine** (`Unit.machine: MachineShape`, all optional):

| Field | Type | Example | Unset |
|---|---|---|---|
| `cpu` | `Option<String>` | `"0.25"`, `"2"`, `"500m"` | the sum of the containers' own limits |
| `memory` | `Option<String>` | `"1Gi"`, `"512Mi"` | same |
| `gpu` | `Option<Gpu { kind: String, count: u32 }>` | `nvidia-l4`, 1 | no GPU |
| `kind` | `Option<String>` | `"n2-highmem-8"` | picked from the numbers above |

A cloud install picks its cheapest machine that holds `cpu` and `memory`,
shared-core ones included (a quarter CPU with `1Gi` is Compute Engine's
`e2-micro`, about $6 a month). `kind` names the cloud's own machine type and
skips the pick; keep it a last resort. A local install caps each container
without limits of its own at `cpu` and `memory`. Whoever uses your node pays
for that machine, so if the right size depends on their load, give the node
inputs for it and read them into `machine` (the Postgres node takes `cpu`,
`memory` and `machineType`).

If you need a GPU on a cloud install, the kinds weft attaches are
`nvidia-l4` (1, 2, 4 or 8), `nvidia-tesla-t4` and `nvidia-tesla-p4` (1, 2
or 4) and `nvidia-tesla-v100` (1, 2, 4 or 8); any other kind or count is
refused at start, naming the accepted ones. A local install cannot pick a GPU
by kind: it hands the container every GPU the machine has and warns, and
refuses the unit when its Docker has no NVIDIA runtime.

The rest of `Unit`: `init_containers` (run one after another before the
containers, each to completion; one failing fails the start), `fs_group`
(a group id every mounted disk is writable by), `on_stop`
(`StopBehavior::Stop` by default; `StopBehavior::KeepRunning` leaves the unit
up through a project stop, for a model that takes long to load) and `health`
(`UnitHealth { flaky_after_seconds, recovery_after_seconds }`).

**A container** is `Container::new(name, image)` plus setters:
`with_command(Vec<String>)` (replaces the entrypoint), `with_args`,
`with_env(vec![EnvEntry::new("MODE", "prod")])`, `with_ports(vec![ContainerPort
{ name, port, protocol: Protocol::Tcp }])`, `with_limits(Limits { cpu, memory })`,
`with_mounts(vec![Mount::new("store", "/data")])`, `with_readiness(probe)`,
`with_liveness(probe)`, `with_run_as("uid")` or `"uid:gid"`.
`Image::Local { name }` is a folder listed in `images` (`images/<name>/Dockerfile`);
`Image::Upstream { reference }` is used exactly as written, so a moving tag
like `postgres:16` changes nothing and you pin by digest to make a new image land.

There is no log setting on a container. Whatever it writes to stdout or
stderr is what `weft infra logs <node>` shows. The node's own lines go
through `ctx.log(LogLevel::Info, "...").await?` (`weft::context::LogLevel`:
`Trace`, `Debug`, `Info`, `Warn`, `Error`).

**A probe** is built with `Probe::http(path, port)` (ready on a 2xx or 3xx),
`Probe::tcp(port)` (ready once the port accepts), or `Probe::exec(command)`
(ready when the command exits 0; use it when the service can answer for
itself, like `pg_isready`), then `.with_initial_delay(seconds)`. The other
fields are public with these defaults: `period_seconds` 10,
`timeout_seconds` 1, `failure_threshold` 3. With no readiness probe the
container counts as ready once it runs; a failing liveness probe restarts it.

**Disks**: `Volume { name, kind: VolumeKind::Disk { size: "10Gi".into(), class: None } }`
outlives stop and upgrade, and terminate deletes it unless its name is in
`InfraSpec.keep_on_terminate`. `VolumeKind::Scratch { size_limit: None }` is
shared scratch space, emptied every time the unit starts. On a `Mount`,
`sub_path` mounts one directory of the volume (an init container has to
create it) and `read_only` does what it says.

**Endpoints**: `Endpoint { name, target, expose }`. The target is
`EndpointTarget::Unit { unit, container, port }` (`port` is the
`ContainerPort` NAME) or `EndpointTarget::External { url }` for a service
that already runs elsewhere. `expose` is `Expose::Project` (the default),
`Expose::SameNetwork` (above), or `Expose::Public { path }`, which serves
HTTP through the install's front door at `/infra/<project>/<copy_id>/<path>`
and is reachable from the internet.

The node reaches an endpoint at run time with
`let api = ctx.endpoint("api").await?;`, which waits until something answers
there and hands back a handle: `api.url()`, `api.host_and_port()?`,
`api.public_url()` (`Some` only for `Expose::Public`), and
`api.call(EndpointMethod::Get, "/path", None).await?` or
`api.call(EndpointMethod::Post, "/path", Some(json!({..}))).await?`, which
answers the response as JSON (`EndpointMethod` is `weft::EndpointMethod`; GET
and POST are the two it has). The path starts with `/`. A non-2xx answer, a
network error or a body that is not JSON is an error. The call has no
timeout of its own, so a call that waits on slow work never returns while a
person presses stop: that is what the shape below is for.

The worker keeps what `ctx.endpoint`, `ctx.published_access`,
`ctx.publish_access` and `ctx.open` answered from one run to the next (weft
tells it when any of that changes), so those cost nothing after the first run,
but every `call(...)` to the service is a trip there on every run: quick
inside the install's own network, and still one more thing that can fail while
the service restarts. Keep `run` to reading what weft holds and handing it on,
plus whatever the service itself has to be asked every time.

**If other nodes need your service** (a bridge every send node talks
through), pass them `api.infra_handle()` instead of `api.url()`, because the address
changes each time the service is set up again. Declare an
output `{ "name": "bridge", "type": "Infra" }` and set it to
`api.infra_handle()`. The handle names your node's place and the endpoint,
plus the instance for a node marked `@per_instance`. A node that uses it
declares an input typed `Infra` and resolves it:

```rust
use weft::infra::InfraHandle;

let bridge: InfraHandle = ctx.inputs.get("bridge")?;
let bridge = ctx.endpoint_of(&bridge).await?;
let sent = bridge.action("sendMessage", json!({ "to": to, "text": text })).await?;
```

It returns the same kind of handle as `ctx.endpoint`. `action(name, payload)` posts
`{"action": name, "payload": payload}` to the container's `/action` and
answers its `result`; a `result.error`, or no `result` at all, fails the
node, like a non-2xx. The compiler refuses a `String` wired into an `Infra` input
and a written value, and a handle resolves only in its own project, to
infra the program declares, for the run's own instance.

**A long job** (a render, a training run, a batch on the GPU) never sits in
one `call`, and never in a loop that sleeps in the body either: that holds a
worker for the whole job. The container answers at once with a job id, runs
the work in its own background thread, shows progress on its display (the
`progress` item under [The display](#the-display)), and answers a status
route. The node starts the job inside `ctx.run` and parks on that route:

```rust
use weft::EndpointMethod;
use weft::signal::{PollEndpoint, Predicate};

let api = ctx.endpoint("api").await?;
let job = ctx.run("start the job", || async {
    api.call(EndpointMethod::Post, "/jobs", Some(json!({ "prompt": prompt }))).await
}).await?;
let id = job["id"].as_str().ok_or_else(|| weft::node_error("the service answered no job id"))?;
let state = ctx.await_signal(PollEndpoint {
    url: format!("{}/jobs/{id}", api.url()),
    interval_secs: 5,
    filters: vec![Predicate::neq("status", "running")],
    ..Default::default()
}).await?;
if state["status"] != "done" {
    return Err(weft::node_error(format!("job {id} ended {state}")));
}
```

The run parks and the worker goes away. The listener checks the route straight
away, then every `interval_secs` (5 is the floor), and the first answer
that passes the filters is what `await_signal` returns. The body then replays
from the top, which is why the start is inside `ctx.run`: without it the
replay would start a second job. `api.url()` is the address the workers
reach; the listener sits elsewhere on a local install, so before every check
it asks for the same endpoint's address as weft's own roles reach it, and
only this project's infra is looked up. The routes (`/jobs`, `/jobs/<id>`)
are your image's own API.

Stopping a parked run runs none of your code: the run ends cancelled, its
wait is removed, and the checks stop. The job in the container carries on.
If it should stop too, the container has to notice on its own, for example
by giving up on a job whose status nobody has asked for in a few intervals
(the listener asks every `interval_secs` for as long as the run waits);
terminating the infra stops it with everything else. To test the node, queue
the answer with `rig.signal(json!({ "status": "done", "result": ... }))`.

**Keeping a file past the run**: a file stored in `StorageScope::Execution`
with no keep is swept shortly after the run ends. If the file is what the
node produced, pass a keep when you store it, the way a speech node does:

```rust
use weft::storage::{KeepTtl, StorageScope};

let file = ctx.storage(StorageScope::Execution)
    .put(bytes, "audio/mpeg", "speech.mp3", Some(KeepTtl::Default))
    .await?;
```

or keep one you already hold with
`ctx.storage(StorageScope::Execution).keep(&file, KeepTtl::Default).await?`
(a keep cannot be taken back). `KeepTtl::Default` is 30 days,
`KeepTtl::Secs { secs }` is a number of seconds, and both start again at every
access; `KeepTtl::Never` lasts until `weft files rm` or `weft clean`. Files in
the project, shared or instance scopes outlive runs anyway: there, `None` or
`Never` means until deleted. A file the container itself needs to keep goes on
a `Disk` volume instead.

**The rig for an infra node** (`fake` tier):

- `rig.run_provision_infra(&MyNode, json!({..})).await.ok()?.infra_spec()?`
  is the spec your node declared, to assert on its units, volumes and
  endpoints.
- `rig.declare_endpoint("api", "http://wi-app:8080")` makes
  `ctx.endpoint("api")` resolve; without it the call fails the way it does
  when the infra is not running. Two endpoints on one address panic.
  `rig.declare_public_url("api", url)` gives it a `public_url()`.
- `rig.answer_endpoint("api", EndpointMethod::Post, "/jobs", json!({ "id": "j1" }))`
  queues the answer to the NEXT such call, one per call and in order, so a
  polling test queues one answer per look; a call with none left fails.
  `rig.refuse_endpoint(endpoint, method, path, status, body)` refuses one.
- `rig.endpoint_calls()` is every call the node made, each an `EndpointCall
  { place, endpoint, method, path, body }` (`place` is the infra node the
  call went to).
- `let bridge = rig.declare_infra("bridge", "api", "http://bridge:8090")`
  declares an endpoint another infra node shares and hands back its `Infra`
  handle, to put on the input; `rig.answer_infra("bridge", "api", method,
  path, answer)` queues its answers the way `answer_endpoint` does.
- `rig.logs()` is every `ctx.log` line as `(LogLevel, String)`.
- `rig.stored_meta(key)?.keep` and `.keep_ttl_secs` say whether a stored
  file was kept, and `rig.stored_files(&scope)?` lists them.

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

Every `put` makes a new stored file. A value that grows with use (a
conversation, a log, a document the node keeps adding to) passes 100 KB one
day, so a node that keeps one takes and gives back a FILE, never a value, and
never both: one file that grows, edited in place. To change it, call
`ctx.storage(scope).edit(&file, |old| ...)`: it reads the content, runs your
function on the bytes, and writes back what it returns. Two writers never
lose each other's change (a parallel loop, two runs on one project file): if
the file moved on in between, `edit` reads it again and reruns the function,
so the function depends on the bytes it is given and nothing else. The file
keeps its key, so every reference already handed out reads the new content,
and every edit is recorded on the run as a readable diff the graph shows
under "Files edited". `ctx.storage(scope).replace(&file, bytes)` overwrites
whatever is there without reading it. A one-shot answer (a reply, a fetched
page, a query's rows) stays a value: it does not grow through use.

**A file input**: the value on an `Image` / `Audio` / `Video` / `Blob` port
is the stored-file marker (`__weft_image__`, `__weft_video__`,
`__weft_audio__` or `__weft_blob__`, the kind taken from the mime type), and inside the running node it also carries a
`url` minted for this firing (an hour), so a body that only speaks URLs (a
Python snippet, a provider's API) reads it straight off. A Rust node takes
the input as a `FileHandle` (`let file: FileHandle =
ctx.inputs.get("file")?;`) and reads it with
`ctx.storage(StorageScope::Execution).get_bytes(&file).await?`, which hands
back `(meta, bytes)`: `meta.filename` and `meta.mime_type` are the file's
details. If you want the details without the bytes,
`StoredFile::from_value(&value)?` reads the value into `.key`, `.mime_type`,
`.filename` and `.size_bytes`. Never read `mimeType` off the raw JSON: it sits
one level down, inside the kind marker. The link never leaves
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
  `{ "result": { "error": "why" } }`; the reader sees that text as it is. A press that changed a value weft holds pushes it to `$WEFT_VALUES_URL` first (see Infra node, the bake paragraph). If that push fails, it answers with `{ "result": { "error": "..." } }` and tells the person to run `weft infra rebake <node>`.
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
An infra node gets no `live` test for now (a live test needs a service with
a stored connection): its `basic` and `fake` tests cover the node's own code,
and the container is proven inside a real program. Make a scratch project
(`weft new` in your scratch folder), copy the node in under `nodes/`, use it
in `src/main.weft`, then `weft infra start`, read `weft infra status` and
`weft infra logs`, run through it (carved with `weft run --from` / `--emit`
/ `--target`, the `weft-sdp` skill), and finish with `weft infra terminate
--yes` on that scratch project. Running the image by hand with `docker` to
check it builds and starts is fine too.

**The rig, in full.** What a `fake` test reaches for most:

- Canned answers from the outside service: `rig.respond(method, path,
  json)`, `rig.respond_status(method, path, status, json)`,
  `rig.respond_raw(..)`, `rig.respond_with_headers(..)`, and
  `rig.fail_connection(method, path)` for a service that cannot be reached
  at all (the node's `send()` errors, with no status and no body).

  How a request finds its answer. A route is the method, the path, and the
  query if you wrote one in `path` (`"/items?page=2"`). Query parameters
  match as a set: their order and their encoding (`a+b` or `a%20b`) never
  matter. When the node sends a request:
  1. a route with exactly the request's method, path and query wins;
  2. a request with no query takes the bare route (`"/items"`);
  3. a request with a query that matched nothing takes the bare route ONLY
     if no route on that path has a query. As soon as one does, an
     unmatched query fails the test, and the message lists the queries
     declared on that path.
  A route gives the same answer to every call that reaches it. You can
  declare each route only once (a second `respond` on the same route
  panics), so if the node calls one path several times and needs a
  different answer each time, tell the calls apart by their query:

  ```rust
  rig.respond("GET", "/items?page=1", json!({ "items": [1, 2], "next": 2 }));
  rig.respond("GET", "/items?page=2", json!({ "items": [3], "next": null }));
  // `GET /items` with no query would fail here: no bare route.
  // `GET /items?page=3` fails too, even if you add a bare `/items`,
  // because this path has query routes.
  ```

  ```rust
  rig.respond("GET", "/status", json!({ "state": "ready" }));
  // `GET /status`, `GET /status?verbose=1`: both get this answer,
  // because no route on `/status` has a query.
  ```

  The one place a repeated call gets a different answer each
  time is the node's own infrastructure (`answer_endpoint` below),
  whose answers are a queue. To check order, `rig.requests()` lists every
  request in the order the node sent it, matched or not.
- What the node sent: `rig.requests()` is a list of `SentRequest`, with
  `method`, `path`, `query`, `body_text`, `body`, `body_streamed` and
  `headers`, and `.header(name)` reads one header whatever its case.
  `rig.assert_sent(method, path)` is the short form.
- Files: `rig.store_file(filename, mime, bytes)` makes an input file,
  `rig.stored_bytes(key)` reads what the node stored, `rig.stored_meta(key)`
  and `rig.stored_files(scope)` the rest.
- The node's own infrastructure: `rig.answer_endpoint(endpoint, method,
  path, answer)` queues the answer to the NEXT call there, one per call and
  in order (a call with none left fails the test);
  `rig.refuse_endpoint(endpoint, method, path, status, body)` refuses one;
  `rig.endpoint_calls()` lists what the node called.
- Pressing stop: `rig.stop_after_calls(n)` stops the run, as a person's
  `weft stop` would, once the node has made `n` calls (web requests and
  endpoint calls, counted together). The call that reaches `n` still gets its
  answer; the node sees the stop at its next cancellation check. `0` stops it
  before it starts.
- `rig.hang_up_server()` is a port that accepts and hangs up at once, for a
  node that opens its own raw socket. Web calls in the `fake` tier never
  reach the network, so it is not how you fail one: use
  `fail_connection`.

## After writing the node

The catalog walk picks the folder up automatically; no registration exists.
The edit hook answers every write under `nodes/` with what `weft validate`
finds in `src/main.weft`. It only reports: it never changes, reverts or
undoes a file. It leaves out node types no folder declares yet (another
node's unwritten work, not your error); every other finding it prints is
yours to read.

Check it landed: `weft describe-nodes --node MyThing --compact` must
succeed, or re-run `weft validate`, which compiles against `nodes/` fresh.
Then use the type in `src/main.weft` like any catalog node. A custom type
name must not collide with an existing one (loud error, no shadowing).
