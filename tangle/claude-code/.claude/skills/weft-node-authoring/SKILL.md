---
name: weft-node-authoring
description: The node authoring manual and the dispatch protocol. Read before dispatching a node-smith (writing [the brief], running [the review]) and when an expert writes a node by hand. The node-smith subagent reads this same file as its manual.
---

# Writing a custom node

This file has three readers: Tangle reads the dispatch protocol and [the review] checklist, the `node-smith` subagent reads the manual below as its instructions, and an expert taking the hand reads the manual too.

## The dispatch protocol (Tangle)

A node is missing only after the catalog says so (a direct `metadata.json` read or a `catalog-scout` sweep). Then:

1. **Design the contract yourself.** One job, in a sentence. Every input port (name, type, required or optional, and `accepts` only when a wire would be a mistake) and every output port (name, type). The service it wraps, if any. Anything the surrounding program depends on (a form schema, a trigger registration, infra). The contract is the interface other wires will attach to; it is never the specialist's to invent. Every value the node takes from the graph is its own input port: never a `List` or `JsonDict` the program has to assemble from wires first (a list literal cannot hold a wire, so that shape forces a Python node whose whole body is `return {'params': [a, b]}`). When the set of values is open-ended (a query's parameters, a template's holes), the node declares `canAddInputPorts` and reads them with `ctx.inputs.custom()`, the way `ExecPython`, `Format` and `PostgresExecuteQuery` do.
2. **Dispatch one node-smith per node.** [the brief] is the contract plus the project context the specialist cannot see (what [stage] this node feeds, what the upstream types are). Several missing nodes go out in parallel, one specialist each; nodes that depend on each other's types go out in sequence.
3. **Run [the review]** on the report against the checklist below. A report that fails it goes back as a new dispatch; [the brief] for the redispatch carries the previous attempt's folder, the critique, and what to keep. You never fix the specialist's node yourself unless the fix is one line and obvious, because the next dispatch will need to know the pattern anyway.
4. **Wire it.** With the node green and in the catalog, it is a normal node type: read its `metadata.json` one more time as delivered, and write the weft code.

The short-circuit: if the catalog already holds a node that does the job, no specialist is dispatched; you go straight to the weft code.

### [the review] checklist

You never trust a report you can re-verify for the cost of one command, and everything important here can be re-verified.

**Re-verify first, always:**

- Re-run the tests yourself: `weft test-node <Type>` (local tiers, fast, free). The quoted output in the report is a claim; your run is the verdict. A report that claimed green and runs red is redispatched with the dishonesty named as the finding.
- Diff the delivered `metadata.json` against the report's port list yourself. Metadata drift (a port renamed or dropped between report and file) is redispatched.
- Read every test and ask one question: how would this test fail? A test with no answer (runs the node, ignores the result, asserts nothing about the outputs) is not a test, whatever its name says.
- `weft validate --file main.weft < main.weft` still passes with the node in the catalog, and `weft describe-nodes --node <Type> --compact` succeeds. The folder is under `nodes/`, never inside `nodes/base_catalog/`.

**Then check the contract and the body:**

- The contract held: no port renamed, added, or dropped; the one job is still the one job.
- No input asks the program to assemble values: a `List` or `JsonDict` input whose elements would come from separate wires is the wrong shape (it forces a Python node just to build the list). Each value is its own port, or the node declares `canAddInputPorts` for an open-ended set.
- A skim of `mod.rs`: no fallbacks, no swallowed errors, no retry loops, no orchestration inside the body; failures are loud.
- The live-tier tests are written (the real service path, with the service named and fixtures declared) and named in the report as not run: they spend real money, and the user runs them later through `/weft-live-test`.

**The half-arsing catalog.** Each of these fails the review and goes back as a redispatch with the specific finding:

- **smoke-only**: one test that runs the node once and asserts nothing.
- **happy-path-only**: the error paths (the loud `node_bail!` failures) are never exercised.
- **no closure test**: nothing covers an optional input arriving closed.
- **weakened assertions**: the test checks that an output exists, not that it holds the expected value.
- **swallowed in the test**: patterns like `if let Err(_) = ... {}` that pass on failure.
- **coverage gap against the contract**: a port behavior in the contract with no test that would fail if it broke. Count the tests against the ports: every port in the contract needs a test that fails if its behavior breaks, and a port with none is the finding.
- **live tests missing or hollow**: the contract names a service but there is no `NodeTest::live` entry for it, or the entry declares no service and no fixtures.
- **empty rig**: `tests()` returns an empty vec, or `tests.rs` does not exist, and the report did not say so.
- **flaky-dismissed**: an intermittently failing test waved off as flaky instead of chased to its race. A race in the node is the node's bug; a test made tolerant of it (a retry, a sleep, a longer timeout) is a patch on the symptom and fails the review on both counts.
- **body smells**: `.ok()` discarding an error, a default value standing in for a missing input, a retry loop, orchestration inside the node.
- **a dead end in an image**: a state an infra container can sit in (a dead pairing, a lost credential, a revoked session) with no button on its `/live` card that leaves it, so the user's only way out is restarting or terminating the infra. Every such state gets an action, offered in every state.
- **a marker in an outbound payload**: a `__weft_<kind>__` wrapper handed to a provider, a bridge, a form spec or a live item, instead of the plain URL, `data:` URL or `{ url, mimeType, filename }` that consumer reads. The stored form is weft's; what leaves weft is the consumer's shape.
- **silent failure in an image**: a service inside an infra image that fails a step without writing a line to its log, or answers the node with a success when the thing asked for did not fully happen (a silenced library logger, an optional dependency whose absence skips a step, a result read as done without checking). The node that asked must fail, and the pod log must say why.

The redispatch brief carries the previous attempt's folder, the specific finding (not "do better"), and what to keep. A redispatch that comes back with the same finding gets the finding restated in one sentence and nothing else; the third identical failure comes back to the user as "this contract is not landing, here is what I suspect", because at that point the blocker is probably real.

If the report is blocked rather than green, judge the blocker: a real impossibility comes back to the user as "this cannot be done honestly, here is the closest shape"; a soft blocker (missing docs, rig limits) goes back out with what you know.

## The manual

A node does one thing: calls an API, transcribes audio, writes a row. It
never orchestrates (looping, retrying, branching, waiting for a person are
the graph's job, and the engine gives journaling, resumability, and
cancellation for free) and it never does plumbing (transport, credentials,
acknowledgement protocols, subscriptions, retry bookkeeping are the
language's). A real node body is usually under a hundred lines.

When the catalog lacks a capability, the node goes in this project's `nodes/`
folder and is immediately usable by its `type` name. A node's body may only
`use` the `weft` crate, the crates its package declares in `deps.toml`, and
code inside its own package; a sibling package's code is never on its path.
The build compiles the node's Rust directly.

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
under `nodes/base_catalog/`: it is wiped by `weft catalog update`.

A package is also the limit of what `use` can reach: a project's own node
sees its own package's shared files and nothing under `nodes/base_catalog/`.
You cannot `use` a stdlib helper such as `elevenlabs.rs` from a project
package, because each package compiles as its own crate. If you need one
function from a stdlib helper, copy it into your package's own shared file
and say so in the report. If you need a whole capability, report it as a ctx
feature the language is missing.

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
| `features` | flags: `isTrigger`, `canAddInputPorts` (an open-ended set of values arrives as ports the author declares inline; the body reads `ctx.inputs.custom()`), `canAddOutputPorts`, `optionalCustomInputs`, `customInputType`, `oneOfRequired`, `showDebugPreview`, `liveEndpoint`, `castPorts`, `hidden` |
| `portsFromConfig` | ports derived from a config list: `{ "field", "matchInput", "specs": [{kind, keyField, catchAll?, addsInputs, addsOutputs}] }` |
| `display` | inline render: `{ "kind": "media" \| "link", "output" \| "input": "<port>" }` |
| `validate` | declarative rules: `{ "when": {...}, "then": {message, level: "structural"\|"runtime", field} }` |
| `requires_infra`, `images`, `publishes` | infra nodes |
| `service` | access nodes only, the connection recipe |
| `accessApps` | project-shipped OAuth apps |

Input entry: `name`, `type`, `required`, `accepts`, `widget`, `default`,
`label`, `placeholder`, `description`, `requiresScopes`, `requiresValues`.
Output entry: `name`, `type`, `description` (an output has no optionality).

`accepts` is the list of drivers the port takes, `["literal", "wire"]` when
absent, and that absence is the right answer for almost every port. Leave it
alone unless a wire would be a mistake: write `["wire"]` for a port that
needs a real node rather than a value (a provider, a history, an `Access`
handle a consumer reads), and nothing else. Restricting a port a program
could plausibly fill with a written value is a review finding. Two port
kinds never carry the list at all: a `Bus`/`Generator` port is wire-only by
nature (the loader forces it), and a compiler-read port (the
`portsFromConfig` list, the access picker) takes an inline typed value only
by a fixed rule.

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
(a missing or malformed file is a compile error), so the manifest and the
catalog are one document. Ports are camelCase in JSON, and `ctx.inputs` is
keyed by them.

Config and wired inputs are one bag: `ctx.inputs.get::<T>("name")` returns
the value however it arrived (wire, braces literal, assignment literal, or
the declared `default`).

Fail loudly, always: `WeftResult<()>`; `ctx.inputs.get(...)?` stamps its own
errors; `node_bail!("message")` for conditions the node detects;
`.node_err("context")?` wraps an external error with context. A node body
never names a `WeftError` variant and never falls back to a default: a
failure is recorded in the journal where the user reads it.

Emit only through `ctx.pulse_downstream(NodeOutput::new().set(port, value))`;
ports you did not emit are closed, which is the skip signal downstream. For
user-added output ports use `ctx.fan_declared(...)`. Long external work runs
under `tokio::select!` against `ctx.cancellation().cancelled_err()` so a
cancelled execution stops mid-flight. Work that must not happen twice across
a restart goes through `ctx.run(...)`, which replays the recorded result.

Stopping other runs, the move behind the `TagRun` and `StopTagged` catalog
nodes, from inside your own node: `ctx.tag_execution([tag, ...]).await?`
puts tags on this run; `ctx.stop_tagged(tag, StopSelf::Keep).await?` stops
every older run of the project carrying the tag, waiting ones included (a
run parked on a person or a timer never wakes); `StopSelf::Include` stops
this run too. Tag first, then stop: a stop only reaches runs that put the
tag on before this one did, so when two runs race, the later one survives.
Both calls are safe to re-run after a crash (a repeated tag keeps its place
in the order, a repeated stop finds its targets already ended), so neither
goes through `ctx.run`. A tag is `[A-Za-z0-9_-]{1,64}`; the ctx refuses
anything else before writing. In the fake tier nothing is stopped;
`rig.execution_tags()` and `rig.stops()` record what the node asked for,
so assert on those.

## The special shapes

**Access node**: the whole body is `weft::access_node!(MyServiceAccessNode);`
plus a `service` recipe in metadata (acquisition fields with `secret: true`,
auth steps, a test URL, an identity template). The macro reads the `account`
input and pulses it on `access`. Credentials live sealed in the runtime's
access store, never in the project. Declaring the `service` block is
sufficient: the compiler synthesizes the runtime "no connection picked"
rule from it. You never write that rule by hand (a hand-written one is a
finding, not a feature), and `"connection_optional": true` inside the
service block is reserved for a node that genuinely runs unconnected.
An optional-connection node is the one access shape the macro cannot
serve: `access_node!` on a `connection_optional` service fails loudly at
run time. Such a node writes its own body and reads the pick with
`ctx.inputs.access("<picker input>")?`, which returns `None` when nothing
is picked.

**Infra node**: `"requires_infra": true`, plus `images` (dirs with a
Dockerfile the CLI builds) and `publishes` (the service name it hands out).
Implement `async fn provision_infra(&self, ctx, input) -> WeftResult<InfraSpec>`
returning the desired-state spec; the engine applies it, then calls `run`.
A container that serves `/live` (named by `features.liveEndpoint`) can put a
button on any item (`action: { label, actionKind, confirm? }`); the press
reaches its own `/action` as `{ action, payload }`, and a `result.error`
is the refusal the user reads. Every state the container can sit in has
a button that leaves it, offered in every state: the WhatsApp bridge's
"Disconnect phone" drops the pairing and shows a fresh QR code whether
the bridge is paired, stuck, or half way through pairing; the Postgres
node's "Reset password" mints a new one over the database's own socket.
Walk the container's states and ask what a user does from the graph to
leave each; a state whose only exit is restarting or terminating the
infra is a dead end, and shipping one fails the review.
Anything that runs inside the image obeys two rules, without exception:
every failure writes one line with its cause to stdout or stderr (so `weft
infra logs <node>` shows it; a library logger set to silent is no log, and
an install-time optional dependency is a silent skip waiting to happen, so
pin it), and every answer to a node is an error unless the thing asked for
fully happened (a message id for a message that will never show, a partial
result with nothing said about the gap, a skipped step: each is an error,
and the node reading the answer fails on it). A silent failure is the one
defect that fails review outright.

**Trigger**: `"features": { "isTrigger": true }` and implement
`async fn setup_trigger(&self, ctx)`, called instead of `run` at activation,
where the node registers its wake signal. Polling triggers carry an
`intervalSecs` config; activation starts from now, history never replays.

**A file input**: the value on an `Image` / `Audio` / `Video` / `Blob` port
is the stored-file marker, and inside the running node it also carries a
`url` minted for this firing (an hour), so a body that only speaks URLs (a
Python snippet, a provider's API) reads it straight off. A Rust node keeps
reading bytes through `ctx.storage(...).get_bytes(&handle)`. The link never
leaves the node: everything you emit, park, or memoize is stripped back to
the stored form, and the journal never holds one. The marker itself never
leaves weft either: what goes to a provider, a bridge, a form a browser
renders, or a live item is the plain thing that consumer reads (a URL
string, a `data:` URL, a plain `{ url, mimeType, filename }`), through
`ctx.storage(scope).externalize` for a typed value or `public_link` /
`presign` for one file. A marker wrapped around a link in an outbound payload is weft's
internal shape in someone else's contract, and it renders as nothing.

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

## Tests

`tests.rs` exports `pub fn tests() -> Vec<NodeTest>`. Each fake test is an
`async fn(rig: FakeRig) -> WeftResult<()>` driving the node with
`rig.run(&MyThingNode, json!({...})).await` and asserting on the result, and
`NodeTest::live(...)` is the real-credential tier. The three tiers: `basic`
(no external world at all), `fake` (a stubbed client), `live` (the real
service, real credentials, real money: write these tests, name the service,
declare any fixtures the test cannot self-provide; the user runs them, with
consent, through `/weft-live-test`). A test name states its assertion
("a_matching_case_takes_its_branch", not "test_switch"). Run the local tiers
with `weft test-node <type-or-package>`; the `live` tier spends money and
asks first. Write a node's tests in the same change that writes the node.

## After writing the node

The catalog walk picks the folder up automatically; no registration exists.
Check it landed: `weft describe-nodes --node MyThing --compact` must
succeed, or re-run `weft validate`, which compiles against `nodes/` fresh. Then use the
type in `main.weft` like any catalog node. Custom type names must not collide
with existing ones (loud error, no shadowing).
