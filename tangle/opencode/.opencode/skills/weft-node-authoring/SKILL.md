---
name: weft-node-authoring
description: "Read before dispatching a node-smith (the brief and the review) and when an expert writes a node by hand: the node authoring manual, the test tiers, and the shapes half-done work takes. The node-smith reads this same file as its manual."
---

# Writing a custom node

Tangle reads the dispatch protocol and [the review] checklist. The `node-smith` subagent, and an expert taking the hand, read the manual that follows them.

Three terms hold across the file:

- [the contract] is the node's typed interface, and Tangle designs it: one job in a sentence; every input port (name, type, required or optional, and `accepts` only when a wire would be a mistake); every output port (name, type); every metadata port, if the node has one (name, type, and its value, written by hand); the service it wraps, if any; anything the surrounding program depends on (a form schema, a trigger registration, infra). Every value the node takes from the graph is its own input port. A `List` or `JsonDict` input whose elements would come from separate wires is the wrong shape: a list literal cannot hold a wire, so every program would need a `List` node in front of it just to feed the port. (A port that genuinely takes a list of things, like `LlmInference`'s `tools`, a `List[LlmTool]`, is fine: the program builds it with the `List` node, never with Python.) An open-ended set of values (a query's parameters, a template's holes) is declared with `canAddInputPorts` and read with `ctx.inputs.custom()`. `PostgresExecuteQuery` is the pattern to copy, and any node's own `features` block (`weft describe-nodes --node <Type> --compact`) says whether it carries the flag, so you confirm there rather than trusting a name you remember.
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
- **an output with no bound**: a port whose value grows with what the service answers or what the node is given (a list of results, a page of text, a response body, a transcript) and nothing that keeps it under the 100 KB wire limit: no cap input or `LIMIT`, no paging, no stored file in its place. [the report] names each such port and its bound, and a `fake` test emits the largest value the bound allows; a port missing from that list is the finding. The rule is under The wire limit.
- **body smells**: `.ok()` discarding an error, a default value standing in for a missing input, a retry loop, orchestration inside the node, a database client or a process opened fresh on every run where `ctx.shared` would hold one, heavy work (a big parse, image or audio work, compression, hashing a large file) done inline instead of in `tokio::task::spawn_blocking`.
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
| `metadataPorts` | ports whose value you write here by hand and no run computes, each `{ "name", "type", "description", "value" }` (`description` optional). A wire leaving one is a metadata wire: the compiler writes the value into the input it points at, as if typed there, and the wire never runs. Their names share one namespace with `inputs` and `outputs` |
| `types` | named type declarations, e.g. `"ChatHistory": "List[ChatMessage]"` |
| `features` | flags: `pure` (the body does nothing outside its run beyond answering its caller and the run's own files: no network, connection, `ctx.run`, wait, tag, or storage beyond this run; a durable run starts it without waiting for the database and runs it again after a crash if it had sent nothing on and reads no stream. `Text`, `Switch` and `Reply` are pure, `HttpRequest` is not; an outside ctx call on a pure node fails, naming the flag), `isTrigger`, `canAddInputPorts` (an open-ended set of values arrives as ports the author declares inline; the body reads `ctx.inputs.custom()`), `canAddOutputPorts`, `join` (the node takes several branches and carries on with whichever came, like `FirstInOrder`: every input you add is optional, it fires once all it waits on has arrived or closed, closes its own output when nothing came, and needs no `_should_flow`), `customInputType`, `customOutputType` (the type every added output takes, as `customInputType` is for added inputs; a variable one of the node's inputs uses, `T`, types each added output as that input), `pairsAddedPorts` (each input a program adds comes with an output of the same name, which the compiler makes and the program never writes; an added output nothing reads fails the build; declared beside `canAddOutputPorts`, as `Dispatch` does), `oneOfRequired`, `showDebugPreview`, `liveEndpoint` (the endpoint serving this infra node's display, see [The display](#the-display)), `castPorts`, `hidden`, `answersCaller` (`"whole"`, `"stream"` or `"end"`: this node answers a live caller the way `Reply`, `Stream` or `Close` does; any custom node that answers the caller declares it, so the compiler counts it), `liveConnection` (`"http"` or `"websocket"`: this trigger holds a caller, as `Route` and `Socket` do), `keepsInputsPastRun` (the node keeps what its inputs are given after the run ends, writing them into a table, a record or an instance's values; the compiler then warns when a file stored for its run only is wired into it, `run-file-kept-past-run`), `addedPortsAreDataKeys` (under Spelling) |
| `portsFromConfig` | ports derived from a config list: `{ "field", "matchInput", "specs": [{kind, keyField, catchAll?, addsInputs, addsOutputs}] }` |
| `firesWith` | trigger only: EVERY field a firing can carry, name to weft type, `?` on the name for sometimes-present (`{"scheduledTime": "String", "caller?": "JsonDict"}`). Checked exactly: a firing missing a required field is refused, and so is one carrying a field you did not name. On a node without `features.isTrigger`, or with a type weft cannot parse, the node fails to load |
| `display` | inline render: `{ "kind": "media" \| "link", "output" \| "input": "<port>" }` |
| `validate` | declarative rules: `{ "when": {...}, "then": {message, level: "structural"\|"runtime", field} }` |
| `requiresInfra`, `images`, `publishes` | infra nodes |
| `service` | access nodes only, the connection recipe |
| `claimsRoute` | a node that claims a public address: `pathField` names the input holding the route pattern, and the optional `methodField` the input holding the HTTP method. Leave `methodField` out and the node serves every method; with it set, a program that leaves that input empty serves every method too. Naming an input the node does not declare fails the load |
| `accessApps` | project-shipped OAuth apps, keyed by service name (`google`); what an entry may hold is below the table |
| `placeholdersAreInputs` | a node whose added inputs are named inside one of its text settings (a query's `$userId`, a template's `{{name}}`): `{ "in": "<that setting>", "pattern": "<a regex whose group (?<name>...) is the input>" }`. The compiler then holds the two to each other, so every placeholder the setting holds must be an input of the node and every added input must appear in it, and a typo in a query fails the build instead of the first call. The pattern may also match text to skip (a string literal, a comment) through an alternative without the `name` group; those matches are ignored. Copy `Format`'s or `PostgresExecuteQuery`'s before writing your own |

An `accessApps` entry holds `label`, `client_id` and the service's
`registrationFields` that it marks `"secret": false` (a field is secret unless
marked otherwise). The entry ships to everyone who uses the node, so a
`client_secret` or a secret registration field in it fails the node's load.
The load also fails when the key names no service in the project's catalog,
or a service that does not sign in through OAuth, or a service that needs a
secret field in every app. It fails too when `label`, `client_id` or a
registration field the service does not mark `optional` is missing or empty,
and when the entry has a key the service does not declare.

### Spelling

The `type`, and any type you declare under `types`, is PascalCase
(`WordCount`). Every key weft defines is camelCase (`requiresInfra`,
`freeText`, `dependsOn`, `connectionOptional`), and so is every port, setting
and record field you name, each field under `firesWith`, the names under each
event's `fields` in a service's `events`, and each name of a value a
connection stores (`imapHost`), both where a service declares it (`fields`,
`captures`, `registrationFields`) and where an input asks for it
(`requiresValues`). Two kinds of key are snake_case: an event topic
(`drive_changes`) and a key under `accessApps` (`google`). They are
snake_case because a node also writes them as values (a trigger picks a topic
by name, and a node names its service as `"service": "google"`), and every
value from a fixed list weft defines is snake_case (a widget's `kind`, a
rule's `kind`). The `options` of a select you write keep whatever spelling
the values have (`Africa/Abidjan`). A
name somebody else chose keeps their spelling: an OAuth parameter under
`authParams` (`access_type`), `client_id` in an `accessApps` entry, a field
of a body you send a provider.

A key weft used to spell in snake_case (`requires_infra`) fails the load,
and the error names the new spelling (``requires_infra` is spelled
`requiresInfra` now``); in a package root's defaults, the whole package is
left out. A snake_case key weft never had is an unknown key: on a node it
fails the load, and in a package root's defaults the root gets a warning
instead and every member that inherits it fails to load. Any other name in
the wrong casing loads, with a catalog warning:
`weft describe-nodes --list` and `weft validate < src/main.weft` print it on
stderr as `warning: <NodeType> (<path>): <message>` (a package root's
defaults as `warning: package '<name>' defaults (<path>): ...`), and the
graph view lists it as "node spelling warning". Clear every one on a node you
write.

If the ports a program adds to your node are named after data from outside
weft (the captures in a route's path, a query's parameters, a template's
ids), set `features.addedPortsAreDataKeys`. The compiler's
`not-camel-case` warning then leaves the ports a program adds alone, and still
checks the name the program gives the node.

Input entry: `name`, `type`, `required`, `accepts`,
`widget`, `default`, `label`, `placeholder`, `description`, `requiresScopes`,
`requiresValues`.

If an unwired input means something of its own (no `instance`: the program's
own copy), leave it optional (no `required`): unwired, the node runs without
it; wired, the node waits for it and skips if the wire closes, so a wire that
delivers nothing never falls back to that meaning. A program can change this
on its own use of the node (`x: T` requires it, `x?: T` runs without it even
when wired; the `weft-language` skill has the table), but never make a
`required` input optional. If an optional input needs one more permission when it is used,
leave it out of `requiresScopes`, which every program using the node is asked
for. In `run`, when the input is used and before `ctx.client(&account)`, copy
`account.required_permissions()` (it already holds the declared ones), push the
extra one, and pass the list to `account.with_required_permissions`. Opening
then refuses a connection that lacks it, naming the permission.

A `select` or `multiselect` widget's `options` are a closed list: a program
writing anything else fails with `literal-not-an-option`. Keep it closed when
your code matches on the value. When the list is only the ones you know of
and a provider can add more (model ids, voices), add `"freeText": true` to
the widget, and the options become suggestions.
A `code` widget's `language` is one of `python`, `javascript`, `sql`, `json`;
any other word fails to load. `json` also fits a `JsonDict` input
(edited as JSON text); the others edit a `String`. A `number` widget with a whole `step`
(`"step": 1`) takes whole numbers only: the compiler checks a written value
and the runtime a wired one, so your body can cast it to an integer.
Output entry: `name`, `type`, `description` (an output has no `required`), `narrowable`, `fileScopeFrom` (on an output carrying a file the node stores where the program chooses: the name of the input whose value names the scope, `run` or `project`; it must name an input of the node, or the node does not load), and on an infra node `baked` (see Infra node). If only the program knows an output's shape (a model's JSON answer, a request's body, a query's rows), type it broadly and set `"narrowable": true` (any type that fits inside `type`), or a type string (anything that fits inside that type, which may be wider than `type`): the program then declares what it holds (`-> (response: Verdict)`), and each value is checked against that at run time. Without it, a program can only restate the output's own type.

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
  `role: "infra"` or `"trigger"` and `perInstance: true/false`;
  `names: {"field": {}}` is a field written `node.field`, narrowed with
  optional `instanceFilled: true/false`. Wrap it in `not` to refuse a bad
  name. A wired name is not known yet, so the rule does not fire on it.

`{with}`, `{perInstanceReason}` and `{names}` in the message name what the
check found (`{perInstanceReason}` says why, path included: "it reads
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
  "description": "Emit a literal string (a prompt, a label, a config value).",
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
  "features": { "pure": true }
}
```

## mod.rs

```rust
//! One doc line saying what the node does.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct MyThingNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MyThingNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> { tests::tests() }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

A program may want to handle a node's failure, and weft lets it choose
without your node writing any error handling: every node has a `_error`
output, added by weft. With `_error` wired, a failure of the body becomes its
message on `_error` and every output the body had not emitted closes;
unwired, the failure fails the run. What is caught is a failure of the call
itself (a `node_bail!`, a runtime error, a wait that gave up, the worker
going away mid-run). A bad setting, input or type, a suspension or a cancel
always fails the run, and so does a failure after the node already sent on
a `Bus` or `Generator` output: that stream ends failed. The body stays loud:
it returns its errors with `?` and `node_bail!` exactly as any node does.
Weft owns `_error`: declaring it in the metadata is refused, and so is a body
emitting on it. `error` without the underscore is an ordinary name your node
may use for its own output. There is no feature flag for catching any more:
a `metadata.json` that still sets the old one is refused, naming the key to
remove.
`ctx.is_output_wired(port)` tells a body whether anything reads a port. A
`fake` test of the caught path calls `rig.wire_output("_error")` first and
reads `outcome.output("_error")?`; a rig wires nothing by default, so without
it the failure fails the test run. `nodes/base_catalog/ai/elevenlabs/clone_voice/tests.rs`
has both sides: `refused_wired` catches a refusal, and `no_samples` shows a
bad input still failing with `_error` wired.

**Calling a service.** `weft::access::client` already holds what every API
node repeats, so you never write it again: `get_json` / `post_json` (send,
refuse a non-success status quoting the provider's own words, parse),
`json_call` for a request you prepared yourself, `checked_send` when the
answer is not JSON, `require_ok_flag` for a service that answers 200 and says
`ok: false` in the body, `cursor_paged` with a `CursorPaging` (its
`past_cap_hint` tells the user what to narrow when the list runs past the page
cap) for a cursor-paged list, and `required_str` for a field you cannot do
without. To hand a stored file to a provider,
`ctx.storage(scope).external_file(&file)` gives its link when the open
internet can open it, else a `data:` URL of its bytes, with its mime type and
filename; never write that fallback yourself. To emit a struct, use
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

If the service can cancel a job, hand the wait its cancel call, so a run
stopped while it waits stops the job and its bill too:
`ctx.await_signal_with_cancel(PollEndpoint { .. }, CancelCall::put(cancel_url))`
(`weft::signal::CancelCall`, also `post` and `delete`, and `.with_body(json)`).
weft sends it once, with the wait's own `access`, when the run is cancelled
while it waits, and never when the wait is answered or the run ends any
other way. `run_queued` in that same file does it.

You emit only through `ctx.pulse_downstream(NodeOutput::new().set(port, value))`;
ports you did not emit are closed, which is the skip signal downstream. For
user-added output ports use `ctx.fan_declared(...)`.

**Share what is slow to build**: a client pool, a process. `ctx.shared(&access, |opened| async move { .. })` builds it the first time a run asks and hands every later run on that worker the same one; it is built again when the connection's values change, or after nobody used it for the project's idle window (300 seconds by default, `weft workers set --shared-idle-seconds`). The closure returns a `WeftResult` of the thing and you get a handle you use as the thing. `ctx.shared("name", |()| async { .. })` is the same when nothing is built from a connection, and `.with_limit(n)` before the `.await` lets at most `n` runs of a worker hold it at once. That never keeps a database under its limit, because any number of workers run a node at once. So have your pool close a connection after a few idle seconds (the Postgres nodes use 5) and never hand a run one idle for longer, because a Cloud Run copy has no CPU between requests to run your close timer, and leave the waiting to whatever sits in front of the database, such as a PostgresDatabase's pooler. If your node keeps a connection idle on purpose (one that `LISTEN`s), the database may close it (a PostgresDatabase does after a minute), so lift `idle_session_timeout` for that session when it connects, the way PostgresWatchQuery does. A database client opened on every run pays the connection each time; for a worked pool, go and read `nodes/base_catalog/postgres/postgres.rs`.

If the worker dies while your body runs, a fast run (the default) ends cancelled, and a durable run fails the node: its start was on record before the body began, so the next worker knows it was running and fails it rather than start it again. With `_error` wired, that failure goes to `_error` like any other. A node marked `pure` is the exception: a firing of it that had sent nothing on and read no stream runs again, since it did nothing anyone could see. The one other body weft starts again on purpose is one parked on `ctx.await_signal`, which replays from the top when its answer comes, so the work before the wait goes through `ctx.run(...)`, which
gives back the recorded result. An emit on a port already sent before the wait does nothing on the replay, so a node can send `stats` at once, wait, then send `image`; what it cannot do is wait after yielding on or closing a stream output, which is refused. In a run that cannot pause (a route's caller on the line, `recorded: false`, a bus open), `ctx.await_signal` holds in your call instead and returns the answer without a replay; if the run stays quiet for its trigger's `holdSecs`, the call fails with `WeftError::WaitGaveUp`, which your node may match and handle (test it with `rig.signal_given_up()`).

**Stopping is cooperative, and a node that ignores it cannot be stopped.**
Nothing kills a node mid-call: a cancelled run (the caller left, a
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

**Heavy work goes through `tokio::task::spawn_blocking`.** A body runs on
the same few threads (one per CPU) as the worker's door and its other runs,
and a thread changes hands only at an `.await`. Parsing a big document, image
or audio work, compressing, or hashing a large file in one go holds that
thread until it is done, and every caller and run waiting for it waits too; on
a one-CPU worker that is all of them. The worker renews its lease on its runs
from a separate thread, so a slow body never loses its run; it only stalls its
neighbours.
Hand the work to the blocking pool and await it:

```rust
let rows = tokio::task::spawn_blocking(move || parse_csv(&text))
    .await
    .node_err("parsing the export")?;
```

The stdlib's `nodes/base_catalog/google/sheets_read/mod.rs` does it for a
whole tab.

Any node can stop other runs of its project from inside its own body, and
two ctx calls are the whole of it (`TagRun` and `StopTagged` are thin
wrappers over exactly these, with no privilege yours lacks):
`ctx.tag_run([tag, ...]).await?`
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
`rig.run_tags()` and `rig.stops()` record what the node asked for, so
you assert on those.

A program with instances (separate running copies of part of it, each under
its own id) reaches them from a node through the ctx.
`ctx.instance()` is which instance this run is for (`Option<&InstanceId>`, `None` for a run
for no instance, `.as_str()` for the id). The rest name an instance with `.instance(id)`
and end in one call:

| Call | Answers |
|---|---|
| `ctx.infra(node).instance(id).start()` | `()` once the instance's container runs (the run parks between looks); fails with its `failure` |
| `.stop(take_down, stop_self)` / `.terminate(take_down, stop_self)` / `.wipe(take_down, stop_self)` / `.upgrade(take_down, stop_self)` | `InfraDownAnswer` from stop, terminate and wipe (`TakenDown`, `AlreadyDown` or `NoCopy`), `()` from upgrade. `take_down` is a `TakeDownChoice`, the answers of `weft deactivate` as optional fields (`door`, `door_for_minutes`, `held`, `running`): `TakeDownChoice::default()` takes every default, and `TakeDownChoice { running: Some(Running::Cancel), ..Default::default() }` changes one. `stop_self` is a `StopSelf` |
| `.status()` | `Option<InfraCopy>`: `{ node, instance, status, failure }`, `None` when there is no container |
| `ctx.infra(node).copies()` | `Vec<InfraCopy>`, the shared container and each instance's |
| `ctx.triggers().instance(id).activate(held)` / `.deactivate(take_down, stop_self)` | `()`; `held` is a `ReleaseHeld` (`Run` or `Drop`); `.only([..])` narrows to named triggers, `.all_instances()` reaches every instance's |
| `ctx.values().instance(id).get()` | `InstanceValues`: node -> field -> value, what was given for the instance's `@instance_filled` fields |
| `ctx.values().instance(id).set(node, field, value).clear(node, field).apply()` | `Vec<String>`, the instance's triggers set up again; each value is checked against its node first, all or none |
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

A node can also act on another project of the same owner, with the same
parameters and the same refusals as the CLI. `ctx.project(id)` gives every
builder above on that project (`ctx.project(id).runs()`,
`ctx.project(id).infra(node)`, ...), while the bare calls stay on the node's
own project. `ctx.projects().list()` lists the owner's projects, and
`ctx.projects().declare(name)` makes a new one. Two more reach a project's
code and any one run:

| Call | Answers |
|---|---|
| `ctx.project(id).code()`, or `ctx.code()` on its own project | a working copy's code: `.state()` (the working copy and its files, `path -> sha256`), `.file(path)`, `.read_text(path)`, `.write_text(path, text)`, `.remove(path)`, `.edit().text(..).stored(..).blob(path, hash)?.remove(..).expecting(hash).apply()` (every file lands or none does; `.blob` puts a file the owner already holds, by the sha256 `.state()` gave, with nothing downloaded, so copying another project's files, such as its `nodes/base_catalog/`, is one `.state()` and one `.apply()`; a hash that is no sha256 fails `.blob`, one the install does not hold refuses the write), `.checkpoint(label)`, `.switch_to(version)` / `.switch_to_run(run)` (what `weft branch` does, but `.switch_to` takes a full version id, never a label or a prefix; `.tree()` on the project gives a label's id), `.build().start()`, and `.run()` with a setter named after most `weft run` flags (`.example`, `.from`, `.emit`, `.target`, `.before`, `.group`, `.feed`, `.fire`, `.instance(id)?` (it checks the id's form), `.seed`, `.seed_run`, `.seed_until`, `.seed_before`, `.durable`, `.fast`, `.keep_for`, `.referenced`, `.root`; none for `--check`, `--save`, `--clear`, `--hold-secs` or `--detach`, and `.start()` returns once the run has started; `.example(name)` is the example name `weft run` takes), then `.start()`, which builds and starts the run. If you name no working copy with `.working_copy(id)`, it acts on the project's most recently changed folder: on a local install the files are written into that folder, and on a cloud install they reach it with the person's next `weft` command; a project with no folder uses the copy programs write into. `ctx.project(id).forget_working_copy(copy)` is `weft forget-copy`: the copy's files become a version (`saved`) and the copy is removed |
| `ctx.run_by_id(run)` | one run of any of the owner's projects: `.stop()`, `.wake(node)`, `.logs(n)`, `.events()...read()`, `.outputs(focus)`, `.detail()`, `.node_state(node)`, `.delete()` |

On its own project a program cannot start a run (every run would start the
next, with nothing to end the chain; reach the part you want with a trigger
instead), resync it (the resync waits for the runs on the old version to
end, this run among them, so it would wait on itself) or remove it (the
removal cancels every run of it, this one included, before it finishes).
Each refusal says why. A run's refusal comes before anything is recorded as a version or built. From another project, or with `weft run`, `weft
resync` and `weft rm`, all three work.

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
a finding. `"connectionOptional": true` inside the service block is
reserved for a node that genuinely runs unconnected. That node is the one
access shape the macro cannot serve (`access_node!` on a
`connectionOptional` service fails loudly at run time): it writes its own
body and reads the pick with `ctx.inputs.access("<picker input>")?`, which
returns `None` when nothing is picked.

The sign-in is declared, never coded: when a service signs in differently
from the standard OAuth flow, there is a key for it in the `service` block.
`clientParams` renames what the token calls send (a `client_key` instead of
`client_id`), `extraParams` adds fields to every token call, `afterExchange`
swaps the first token for a longer-lived one, `refresh` replaces the standard
renewal with a call you write out, and the answer reader says where
each value sits in the token answer (`tokenPath`, `refreshTokenPath`,
`expiresInPath` or `expiresPath`, `scopePath`), where an error answered with a
`200` shows (`errorPath`, `okPath`), and `{{`/`}}` writes a literal brace in a
template. A token that renews must say where its expiry is, or declare
`neverExpires`; connecting refuses it otherwise. A gate that checks callers (a
webhook signature, a bearer from an identity provider) is a `verify` block.
For every key and its rules, go and read
https://weavemindai.github.io/weft/connections/declaring-a-service.html and
https://weavemindai.github.io/weft/connections/verifying-requests.html, and
start from the catalog access node closest to the service (Slack's declares
`okPath`, `HmacAuth` a `verify` block).

**Infra node**: `"requiresInfra": true`, plus `images` (dirs with a
Dockerfile the CLI builds) and `publishes` (the service name it hands out).
Implement `async fn provision_infra(&self, ctx, input) -> WeftResult<InfraSpec>`
returning the desired-state spec; the engine applies it, then calls `run`.

**Bake every output that holds as long as the infra does** (an address, a
connection, a handle): `"baked": true` on the output in `metadata.json`. A baked output is made when the infra is applied, and a run that reads nothing from your node but baked outputs uses the saved values instead of running it, so a route that queries a database never calls the database's side container. A run that also reads an output that moves on its own (a status, a phone number a bridge pairs later) runs your node, and a baked output your `run` sends nothing on then carries the saved value. weft bakes again every time the infra is applied, and on `weft infra rebake <node>`. In between, the container itself tells weft when a value changes (a reset button, a rotated password): it `POST`s `{"connection": {"password": "..."}}` to `$WEFT_VALUES_URL`, which weft sets on every container of the unit (`connection` changes values inside the connection your node published, `outputs` changes baked outputs by name). Only what your node already handed weft can change. The push answers `200` once weft has written the value, or `422` with a body naming the key or output it could not change. If the push came from a button and failed, answer the press with `{ "result": { "error": "..." } }` telling the person to run `weft infra rebake <node>`. The value itself never goes in the press's answer. The Postgres node's reset is the worked example (`nodes/base_catalog/postgres/database/images/credential/bootstrap.py`). Changing what `run` emits on a baked output is not noticed on its own: run `weft infra upgrade`, which rebuilds the infra against your current source and bakes again. A node that fails using a connection an infra node made says to run `weft infra rebake` on that node; your node's own error says only what the driver said.

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
`provision_infra` returns; `metadata.json` carries just `requiresInfra` and
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
(`UnitHealth { flaky_after_seconds, recovery_after_seconds }`: how long a
unit stays not ready before it counts as flaky, and how long it stays ready
again before it stops counting).

**Health rules** (`InfraSpec.health_rules`) say what a copy of your node
does to the triggers whose fires reach it when its units go wrong. Leave it
`None` and two built-in rules run: while a unit is flaky and some trigger
is on, take those triggers down (what arrives is held, the runs reaching
the copy are cancelled), and once no unit is flaky, bring back what the
health check took down. `Some(vec![])` runs no rule at all. Write your own
only when that pair is wrong for the service. Say your node has a `cache`
unit that may restart without the triggers caring: narrow both rules to the
`db` unit, so only `db` going flaky takes them down.

```rust
use weft::infra::health::{HealthAction, HealthCondition, HealthRule, DEFAULT_RULE_TIMEOUT_SECONDS};

let db_flaky = HealthCondition::UnitFlaky { unit: Some("db".into()) };
let health_rules = Some(vec![
    HealthRule {
        name: "take-down-while-db-flaky".into(),
        when: HealthCondition::All { conditions: vec![db_flaky.clone(), HealthCondition::Listening] },
        action: HealthAction::TakeDownTriggers,
        timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
    },
    HealthRule {
        name: "bring-back-once-db-healthy".into(),
        when: HealthCondition::All {
            conditions: vec![HealthCondition::Not { condition: Box::new(db_flaky) }, HealthCondition::TakenDownByHealth],
        },
        action: HealthAction::BringBack,
        timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
    },
]);
```

A rule reads one copy alone: its own units (`UnitFlaky`, with `unit: None`
for any of them), `Listening` (a trigger of the project is on and none is
being switched on) and `TakenDownByHealth`, combined with `All`, `Any` and
`Not`. `BringBack` never brings back what a person or an infra command took
down, and a take-down rule with no bring-back rule leaves the triggers it took down
down until somebody runs `weft activate`. Names are unique within the node, a
rule naming a unit the spec does not declare fails the spec, and
`timeout_seconds` is how long the action may take before the attempt counts
as failed (`weft status` names a rule that keeps failing).

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

**Keeping a file past the run**: a file stored in `StorageScope::Run`
with no keep is swept shortly after the run ends. If the file is what the
node produced, pass a keep when you store it, the way a speech node does:

```rust
use weft::storage::{KeepTtl, StorageScope};

let file = ctx.storage(StorageScope::Run)
    .put(bytes, "audio/mpeg", "speech.mp3", Some(KeepTtl::Default))
    .await?;
```

or keep one you already hold with
`ctx.storage(StorageScope::Run).keep(&file, KeepTtl::Default).await?`
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
fails the node right there: `port 'results' of 'search' carries 107 KB, and a wire carries at most 100 KB. Return less: one page of it with where the next one starts, or a part that says it was cut. Or store the data and send the stored file instead, whose value weighs a few hundred bytes whatever the file holds`. Nothing downstream can trim
it, because the check is on what YOU emit, so an output that can grow is
bounded when you write the node, one of three ways. Page it: an offset or
cursor in, where the next page starts and whether there is more out
(`GoogleSheetsRead`'s `offset` and `maxRows` in, `nextOffset` and `hasMore`
out; `S3ListObjects`'s `startAfter` and `maxKeys`, `lastKey` and `hasMore`).
Cap it: a size setting with a default and an output saying it was cut
(`FetchPage`'s `maxChars` and `truncated`). Or put bytes in storage,
`ctx.storage(scope).put(..)`, and emit the file value, which weighs a few
hundred bytes whatever the file does. `weft::storage::wire_weight(&value)`
measures a value the way the check does, `cut_to_weight(text, budget)` cuts
text to a weight, and `cap_text(text, max_chars)` gives back the text cut to
that many characters and under the limit, with whether it cut: the pair a
`maxChars` setting and a `truncated` output need. A node whose output CAN
exceed the limit on a bad day (a long article, a big listing) and does none
of these is a node that fails on that day.

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
page, a query's rows) stays a value, since it does not grow through use, but
it is still paged or capped as above.

**A file input**: the value on an `Image` / `Audio` / `Video` / `Blob` port
is the stored-file marker (`__weft_image__`, `__weft_video__`,
`__weft_audio__` or `__weft_blob__`, the kind taken from the mime type), and
it carries no link. A Rust node takes
the input as a `FileHandle` (`let file: FileHandle =
ctx.inputs.get("file")?;`) and reads it with
`ctx.storage(StorageScope::Run).get_bytes(&file).await?`, which hands
back `(meta, bytes)`: `meta.filename` and `meta.mime_type` are the file's
details. If you want the details without the bytes,
`StoredFile::from_value(&value)?` reads the value into `.key`, `.mime_type`,
`.filename` and `.size_bytes`. Never read `mimeType` off the raw JSON: it sits
one level down, inside the kind marker. When something needs a link, make one:
`ctx.storage(scope).internal_link(&file)` for code inside the install (a
script or a process your node runs), `external_link(&file)` for anything
outside it. Both give a `FileLink { url, public }`, `public` saying whether
the open internet opens it, and the link lives the value's `linkSecs` (15
minutes unless `link_lifetime` on the storage handle or a `LinkOptions` node
set another, at most seven days). A file value never holds a link, so nothing
you emit, park or memoize carries one, and the journal never holds one. The marker never leaves weft
either: what goes to a provider, a bridge, a form a browser renders, or a
live item is the plain thing that consumer reads (a URL string, a `data:`
URL, a plain `{ url, mimeType, filename }`), through
`ctx.storage(scope).externalize` for a typed value or `external_link` /
`external_file` for one file. A marker wrapped around a link in an outbound
payload renders as nothing.

**A bus**: a live channel one node opens and others read, for anything that
has to move while the run is still going (rows kept live, a model's deltas,
audio frames). A `Bus` port is wire-only, and what travels it is a marker,
not the data.

If you want to produce one, open it on your FIRST piece, never before the
call that can fail: `let mut bus = ctx.pending_bus(port, BusOptions::default(), "watch");`,
then, once the first piece is in hand, `bus.open().await?.send("<kind>", json!(value))`
(or `send_bytes` on a `Bytes` bus), and `bus.open().await?.close()` at the
end (`nodes/base_catalog/ai/llm/stream/mod.rs` is the model). `open()`
creates the bus, pulses its marker on that output port and registers you on
it under that name. Until it runs, your node has started no stream, so a
failure before then is still caught on `_error` and the port closes empty
(its readers never start); a producer that never opens it leaves the port
closed. `ctx.open_bus(port, opts, name)` does the same in one call; use it
directly only when no failing call comes before the first piece. The
options, all declared at creation and read back by every consumer: `payload`
(`Json` by default, `Bytes` for media frames, wrong shape refused loud), `meta`
for facts a reader needs about the stream (sample rate, model name),
`journal` for what the run's record keeps of the stream (journaled or
ephemeral, and the window one row gathers), `window` for how far a slow
reader may fall behind (64 frames). If your node's users should choose the
journal, set `features.journalSettings: true` in its `metadata.json` (it then
gets the inputs `journalEphemeral` and `journalWindowMs`, so do not declare
them, and a `liveConnection` trigger already has them, so do not set both) and
pass `journal: ctx.journal_policy()?`. Without it the default applies;
`JournalPolicy::ephemeral()` keeps payloads out whatever anyone sets.

A bus must end, because a reader parked on one that never ends waits for
ever. It ends when the body of the firing that created or joined it
returns: finished if the body returned `Ok`, failed with the error if it
failed (a body that paused on a wait ends its buses failed too). Dropping a
handle ends nothing. `bus.close()` ends it early, as finished; a failed end
is never yours to write. So what you have to get right is not holding the
body open past the work: a node that runs until the run ends waits on
`ctx.cancellation()` and returns, and that return is what the reader
downstream sees as the end of the feed.

If you want to read a bus the graph handed you, the choice is what you must not
miss:

| Call | Reach for it when |
|---|---|
| `ctx.join_bus(port, name)?`, then `bus.cursor()` | you take part in the exchange and want what is sent from now on |
| `ctx.bus_from_input(port)?`, then `bus.cursor_from_start()` | you must not miss what was sent before you attached, or the producer may already have finished |

A bus you `join_bus` is held by your firing like one you created, so if your
body fails, the bus ends failed. `bus_from_input` never ends it, which is
what an observer wants (a debug tap, a reader streaming it out), because
ending a feed the producer still owns is a bug. Read with
`cursor.next().await` for every entry, or `cursor.next_json("<kind>").await`
for one kind's payloads. `None` means the bus ended; then check `bus.end()`:
`Some(StreamEnd::Failed { error })` means the stream did not complete, and
your reader says so in its own protocol rather than passing it on as whole
(`nodes/base_catalog/api/stream/mod.rs` calls `http.fail(..)` there).

Do every bus read and wait on your own task, and never move a handle or a
cursor into a `tokio::spawn`. The engine works out "everyone is stuck, close
the buses" by watching what each node's firing waits on, a wait on a task you
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

[[needs]]
name = "ffmpeg"
check = ["ffmpeg", "-version"]
[needs.install.apt]
default = ["ffmpeg"]
[needs.install.brew]
default = ["ffmpeg"]
```

Sections: `[dependencies]` (cargo), `[build-dependencies]`, `[[needs]]`,
`[build.env]`. Any other key fails the build, naming it.

A `[[needs]]` entry is one thing the machine the worker runs on must have:
a program the node starts (the default, `stage = "run"`), or, with `stage =
"build"`, something its package needs to compile, like a C library's
headers. `name` is what the user is told, `check` a command that exits 0
when it is there (its first word looked up on the `PATH`), and
`[needs.install.<manager>]` the packages that provide it, for `apt`, `dnf`,
`apk` and `brew`, keyed by distro like `debian_12` or `ubuntu_24_04`, or
`default`. On a local install a program that uses the node is refused
before it runs, activates or resyncs when a `run` check fails, naming `weft
deps-install <NodeType>`; a node it does not use is never checked. A `build` need is not checked before the
package compiles, so a missing one shows as the package failing to compile
and being left out. A cloud image installs the same packages, using the entry for its own
package manager: the one for the image's distro, else that manager's
`default`. If a `run` need has no entry for the manager, every program
using the node is refused; if a need has entries for the manager but none
for the distro and no `default`, the image build fails, so give each
manager a `default`. The image starts from `debian:trixie-slim` unless
the project names another, so give every need an `apt` entry. If the
project's `weft.toml` sets `[build.worker] base_image` to an image of
another family, each need also has to carry an entry for that family
(`apk` for Alpine, `dnf` for the Red Hat family, `brew` for Homebrew); for
that section, read the `weft-deploying` skill. When the
node launches the program and it is missing anyway, it fails with
`weft::error::need_missing("<the need's name>")`, the way `ExecPython` does
for `python3`, and weft adds the node and how to meet the need on that
install.

If the package needs a build script, it is a `build.rs` at the package's
root, and its crates go under `[build-dependencies]` (declaring those with
no `build.rs` fails the build). The script runs for that package's own
crate alone, so a script or a build dependency that fails leaves only that
package out of the worker. What it prints with `cargo:rustc-env`,
`cargo:rustc-cfg` and `OUT_DIR` reaches the package's own code, and
`cargo:rustc-link-lib` and `cargo:rustc-link-search` reach the worker that
links it; a `cargo:rustc-link-arg` does not, since cargo applies one only to
the binaries of the package that prints it.

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
(`weft new <name> --assistant none` in `.scratch/node-smith/`, inside the project), copy the node in under `nodes/`, use it
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

A package whose Rust does not compile is left out of the worker, and every
run that reaches one of its nodes is refused, naming the package. Only a
green `weft test-node` proves your node compiles. For the exact messages,
go and read the `weft-running` skill, "When files change, or part of the
program does not compile".
Then use the type in `src/main.weft` like any catalog node. A custom type
name must not collide with an existing one (loud error, no shadowing).
