---
name: weft-language
description: The weft language reference. Read before writing or editing any .weft source: declarations, wiring, config literals, reserved keys, inline signatures and expressions, types, groups, loops, the pulse execution model, @file/@asset/@include, and every compiler error slug.
---

# The weft language

The surface is small and strict: one way to say each thing, and the compiler
refuses everything else. This page is the whole surface, current as of this
template. When it disagrees with your memory, this page wins.

## Declaring a node

````weft
name = NodeType
name = NodeType { config_field: value }
name = NodeType {}
````

`name` is the node id, unique in its scope, snake_case. `NodeType` must exist
in the project's `nodes/` catalog (read its interface with
`weft describe-nodes --node <Type> --compact` for its real ports, never trust
memory).

## Connecting

````weft
target.input_port = source.output_port
````

Right to left: the value flows from `source.output_port` into
`target.input_port`, types must be compatible. Every required input gets a
wire or a literal; an optional one (`port?`) may be left alone.

## Config values

Typed JSON-ish literals:

````weft
t    = Text     { value: "a string" }
n    = Range    { to: 10, step: 2 }
flag = SomeNode { enabled: true }
arr  = SomeNode { items: [1, 2, 3] }
obj  = SomeNode { opts: { "k": "v" } }
````

Commas between fields are optional; a field per line with no commas reads the
same. `null` is never a literal: omit the field instead (`config-null-literal`).

### Wires in the braces

A field whose value is `source.port` is a wire, the same edge as a connection
line, written inside the node it feeds:

````weft
reply = TelegramSendMedia {
  kind: "photo"
  chatId: ask.chatId          # same as `reply.chatId = ask.chatId`
  file: picture.image
}
````

A `Group` is the exception: its braces hold its children, so the only field it
reads there is `_should_flow`.

### A key that CREATES a port

On types that accept extra inputs (`ExecPython`, `FirstInOrder`), a key naming
no declared port creates one. A wire types it from the source, a literal from
its own type, `null` is an error. `key?:` makes the created port optional.

````weft
step = ExecPython -> (out: String) {
  code: @file("scripts/step.py")
  text: draft.answer      # a String port, from the wire
  limit: 3                # a Number port, from the literal
  notes?: review.notes    # optional: a closure here does not skip the node
}
````

Created ports keep written order, which is what `FirstInOrder` reads: its
first input that carried a value is the one it emits. Reordering its lines
changes which branch wins, and it is the only place where line order means
anything.

### Literals on a connection line

````weft
post = SlackSendMessage { channel: "#alerts" }
post.text = "deploy finished"
````

Which form a port accepts is its `exposure` in metadata: `all` takes both,
`assignment` only the line, `config` only the braces, `wire` neither (it must
be driven by another node). Wrong form is `port-literal-placement`. The line
also works on a Group, Loop, or `@include` alias for its interface ports.
One target takes no literal: a group's own output written from inside
(`self.result = "lit"` is refused).

### Multi-line strings

````weft
step = ExecPython() -> (out: Number) {
  code: ```
    return {'out': 42}
  ```
}
````

The opening fence goes on the key's line, content starts at the next line,
the closing fence on its own line.

## Reserved keys

Exactly four, all starting with `_` (any other `_` key is an error):

| Key | Effect |
|---|---|
| `_label: "..."` | display label. A string, set once, never by wire |
| `_tags: ["a", "b"]` | tags, used by signal scoping |
| `_is_output: true` | overrides whether this node counts as a production target |
| `_should_flow: <wire or false>` | decides whether this node runs at all |

`_should_flow`: leave it out and the node runs. Wire it and the node runs
only when what arrives is not `false`; a `false` or a closure (whatever
decides never spoke) skips the node, closing its outputs, skipping everything
behind it. `_should_flow: false` in the braces turns one node off. Groups and
Loops take it too, inside the braces or on the container's name from outside.

## Inline port signatures

Types that leave ports open declare them in the declaration:

````weft
calc = ExecPython(a: Number, b: Number) -> (sum: Number, diff: Number) {
  code: "return {'sum': a + b, 'diff': a - b}"
}
answer = LlmInference -> (response: String)
ok     = Cast -> (value: Boolean)
````

Inputs arrive in Python as variables named after ports; the code returns a
dict keyed by output port name; `None` or a missing key emits no pulse on
that port. Write only the ports the type leaves open (`MustOverride` outputs
must be pinned). An empty body equals no body.

## Inline expressions

A node literal as a value, with a mandatory trailing `.port`:

````weft
out.data = Text { value: "hi" }.value
provider: OpenRouterProvider { model: "z-ai/glm-5.3" }.provider
````

It synthesizes an anonymous child with id `{host}__{field}` plus the edge.
Full node syntax nests inside, including its own signature. Omitting `.port`
is an error (ambiguity). Bare form `Type.port` takes default config.

## Comments and descriptions

`#` starts a line comment. If the first line inside a group or loop body is a
plain comment, it becomes that container's description, shown when collapsed.
Keep it one line, saying what the container does for its caller. Project name
and id live in `weft.toml`, never in a source header.

## Directives

````weft
lookup = SlackFindUser {
  @require_one_of(email, phone)
}
````

`@require_one_of(a, b)` on its own line in a node, group, or inline
signature: at least one named input must be satisfied. Compile error when
unmet; at run time the node skips when every port in the group arrives closed.

## Types

| Kind | Written |
|---|---|
| primitives | `String`, `Number`, `Boolean`, `Null` |
| stored files | `Image`, `Video`, `Audio`, `Blob` (references, not bytes) |
| aliases | `Media` = Image \| Video \| Audio; `File` = Media \| Blob |
| containers | `List[Number]`, `List[List[String]]`, `Dict[String, String]` |
| unions | `String \| Number`, `Number \| Null` |
| records | `{ role: String, name?: String }`, strict: undeclared keys refused |
| opaque | `JsonDict`, compatible with any `Dict[String, V]` both ways |
| type variables | `T`, unified across a node's ports, must pin to concrete (`unresolved-typevar`) |
| `MustOverride` | the author must pin it in an inline signature |
| live handles | `Bus`, `Generator[T]`, `Access`; never literals |

Named custom types (declared in a node's metadata `types`, e.g.
`ChatHistory`) are nominal: the name is the contract. Nothing unnamed wires
into a named target; the door between the worlds is `Cast`.

`?` means three things by position: on an input port, accepts a closed pulse
(fires with the input absent); on an output port, records that it may emit
nothing (documentation, matches metadata); on a config key, the port it
creates is optional.

`Cast` converts a value to the type declared on its output:
`history = Cast() -> (value: ChatHistory)`. The conversion table is checked
at compile time (`cast-not-allowed` for impossible pairs); record and named
targets are validated at run time, errors name the offending field.

Compatibility: identical types; unions member-wise; `JsonDict` with any
string dict; named types with `JsonDict` and their own shape; containers
element-wise; type variables unify. Everything else is `type-mismatch` naming
both types.

## How a program runs

Nothing walks the graph. Nodes emit values; each emission is a pulse addressed
to one input port of one node. A node fires when every required input holds a
pulse that agrees on color (execution) and frames (loop iterations).

A node that emits on some outputs and not others closes the rest: a pulse
carrying no value, meaning "nothing will ever arrive here". A closed pulse on
a required input skips the node and closes its outputs; on an optional input
the node fires anyway; on `_should_flow` the node skips, period. Skips cascade
forward until a node opted into absence. A failure propagates exactly like a
missing value, and a downstream optional input is the recovery path.

Branching is only this. `Switch` tests a value against its `cases` config and
emits `true` on the winning case's port, closing the rest; wire a case port
into the branch's `_should_flow`. `FirstInOrder` emits the first of its inputs
that carried a value, in written order, merging alternative paths.

What runs: a manual run starts from every output node and walks upstream,
executing the union (`--target <node>` narrows it; a target must be an output
node). A trigger fire starts at the trigger that fired, walks downstream to
the outputs it can reach, then back up, stopping at other triggers. A branch
wired to no output never executes. One file can hold several programs, one
per trigger, and they may share upstream nodes (one database, one provider):
on a fire the shared node runs for the fired program and the other programs
are left without a trace. A node skipped with reason `outside_this_run` on a
fire is in the fired program but no output depends on it: almost always a
side-effect node missing `_is_output: true`, so fix the wiring, never the
runtime.

Ends: completed (no pulse in flight), suspended (every live firing parked on
an external wait: a person, a timer; costs nothing), or stuck (provably
deadlocked, fails loudly). Everything is journaled; a run is readable node by
node with the values on the wires.

## Groups

````weft
preprocessor = Group(raw: String) -> (result: String) {
  # Cleans and transforms text

  clean = ExecPython(text: String) -> (out: String) {
    code: "return {'out': text.strip()}"
  }
  clean.text = self.raw
  self.result = clean.out
}

preprocessor.raw = input.value
output.data = preprocessor.result
````

`self` is the boundary: read `self.<input>` for what the group received,
write `self.<output>` for what it emits. Children reach each other and
`self`, nothing else (`scope-reachability`). Interface ports are set from
outside by wire or literal on their own lines. Groups nest, names are scoped,
and the compiler flattens them away: at run time there is one flat graph. A
group is a region, not a function: it has no call sites and does not return.

Reuse across files: `triage = @include("triage.weft")`, where the included
file is exactly one anonymous top-level group; its ports become `triage`'s.

## Loops

````weft
doubler = Loop(values: List[Number]) -> (results: List[Number | Null]) {
  parallel: false
  over: ["values"]

  step = ExecPython(n: Number) -> (out: Number) {
    code: "return {'out': n * 2}"
  }
  step.n = self.values
  self.results = step.out
}
````

Four port roles, derived from the config:

| Role | Rule |
|---|---|
| iter input (in `over`) | outside `List[T]`, inside `T`; several zip to the shortest |
| carry (in `carry`, declared on outputs) | accumulator; the compiler creates the matching input for the initial value |
| gather output | in the output signature, not in `carry`; outside must be `List[T \| Null]`; inside the write port is `T?` |
| broadcast input | in the input signature, not in `over`; same type both sides |

Two implicit ports: `self.index: Number` (read-only, zero-based) and
`self.done: Boolean` (write `true` to stop launching, sequential only).
Both names are reserved.

`parallel` defaults `false`. Five shapes: parallel map (true, over, no
carry), sequential map, fold (carry), while (no over, stop vote or
`max_iters`), side effect (no over, no carry). Refused: parallel with carry;
parallel with empty over; parallel with any `self.done` write; a port in both
`over` and `carry`; a sequential loop with nothing that can end it.

A `Generator[T]` port in `over` pulls the stream (must be the only over
port). A loop is a launcher, not an owner: work started in an iteration keeps
running after the loop emits; only the branch wired to the outputs holds the
emit back.

## Files and reuse

| Marker | Pulls | Writes back |
|---|---|---|
| `@include("x.weft")` | a program as a group | no |
| `@file("x.md")` | a file's contents as a value | yes |
| `@asset("x.png", Image)` | a file's contents as a value | no |
| `@asset("x.txt")` | a text file's contents inline | no |

`@file` is bidirectional (the editor writes edits back into the file) so
binary types are refused; it is the marker for `prompts/`, `scripts/`,
`sql/`. `@asset` with a file type resolves through the build's asset sync
(hash-addressed storage; the source keeps one line, never a blob). A list of
markers feeds multi-file ports. `@asset` sources may also be an outside path
or an `http(s)` URL (fetched at run time).

## Compiler error slugs

Wiring: `type-mismatch`, `required-port-unmet`, `unknown-source-node`,
`unknown-target-node`, `unknown-source-port`, `unknown-target-port`,
`double-driven-port`, `input-not-wireable`, `duplicate-input-port`,
`duplicate-node-id`, `port-literal-placement`, `undeclared-port-no-custom`.

Types: `unresolved-typevar`, `must-override-unmet`, `cast-not-allowed`,
`config-type-mismatch`, `config-null-literal`, `literal-out-of-range`,
`named-type-conflict`.

Shape: `graph-cycle` (iterate with a Loop, exchange feedback over a Bus),
`scope-reachability`, `orphan-outputs`, `unreachable-from-output`,
`no-output-node` (set `_is_output: true` on the deliverable).

Triggers: `trigger-in-loop`, `trigger-into-trigger`, `trigger-into-infra`,
`duplicate-port`, `config-ports-not-a-list`, `config-entry-not-an-object`,
`unknown-config-entry-kind`, `config-entry-without-a-port`,
`unknown-config-entry-key`, `config-entry-bad-test`, `duplicate-catch-all`,
`catch-all-not-last`.

Loops: `loop-unbounded-no-termination`, `parallel-with-carry`,
`parallel-without-over`, `parallel-with-done`, `over-and-carry-overlap`,
`over-not-a-list`, `over-stream-not-alone`, `gather-output-must-be-nullable`,
`carry-port-type-mismatch`, `loop-over-unknown-port`,
`loop-carry-unknown-port`, `loop-parallel-not-boolean`,
`loop-trim-not-boolean`, `loop-max-iters-not-integer`,
`loop-unknown-config-field`.

Streams: `generator-multiple-consumers`, `generator-through-group`,
`generator-in-container`, `generator-not-carriable`,
`generator-input-must-be-required`, `generator-not-iterated`,
`generator-into-generic-port`.

Names: `reserved-name`, `reserved-port-name`.

Requirements: `require-one-of-unmet`, `no-required-skip` (a warning: all
wireable inputs optional, add `@require_one_of`), `rule-structural`,
`rule-runtime` (a node's own declarative validation; the message is the node
author's).

Where these run, in tiers: the **edit tier** is the strict parse plus the
structural rules, fast and local (this is what the agent's edit loop and
the PostToolUse hook enforce, and what keeps the graph rendering);
**runtime rules** (`rule-runtime`) are things only the running program can
know, chiefly a connection not picked on an access node (that rule is
synthesized by the compiler from the node's `service` declaration; node
authors never write it, and `connection_optional: true` in the service
block opts a genuinely-unconnected node out): a build
deliberately skips them (a half-wired program still compiles), they fire
at execution as loud node failures, and `weft validate` reports them
early, which is the editor's Problems panel mode (structural plus
runtime); **full compilation** is `weft build`, structural errors plus
the cargo and image build. The slug names the rule, the message names the
fix.
