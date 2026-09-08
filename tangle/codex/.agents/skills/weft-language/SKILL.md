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
wire or a literal; an optional one (`port?`) may be left alone. This line is
the longhand; the same wire in the target's braces is [the shorthand] (next
section), and the standalone line survives where the language leaves no
choice: a group's or an include's boundary ports.

One key of a record value: keep going with dots (`wpm: reader.profile.stats.wpm`
in the braces, or `speed.wpm = reader.profile.stats.wpm`). Still one wire; the
compiler checks every key against the record type at that level and the wire
carries the last key's type. A source typed `JsonDict` or a scalar has no keys
to read (`deref-path`): declare the shape on the source port, or `Cast` first.
At run time a `?` key found absent (or `null`) closes that wire alone; a
required key absent fails the firing. Never write a Python node whose only
job is to pull a field out of a dict.

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

A list or object literal holds plain values only, so `params: [self.chatId,
self.pushName]` is refused. When a node needs several values from the graph,
each one is its own input port. When a node accepts however many values you
give it, declare them in its inline signature:
`PostgresExecuteQuery(chat_id: String, push_name: String) { ... }` and the
SQL reads `$chat_id`; `Format(user: String) { template: "Hi {{user}}" }`.
If you find yourself writing a Python node whose only job is to put wires
into a list, declare the ports on the consumer instead.

### Wires in the braces

A field whose value is `source.port` is a wire, the same edge as a connection
line, written inside the node it feeds. This is [the shorthand]: a node's
wires sit next to its settings, one block for its whole life, instead of a
stack of lines each starting with the same name:

````weft
reply = TelegramSendMedia {
  account: telegram.access
  kind: "photo"
  chatId: ask.chatId          # same as `reply.chatId = ask.chatId`
  file: picture.image
}
````

A `Group` is the exception: its braces hold its children, so the only field it
reads there is `_should_flow`, and its boundary ports are wired from outside
on their own lines.

### A key that CREATES a port

On types that accept extra inputs (`ExecPython`, `FirstInOrder`, `TagRun`,
`StopTagged`), a key naming
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
changes which branch wins. This is the only place in the language where line
order changes behavior.

### Literals on a connection line

````weft
post = SlackSendMessage { channel: "#alerts" }
post.text = "deploy finished"
````

Both spellings are one thing: a constant written for the port, and no port
takes one spelling and refuses the other. A port refuses a FAMILY, if
anything: `literal` (a value written in the source, markers included) or
`wire` (a value another node produces), through `accepts` in its metadata;
absent means both. Wrong family is `input-accepts`, and the message reads
the list back ("`params` accepts: wire"). A compiler-read port (a form's
`fields`, a switch's `cases`, the access picker) takes an inline typed value
only: no wire, no `@file`, no `@asset`. The line also works on a Group,
Loop, or `@include` alias for its interface ports. An output port never
takes a value, on any node: `step.out = "lit"` and `out: "lit"` beside a
`-> (out: String)` signature are refused, and so is a group's own output
written from inside (`self.result = "lit"`). A firing emits on an output;
you read it as `node.port`.

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

Exactly three, all starting with `_` (any other `_` key is an error):

| Key | Effect |
|---|---|
| `_label: "..."` | display label. A string, set once, never by wire |
| `_tags: ["a", "b"]` | tags, used by signal scoping |
| `_should_flow: <wire or false>` | decides whether this node runs at all |

`_should_flow`: leave it out and the node runs. Wire it and the node runs
only when what arrives is not `false`; a `false` or a closure (whatever
decides never spoke) skips the node, closing its outputs, skipping everything
behind it. `_should_flow: false` in the braces turns one node off. Groups and
Loops take it too, inside the braces or on the container's name from outside.

`_should_flow` is also the ORDERING wire. It takes a value of any type and
only `false` says no, so `typed = ExecPython { _should_flow: typing.done }`
means run once `typing` has fired. The value itself is never read. Never
invent an input port for that (an `after: typing.done` port the code
ignores): it puts a value on the graph that nothing reads, and
`_should_flow` already shows in the graph as a permission wire.

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
that port. Write only the ports the type leaves open (a `MustOverride`
output must be pinned once something reads it; one nothing reads may stay
unpinned). An empty body equals no body.

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
  @require_one_of(email, id)
}
````

`@require_one_of(a, b)` on its own line in a node or inline signature (a
group or loop refuses it; put it on the node inside that needs the ports):
at least one named input must be satisfied. Compile error when
unmet, and a compile error when a name is not a port of that node (on a node
that takes custom ports, a name is a port once the header declares it, a wire
lands on it, or a config key names it); at run time the node skips when every
port in the group arrives closed.

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
| `MustOverride` | the author pins it in an inline signature; wired and unpinned is `must-override-unmet`, unwired is left alone |
| live handles | `Bus`, `Generator[T]`, `Access`; never literals |

A record is usable inline in any signature (`p: { wpm: Number, delay: Number
}`), and a type gets a name either in a node's metadata `types` (e.g.
`ChatHistory`) or in the source, at the top of a scope:

````weft
type Profile = {
  wpm: Number,
  delay: Number
}
typing = ExecPython(p: Profile) -> (ms: Number) { code: "..." }
````

Any type, multi-line allowed, other declared names allowed on the right.
File level is visible to the whole file; directly inside a group or loop
body, to that body and its nested bodies only (the group's own signature
sits outside, so it cannot use a type declared inside). Never inside a
node's braces. A visible name cannot be declared again (no shadowing). Named
types are nominal: the name is the contract. Nothing unnamed wires into a
named target; the door between the worlds is `Cast`.

`?` goes on the NAME and means "may be absent": `here?: String` on an input
port (accepts a closed pulse, fires with the input absent), `name?: String`
on a record field, `notes?: review.notes` on a config key that creates a port.
`here: String?` is refused. An output port takes no `?`: emitting nothing on
it closes it, and no marker changes that.

`Cast` converts a value to the type declared on its output:
`history = Cast() -> (value: ChatHistory)`. The conversion table is checked
at compile time (`cast-not-allowed` for impossible pairs); record and named
targets are validated at run time, errors name the offending field.

If you have a file and the port wants one KIND of file, that is a Cast too.
A node that fetches whatever a message held emits `File` (any stored file);
a node that transcribes emits nothing until it is given `Audio`. `File` into
`Audio` is `type-mismatch`, because the kind is only known once the bytes are
there. So say which kind you are claiming, and the run checks the claim:

```weft
voice = Cast -> (value: Audio) { value: msg.file }
say   = ElevenLabsTranscribeFile { account: key.access, audio: voice.value }
```

A file that really is audio passes through untouched; one that is not fails
the Cast loudly, naming what it got.

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

What runs: a manual run kicks every root (a top-level node no wire feeds).
A trigger is a root, and with no event behind it its outputs close and prune
its branch, so a hand run exercises the paths that need no trigger.
`--target <node>` (repeatable, any node) runs those nodes and what they
need: the union when you name several, nothing past a target, no sibling
branch even one sharing a root, and a target inside a group brings the
whole group and whatever feeds the group's inputs. A trigger fire runs the
fired trigger's own program:
everything downstream of it, plus what that needs upstream, stopping at
other triggers (a trigger's outputs are the event, not a function of its
inputs, which were read once at activation). Every node the fire reaches
runs. One file can hold several programs, one per trigger, and they may
share upstream nodes (one database, one provider): on a fire the shared
node runs for the fired program and the other programs are left without a
trace.

Ends: completed (no pulse in flight), suspended (every live firing parked on
an external wait: a person, a timer; costs nothing), stuck (provably
deadlocked, fails loudly), or cancelled (a person pressed Stop, or a sibling
run stopped it). Everything is journaled; a run is readable node by node with
the values on the wires.

## Stopping other runs

When something new makes the work already in flight pointless, stop the
old runs instead of working around them: a person sends a second message
before the first answer is finished, a new upload replaces a file still
being processed, one run of a batch finds the batch broken and the whole
batch should go down. Never build this with a loop, a flag, a Python
node, or a table: code inside a node cannot stop a run that is already
waiting on a person or a timer, and two events arriving together will
race. A run can put tags on itself, and a new run can stop every older
run carrying a tag. Two catalog nodes do it, wired at the top of the
program, right after the trigger and before any work:

```weft
telegram = TelegramAccess

ask = TelegramReceiveMessage { account: telegram.access }

claim = TagRun { sender: ask.chatId }

stop = StopTagged {
  _should_flow: claim.done
  sender: ask.chatId
}

draft = LlmInference {
  _should_flow: stop.done
  ...
}
```

Every input you wire onto `TagRun` is a tag the run puts on itself; here
the sender, so one person's runs all carry the same tag. `StopTagged`
reads its tags the same way and stops every older run carrying one,
waiting runs included: a run parked on a person or a timer is stopped and
never resumes, its form and its timer are gone, and answering the old
form does nothing. Two messages arriving a moment apart do not kill each
other, because a stop only reaches runs that put the tag on before this
one did: **the newer run survives, the older ones die**. The two
`_should_flow` wires set the order that makes that true: tag first, then
stop, then the work.

`includeSelf: true` on `StopTagged` stops this run too, with the rest:
the shape for one broken run taking its whole batch down. And a
`StopTagged` whose run never put the tag on itself (a supervisor run
clearing one user's whole backlog) has no place in the order, so it
reaches every run carrying the tag.

A stopped run ends cancelled, and its journal names who did it:
`Stopped by execution <color> (tag <tag>)`. Any string is a tag: a
Telegram chat id, a WhatsApp address (`49151@s.whatsapp.net`), a phone
number with a `+` and spaces. Both nodes turn it into a safe tag the same
way (unsafe characters become `_`, a short fingerprint of the original is
appended, so two different values never share one), so wire the raw value
and never clean it up yourself. Inside your own node the same two moves are
`ctx.tag_execution` and `ctx.stop_tagged`, which take a tag already clean
(letters, digits, `_`, `-`, at most 64) and refuse anything else, naming
the character; for those, go and read `weft-node-authoring`.

## Groups

A group is the unit of readable size, the thing [the level rule] asks
for: every level of the graph, the file and the inside of every group,
holds at most about six items, nodes or groups, and past fifteen the
compiler warns `level-too-large`, because a level is what a reader scans
in one look and a sixteenth item ends that. When a level grows past six,
the nodes cooperating on one job become a group of their own, and because
groups nest, depth absorbs size: growing work goes down into a nested
group, never wide across a level, and a group's inside holding another
group or two is the normal shape, not a special one. There is almost
always a way to group: a pipeline stage, one step of the program's story,
the nodes serving one external service. A level that truly cannot shrink
stays flat, but that answer comes last, and past fifteen it is wrong
anyway. Keep the boundary small too: a group with a
dozen ports is two groups, or the wrong split.

````weft
preprocessor = Group(raw: String) -> (result: String) {
  # Cleans and transforms text

  clean = ExecPython -> (out: String) {
    code: "return {'out': text.strip()}"
    text: self.raw
  }
  self.result = clean.out
}

# a group's boundary ports: wired from outside, on their own lines
preprocessor.raw = input.value
````

`self` is the boundary: read `self.<input>` for what the group received,
write `self.<output>` for what it emits. Children reach each other and
`self`, nothing else (`scope-reachability`). Interface ports are set from
outside by wire or literal on their own lines. Groups nest, names are scoped,
and the compiler flattens them away: at run time there is one flat graph. A
group is a region, not a function: it has no call sites and does not return.

A group stops as a whole only through its `_should_flow`; then every node
inside is skipped with the group's name as the reason (`scope_skipped`). A
group input arriving closed passes through to the nodes inside that read it,
which skip, while the rest runs. When a group starts, every node inside that
no wire feeds starts with it (once per iteration in a loop body), so a group
may hold its own source.

Reuse across files: `triage = @include("triage.weft")`, where the included
file is exactly one anonymous top-level group; its ports become `triage`'s.

## Loops

````weft
doubler = Loop(values: List[Number]) -> (results: List[Number | Null]) {
  parallel: false
  over: ["values"]

  step = ExecPython -> (out: Number) {
    code: "return {'out': n * 2}"
    n: self.values
  }
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

Inside a loop, `self` is the LOOP's own boundary, never the group around it.
A loop nested in a group cannot read the group's `self.db`; the value has to
come in as a broadcast input of the loop, declared in its signature and wired
from the group (`run = Loop(db: Access, ...) { ... }` then `run.db = self.db`
at the group level). If you try, you get `unknown-source-port`, and it
names both scopes and the two lines to write.

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
| `@asset("x.txt", String)` | a text file's contents inline | no |

`@file` is bidirectional (the editor writes edits back into the file) so
binary types are refused; it is the marker for `prompts/`, `scripts/`,
`sql/`; its type defaults to `String`. `@asset` ALWAYS names its type, and
a file type names one kind: `Image`, `Video`, `Audio`, or `Blob` (never
`File` or `Media`; the compiler never guesses a kind from a name or bytes).
A file-typed `@asset` resolves through the build's asset sync
(hash-addressed storage; the source keeps one line, never a blob), and the
sync checks the file's bytes against the declared kind (an `Image` over an
mp3 fails the build; `Blob` checks nothing). A list of markers feeds
multi-file ports. `@asset` sources may also be an outside path, an `http(s)`
URL, or a stored file's key; a text-typed one from a URL or a key is
fetched at build.

## Compiler error slugs

Wiring: `type-mismatch`, `deref-path` (a key read off a wire that the
source type does not have), `required-port-unmet`, `unknown-source-node`,
`unknown-target-node`, `unknown-source-port`, `unknown-target-port`,
`double-driven-port`, `input-accepts` (a driver the port refuses, or any
wire or marker on a compiler-read port), `should-flow-not-boolean`,
`value-on-output` (a value written on an output port; an output is emitted
on, never written), `duplicate-input-port`, `duplicate-node-id`,
`undeclared-port-no-custom`.

Types: `unresolved-typevar`, `must-override-unmet`, `cast-not-allowed`,
`config-type-mismatch`, `config-null-literal`, `literal-out-of-range`,
`named-type-conflict`.

Shape: `graph-cycle` (iterate with a Loop, exchange feedback over a Bus),
`scope-reachability`, `level-too-large` (a warning: a
level holds more than fifteen items; group the nodes cooperating on one
job, and nest rather than widen).

Triggers and infra: `trigger-in-loop`, `infra-in-loop`, `trigger-into-trigger`, `trigger-into-infra`,
`duplicate-port`, `config-ports-not-a-list`, `config-entry-not-an-object`,
`unknown-config-entry-kind`, `config-entry-without-a-port`,
`unknown-config-entry-key`, `config-entry-bad-value`, `config-entry-missing-value`, `duplicate-catch-all`,
`catch-all-not-last`.

Loops: `loop-unbounded-no-termination`, `parallel-with-carry`,
`parallel-without-over`, `parallel-with-done`, `over-and-carry-overlap`,
`over-not-a-list`, `over-stream-not-alone`, `gather-output-must-be-nullable`,
`carry-port-type-mismatch`, `loop-over-unknown-port`,
`loop-carry-unknown-port`, `loop-parallel-not-boolean`,
`loop-trim-not-boolean`, `loop-max-iters-not-integer`,
`loop-unknown-config-field`. (`loop-boundary-unpaired` and
`loop-config-missing-parallel` are internal invariants: neither is
reachable from source, and hitting either is a compiler bug worth
reporting.)

Streams: `generator-multiple-consumers`, `generator-through-group`,
`generator-in-container`, `generator-not-carriable`,
`generator-input-must-be-required`, `generator-not-iterated`,
`generator-into-generic-port`.

Names: `reserved-name`, `reserved-port-name`, `unknown-type` (a warning:
the type is not in the project's catalog).

Requirements: `require-one-of-unmet`, `require-one-of-unknown-port`,
`no-required-skip` (a warning: every input a wire feeds is optional, so
the node runs even when all of them closed; add `@require_one_of`),
`rule-structural`,
`rule-runtime` (a node's own declarative validation; the message is the node
author's).

Where these run, in tiers: [the edit tier] is the strict parse plus
the structural rules, fast and local, enforced by the agent's edit loop and the
`PostToolUse` hook, and what keeps the graph rendering. [the runtime tier]
(`rule-runtime`) is what only a running program can know, chiefly a
connection not picked on an access node (the compiler synthesizes that rule
from the node's `service` declaration; `connection_optional: true` in the
service block is the opt-out, for a node that can run with no connection
picked). `weft build` deliberately skips it, so a half-wired program still
compiles, and the rules fire at execution as loud node failures. `weft
validate` reports them early, and so does the editor's pre-flight gate on
Run, Activate and Resync; the Problems panel is structural only and never
shows them. [the build tier] is `weft build`: structural errors plus the
cargo and image build. The slug names the rule, the message names the fix.
