---
name: weft-language
description: "Read before writing or editing any .weft source: declarations, wiring, config literals, reserved keys, inline signatures and expressions, types, groups and the level rule, included files, loops, the pulse execution model, @file, @asset and @include, and every compiler error slug."
---

# The weft language

One way to say each thing, and the compiler refuses everything else. This
page is the whole surface as of this template; when it disagrees with your
memory, this page wins.

Two words the whole page leans on. A [pulse] is one value emitted on one
output port and addressed to one input port of one node; nothing walks the
graph, a node fires when every required input holds a [pulse]. A [closed]
pulse carries no value and means "nothing will ever arrive here"; a port
that never receives anything is closed the same way.

## Declaring a node

````weft
name = NodeType
name = NodeType { config_field: value }
name = NodeType {}
````

`name` is the node id, unique in its scope, snake_case. `NodeType` must exist
in the project's `nodes/` catalog; you read its real ports with
`weft describe-nodes --node <Type> --compact`, never from memory.

## Connecting

````weft
target.input_port = source.output_port
````

Right to left: the value flows from `source.output_port` into
`target.input_port`, and the types must be compatible. Every required input
gets a wire or a literal; an optional one (`port?`) can stay unwired. This
line is the longhand; [the shorthand] (next section) is the same wire inside
the target's braces, and the longhand survives only where the language
leaves no choice: a [boundary port], one of the ports a group, a loop, or an
`@include` alias declares in its signature.

If you want one key of a record value, keep going with dots
(`wpm: reader.profile.stats.wpm` in the braces, or
`speed.wpm = reader.profile.stats.wpm`). Still one wire; the compiler checks
every key against the record type at that level and the wire carries the
last key's type. A source typed `JsonDict` or a scalar has no keys to read
(`deref-path`): narrow the source port to the shape it carries (next), or
`Cast` first. At run time a `?` key found absent (or `null`) closes that
wire alone; a required key absent fails the firing.

### Narrowing a port to the shape it carries

A port's type is a contract, and you may write a TIGHTER one on the arrow
where you use the node. The compiler checks yours against the node's own and
refuses a contradiction (`declared type ... incompatible with catalog type`),
so a narrowing that compiles is one the node can honour. This is how a loose
container becomes a shape you can read keys off, and it costs nothing at run
time: the value is already that shape, the port now says so.

A port the node already declares takes the tighter type the same way one you
add on the arrow does. Here the node's own `rows` port is `List[JsonDict]`,
which has no keys to read, and naming the row's shape makes `.title` legal:

````weft
type Card = { title: String, seen: Number }

rows = PostgresExecuteQuery -> (rows: List[Card]) {
  account: store.access
  query: "select title, seen from cards"
}
````

`Cast` is for a value that has to CHANGE type while it travels. A value that
already fits needs no node at all, only a port that names the type. If you
catch yourself adding a `Cast` to put a shape on a value that already has
it, stop and write: "Wait. Narrow the port." Then write the type on the
arrow and delete the `Cast`.

**Narrow only what you control the shape of.** A port fed by the outside
world (a query string, request headers) holds whatever the caller sent, and
a record refuses a key it does not declare, at run time, on the real
request. Narrowing those compiles and then fails the first time somebody
appends a tracking parameter. Leave them as the loose type the node declares
and read the one key you want with a node.

When the opaque value comes from a node that lets you ADD OUTPUT PORTS, name
the pieces you want on the arrow instead. Each arrives on its own port under
the type you wrote, so there is nothing to unpack and nothing to Cast:

```weft
look = PostgresExecuteQuery(id: String) -> (title: String, seen: Number) {
  query: "select title, seen from cards where id = $id"
}
```

Declare the type you actually want there, a named one included
(`-> (card: Card)`): the value is checked against it when it arrives, so a
shape that fits flows and one that does not fails loudly, naming the field.
Reach for the record type when the source cannot name the pieces for you, and
prefer a NAMED type over an inline one once more than one node speaks it:
the name is the contract, and it is written once.

## Config values

Typed JSON-ish literals:

````weft
t    = Text     { value: "a string" }
n    = Range    { to: 10, step: 2 }
flag = SomeNode { enabled: true }
arr  = SomeNode { items: [1, 2, 3] }
obj  = SomeNode { opts: { "k": "v" } }
````

Commas between fields are optional; a field per line with no commas reads
the same. `null` is never a literal: omit the field instead
(`config-null-literal`).

A list or object literal holds plain values only, so `params: [self.chatId,
self.pushName]` is refused. When a node needs several values from the graph,
each one is its own input port. When a node accepts however many values you
give it, declare them in its inline signature:
`PostgresExecuteQuery(chat_id: String, push_name: String) { ... }` and the
SQL reads `$chat_id`; `Format(user: String) { template: "Hi {{user}}" }`.

A Python node whose code has no branch, no loop and no call is moving
values, not deciding anything, and the language moves values. Read the code
you are about to write and apply that test to it. Pulling a field out of a
dict is the case you will meet most; the test is what catches the rest.

If you catch yourself writing one, stop and write: "Wait. That is
plumbing." Then read the key with dots, narrow the port to the shape it
carries, declare the ports on the consumer, or wire `_should_flow` (under
Reserved keys).

A branch, a loop or a call is real work, and Python is the right answer for
it: filtering a list on a condition, parsing what no type describes, calling
a library.

Building an object out of values you already hold (a reply body, a payload)
is the case you will meet most, and it has its own node: wire a value onto
each key you want and the object comes out keyed by those names. An object
LITERAL cannot hold a wire, which is what used to force a script here.

### Wires in the braces

A field whose value is `source.port` is a wire, written inside the node it
feeds. This is [the shorthand]: a node's wires sit next to its settings:

````weft
reply = TelegramSendMedia {
  account: telegram.access
  kind: "photo"
  chatId: ask.chatId          # same as `reply.chatId = ask.chatId`
  file: picture.image
}
````

A `Group` is the exception: its braces hold its children, so the only field it
reads there is `_should_flow`, and its [boundary port]s are wired from
outside on their own lines.

### A key that CREATES a port

Some node types accept extra inputs beyond the ones they declare, and on
those a key naming no declared port creates one. A wire types it from the
source, a literal from its own type, `null` is an error. `key?:` makes the
created port optional, whatever the node's own default is.

Which types accept them is the node's to say, not this page's: its wiring
view carries a `features` block, and `canAddInputPorts` there means you may
add inputs, `canAddOutputPorts` that you may name outputs on the arrow. A
node with neither key, or no `features` block at all, takes only the ports
it declares.

````weft
step = ExecPython -> (out: String) {
  code: @file("assets/scripts/step.py")
  text: draft.answer      # a String port, from the wire
  limit: 3                # a Number port, from the literal
  notes?: review.notes    # optional: a closed pulse here does not skip the node
}
````

Created ports keep written order, and whether a node reads that order is the
node's own business, stated in its description. `FirstInOrder` is the
catalog node that does: its first input that carried a value is the one it
emits, so reordering its lines changes which branch wins. That is the only
way line order changes what a program does.

### Literals on a connection line

````weft
post = SlackSendMessage { channel: "#alerts" }
post.text = "deploy finished"
````

Both spellings are one constant, and no port takes one spelling and refuses
the other. What a port can refuse is a family:
`literal` (a value written in the source, markers included) or `wire` (a
value another node produces), through `accepts` in its metadata; absent means
both. Wrong family is `input-accepts`, and the message reads the list back
("`params` accepts: wire"). A compiler-read port (a form's `fields`, a
switch's `cases`, the access picker) takes an inline typed value only: no
wire, no `@file`, no `@asset`. The line also works on a [boundary port]. An
output port never takes a value, on any node: `step.out = "lit"` and
`out: "lit"` beside a `-> (out: String)` signature are refused, and so is a
group's own output written from inside (`self.result = "lit"`). A firing
emits on an output; you read it as `node.port`.

### Multi-line strings

````weft
step = ExecPython() -> (out: Number) {
  code: ```
    return {'out': 42}
  ```
}
````

The opening fence goes on the key's line, content starts on the next line,
the closing fence on its own line.

## Reserved keys

Exactly four, all starting with `_` (any other `_` key is an error):

| Key | Effect |
|---|---|
| `_label: "..."` | display label. A string, set once, never by wire |
| `_tags: ["a", "b"]` | tags, used by signal scoping |
| `_should_flow: <wire or false>` | decides whether this node runs at all |
| `_should_not_flow: <wire>` | the same decision read backwards: runs when what is wired here did NOT arrive |

`_should_flow`: leave it out and the node runs. Wire it and the node runs
only when what arrives is not `false`; a `false` or a [closed] pulse
(whatever decides never spoke) skips the node, closing its outputs, skipping
everything behind it. `_should_flow: false` in the braces turns one node off.
Groups and Loops take it too, inside the braces or on the container's name
from outside.

`_should_not_flow` is the mirror image: wire something into it and the node
runs when that thing closes instead of when it arrives. It is the only port
in the language that can start a node on a closure; every other port does the
opposite, skipping its node once an input closes, which is why running on an
absence needs a dedicated spelling.

Reach for it when the absence is data: a key the caller never sent, an
optional input nobody filled in. When the absence is a decision a node of
yours already made, put that decision on a second output port and gate on it
with `_should_flow` instead; the wire then reads forwards, cause before
effect.

A node carries one gate, never both: wiring `_should_flow` and
`_should_not_flow` on the same node is a compile error, `two-gates`. The
editor draws both as the same triangle, with a small circle marking
`_should_not_flow`; right-click the gate to flip which one it is.

`_should_flow` is also the ordering wire, and it accepts ANY port: any type,
any node, no declaration needed on either side. `typed = ExecPython {
_should_flow: typing.done }` means run once `typing` has fired. The value is
never read, so you never invent a port to carry it (an `after: typing.done`
port the code ignores puts a value on the graph that nothing reads, while
`_should_flow` already shows in the graph as a permission wire). The one
value that IS read is `false`, so wire a Boolean port here only when its
`false` should mean "do not run".

That is also how a node with nothing to receive gets its turn. `Close` takes
no data at all, so ending a branch early is one wire from any port on it:

````weft
bye = Close { status: 204 }
bye._should_flow = cleanup.removed
````

A branch that skipped closes its ports, so whatever hangs off it by
`_should_flow` skips with it: the branch that did not run does not act.

On a group or an included file the wire means "run what is in here": a
route's run takes the whole group along, and everything the group needs
(a database wired into it from outside), exactly as it takes a node.

````weft
live = Route { path: "live/count", method: "GET" }
work = Group(db: Access) {
  rows = PostgresExecuteQuery { account: self.db, query: "select count(*) from cards" }
  out = Reply
  out.body = rows.rows
}
work.db = db.access
work._should_flow = live.method     # the route runs the group; nothing else connects them
````

### Running something once, when the program goes live

If you want something to happen once before a program serves anything
(creating tables, seeding a row, warming a cache), write the node and wire it
into the trigger. That is the whole mechanism, and it is worth understanding
rather than memorising.

Everything UPSTREAM of a trigger is the trigger's setup program, and weft
runs that program once, at activation. On a fire the trigger reads none of
it: a fired trigger's inputs come from its bake, so nothing upstream runs
again.

Any wire into the trigger puts the producer there, not only the gate. The
gate is just the wire with nothing else to say:

````weft
make = PostgresExecuteQuery { account: db.access, query: @file("assets/sql/schema.sql") }
live._should_flow = make.count
````

Whatever that setup does, it does again on every activation, so it has to be
safe to do twice: doing it a second time must not undo, duplicate or refuse
what the first time did. How you write that is the node's business, and the
node's own description says how; the language's part is only that it will
happen more than once.

The same holds for infrastructure: anything upstream of an infra node is that
node's setup program and runs when infra starts.

Two things follow that surprise people. A `weft run --fire` warns that the
wire was not delivered, which is correct and not a problem: that wire's whole
job was to run at activation. And when the trigger lives in an included file
and the setup node does not, the value has to cross the include's boundary
like any other, so the file declares a port for it; there is no shortcut, and
keeping the setup node in the same file as its trigger avoids the crossing
entirely.

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
dict keyed by output port name; `None` or a missing key emits no [pulse] on
that port. Write only the ports the type leaves open (a `MustOverride`
output must be pinned once something reads it; one nothing reads can stay
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
at least one named input must be satisfied. Compile error when unmet, and a
compile error when a name is not a port of that node (on a node that takes
custom ports, a name is a port once the header declares it, a wire lands on
it, or a config key names it); at run time the node skips when every port in
the group arrives [closed].

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
Declared at file level it is visible to the whole file; directly inside a
group or loop body, to that body and its nested bodies only (the group's own
signature sits outside and cannot use it). Never inside a node's braces. A visible name cannot be declared again (no
shadowing). Named types are nominal: the name is the contract, nothing
unnamed wires into a named target, and the door between the two is `Cast`.

`?` goes on the name and means "may be absent": `here?: String` on an input
port (accepts a [closed] pulse, fires with the input absent), `name?: String`
on a record field, `notes?: review.notes` on a config key that creates a port.
`here: String?` is refused. An output port takes no `?`: emitting nothing on
it closes it, and no marker changes that.

`Cast` CONVERTS a value to the type declared on its output:
`history = Cast() -> (value: ChatHistory)`. The conversion table is checked
at compile time (`cast-not-allowed` for impossible pairs); record and named
targets are validated at run time, errors name the offending field.

Reach for it when a value has to CHANGE type while it travels, and only
then. It is not how you put a type on a value: a port you declare already
carries the type you wrote on it, and the value is checked against that on
arrival, so a shape that fits flows and one that does not fails loudly. If
the node can name the port, name it there and skip the `Cast`. And a value
that already fits its target passes through a `Cast` untouched, so one
written "to be safe" is a node that does nothing.

If you have a file and the port wants one kind of file, that is a Cast too:
a node that fetches whatever a message held emits `File` (any stored file),
a transcriber takes only `Audio`, and `File` into `Audio` is
`type-mismatch`, because the kind is only known once the bytes are there. Say
which kind you are claiming, and the run checks the claim:

```weft
voice = Cast -> (value: Audio) { value: msg.file }
say   = ElevenLabsTranscribeFile { account: key.access, audio: voice.value }
```

A file that really is audio passes through untouched; one that is not fails
the Cast loudly, naming what it got.

Compatibility: identical types; unions member-wise; `JsonDict` with any
string dict; a named type only with the same name (a `JsonDict` into it is
`type-mismatch`, and the door is a Cast, which checks the shape; a group that
carries an `LlmProvider` declares its port `LlmProvider`, never `JsonDict`);
containers element-wise; type variables unify. Everything else is
`type-mismatch` naming both types.

## How a program runs

A node fires when every required input holds a [pulse] that agrees on
[color] (one run: every run gets one color, and a re-run is a new color) and
on frames (loop iterations).

A node that emits on some outputs and not others closes the rest. A [closed]
pulse on a required input skips the node and closes its outputs; on an
optional input the node fires anyway; on `_should_flow` the node skips,
period. Skips cascade forward until a node opted into absence with `?`. A
failure propagates exactly like a missing value, and a downstream optional
input is the recovery path.

Branching is only this: a branch in weft is a node that ran or a port that
closed, and nothing else. Any Boolean reaches a `_should_flow`, so a node of
any kind decides a branch by emitting one (a moderation check's `flagged`, a
request's `ok`, a lookup's `found`). Three nodes exist to shape the decision
itself, and you reach for them rather than deriving them: `Switch` tests a
value against its `cases` config and emits `true` on the winning case's
port, closing the rest; wire a case port into the branch's `_should_flow`.
`FirstInOrder` does not decide anything, it MERGES: it emits the first of
its inputs that carried a value, in written order, so alternative paths
rejoin into one wire. `All` says
yes only when every input wired onto it arrived and none of them is `false`,
and closes its output otherwise; it is where the second answer goes when a
gate takes one wire and the permission has two parts.

Three shapes come up constantly, so reach for them rather than deriving them
again:

````weft
# One gate, two conditions: delete only if the model said rude AND said sure.
agreed = All
agreed.rude = judge.rude
agreed.sure = judge.sure
remove._should_flow = agreed.yes

# A default: the caller's `limit` when they sent one, else 20.
limit = FirstInOrder
limit.asked = door.limit
limit.fallback = 20

# An object out of values the graph computed: one key per wire.
body = JsonObject {
  job: open.id
  status: "running"
}
````

That last one is worth knowing before you reach for a script: an object
LITERAL holds plain values only, so `{ "job": open.id }` is refused, and
building one out of wires is a node's job. Declare the shape on its output
when the consumer wants a named type (`-> (object: Ticket)`), and nest by
wiring one onto a key of another.

`All` reads a value exactly the way a gate does, so only `false` is a no and
anything else is a yes, and a branch that closed never arrives at all, which
is a no as well. That makes it the AND of decisions and of arrivals in one
node. `FirstInOrder`'s written order is priority, so the fallback goes last.

A node can also branch by ABSENCE, which is often shorter than a `Switch`.
A port that emits nothing is closed, and a closed port skips what hangs off
it, so one query whose columns are null except the one that applies picks the
branch by itself:

```weft
look = PostgresExecuteQuery(id: String) -> (card: JsonDict, missing: Boolean) {
  query: "select c.data as card, (c.id is null) as missing from ..."
}
```

`card` closes on the rows where the SQL says null, `missing` closes on the
others, and each one gates its own branch. A port that DECLARES null among
its types (`String | Null`) is the opposite: a null arrives as data and the
node runs. And a statement with nothing to say still emits `count` (0) and
`rows` ([]), so a setup script is gated on `count` without inventing a row
for it to return.

A port's DECLARED type is what judges every value on it, at run time as at
compile time: a `JsonDict` port takes any object whatever keys it carries, and
only a port declared `Image` reads a picture out of one. A value the declared
type refuses fails the node by name, with whatever it already sent still
sent.

What runs: a manual run starts ordinary roots within its selection. A trigger
requires one explicit fire or supplied emitted outputs. `--target <node>` runs
through that endpoint; `--before <node>` excludes it. Repeated endpoints
select the union of their required work. Ordinary groups are cut precisely,
with their flow gates still applied; loops stay whole and refuse interior
cuts. A shared root does not dispatch work outside the selection.

A trigger fire starts from exactly that trigger and follows its program,
including the dependencies needed downstream. Its input settings come from
matching preparation; its wake payload belongs to this event. Other triggers
are not fired. Preparation and infra extraction also stop precisely inside
ordinary groups and refuse interior loop cuts. One file can hold several
trigger programs sharing dependencies without executing unrelated branches.

Ends: completed (no pulse in flight), suspended (every live firing parked on
an external wait: a person, a timer; costs nothing), stuck (provably
deadlocked, fails loudly), or cancelled (a person pressed Stop, or a sibling
run stopped it). Everything is journaled; a run is readable node by node with
the values on the wires.

## Stopping other runs

When something new makes the work already in flight pointless (a second
message before the first answer is finished, a new upload replacing a file
still being processed, one run of a batch finding the batch broken), you
stop the old runs with two catalog nodes, wired right after the trigger and
before any work. A run puts tags on itself with `TagRun`, and a new run
stops every older run carrying a tag with `StopTagged`:

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

If you catch yourself building this with a loop, a flag, a Python node, or
a table, stop and write: "Wait. Tag the run, then stop the tagged ones."
Bookkeeping you write yourself cannot reach a run already parked on a person
or a timer, and two events arriving together will race it. Tagging and
stopping are the engine's, reached by two calls: the catalog wraps them as
the pair below, and a node of your own makes the same two calls (named at
the end of this section).

Every input you wire onto `TagRun` is a tag the run puts on itself; here the
sender, so one person's runs all carry the same tag. `StopTagged` reads its
tags the same way and stops every older run carrying one, waiting runs
included: a run parked on a person or a timer is stopped and never resumes,
its form and its timer are gone, and answering the old form does nothing. A
stop only reaches runs that put the tag on before this one did, so two
messages arriving a moment apart do not kill each other: **the newer run
survives, the older ones die**. The two `_should_flow` wires set the order
that makes that true: tag first, then stop, then the work.

`includeSelf: true` on `StopTagged` stops this run too, with the rest: the
shape for one broken run taking its whole batch down. A `StopTagged` whose
run never put the tag on itself (a supervisor run clearing one user's whole
backlog) has no place in the order, so it reaches every run carrying the tag.

The other reason to reach for the pair is a run that HOLDS something rather
than one that has gone stale. A run that holds a caller's connection open
lives as long as that caller: one run per open tab, and a person who
reloads three times leaves three behind. A socket run always holds one, and
so does a route whose answer arrives over time rather than in one
piece. Tag such a run by
whatever names the connection (the room, the board, the person) and the next
connection from it stops the one before, so a reconnect REPLACES its run
instead of stacking on it.

A stopped run ends cancelled, and its journal names who did it:
`Stopped by execution <color> (tag <tag>)`. Any string is a tag: a Telegram
chat id, a WhatsApp address (`49151@s.whatsapp.net`), a phone number with a
`+` and spaces. Both nodes clean it the same way (unsafe characters become
`_`, a short fingerprint of the original is appended, so two different
values never share one), so you wire the raw value and never clean it up
yourself. Inside your own node the same two moves are `ctx.tag_execution`
and `ctx.stop_tagged`, which take a tag already clean (letters, digits, `_`,
`-`, at most 64) and refuse anything else, naming the character; for those,
go and read `weft-node-authoring`.

## Groups

A group is the unit of readable size, what [the level rule] asks for: every
level of the graph, the file and the inside of every group, holds at most
six items, nodes or groups; past fifteen the compiler warns
`level-too-large`. At the file's top level the count is per connected
branch, the items one wire walk reaches plus the infra nodes it touches
(worked example below). Growing work goes down into a nested group, never
wide across a level, and a group's boundary stays small: a group with a
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
`self`, nothing else (`scope-reachability`). A [boundary port] is set from
outside by wire or literal on its own line. Groups nest, names are scoped,
and the compiler flattens them away: at run time there is one flat graph. A
group is a region, not a function: it has no call sites and does not return.

A group stops as a whole only through its `_should_flow`; then every node
inside is skipped with the group's name as the reason (`scope_skipped`). When
a group starts, every node inside that no wire feeds starts with it (once per
iteration in a loop body), so a group can hold its own source.

A [closed] pulse on a group INPUT does not stop the group. It travels in like
any other pulse and lands on the nodes that read that input; those skip, and
everything else in the group runs. Two readers of this page in a row got it
backwards and one of them reported a working program as a critical bug, so
here it is worked through. A card whose picture is optional:

````weft
look = Route -> (text: String, photo: Image) { path: "cards", method: "POST" }

card = Group(body: String, picture?: Image) -> (saved: String) {
  store = ExecPython(text: String, shot?: Image) -> (line: String) {
    code: "return {'line': text + (' with a picture' if shot else '')}"
    text: self.body
    shot: self.picture
  }
  self.saved = store.line
}
card.body = look.text
card.picture = look.photo

answer = Reply
answer.body = card.saved
````

Post a card with no picture. `look` emits nothing on `photo`, so `photo`
closes; `card.picture` carries that closure in. `store` declares `shot?`, so
a closed pulse there does not stop it: it runs with `shot` absent, writes its
line, and the caller gets an answer. Nothing else in the group noticed.

Drop the `?` from `shot` and `store` skips instead, `self.saved` never gets a
value, `saved` closes, `answer` skips, and the caller is left with nothing.
That one character is the whole difference between "the picture is optional"
and "there is no card without a picture". Note where it sits: on the port of
the node that reads the value, never on `look`'s output (an output carries no
`?`; emitting nothing IS how it says "absent").

Worked, for the per-branch count: a file holding `db = PostgresDatabase`, a
`cards` group (four nodes inside), a `gallery` group (three nodes inside) and
a `stats` group (two nodes inside), each of the three wired to `db`, is three
branches of two items each (the group and the database), well inside the
rule, whatever the groups hold inside; the inside of each group answers for
itself. Twelve flat nodes all wired to `db` is one branch of thirteen, and
the answer is those three groups.

### Included files

Reuse across files: `triage = @include("triage.weft")`, where the included
file is exactly one anonymous top-level group and nothing else; its ports
become `triage`'s:

```weft
# src/triage.weft: sorts a message into a lane
Group(text: String) -> (lane: String) {
  sort = LlmInference -> (lane: String) {
    provider: model.provider
    prompt: @file("assets/prompts/triage.md")
    parseJson: true
    input: self.text
  }
  self.lane = sort.lane
}
```

A trigger inside an included file works like a trigger inside a group: it
registers when the including program activates, and it is addressed through
its site (`one.door`).

The file is compiled once and every `@include` of it is a call: a run
reaching `triage` sends its port values into the file's one body under a
frame naming the call site, and the results come back to that site alone.
Includes and loops nest freely.
An included file has no name you write or read: a node inside is always
addressed through the site, the way the source reads. `weft events <run>
--node triage.classify` shows that one use of it and every row prints its
node that way, `weft run --group triage` runs the call, and a cut inside the
file is spelled the same (`--from triage.classify`, `--target
triage.classify`) and runs inside that one call. The same file included
twice reads apart (`triage.classify`, `again.classify`).

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

Inside a loop, `self` is the loop's own boundary, never the group around it.
A loop nested in a group cannot read the group's `self.db` (that is
`unknown-source-port`, naming both scopes and the two lines to write); the
value comes in as a broadcast input of the loop, declared in its signature
and wired from the group (`run = Loop(db: Access, ...) { ... }` then
`run.db = self.db` at the group level).

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

A stored file travels a wire as a small reference (its key, name, mime type
and size), never as bytes. Inside the node that receives it the reference
also carries a `url`, a download link minted for that one firing; the link
is stripped from everything that leaves the node and is never written to
the journal, so `weft events` shows the reference without it, and that is
expected. `ExecPython` hands the reference over unwrapped, as a plain dict
(`photo["url"]`); its description says how.

`@file` is bidirectional (the editor writes edits back into the file), so
binary types are refused; it is the marker for `assets/prompts/` and
`assets/scripts/`, and its type defaults to `String`. `@asset`
always names its type, and a file type names one kind: `Image`, `Video`,
`Audio`, or `Blob` (never `File` or `Media`; the compiler never guesses a
kind from a name or bytes). A file-typed `@asset` resolves through the
build's asset sync (hash-addressed storage; the source keeps one line, never
a blob), and the sync checks the file's bytes against the declared kind (an
`Image` over an mp3 fails the build; `Blob` checks nothing). A text-typed
`@asset` is read once at build from wherever its path points, in the project
or anywhere on the machine, and its text is inlined. A list of markers feeds
multi-file ports. An `@asset` source can also be an outside path, an
`http(s)` URL, or a stored file's key; a text-typed one from a URL or a key
is fetched at build.

## When a check runs

Every compiler error carries a slug naming the rule and a message naming
the fix, so read the one you got rather than looking a list up. What is
worth knowing in advance is WHEN each kind of check runs, because that
decides whether a mistake reaches you while you type or while the
program runs.

[the edit tier] is the strict parse plus the
structural rules, fast and local, run by the `PostToolUse` hook after every
edit and by your own validate run, and what keeps the graph rendering. [the runtime tier] is
`rule-runtime`: what only a running program can know, chiefly a connection
not picked on an access node (the compiler synthesizes that rule from the
node's `service` declaration; `connection_optional: true` in the service
block is the opt-out, for a node that runs with no connection picked). `weft
validate` and the editor's pre-flight gate on Run, Activate and Resync report
it; the Problems panel is structural only and never shows it; `weft build`
skips it, so a half-wired program still compiles, and the rule fires at
execution as a loud node failure. [the build tier] is `weft build`:
structural errors plus the cargo and image build. The slug names the rule,
the message names the fix.
