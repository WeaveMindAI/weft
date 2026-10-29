# Groups

A group holds several nodes under one name and folds shut like a folder. Its
signature says what the outside can see.

```weft
credit = Group(db: Access, user: String) -> (paid: Boolean, refusal: String) {
  # Take one credit off this account, or say why we cannot
  debit = PostgresExecuteQuery(telegram_id: String) {
    query: @file("assets/sql/spend_credit.sql")
    account: self.db
    telegram_id: self.user
  }

  read = ExecPython(rows: List[JsonDict]) -> (paid: Boolean, refusal: String) {
    code: @file("assets/scripts/outcome.py")
    rows: debit.rows
  }

  self.paid = read.paid
  self.refusal = read.refusal
}
credit.db = pg.access
credit.user = ask.user
```

`self` is the group's own interface. On the right of an arrow it means the
values coming in from outside; on the left it means the values going out. It is
always the innermost scope, so inside a loop inside a group, `self` is the
loop.

Set the group's inputs from outside, on its name, as `credit.db` does. You
cannot write a literal onto `self.port`: a group's input is given its value
from outside. The one exception is a [metadata port](#metadata-ports), whose
value you write in the body.

A plain comment as the first thing in the body becomes the group's description,
which is what the graph shows when it is folded shut.

## No node stands for a group when the program runs

A wire through a group's ports is one wire, from the node that sends to the
node that reads, and the group's ports change nothing on the way. The group
itself is a **scope**: the runtime decides, once for each copy of it (one per
iteration of a loop around it, one per `@include` of a file), whether it opens
or skips, and everything inside follows.

So folding a group in the editor costs nothing, and neither does nesting five
deep.

## When a group starts

A group with a gate wired to it (`_should_flow` or `_should_not_flow`) waits
for that value and opens or skips on what it says. One with a gate written in its braces decides at once.
One with no gate opens on the first value that reaches any node inside it from
outside, or at once when nothing comes in from outside; if every wire coming in
closes with nothing on it, it skips. An included file is a group, so each
`@include` starts the same way.

When it opens, every node inside that no wire feeds starts with it. So a node
with no inputs needs no `_should_flow` to get its turn. Every other node inside
waits on its own inputs, and every output leaves the moment it is made.

It only runs in a run that takes it along, though. A trigger's run holds what
the trigger feeds plus what those nodes read, so a node with no inputs inside a
group comes along when something that run uses reads it, or when the run feeds
the group's `_should_flow`, which brings the whole group in. If several routes
feed one include, a route that did not fire never holds it up: it is either
left out of the run, or it runs only to close its outputs, and a closure counts
as arrived.

## A group input passes through

This is the part that surprises people.

You give a group a `db: Access` input, then the thing feeding it closes. You
would expect the whole group to skip.

It does not. A group input holds nothing: whatever reaches it, a value or a
closure, goes straight **inside**, and whichever node actually needed that
value skips there, and the skip cascades from that node outward. Without a
gate, the whole group skips only when every wire coming into it closes with
nothing on it.

That is deliberate. A group is usually several jobs, and one missing input
often only kills one of them. Skipping the whole group would throw away work
that had everything it needed.

So a group input is never required or optional itself, and a `?` on one is
refused with `optional-pass-through-port`. The editor draws these ports with
their own marker, a ring with a dot in the middle. Whether a node runs without
the value is said on that node's own input. It also means `@require_one_of` is
refused on a group: put it on the node inside that needs one of those values.

A loop is the exception, for the ports it iterates or threads: with no list to
walk, there is no iteration to launch. Those ports keep their `?`, and every
other port of a loop passes through like a group's.

## When a group input has to be connected

What the compiler checks is whether a node inside needs the port.

It follows the port inward, through any group, loop or included file nested
behind it, until it reaches the nodes that read it. If one of them takes it
into an input it needs (one written without `?`, see [optional and
required inputs](syntax.md#optional-and-required-inputs); a default does not
help, since it only fills an input no wire reaches, and an input of a join node
such as `FirstInOrder` is optional without any `?`), or into the only
member of a `@require_one_of` set that could get a value, and nothing outside
connects or writes the group's port, the build fails with
`required-port-unmet`, naming both ends:

```text
input 'g.b' has nothing connected, and inside it feeds 'g.f.y', a required
input, so 'g.b' is required too: wire or write it
```

A port that only inputs written with `?` read may stay unconnected: the closure goes
inside and each of them handles it the way it handles any closure. An included
file's ports and a loop's other ports follow the same rule.

## Skipping the whole thing

`_should_flow` goes inside the braces, and when it skips a group it skips
**everything** in it, however deep, including nested groups. One line guards a
whole subgraph.

```weft
brief = Group(request: String) -> (prompt: String) {
  write = Format(request: String) {
    template: "Draw this: {{request}}"
    request: self.request
  }
  self.prompt = write.text
}
brief.request = ask.text
brief._should_flow = credit.paid
```

The gate is consumed at the boundary. The nodes inside never see it, and they
do not each get to decide: they take the group's decision as a whole.

Only the incoming side takes a gate. The outgoing side has nothing to decide,
because a group that did not run has nothing to forward.

## What crosses the boundary

| Thing | Crosses? |
|---|---|
| Ordinary values, connections, file markers, bus markers | Yes |
| A closure | Yes, and it cascades to whatever inside needed the value |
| `_should_flow` and `_should_not_flow` | No. Read by the group itself; no node inside sees it |
| A stream (`Generator[T]`) | **No** |
| A wire from inside to outside that skips the interface | No |

A stream is a live edge between two running nodes, so both ends have to be in
the same scope. If you need a stream to cross, either move the consumer in, or
have a node read the stream and emit ordinary values. A loop is the one thing
that takes a stream across its edge, through its own `over`.

That last row is the `scope-reachability` error. A value crossing the line has
to appear on the interface, where somebody reading the group can see it.

## Metadata ports

Say a group is a tool a model can call. The model has to be told about the
tool (its name, what it does, what arguments it takes) before it runs, and the
group only runs once the model has asked for it. The description belongs to
the group, but the node that needs it sits before the group in the graph. An
ordinary wire from the group back to the model would be a cycle.

A **metadata port** holds a value you write in the program, which no run
computes. You declare one among the group's outputs with a `*` before its name,
and write its value in the body, on `self`:

```weft
search = Group(query: String) -> (results: String, *tool: { name: String, description: String }) {
  self.tool = { "name": "search", "description": "Search the web." }
  echo = Text {}
  echo.value = self.query
  self.results = echo.value
}
search.query = "cats"
tools = List {}
tools.search = search.tool
```

The value is JSON, so its keys are quoted. The port needs a type, and the type
cannot hold a type variable like `T`, because nothing wired to the port could
tell what `T` stands for. (A tool a real model reads also needs `parameters`:
see the `LlmTool` further down.)

A wire that leaves a metadata port is a **metadata wire**, and it never runs.
When the program compiles, the compiler writes the value into the input the
wire points at, as if you had typed `tools.search = { "name": "search", ... }`
yourself. So `search` does not have to run for `tools` to have its value.

That is why a metadata wire may point backwards. This compiles, and with an
ordinary wire from `g` into `tools` it would be a `graph-cycle`:

```weft
g = Group(items: List[{ name: String }]) -> (*tool: { name: String }) {
  self.tool = { "name": "g" }
  shown = Debug { data: self.items }
}
tools = List {}
tools.g = g.tool
g.items = tools.list
```

### Where the value lands

The value ends up where the wire ends, and from there on it is a value you
wrote:

- **On a node's input.** The input takes the metadata port's type, so in the
  first example the List's output is
  `List[{name: String, description: String}]`. If the value does not fit the
  input, that is `config-type-mismatch`. If the input already has a value
  written on it, or a second metadata wire, that is `double-driven-port`. An
  input that takes only wires refuses it with `input-accepts`.
- **On a group's input, from outside** (`h.t = search.tool`). The value is
  written on the group's port, and the nodes inside that read `self.t` get it
  when the group opens, like any value written there.

If the wire reads one key of the value (`search.tool.name`), that key's value
is what gets written, and a key the value does not hold is `deref-path`.

### Taking the value from other metadata ports

Instead of writing the value out, a group can take it from other metadata
ports. Name one where a value goes, as `member.port`, and the compiler puts
that port's value there. A name reads the way it does where you write it: a
member of the group first, else something of that name in an enclosing
group, nearest first, else something at the top of the file. That is how a
value gets out of a nested group:

```weft
outer = Group() -> (*tool: { name: String }) {
  inner = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "deep" }
  }
  self.tool = inner.tool
}
```

A name can also sit inside the JSON, and inside a list `...` puts a list's
items in place of the name, so a group can gather what its members describe:

```weft
toolbox = Group() -> (*tools: List[{ name: String }]) {
  one = Group() -> (*tools: List[{ name: String }]) {
    self.tools = [{ "name": "one" }, { "name": "uno" }]
  }
  two = Group() -> (*tool: { name: String }) {
    self.tool = { "name": "two" }
  }
  self.tools = [...one.tools, two.tool, { "name": "three" }]
}
```

`toolbox.tools` is the four tools, in that order. A name reads keys off a
value the way a wire does (`act.tools.anthropic`), and `...` on a value that
is no list is `type-mismatch`. What comes out is held to the type the port
declares, item by item, so one spliced item of another shape is
`config-type-mismatch`.

Only another metadata port can fill one this way. If `made` is a node,
`self.tool = made.value` is `metadata-value-not-literal`, because `made.value`
only exists once the program runs. A name that is nothing declared is
`unknown-source-node`, a port that member does not have is
`unknown-source-port`, and two metadata ports that take their value from each
other are `metadata-reference-cycle`. A group's own ports are no value of its
own: `self.x` inside the value is a `parse` error.

If you write a tool once in its own file and `@include` it in several places,
every include hands out the same value, the one the file works out.

### What is refused

- A metadata port with no value is `metadata-port-not-written`, and the
  message shows the line to add.
- A node inside the group cannot read the group's own metadata port:
  `shown.data = self.tool`, written inside `search`, is
  `metadata-port-read-inside`. If a node inside needs the same value, write
  it on that node's input too, or keep it on a member's metadata port and
  name that port in both places.
- Nothing goes into a metadata port from outside. A wire into `search.tool` is
  `metadata-port-as-target`, and a value written there (`search.tool = ...`)
  is refused with the same message.
- The value cannot leave a group through an ordinary output:
  `self.out = search.tool`, where `out` has no `*`, is `metadata-into-output`.
  Declare `out` as a metadata port instead.

A few mistakes are caught while the file is read, and come back as a `parse`
error: a `*` in the input list, a `*` on a node (a node type declares its own,
in its [metadata.json](../nodes/metadata.md#metadataports)), a metadata port
with no type or with a type variable, a metadata port with the name of an
input or an output, two `self.tool = ...` lines for one port, a value
written on a key of one (`self.tool.name = ...`), an inline node as a
metadata port's value (it computes its value during the run), and a value
that does not read. For why a name may not
repeat an input's, go and read [names you cannot use](#names-you-cannot-use).

### A model that calls tools

Here is one round of tool calls: the model is asked once, and each tool call
it makes is answered. It compiles as written, with `model` standing in for
whichever provider node you use. The loop runs once per call: because of
`over`, `self.calls` holds one call each time round, and `carry` hands the
conversation file from one call to the next. For how `over` and `carry` work,
go and read [Loops](loops.md).

```weft
model = CustomProvider { baseUrl: "http://127.0.0.1:9/v1", model: "m" }

handle = Loop(calls: List[ToolCall]) -> (historyFile: File, *dogTool: LlmTool) {
  over: ["calls"]
  carry: ["historyFile"]
  dog = Group(call: ToolCall) -> (callId: String, result: String, *tool: LlmTool) {
    self.tool = { "name": "dogName", "description": "The dog's name.", "parameters": { "type": "object" } }
    work = ExecPython(call: ToolCall) -> (callId: String, name: String) {
      code: "return {'callId': call['id'], 'name': 'Biscuit'}"
    }
    work.call = self.call
    self.callId = work.callId
    self.result = work.name
  }
  self.dogTool = dog.tool
  route = Dispatch
  route.value = self.calls
  route.dog = dog.tool
  dog.call = route.dog
  unknown = Debug
  unknown.data = route.otherwise
  answer = ChatHistoryAppend { role: "tool" }
  answer.historyFile = self.historyFile
  answer.toolCallId = dog.callId
  answer.text = dog.result
  self.historyFile = answer.historyFile
}

tools = List
tools.dog = handle.dogTool

ask = LlmInference { prompt: "What is my dog's name?" }
ask.provider = model.provider
ask.tools = tools.list
handle.calls = ask.toolCalls
handle.historyFile = ask.historyFile
done = Debug { data: handle.historyFile }
```

The tool is the group `dog`. Its `*tool` port describes it as an `LlmTool`,
`{ name: String, description?: String, parameters: JsonDict }`, where
`parameters` is a JSON Schema for the tool's arguments (`{ "type": "object" }`
is a tool that takes none). Each call the model makes is a `ToolCall`,
`{ id: String, name: String, arguments: String }`, with `arguments` holding
the model's JSON as text.

Three metadata wires carry that description. Two take it to the model:
`self.dogTool = dog.tool` hands it out of the loop on the loop's own metadata
port, and `tools.dog = handle.dogTool` puts it in the List that feeds the
model's `tools`. The model runs before the loop, so that wire points
backwards.

The third, `route.dog = dog.tool`, adds a branch named `dog` to the
`Dispatch` node, which sends each call to the branch whose `name` matches the
call's. `route.dog` is written twice, and the two are different ports: on the
left of `=` it is the branch's input, which takes the description, and on the
right (`dog.call = route.dog`) it is the output of the same name, which the
compiler makes and which carries the calls that branch claims. A call that
matches no branch leaves on `route.otherwise`. For how a node asks for that
pairing, go and read [`pairsAddedPorts`](../nodes/metadata.md#features). The
tool's name is written once, on its own group, and both the model and the
routing read it from there.

If you want a second tool, write a second group beside `dog` with its own
`*tool`, add `*catTool: LlmTool` to the loop's outputs, and give `cat` the
same wires as `dog`: `self.catTool = cat.tool` fills that port,
`tools.cat = handle.catTool` adds it to the List, `route.cat = cat.tool` adds
its branch, and `cat.call = route.cat` takes its calls. `answer` would then
get two wires into `toolCallId` and two into `text`, and an input takes one.
So read the id straight off the call instead of `dog.callId`
(`answer.toolCallId = self.calls.id`),
and take whichever result came back with `FirstInOrder`
(`answer.text = FirstInOrder { dog: dog.result, cat: cat.result }.value`).

For a model that keeps calling tools until it answers, this round goes inside
a loop of turns that carries the conversation file. For that shape, read the
`LlmInference` node's description: `weft describe-nodes --node LlmInference`.

## Names you cannot use

`Group`, `Loop`, `LoopIn`, `LoopOut` and `self` are the
language's. So is any name containing a double underscore, which is the
compiler's own separator.

A metadata port's name may not be the name of an input or an output of the
same group, so `self.tool` and `search.tool` only ever mean one port. An input
and an output may still share a name.

Naming a group or a node after a node type is also refused, because
`Ticket.body` would then read as an inline node rather than a reference. That
is the `reserved-name` error.

## How big a level should be

Six items on one level is what the language asks for. The compiler warns past
fifteen, names the level, and tells you to group.

A group counts as **one** item on its parent's level, whatever is inside it. So
a program grows downward into nested folders rather than sideways into a wall,
and every level stays something you can hold in your head.

A file's top level is counted per connected part. Everything a wire links
counts together, a database or a key set that several pieces use included, so
the warning reads "one connected part of the top level holds N items". Only
pieces that share no wire at all are counted apart. The inside of a group, a
loop and an included file is counted whole ("this included file holds N
items").
