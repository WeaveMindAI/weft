# Groups

Any subgraph can be a box with typed input and output ports. From outside, the
box behaves exactly like a node. Groups nest arbitrarily.

```weft
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
```

## The readable size

A weft program is read as a graph, and a level (the file, or the inside
of a group or loop) is what the reader scans in one look. The readable
size is about six items, nodes or groups; past fifteen the compiler warns
(`level-too-large`), because by then the level has stopped being something
a person can scan. Groups are the tool for staying readable: when a level
grows past six, the nodes cooperating on one job become a group of their
own. And because groups nest, depth absorbs size: growing work goes down
into a nested group, never wide across a level. A group whose inside holds
another group or two is the normal shape, not a special one.

## `self`

Inside a group, `self` is the group's own boundary.

- Reading `self.<input>` pulls a value the group received.
- Writing `self.<output>` sets a value the group emits.

The value flows right to left, so `clean.text = self.raw` pulls the group's
input into the child and `self.result = clean.out` pushes the child's output
out of the group.

## The boundary is real

A group's children can talk to each other and to `self`. That is the complete
list, and the compiler enforces it, so a group is a contract: these inputs,
these outputs, nothing else crosses. You can reason about what a group does
without opening it, and change what is inside it without checking the rest of
the program.

The wiring outside a group is identical whether it is collapsed or expanded in
the editor.

## Setting a group's ports from outside

A group's input port takes a wire, or a written value, on its own line:

```weft
triage.tone = "formal"
triage.email = inbox.message
```

The value reaches everything inside the group that reads that port.

## Turning a whole group off

`_should_flow` is on a group like it is on a node, and it decides whether the
group runs. Write it inside the braces, or from outside on the group's name:

```weft
escalation = Group(question: String) -> (answer: String) {
  _should_flow: route.needs_a_person
  ...
}
```

A group that does not run closes its outputs, so everything behind it closes
in turn, and every node inside it, however deeply nested, is marked skipped
with the group's name as the reason. For what counts as a no, go and read
[How a weft program runs](mental-model.md).

That is the only way a group as a whole stops. A group input that arrives
closed does not stop it: the closure passes through the boundary to the
nodes inside that read that port, those skip, and the rest of the group
runs. If you want the whole group to depend on one input, wire the group's
`_should_flow` from whatever decides that input. For the same reason
`@require_one_of` is refused on a group; put it on the node inside that
needs one of the ports.

## What starts inside

When a group starts, every node inside it that no wire feeds is started
too, at the same moment. A group can hold a source of its own, a fixed
`Text` or a node that reads the clock, and it fires once per start of the
group: once for a plain group, once per iteration for a loop body.

## The description line

The first line inside a group body, if it is a plain comment, is the group's
description. The editor shows it when the group is collapsed.

```weft
triage = Group(email: JsonDict) -> (severity: String) {
  # Classify an inbound ticket and normalise its severity
  ...
}
```

Keep it to one line and make it say what the group does for its caller.

## Why this scales

Groups are what keep debugging tractable however large a program gets.

When a value comes out wrong at the end, you look at the top-level boxes, find
the first one whose output is already wrong, open it, and repeat inside. Each
level of descent divides the search space, because each boundary you cross is a
place where the value was either already wrong or still fine.

The same property is what lets you hand a group to somebody else. "Build the
thing that turns a raw email into a normalised ticket, here are its input and
output types" is a complete task, buildable without seeing the rest of the
program.

## Nesting

Groups nest to any depth, and names are scoped: two groups can each contain a
node called `clean` without collision.

## A group does not exist at run time

The compiler **flattens** groups away. Your group becomes two ordinary boundary
nodes, one for the inputs and one for the outputs, and its children become
ordinary nodes with scoped ids. By the time anything runs there is one flat
graph of nodes and pulses, and the executor has never heard of a group.

So nesting costs nothing at run time and there is no per-group bookkeeping to
go wrong. The boundary is a compile-time contract that the compiler checks and
then deletes.

[Loops flatten the same way](loops.md), into a pair of boundary nodes plus an
iteration number carried on each pulse.

## What a group is not

A group is not a function. It has no call sites and does not return; it is a
region of the graph with a boundary drawn around it, and pulses cross that
boundary the same way they cross any wire.

So a group does not run "once per call". Two pulses arriving at its input at
different frames both flow through the same children at their own frames,
exactly as they would have without the box. If you want "run this subgraph N
times", that is a [Loop](loops.md).

## Reusing a group across files

```weft
triage = @include("triage.weft")
```

The included file must be exactly one anonymous top-level group. Its ports
become `triage`'s ports and you wire it like any node. See
[Files and reuse](files-and-reuse.md).
