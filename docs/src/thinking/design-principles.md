# Design principles

The rules we use when deciding whether something belongs in weft.

## Put the coordination where you can read it

Whatever decides what connects to what, or what runs next, should be something
you can read.

In most systems that logic is buried in code you never open: a retry loop, a
queue, an `if` three calls deep. In weft it is written down in the graph and
checked before the run, so you can find where a decision is made and change it
without touching the steps around it.

You read it from the outside in. A group's interface says what goes in and what
comes out, and its boundary is real to the compiler, so nothing reaches inside
without going through it. A group does not exist at run time, so nesting is
free: it costs nothing when the program runs, and nothing when you read it.

## The plumbing belongs to weft

A node's code should be its own job and nothing else. Handling a credential,
keeping a subscription alive, saving state, writing down what happened: those
belong to the runtime, where they are written once and hardened for every node.
When two nodes would otherwise write the same thing, that thing gets built once
for both.

For where the line sits today, go and read
[the commandments of plumbing](plumbing.md).

## The language knows nothing about your nodes

The compiler, the dispatcher and the runtime never mention a node by name or
hardcode its fields. A Postgres step and a model step look the same to the
language, and everything a node needs, it asks for through the ctx.

So adding a node that needs something new means building a general mechanism in
weft, never a branch that already knows that node.

## Refuse a mistake as soon as anything can see it

Put each check at the earliest place with enough information to make it, and
have it say what went wrong and what to do about it. For the full rule, go and
read [the three fronts](how-we-decide.md).

## Fail loudly

When something goes wrong, it says so. A fallback that quietly returns a
second-best value turns a broken program into one that looks like it works, and
the person running it never finds out.

The failure is part of the design too: name what broke, and what the person can
do next.
