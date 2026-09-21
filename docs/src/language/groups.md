# Groups

A group holds several steps under one name and folds shut like a folder. Its
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
from outside.

A plain comment as the first thing in the body becomes the group's description,
which is what the graph shows when it is folded shut.

## They do not exist when the program runs

The compiler turns every group into two nodes, one holding its inputs and one
holding its outputs, and hands the runtime one flat graph with the scope
written on each node.

So folding a group in the editor costs nothing, nesting five deep costs
nothing, and a group is never a thing the runtime has to step through. It is a
way of writing and reading, and it is gone by the time anything runs.

## Why a required group input does not skip the group

This is the part that surprises people.

You declare `db: Access` without a `?`, so it is required. Then the thing
feeding it closes. You would expect the whole group to skip.

It does not. The closure goes **inside**, and whichever step actually needed
that value skips there, and the skip cascades from that step outward. Every
port on a group's boundary is optional, and the requiredness you declared is
enforced by the node that consumes it.

That is deliberate. A group is usually several jobs, and one missing input
often only kills one of them. Skipping the whole group would throw away work
that had everything it needed.

It also means `@require_one_of` is refused on a group. Put it on the step
inside that needs one of those values.

A loop is the exception, for the ports it iterates or threads: with no list to
walk, there is no iteration to launch.

## Skipping the whole thing

`_should_flow` goes inside the braces, and when it skips a group it skips
**everything** in it, however deep, including nested groups. One line guards a
whole subgraph.

```weft
brief = Group(request: String) -> (prompt: String) {
  ...
}
brief.request = ask.text
brief._should_flow = credit.paid
```

The gate is consumed at the boundary. The steps inside never see it, and they
do not each get to decide: they take the group's decision as a whole.

Only the incoming side takes a gate. The outgoing side has nothing to decide,
because a group that did not run has nothing to forward.

## What crosses the boundary

| Thing | Crosses? |
|---|---|
| Ordinary values, connections, file markers, bus markers | Yes |
| A closure | Yes, and it cascades to whatever inside needed the value |
| `_should_flow` and `_should_not_flow` | No. Consumed at the boundary |
| A stream (`Generator[T]`) | **No** |
| A wire from inside to outside that skips the interface | No |

A stream is a live edge between two running nodes, so both ends have to be in
the same scope. If you need a stream to cross, either move the consumer in, or
have a node read the stream and emit ordinary values. A loop is the one thing
that takes a stream across its edge, through its own `over`.

That last row is the `scope-reachability` error. A value crossing the line has
to appear on the interface, where somebody reading the group can see it.

## Names you cannot use

`Group`, `Loop`, `Passthrough`, `LoopIn`, `LoopOut` and `self` are the
language's. So is any name containing a double underscore, which is the
compiler's own separator.

Naming a group or a node after a node type is also refused, because
`Ticket.body` would then read as an inline node rather than a reference. That
is the `reserved-name` error.

## How big a level should be

Six items on one level is what the language asks for. The compiler warns past
fifteen, names the level, and tells you to group.

A group counts as **one** item on its parent's level, whatever is inside it. So
a program grows downward into nested folders rather than sideways into a wall,
and every level stays something you can hold in your head.
