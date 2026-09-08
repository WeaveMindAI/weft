# What the compiler refuses

Every validation error carries a stable slug. This is all of them, grouped by
what they protect, with the fix.

Parsing errors, and errors from the pass that looks up each node's ports, carry
no slug. They point at the exact spot in your file and say what is wrong
there.

> **Reading a diagnostic.** The slug names the rule. The message names the
> fix. If you find one that names the problem but not the fix, that is a bug
> worth reporting.

## Wiring

| Slug | Meaning |
|---|---|
| `type-mismatch` | a connection's source type is not compatible with its target. The message names both. |
| `deref-path` | a wire reads a key off its value (`t.n = s.out.profile.wpm`) that the source type does not have: the message names the keys that exist, or, when the type has no keys at all (`JsonDict`, a scalar), says to declare the shape on the source port or `Cast` first. |
| `required-port-unmet` | a required input has no wire and no literal. Wire it, give it a literal, or mark it optional with `?`. |
| `unknown-source-node` | the left side of a connection names a node that does not exist in this scope. |
| `unknown-target-node` | the right side names a node that does not exist in this scope. |
| `unknown-source-port` | the node exists; that output port does not. |
| `unknown-target-port` | the node exists; that input port does not. |
| `double-driven-port` | one input has two drivers: two wires, or a wire and a literal. An input has exactly one source. |
| `input-accepts` | a driver the port does not take: a wire on a port whose `accepts` is `["literal"]`, a written value on one whose `accepts` is `["wire"]`, or a wire or a `@file`/`@asset` on a port the compiler reads to build the node. The message reads the list back. |
| `duplicate-input-port` | the same input name declared twice on one node. |
| `duplicate-node-id` | two nodes share an id in one scope. |
| `should-flow-not-boolean` | a `_should_flow` written down that is not `true` or `false`. A wire may carry any value; a constant is a Boolean. |
| `undeclared-port-no-custom` | a port was referenced that the node neither declares nor allows you to add. |
| `value-on-output` | a value was written on one of the node's output ports. An output takes no value: a firing emits on it, and you read it as `node.port`. |

## Types

| Slug | Meaning |
|---|---|
| `unresolved-typevar` | a type variable was never pinned to a concrete type. Pin it with an inline signature or wire something concrete in. |
| `must-override-unmet` | a `MustOverride` port was left unpinned. Declare its type in the inline signature. |
| `cast-not-allowed` | this `Cast` conversion is impossible, for example `JsonDict -> Number`. |
| `config-type-mismatch` | a config literal's type does not match its field. |
| `config-null-literal` | a config field was given `null`, which is not a way to say "unset". Omit the field. |
| `literal-out-of-range` | a number literal falls outside the `min`/`max` its widget declares. |
| `named-type-conflict` | one type name declared twice with different bodies. Rename one. Identical bodies are fine. |

## Graph shape

| Slug | Meaning |
|---|---|
| `graph-cycle` | a cycle in the wire graph. Iterate with a `Loop`; exchange feedback over a bus. |
| `scope-reachability` | a connection reaches across a group boundary. Children reach each other and `self`, nothing else. |
| `level-too-large` | **a warning.** A level of the graph (the file, or the inside of a group or loop) holds more than fifteen items, nodes or groups. At the file's top level the count is per connected branch (the items one wire walk reaches, plus the infra nodes it touches; an infra node written at that level joins nothing, while a group holding one is an item like any other). About six per level is what reads; group the nodes cooperating on one job, and nest groups rather than widen the level. The program still runs: this is advice about how it reads. |
| `loop-boundary-unpaired` | a loop's internal boundary nodes do not line up. This is an internal invariant; hitting it is a compiler bug worth reporting. |

## Triggers

| Slug | Meaning |
|---|---|
| `trigger-in-loop` | a trigger inside a `Loop`. An entry point per iteration is meaningless. |
| `infra-in-loop` | an infra node inside a `Loop`. Infra is provisioned once for the project, not once per item. |
| `trigger-into-trigger` | a trigger wired into another trigger. No phase delivers that. |
| `trigger-into-infra` | a trigger wired into an infra node. Provisioning happens before any fire exists. |
| `duplicate-port` | two ports on the node share a name on one side, which config-derived ports are the usual way to reach. Give them different names. |
| `config-ports-not-a-list` | the config key a node derives its ports from does not hold a list. |
| `config-entry-not-an-object` | an entry of that list is not an object. |
| `unknown-config-entry-kind` | an entry names a `kind` this node does not offer. |
| `config-entry-without-a-port` | an entry does not name the port it adds. |
| `unknown-config-entry-key` | an entry carries a key its kind does not take, usually a mistyped test. |
| `config-entry-bad-test` | a test carries the wrong shape of value (a number where the matched input is a String, a regex that does not compile). |
| `duplicate-catch-all` | two entries match anything; the second could never be reached. |
| `catch-all-not-last` | an entry matches anything and is not last, so the entries after it could never be reached. |

## Loops

| Slug | Meaning |
|---|---|
| `loop-unbounded-no-termination` | a sequential loop with no `over`, no `max_iters`, and no `self.done` write. Provably infinite. Give it something to exhaust, a cap, or a stop vote. |
| `parallel-with-carry` | `parallel: true` with a non-empty `carry`. Carry implies an order. |
| `parallel-without-over` | `parallel: true` with an empty `over`. The count has to be known up front. |
| `parallel-with-done` | `parallel: true` with a `self.done` write anywhere in the body. |
| `over-and-carry-overlap` | a port listed in both `over` and `carry`. |
| `over-not-a-list` | a port in `over` is neither a `List[T]` nor a `Generator[T]`. |
| `over-stream-not-alone` | a stream in `over` alongside another over port. A loop iterates one stream at a time. |
| `gather-output-must-be-nullable` | a gather output not typed `List[T | Null]`. An iteration can fail to write, so the type has to admit it. |
| `carry-port-type-mismatch` | a carry port's inside and outside types disagree. |
| `loop-over-unknown-port` | `over` names a port the loop does not have. |
| `loop-carry-unknown-port` | `carry` names a port the loop does not have. |
| `loop-config-missing-parallel` | an internal invariant broke while flattening the loop. You cannot cause this from source, so hitting it is a compiler bug worth reporting. |
| `loop-parallel-not-boolean` | `parallel` is not `true` or `false`. |
| `loop-trim-not-boolean` | `trim_on_mismatch` is not `true` or `false`. |
| `loop-max-iters-not-integer` | `max_iters` is not a whole number. |
| `loop-unknown-config-field` | an unrecognised key in a loop's config. |

## Streams

Every one of these enforces the same property: a stream is a live handle with
one producer and one taker.

| Slug | Meaning |
|---|---|
| `generator-multiple-consumers` | two consumers on one stream. Broadcasting is a `Bus`. |
| `generator-through-group` | a stream crossing a group boundary. |
| `generator-in-container` | a stream inside a `List` or `Dict`. |
| `generator-not-carriable` | a stream carried between loop iterations. |
| `generator-input-must-be-required` | an optional `Generator` input. An unwired stream has no meaning. |
| `generator-not-iterated` | a `Generator` input on a loop that is not the `over` port. A stream cannot broadcast into a body. |
| `generator-into-generic-port` | a stream wired into a port whose type is a bare type variable. |

## Names

| Slug | Meaning |
|---|---|
| `reserved-name` | a node named `self`, a type keyword, or another reserved word. |
| `reserved-port-name` | a port named `index` or `done` on a loop. Both are implicit. |

## Requirements and warnings

| Slug | Meaning |
|---|---|
| `require-one-of-unmet` | an `@require_one_of` group where nothing is satisfied. |
| `unknown-type` | **a warning.** A declared node type is not in the project's catalog: a typo, or the node was never built. The message names the type. |
| `no-required-skip` | **a warning.** Every input a wire feeds on this node is optional and there is no `@require_one_of`, so the node runs even when everything upstream is dead. Usually not what you want; add `@require_one_of`. A node built from written constants alone has no upstream and never gets this. |
| `rule-structural` | a node's own declarative validation rule failed at compile time. The message is the node author's. |
| `rule-runtime` | a node's own rule flagged something checkable only at run time. The language writes one of these itself: every access node requires a connection picked (unless its recipe declares `connection_optional`), with no rule in its metadata. |

## Where these run

There are two validation modes, and the same slugs appear in both.

**Structural** is what a build runs, and what the editor runs constantly while
you type: it fills the Problems panel and decides whether your program
compiles.

**Runtime** adds the `rule-runtime` checks on top, the ones about things only
knowable once a program is about to run, such as a provider node with no
connection picked. A build and the Problems panel skip those deliberately, so
that a program you are still wiring up builds without squiggles. They run when
the question is whether the program is ready: the editor checks them right
before Run/Activate/Resync (findings land on the action bar, and nothing is
sent until they are fixed), and `weft validate` runs this mode in the
terminal.

Both read source on stdin and print JSON. `--file` does not open a file: it
names the path the source came from, so `@file` and `@include` resolve against
the right directory. A finding inside an `@include`d file carries that file's
path (a `file` key in the JSON), and the terminal output prefixes it as
`path:line:col`.
