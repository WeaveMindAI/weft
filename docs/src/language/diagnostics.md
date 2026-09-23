# What the compiler refuses

Every compiler finding carries a slug naming the rule, and a message naming the
fix. The slug is the stable part, so this page lists all of them.

Everything here is checked when you build, with one exception: `rule-runtime`
is only checked by `weft validate` and by the editor just before you Run,
Activate or Resync. That is deliberate, so a half-wired sketch with no
credentials connected still builds while you work on it.

Almost everything is an error. The three warnings are marked.

## Before the rest can run

| Slug | What it found | What to do |
|---|---|---|
| `parse` | The source did not parse | Read the message; it names the span and the syntax that failed |
| `enrich` | A node's ports could not be worked out: a bad type, or a custom port on a node that does not take them | Fix the port declaration on the node named |
| `unknown-type` (warning) | A node names a type the catalog does not have | Check the spelling, or add the node under `nodes/`. If a folder has a `metadata.json` and no `mod.rs`, the message says so |

`parse` and `enrich` are buckets: they carry the underlying message rather than
a slug per cause.

## Wires

| Slug | What it found | What to do |
|---|---|---|
| `unknown-source-node` | A wire reads from a node that does not exist | Fix the name |
| `unknown-target-node` | A wire writes to a node that does not exist | Fix the name |
| `unknown-source-port` | A wire reads a port the source does not declare | Fix it; the message lists the real ports |
| `unknown-target-port` | A wire writes a port the target does not declare | Same |
| `duplicate-input-port` | Two wires land on one input | Remove one; the message names the first one's line |
| `double-driven-port` | An input is both wired and written into the body | Remove one of the two |
| `scope-reachability` | A wire crosses a group or loop boundary without going through its ports | Route it through the group's own ports |
| `deref-path` | The dotted path on a wire cannot be read off the source's type | Fix the path, or change the source type |
| `type-mismatch` | The source's type does not fit the target's | Change one side, or cast. The message suggests a cast where one exists |
| `must-override-unmet` | A connected port is still `MustOverride` | Declare a real type in the source |
| `unresolved-typevar` | A connected port still has a type variable nothing pinned down | Connect it to something concrete, or declare the type |
| `cast-not-allowed` | A cast node's two ports are a pair the conversion table does not allow | Pick types the table allows |
| `graph-cycle` | The wires form a cycle | Iterate with a Loop, or exchange feedback over a bus |
| `required-port-unmet` | A required input has no wire, no written value and no default. On a group, loop or included file: a port nothing outside connects, whose value a step inside cannot run without (a required input, or the last possible member of a `@require_one_of` set) | Give it one of the three; for a group port, connect or write it from outside |

## Values you wrote

| Slug | What it found | What to do |
|---|---|---|
| `config-type-mismatch` | A written value does not fit the input's type | Fix the value, or the declared type |
| `input-accepts` | The port does not take this kind of driver: a wire on a literal-only port, a written value on a wire-only port | Use the one it takes; the message lists them |
| `gate-not-boolean` | A written `_should_flow` or `_should_not_flow` is not a Boolean | Write `true` or `false`, or wire it |
| `literal-out-of-range` | A written number is outside the input's `min` and `max`, or off its whole-number step | Write a value inside the declared range |
| `value-on-output` | You wrote a value for an output port | Remove it. Outputs are filled by the node, and you read them with `node.port` |
| `undeclared-port-no-custom` | A key names no input, on a node that does not take added inputs | Fix the key, or use a node with `canAddInputPorts` |
| `config-null-literal` (warning) | A key is written `key: null`, which does nothing | Leave the key out and let the default apply |
| `require-one-of-unknown-port` | `@require_one_of(...)` names a port the node does not have | Fix it; the message lists the real inputs |
| `require-one-of-unmet` | None of the ports in a `@require_one_of` group is driven | Drive at least one |

## Names and shape

| Slug | What it found | What to do |
|---|---|---|
| `duplicate-node-id` | Two nodes share an id once the graph is flattened | Rename one; the message names the first one's line |
| `duplicate-port` | A node has two ports of the same name on one side | Rename one |
| `reserved-name` | A node or group is named after a node type, so `name.port` would read as an inline node | Rename it |
| `two-gates` | A node carries both `_should_flow` and `_should_not_flow` | Keep the one that says what you mean |
| `no-required-skip` (warning) | A node has inputs but none is required, so it runs even when everything upstream is null | Mark one required, or add `@require_one_of` |
| `level-too-large` (warning) | One level of the graph holds more than fifteen items | Group the nodes that cooperate on one job. Nesting a group costs nothing at run time |

## Ports that come from a list

For a node whose ports are built from entries you write, like a form's fields
or a switch's cases.

| Slug | What it found | What to do |
|---|---|---|
| `config-ports-not-a-list` | The key holding those entries is not a list | Write it as a list |
| `config-entry-not-an-object` | One entry is not an object | Write it as an object |
| `unknown-config-entry-kind` | An entry's `kind` is not one this node offers | Use one the message lists |
| `config-entry-without-a-port` | An entry does not say which port it adds | Add the key naming the port |
| `unknown-config-entry-key` | An entry carries a key that kind does not take | Remove it, or use one the message lists |
| `config-entry-bad-value` | An entry's value is the wrong shape, or a value the matched input could never hold | Fill in a value of the right shape |
| `config-entry-missing-value` | An entry is missing a value that kind requires | Add the key the message names |
| `duplicate-catch-all` | Two entries match anything, so the second is unreachable | Keep one |
| `catch-all-not-last` | Entries sit after a catch-all, where nothing can reach them | Move the catch-all last |

## Loops

| Slug | What it found | What to do |
|---|---|---|
| `loop-unknown-config-field` | A key that is not `parallel`, `over`, `carry`, `max_iters` or `trim_on_mismatch` | Fix the name |
| `loop-parallel-not-boolean` | `parallel` is not `true` or `false` | Write one of them |
| `loop-max-iters-not-integer` | `max_iters` is not a whole number of zero or more | Write one |
| `loop-trim-not-boolean` | `trim_on_mismatch` is not a boolean | Write `true` or `false` |
| `loop-over-unknown-port` | `over` names an input that does not exist | Fix the name |
| `over-not-a-list` | An `over` port is neither `List[T]` nor `Generator[T]` | Declare it as one |
| `loop-carry-unknown-port` | `carry` names an output that does not exist | Fix the name |
| `carry-port-type-mismatch` | A carry port's two sides have different types | Make them the same |
| `over-and-carry-overlap` | A port is in both `over` and `carry` | Pick one role |
| `parallel-with-carry` | `parallel: true` with carry ports. Carrying means each turn feeds the next, which is sequential | Drop the carry, or drop the parallel |
| `parallel-without-over` | `parallel: true` with nothing to spread out | Give it a non-empty `over` |
| `parallel-with-done` | A parallel loop writes `self.done` | Remove it, or make the loop sequential |
| `loop-unbounded-no-termination` | A sequential loop with no `over`, no `max_iters`, and no `self.done`. Nothing would ever stop it | Iterate a list, cap the count, or wire `self.done` |
| `gather-output-must-be-nullable` | A gather output is not `List[T \| Null]`. An iteration that failed leaves an empty slot | Declare it `List[T \| Null]` |
| `reserved-port-name` | A loop boundary declares `index` or `done`, which the language owns | Rename the port |
| `loop-boundary-unpaired` | A `LoopIn` without its `LoopOut`, or the reverse | This one is a compiler bug, not yours. Please report it |
| `loop-config-missing-parallel` | The loop's config did not come through lowering | Also a compiler bug. Please report it |

## Streams

| Slug | What it found | What to do |
|---|---|---|
| `generator-in-container` | `Generator[T]` nested inside a list, dict, record or union | A stream is a port type. Declare it at the top level of the port |
| `generator-input-must-be-required` | A stream input is optional or has a default. A stream has no empty value | Mark it required and drop the default |
| `generator-multiple-consumers` | A stream output feeds more than one input. A stream has one taker | Wire it to exactly one. To broadcast, use a `Bus` |
| `generator-through-group` | A stream crosses a group or loop boundary | Keep the producer and the consumer in the same scope. A loop takes a stream only through its `over` |
| `generator-into-generic-port` | A stream is wired into a port whose type is unresolved and not a stream | Declare the target `Generator[T]`, or consume the stream and emit values |
| `generator-not-iterated` | A loop takes a stream that is not in `over`. A stream cannot broadcast into a body | Add it to `over` |
| `generator-not-carriable` | A carry port is a stream. A stream is a live edge and cannot cross iterations | Carry a value instead |
| `over-stream-not-alone` | A stream in `over` beside other `over` ports | Make it the only one, and zip upstream if you need more |

## Triggers, infrastructure and addresses

| Slug | What it found | What to do |
|---|---|---|
| `trigger-into-trigger` | A trigger is wired into another trigger | Remove the wire. A trigger's inputs are frozen at setup, so another trigger's output could never reach it |
| `trigger-into-infra` | An infra node sits downstream of a trigger | Remove the wire. Provisioning happens before any event exists |
| `trigger-in-loop` | A trigger inside a loop body | Move it out. A trigger registers once and fires outside any iteration |
| `infra-in-loop` | An infra node inside a loop body | Move it out. Infra is provisioned once for the project, not once per item |
| `route-path-missing` | A node claiming a public address has none, or an empty one | Give it a path, like `hooks/stripe` or `chat/{room}` |
| `route-path-invalid` | The path is not a servable pattern | The message says what is wrong |
| `route-method-unknown` | That is not an HTTP method | Fix it |
| `route-overlap` | Two nodes claim addresses one call could reach, and neither is more specific | Make one spell out what the other captures, or change a path or a method |

## Types you named

| Slug | What it found | What to do |
|---|---|---|
| `named-type-conflict` | A named type appears with two different bodies | Make every use of the name carry the same body |

## Rules a node declared for itself

| Slug | What it found | What to do |
|---|---|---|
| `rule-structural` | A rule the node's own author wrote, checked on every build | Whatever its message says |
| `rule-runtime` | The same, but only checked by `weft validate` and just before you run. Mostly this is "you have not picked a connection yet" | Pick the connection, or fill the credential the message names |

Both take their severity from the rule, so either can be an error, a warning, a
note or a hint.

## Things that have no slug

Some refusals happen before the compiler gets a graph at all, so there is
nothing to look up.

A broken `metadata.json` fails when the catalog loads, with the path and a
sentence. A fire payload that does not match a trigger's `firesWith` is refused
at run time, not at build time. And in the editor, a node whose metadata is
mid-edit is skipped with a warning rather than blanking your graph.
