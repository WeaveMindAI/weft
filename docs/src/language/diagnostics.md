# What the compiler refuses

Every compiler finding carries a message saying what went wrong, usually with
the fix, and a slug naming the rule. The editor shows the slug beside the
message, and `weft validate` puts it under `code`. The slug stays the same when
the wording of a message changes, so look yours up here.

Everything here is checked when you build, except `rule-runtime`, which is
only checked by `weft validate` and by the editor just before you Run, Activate
or Resync, so a half-wired sketch whose provider has no model named yet still
builds while you work on it.

A row marked (warning) is reported and the build goes on, unless the row says
otherwise. Every row with no mark is an error, except `rule-structural` and
`rule-runtime`, which take whatever severity the node's own rule sets:
`error`, `warning`, `info` or `hint`.

## Before the rest can run

| Slug | What it found | What to do |
|---|---|---|
| `parse` | The source did not parse | Read the message; it names the span and the syntax that failed |
| `enrich` | A node's ports could not be worked out: a bad type, or a custom port on a node that does not take them | Fix the port declaration on the node named |
| `unknown-type` (warning) | A node names a type the catalog does not have. A warning in the editor; when you build, in `weft validate`, and in the editor's check before you Run, Activate or Resync, the same node is an `enrich` error | Check the spelling, or add the node under `nodes/`, or next to the module that uses it under `src/`. If the node is there but failed to load, or its folder has a `metadata.json` and no `mod.rs`, the message says so and why |

`parse` and `enrich` are buckets: they carry the underlying message rather than
a slug per cause.

## Wires

| Slug | What it found | What to do |
|---|---|---|
| `unknown-source-node` | A wire reads from a node that does not exist, or a metadata port's value names one | Fix the name |
| `unknown-target-node` | A wire writes to a node that does not exist | Fix the name |
| `unknown-source-port` | A wire reads a port the source does not declare, or a metadata port's value names a metadata port its member does not have | Fix it; the message lists the real ports |
| `unknown-target-port` | A wire writes a port the target does not declare | Same |
| `duplicate-input-port` | Two wires land on one input | Remove one; the message names the first one's line |
| `double-driven-port` | An input is both wired and written into the body, or takes a written value and a [metadata wire](groups.md#metadata-ports), or two metadata wires | Remove one of the two |
| `scope-reachability` | A wire crosses a group or loop boundary without going through its ports | Route it through the group's own ports |
| `deref-path` | The dotted path on a wire cannot be read off the source's type, or, on a metadata wire, the value written does not hold that key | Fix the path, or change the source type or the value |
| `type-mismatch` | The source's type does not fit the target's, or a metadata port's value puts `...` before a value that is no list | Change one side, or cast. The message suggests a cast where one exists |
| `unresolved-typevar` | A connected port still has a type variable nothing pinned down | Connect it to something concrete, or declare the type |
| `type-bound` | A type variable limited to some types got another one: a `String` into a group declared `Group[T: Media]`, or into `KeepFile`, which takes only a file | Connect a value of a type the limit takes; the message names the limit and what you connected |
| `cast-not-allowed` | A cast node's two ports are a pair the conversion table does not allow | Pick types the table allows |
| `graph-cycle` | The wires form a cycle | Iterate with a Loop, or exchange feedback over a bus. If what goes back is a value you can write in the program, like a tool's description, make it a [metadata port](groups.md#metadata-ports) |
| `required-port-unmet` | A required input has no wire, no written value and no default. On a group, loop or included file: a port nothing outside connects, whose value a node inside cannot run without (a required input, or the last possible member of a `@require_one_of` set) | Give it one of the three; for a group port, connect or write it from outside |
| `optional-pass-through-port` | A `?` on a port of a group or an included file, or on a loop port the loop neither iterates nor carries. Such a port passes what reaches it to the nodes inside, so it is never optional itself ([a group input passes through](groups.md#a-group-input-passes-through)) | Remove the `?`; to let a node inside run without the value, write the `?` on that node's input |
| `run-file-kept-past-run` (warning) | A file a node stored for its run only, because its `scope` setting says `run` (written, or that default, as on the storage nodes such as `KeepFile`), whatever `ttlDays` says, is wired into a node that keeps what it is given after the run ends, like a table write or an instance's values. No later run can read that file, so the row ends up holding a dead reference | Store it with `scope: project`. For which files outlive a run, go and read [files at run time](../running/files.md#keeping-something) |

## Values you wrote

| Slug | What it found | What to do |
|---|---|---|
| `config-type-mismatch` | A written value does not fit the input's type, including one a metadata wire writes there, or a metadata port's value does not fit the port's own type | Fix the value, or the declared type |
| `input-accepts` | The port does not take this kind of driver: a wire on a literal-only port, a written value on a wire-only port | Use the one it takes; the message lists them |
| `gate-not-boolean` | A written `_should_flow` or `_should_not_flow` is not a Boolean | Write `true` or `false`, or wire it |
| `literal-out-of-range` | A written number is outside the input's `min` and `max`, or off its whole-number step | Write a value inside the declared range |
| `literal-not-an-option` | A written value is not one of the input's dropdown `options` (for a multiselect, one item is not). A dropdown marked `freeText` never raises it | Write one of the options; the message lists them |
| `value-on-output` | You wrote a value for an output port | Remove it. Outputs are filled by the node, and you read them with `node.port` |
| `undeclared-port-no-custom` | A key names no input, on a node that does not take added inputs | Fix the key, or use a node with `canAddInputPorts` |
| `config-null-literal` (warning) | A key is written `key: null`, which does nothing | Leave the key out and let the default apply |
| `require-one-of-unknown-port` | `@require_one_of(...)` names a port the node does not have | Fix it; the message lists the real inputs |
| `require-one-of-unmet` | None of the ports in a `@require_one_of` group is driven | Drive at least one |
| `placeholder-not-an-input` | A query or template you wrote names a placeholder (`$userId` in a Postgres query, `{{name}}` in a `Format` template) that is not an input of the node | Declare the input (`PostgresExecuteQuery(userId: String)`), or fix the placeholder; the message suggests a close input, lists the inputs you added, or, if you added none, shows how to declare one |
| `input-not-a-placeholder` | You added an input to a node that takes a query or template, and the text never uses it | Use it in the text, or drop the input |

## Names and shape

| Slug | What it found | What to do |
|---|---|---|
| `duplicate-node-id` | Two nodes share an id once the graph is flattened | Rename one; the message names the first one's line |
| `duplicate-port` | A node has two ports of the same name on one side, or a port the program adds to a node has the name of one of its node type's metadata ports | Rename one |
| `unpaired-port` | On a node that pairs each added input with an output of its name, like `Dispatch`, one of three things. An added output has no input of its name: add the input, or remove the output. Nothing reads an added output, so what it gets would be dropped: wire it to what handles it. An added input is named after one of the node's own outputs: rename it | The message says which of the three |
| `reserved-name` | A node or group is named after a node type, so `name.port` would read as an inline node | Rename it |
| `two-gates` | A node carries both `_should_flow` and `_should_not_flow` | Keep the one that says what you mean |
| `run-undecided` | Every input wired into a node is declared with `?`, so nothing says when it runs | Gate it with `_should_flow`, drop the `?` from the input it cannot do without, or add `@require_one_of` |
| `level-too-large` (warning) | One level of the graph holds more than fifteen items | Group the nodes that cooperate on one job. Nesting a group costs nothing at run time |
| `not-camel-case` (warning) | A name you chose in the program is not camelCase | Rename it to camelCase. The message gives the spelling when it can work one out. For which names are checked, go and read [Spelling a name](syntax.md#spelling-a-name) |

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

## Metadata ports

For what a metadata port is, go and read [metadata ports](groups.md#metadata-ports).

| Slug | What it found | What to do |
|---|---|---|
| `metadata-port-not-written` | A group's metadata port has no value | Write it in the body (`self.tool = { ... }`), or take it from another metadata port |
| `metadata-value-not-literal` | A metadata port's value names a node's output, which only exists once the program runs | Write the value itself, or take it from another metadata port |
| `metadata-reference-cycle` | Metadata ports take their value from each other, so none has one | Write the value on one of them |
| `metadata-port-as-target` | A wire goes into a metadata port from outside | Wire from it instead (`other.input = g.tool`) |
| `metadata-port-read-inside` | A node inside a group is wired from that group's own metadata port (`self.tool`). Like an output, a metadata port is read from outside its group | Read it from outside the group, or take the value inside from where the port gets it |
| `metadata-into-output` | A metadata port's value goes into an ordinary output of a group | Make that output a metadata port too (`-> (*out: Type)`) |

## Loops

| Slug | What it found | What to do |
|---|---|---|
| `loop-unknown-config-field` | A key that is not `parallel`, `over`, `carry`, `maxIters` or `trimOnMismatch` | Fix the name |
| `loop-parallel-not-boolean` | `parallel` is not `true` or `false` | Write one of them |
| `loop-max-iters-not-integer` | `maxIters` is not a whole number of zero or more | Write one |
| `loop-output-unwritten` | A loop declares an output nothing inside it writes: a gathered one would be a list of nothing but nulls, a carried one would never change | Write `self.<output>` inside the loop, or remove the port |
| `loop-trim-not-boolean` | `trimOnMismatch` is not a boolean | Write `true` or `false` |
| `loop-over-unknown-port` | `over` names an input that does not exist | Fix the name |
| `over-not-a-list` | An `over` port is neither `List[T]` nor `Generator[T]` | Declare it as one |
| `loop-carry-unknown-port` | `carry` names an output that does not exist | Fix the name |
| `carry-port-type-mismatch` | A carry port's two sides have different types | Make them the same |
| `over-and-carry-overlap` | A port is in both `over` and `carry` | Pick one role |
| `parallel-with-carry` | `parallel: true` with carry ports. Carrying means each turn feeds the next, which is sequential | Drop the carry, or drop the parallel |
| `parallel-without-over` | `parallel: true` with nothing to spread out | Give it a non-empty `over` |
| `parallel-with-done` | A parallel loop writes `self.done` | Remove it, or make the loop sequential |
| `loop-unbounded-no-termination` | A sequential loop with no `over`, no `maxIters`, and no `self.done`. Nothing would ever stop it | Iterate a list, cap the count, or wire `self.done` |
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
| `run-settings` | A trigger's `durable`, `recorded` or `outlivesCaller` is not `true` or `false`, or it is durable or outlives its caller and is not recorded | Write `true` or `false`; turn `recorded` back on, or the other one off |
| `trigger-into-trigger` | A trigger is wired into another trigger | Remove the wire |
| `trigger-into-infra` | An infra node sits downstream of a trigger | Remove the wire |
| `trigger-in-loop` | A trigger inside a loop body | Move it out of the loop |
| `infra-in-loop` | An infra node inside a loop body | Move it out of the loop |
| `route-path-missing` | A node claiming a public address has none, or an empty one | Give it a path, like `hooks/stripe` or `chat/{room}` |
| `route-path-invalid` | The path is not a servable pattern | The message says what is wrong |
| `route-method-unknown` | That is not an HTTP method | Fix it |
| `route-capture-shadows-output` | A path captures a name that is also one of the node's own outputs (`cards/{body}` on a `Route`), so the capture could never be read | Rename the capture |
| `route-output-not-a-capture` | An output added to a `Route` names no capture of its path, or is not typed `String` | Add `{name}` to the path or remove the port, and type every capture `String` |
| `route-overlap` | Two nodes claim addresses one call could reach, and neither is more specific | Make one spell out what the other captures, or change a path or a method |

For why a trigger cannot be wired into another trigger or have an infra node
after it, go and read [what a trigger cannot do](triggers-and-routes.md#what-a-trigger-cannot-do).
For why neither a trigger nor an infra node can sit inside a loop, go and read
[what cannot go in a loop](loops.md#what-cannot-go-in-one).

## Instances

| Slug | What it found | What to do |
|---|---|---|
| `per-instance-ineligible` | `@per_instance` on a node that runs no container of its own. Only an infra node can carry it | Remove it; for a value each instance gets (its connection included), write `@instance_filled` on the field |
| `instance-filled-wired` | An `@instance_filled` field also has a wire into it | Remove the wire, or the marker |
| `instance-filled-place-types` | An `@instance_filled` field of a node in an included file takes a different type at each place the file is included, and one instance's value cannot be both | Declare the field's type on the node |
| `instance-filled-boundary` | `@instance_filled` on a group's or included file's own port | Write it on the input of the node inside that reads the value |
| `instance-filled-not-an-input` | `@instance_filled` on a key that is not one of the node's inputs | Put it on an input |
| `shared-trigger-reaches-per-instance` | A trigger every instance shares (so its runs are for no instance) starts runs that reach something per instance, so every fire would be refused. A route or a socket is not checked: each call can name its instance | Make the trigger per instance too (wire it from what is per instance, or write one of its fields `@instance_filled`), or keep what is per instance out of what it runs |

For what `@per_instance` and `@instance_filled` do, go and read [Programs with instances](../running/instances.md).

## Types you named

| Slug | What it found | What to do |
|---|---|---|
| `named-type-conflict` | A named type appears with two different bodies | Make every use of the name carry the same body |

## Rules a node declared for itself

| Slug | What it found | What to do |
|---|---|---|
| `rule-structural` | A rule the node's own author wrote, checked on every build | Whatever its message says |
| `rule-runtime` | The same, but only checked at the moments listed at the top of this page | Fill what the message names. A node with no connection picked is not one of these: the install refuses the run when it starts, and the message gives the command that picks one, `weft connect --node <node>` |

## Things that have no slug

Some refusals happen before the compiler gets a graph at all, so there is
nothing to look up.

A broken `metadata.json` fails when the catalog loads, with the path and a
sentence. A fire payload that does not match a trigger's `firesWith` is refused
at run time, not at build time. And in the editor, a node whose metadata is
mid-edit is skipped with a warning rather than blanking your graph.
