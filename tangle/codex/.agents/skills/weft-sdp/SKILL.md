---
name: weft-sdp
description: "Read when growing a program stage by stage: carving a run, supplying starting values, baking a trigger and firing it without activating, reusing completed work with a seed, freezing an accepted example, checking a new run against one, and what each refusal means."
---

# Sequential Diffusion Programming

You build one stage against a real input, run it, and read the result
before growing the next stage. You judge a result by what the program is meant to do:
completion alone says nothing. If you catch yourself calling a run good
because it completed, stop and write: "Wait. Read the result." Then
inspect `weft events <color> --node <id>` and `weft logs <color>`.

The terms below:

- A [start] is where a run begins: a node named by `--from`, a group by
  `--group`, a trigger by `--fire`, or outputs supplied by `--emit`.
- A [cut] is where a run stops: `--target` includes that endpoint,
  `--before` excludes it and everything beyond.
- A [backup] is a value you supply on a [start]. The runtime waits for
  real execution input first and uses the [backup] only after the real
  source closes without a value or no source participates. A real value
  wins, including null; a running source is never overwritten; errors and
  invalid real values stay errors, a [backup] does not hide them.
- An [example] is a set of running parameters saved by `--save name` in
  `examples/name.json`. A [frozen example] is an [example] that `freeze`
  has given accepted output evidence, stored as `expected`.
- [head] is the version the project is on; `weft branch` moves it.

## Carve the work you want to exercise

`weft run` builds the current code and starts one execution. You pass
`--detach`, then read `weft executions --json`,
`weft events <color> --node <id>` and `weft logs <color>`: failures and
waiting states as well as outputs. You keep the user's graph intact while
trying a stage:

```bash
weft run --from classify='{"text":"the invoice is wrong"}' --target reply --save invoice --detach
weft run --group triage='{"text":"the invoice is wrong"}' --detach
weft run --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

- `--from node='{"port":value}'` starts that node and its downstream work;
  the downstream work brings its other input producers with it. A bare
  `--from node` supplies no [backup]. Repeat it for several [start]s.
  Walking upstream stops at every named from/emit and at triggers: those
  are the boundaries.
- `--target node` and `--before node` set the [cut]; repeat either for
  several endpoints. A group or loop name includes the whole container
  with `--target`, or excludes it with `--before`.
- `--group group='{"port":value}'` runs the whole group or loop alone;
  `--from group='{"port":value}'` runs it and continues downstream. You
  choose one form: `--group` cannot combine with from, emit, target or
  before. An included file uses its group alias, and a node inside one is
  named through that alias (`--from triage.up`, `--target triage.up`),
  which is also how `weft events` prints it. A group's entry and exit rows
  print under the group's own name (`triage`, with `boundary=in|out`), and
  `--node triage` shows them; there is no `triage__in` to name.
- Whatever cut you choose, an input left with nothing is checked before the
  run starts: its wire's source is outside the run and you handed no value.
  If a step would skip without it (a required input, or the last member of
  a `@require_one_of` set that could get a value; the node's own, or one
  inside the group it enters, followed through every group, loop and
  included file), the run is refused, naming the input, its source, and
  what it feeds. Hand it a value
  at your start, or start further up so the source runs. A start is where
  the walk upstream stops, so neither `--group` nor `--from` runs what feeds
  the start's own ports: only what feeds the nodes AFTER it. On a `--seed`
  run a saved result can feed it instead.
- `--feed start` runs what feeds a start: for each of its inputs you did
  not hand, the node that feeds it (through every group or include door on
  the way, however deep), and nothing above that node. A loop feeding it
  runs whole. The way to run one group whose
  connections come from outside: `--from 'hear.note={}' --feed hear.note
  --target hear.note`. Naming the feeders as starts yourself works too:
  when one start lies upstream of another, the run keeps both.
- A start whose gate (`_should_flow`, its own or a surrounding group's)
  only comes from triggers the run does not fire, or from outside the run,
  is refused: it would skip and still report completed. `--feed` never
  opens a gate: it runs what feeds a start's inputs, and the gate is not
  one of them. Hand the gate a value at its own door, the start for its
  own gate (`--from 'start={"_should_flow": true}'`) and the group for a
  surrounding group's (`--from 'group={"_should_flow": true}'`, or
  `--group` if the run was a `--group` run), or `--fire` the trigger.
- Without `--target` a run reaches everything downstream of its starts, so
  `--from db` runs every branch `db` feeds. `--target` keeps only what the
  target needs, from the starts down: it is a shape, not "run until you
  reach the target".
- `--emit node='{"port":value}'` supplies that node's outputs without
  running its body. Repeat for several supplied nodes. An output with no
  consumer remains valid output evidence.

For a generator port, you supply all its items in one array:
`--emit batches='{"items":["first","second"]}'`. The runtime emits each
item and then closes the port; `[]` closes it without an item. A
list-typed ordinary port receives its array as one value, so you read the
actual port type before choosing.

A [cut] inside an ordinary group stays at the requested node, and the
group's `_should_flow` gate still applies: false stops the branch; true
dispatches only the selected work. Loops are indivisible: you use the
public loop name for a [start], `--group`, `--target`, or `--before`; a
node inside the loop cannot be an endpoint. Trigger setup and infra
preparation obey the same group cuts and loop restriction.

## Choose how a trigger participates

You read the trigger's implementation and its ports before forming either
payload.

```bash
weft bake
weft run --fire incoming='{"event":"the trigger wake payload"}' --detach
weft run --emit incoming='{"message":"the emitted output value"}' --detach
```

If the program carries infrastructure, bring it up first with `weft infra
start`. A RUN refuses outright while a node's container is not up, naming
the node. A bake does not refuse; it brings the infrastructure up itself,
which is right but takes as long as starting it would, so a bake that seems
to hang on a program with infrastructure is usually provisioning.

A fire names exactly one trigger; its payload wakes that trigger, which
runs and decides what to emit. An emit supplies the trigger's declared
outputs directly. You choose one per trigger. Trigger ports receive their
prepared settings through `weft bake`; a trigger cannot be a `--from`
[start].

`bake` runs preparation and saves the trigger settings without listening.
Changed code or configuration needs a matching new bake. Closed group
gates can leave a trigger unprepared; you inspect the bake instead of
forcing it. `weft activate` prepares and arms real listeners.
`weft bake <project-id>` uses that project's registered build. If you
deliberately use `--referenced`, you pass it to both bake and run. Off a
terminal every verb that takes a [mode] (`deactivate`, `resync`, `infra
stop`, `infra terminate`, `infra upgrade`) uses `wipe` unless you say
otherwise, so a change to the program never leaves a run waiting on
something nobody will answer. Say `--mode park` or `--mode hibernate`
when the work in flight has to survive, and say it deliberately.

## Reuse completed work while developing

`weft run --seed --detach` takes eligible results from [head]'s run. If
[head] is a version without a run, it finds a finished run on that
version or its nearest ancestor. To choose an older run, you run
`weft branch <color>` first; that also restores its code, so you
checkpoint edits you want to keep.

- `--seed --seed-before classify` reuses earlier compatible work and runs
  `classify` and what follows.
- `--seed --seed-until classify` also permits reusing `classify`.

These flags limit reuse within the requested run; the [cut] limits
execution itself. Changed implementations, inputs, dependencies, failed
work, and live handles can prevent reuse. A loop is reused whole. An
entirely reused run is valid and does no new node work. This applies
inside a saved or carved run too: `--from` chooses its boundary, not a
forced rerun. Identical used [backup]s permit reuse; changing a used
[backup] reruns its consumers. You omit `--seed` to run the selected work
again. If you catch yourself reporting a seeded run without reading its
warnings and inherited-node markers, stop and write: "Wait. What actually
ran?" Then read them; never assume a requested endpoint was reusable.

Builds include the whole catalog by default. The unchanged standard
library uses a shared, finished worker image, so no project compilation is
needed when that image is installed or published. Node edits, added nodes,
or custom build settings need their own image, and building it compiles
only the nodes no earlier build on this machine compiled. Adding an
unchanged catalog node or editing graph configuration needs no new image,
so you use ordinary `weft run` while iterating. `--referenced` opts into a
separate, reduced build compiling only the graph's node types, which can
rebuild when those types change; implementation edits still rebuild.

## Save parameters, then freeze accepted results

`weft run name` runs the current program with the [example]'s parameters,
frozen or not. You add `--seed` only when deliberately reusing results and
omit it when reviewing how the current program answers the saved use case.

```bash
weft run invoice --detach
weft events <color> --full
weft freeze invoice <color> --expect reply
# After editing the program:
weft run invoice --detach
weft diff example:invoice <new-color>
# After inspecting and accepting the new result:
weft freeze invoice <new-color> --expect reply
```

`freeze` preserves that completed run's starting parameters and accepted
output history together. A seeded whole run preserves its original
starting inputs; a carved run preserves its [cut] and the inputs entering
it. Replaying the [frozen example] recomputes the selected work on current
code; interior reused results do not become hidden fixed inputs.

`expected` holds the output evidence `diff` uses, including finite streams
and closures. You repeat `--expect node` to focus review on particular
node outputs; focus changes comparison, not execution, and a focused
output that disappeared remains visible as a difference. Stored media
compares by its content hash.

`diff` presents changed values for your inspection. Differences are not a
verdict and do not produce a failing exit status: you inspect the run's
status separately, explain meaningful changes, and replace the [frozen
example] only after accepting the new result. Running and diffing leave
the accepted file untouched. `weft examples` lists [example]s and [frozen
example]s; it does not certify them.

## Recover an example after changing the graph

Removed input ports are ignored with a warning. Missing [start]s or [cut]
endpoints are errors: you move the [cut] explicitly before running.

```bash
weft run invoice --clear from --from new_classifier='{"text":"the invoice is wrong"}' --target reply --detach
weft run invoice --clear group --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

Explicit `--from`, `--target`, `--before`, `--group`, and `--fire` replace
the corresponding saved settings. Repeated new from flags build the
replacement starting map. Emit flags replace the named supplied ports and
keep the other saved emit entries. `--clear from|target|before|group|emit|fire`
clears a field before those edits; repeat the flag for several fields.
Duplicate newly supplied ports are errors. Values are never guessed for
renamed nodes.

You use `--save another-name` to keep revised parameters separately, or
freeze an accepted new run to replace the [example]'s parameters and
output evidence together.

## Read outside interactions

A [frozen example] records human questions and answers and incoming caller
messages for inspection; the runtime does not replay them on its own. When
a run waits on a person, you compare the current question with the
recorded question and answer, answer the current token within the user's
authority, and keep inspecting that same run. For live connections, you
send the recorded messages through a new connection and review the new
responses.

`weft wake <color> <node>` resolves a pure time wait. A wait requiring a
value must receive that value instead. Logs and inherited markers identify
which earlier run supplied reused history; historical costs are not new
charges.

## Keep a reviewable trail

`weft checkpoint [label]` saves a source version without executing.
`weft tree` shows versions and runs; `weft branch <version|label|color>`
restores a point in that tree. [head] is shared per project. A dirty
branch refusal names the files: you checkpoint them before switching.
Discarding edits and pruning history require the user's authority.

`weft prune <version>` removes its subtree and runs. It refuses when
[head], a frozen origin, running work, or preserved trigger settings still
need that history: you move the relevant reference or preserve the
history, never bypass the refusal. Unused bakes belonging to explicitly
pruned versions go too.

After an edit, you run the relevant saved use cases, inspect their diffs,
and report what actually ran, what changed, why a result is acceptable or
still wrong, and the run colors.
