---
name: weft-sdp
description: "Grow a program against real use cases. Read when carving a run, supplying starting values, baking triggers, reusing completed work, freezing an accepted example, or reviewing a new run against one."
---

# Sequential Diffusion Programming

You build one stage against a real input, run it, and read the result before
growing the next stage. Once the chain answers that use case, introduce another.
Accepted examples preserve the use cases and the results worth reviewing after
an edit. You judge the new results by what the program is meant to do.

## Start with the work you want to exercise

`weft run` builds the current code and starts one execution, identified by its
color. Use `--detach`, then inspect `weft executions --json`,
`weft events <color> --node <id>`, and `weft logs <color>`. Read failures and
waiting states as well as outputs. Completion alone does not establish quality.

Keep the user's graph intact while trying a stage:

```bash
weft run --from classify='{"text":"the invoice is wrong"}' --target reply --save invoice --detach
weft run --group triage='{"text":"the invoice is wrong"}' --detach
weft run --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

- `--from node='{"port":value}'` starts that node and its downstream work.
  A bare `--from node` supplies no backup. Repeat it for several starts.
  Downstream work brings its other input producers with it. Walking upstream
  stops at every named from/emit and at triggers: those are the boundaries.
- `--target node` includes that endpoint. `--before node` excludes it and
  everything beyond that cut. Repeat either flag for several endpoints.
  A group or loop name includes the whole container with `--target`, or
  excludes the whole container with `--before`.
- `--group group='{"port":value}'` runs the whole group or loop alone.
  An included file uses its group alias. `--from group='{"port":value}'`
  runs the whole group or loop and continues downstream. Choose one form;
  `--group` cannot combine with from, emit, target or before.
- `--emit node='{"port":value}'` supplies that node's outputs without running
  its body. Repeat for several supplied nodes. An output with no consumer
  remains valid output evidence.

Supplied inputs belong to the starts you name. They are backups: wait for real
execution input first; use the supplied value only after the real source closes
without a value or no source participates. A real value wins, including null.
A running source is never overwritten. Errors and invalid real values remain
errors; a backup does not hide them.

For a generator port, supply all its items in one array:
`--emit batches='{"items":["first","second"]}'`. The runtime emits each item
and then closes the port; `[]` closes it without an item. A list-typed ordinary
port receives its array as one value. Read the actual port type before choosing.

Cuts inside ordinary groups stay at the requested node. Their `_should_flow`
gates still apply: false stops the branch; true dispatches only the selected
work. Loops are indivisible. Use the public loop name for starts, `--group`,
`--target`, or `--before`; a node inside the loop cannot be an endpoint. Trigger
setup and infra preparation obey the same group cuts and loop restriction.

## Choose how a trigger participates

Read the trigger's implementation and its ports before forming either payload.

```bash
weft bake
weft run --fire incoming='{"event":"the trigger wake payload"}' --detach
weft run --emit incoming='{"message":"the emitted output value"}' --detach
```

A fire names exactly one trigger. Its payload is what wakes that trigger; the
trigger runs and decides what to emit. An emit supplies its declared outputs
directly. Choose one for that trigger. Trigger ports receive their prepared
settings through `weft bake`; a trigger cannot be a `--from` input start.

`bake` runs preparation and saves the trigger settings without listening.
Changed code or configuration requires a matching new bake. Closed group
gates can leave a trigger unprepared; inspect the bake instead of forcing it.
`weft activate` prepares and arms real listeners. `weft bake <project-id>`
uses that project's registered build. Builds include the whole catalog by
default. If deliberately using `--referenced`, pass it to both bake and run.
`weft deactivate` requires an explicit `--mode wipe`, `--mode park`, or
`--mode hibernate` in scripts. Choose according to whether ongoing work is
cancelled or preserved; there is no automatic destructive choice.

## Reuse completed work while developing

`weft run --seed --detach` takes eligible results from head's run. If head is
a version without a run, it finds a finished run on that version or its nearest
ancestor. To choose an older run, use `weft branch <color>` first; that also
restores its code, so checkpoint edits you want to preserve.

- `--seed --seed-before classify` reuses earlier compatible work and runs
  `classify` and what follows.
- `--seed --seed-until classify` also permits reusing `classify`.

These flags limit reuse within the requested run. `--target` and `--before`
limit execution itself. Changed implementations, inputs, dependencies, failed
work, and live handles can prevent reuse. A loop is reused whole. Read the
warnings and inherited-node markers; never assume that a requested endpoint
was reusable. An entirely reused run is valid and does no new node work.
This applies inside a saved or carved run too: `--from` chooses its boundary,
not a forced rerun. Identical used backups permit reuse; changing a used
backup reruns its consumers. Omit `--seed` to run the selected work again.

The unchanged standard library uses a shared, finished worker image: no
project compilation is needed when that image is installed or published.
Node edits, added nodes, or custom build settings need their own image,
and building it compiles only the nodes no earlier build on this machine
compiled; `--referenced` also requests a separate, reduced build.
Builds include the whole catalog by default. Use ordinary `weft run` while
iterating: adding an unchanged catalog node or editing graph configuration
needs no new image. `--referenced` opts into compiling only the graph's node
types, which can rebuild when those types change. Implementation edits still
rebuild. Use small, quick stages before costly full use cases.

## Save parameters, then freeze accepted results

`--save name` writes running parameters to `examples/name.json`.
`weft run name` runs the current program with those parameters. Loading a
frozen example uses the same command. Add `--seed` only when deliberately
reusing results; omit it when reviewing how the current program answers the
saved use case.

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

`freeze` preserves that completed run's starting parameters and accepted output
history together. A seeded whole run preserves its original starting inputs;
a carved run preserves its cut and the inputs entering it. Replaying the
example recomputes the selected work on current code. Interior reused results
do not become hidden fixed inputs.

`expected` stores the output evidence used by `diff`, including finite streams
and closures. Repeat `--expect node` to focus review on particular node outputs.
Focus changes comparison, not execution. A focused output that disappeared
remains visible as a difference. Stored media compares by its content hash.

`diff` presents changed values for human or AI inspection. Differences are
not an automated verdict and do not produce a failing exit status. Inspect
the run's status separately, explain meaningful changes, and replace the frozen
example only after accepting the new result. Running and diffing leave the
accepted file untouched. `weft examples` lists saved parameters and frozen
examples; it does not certify them.

## Recover an example after changing the graph

Removed input ports are ignored with a warning. Missing starting nodes or cut
endpoints are errors: explicitly move the cut before running.

```bash
weft run invoice --clear from --from new_classifier='{"text":"the invoice is wrong"}' --target reply --detach
weft run invoice --clear group --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

Explicit `--from`, `--target`, `--before`, `--group`, and `--fire` replace the
corresponding saved settings. Repeated new from flags build the replacement
starting map. Emit flags replace the named supplied ports and retain the other
saved emit entries. `--clear from|target|before|group|emit|fire` clears a field
before those edits; repeat the flag for several fields. Duplicate newly
supplied ports are errors. Values are never guessed for renamed nodes.

Use `--save another-name` to keep revised parameters separately, or freeze an
accepted new run to replace the example's parameters and output evidence together.

## Read outside interactions

A frozen example records human questions and answers and incoming caller
messages for inspection. The runtime does not automatically replay them.
When a run waits on a person, compare the current question with the recorded
question and answer, then answer the current token within the user's authority.
Continue inspecting that same run. For live connections, send the recorded
messages through a new connection and review the new responses.

`weft wake <color> <node>` resolves a pure time wait. A wait requiring a value
must receive that value instead. Logs and inherited markers identify which
earlier run supplied reused history; historical costs are not new charges.

## Keep a reviewable trail

`weft checkpoint [label]` saves a source version without executing.
`weft tree` shows versions and runs; `weft branch <version|label|color>`
restores a point in that tree. Head is shared per project. A dirty branch
refusal names the files: checkpoint them before switching. Discarding edits
and pruning history require the user's authority.

`weft prune <version>` removes its subtree and runs. It refuses when head, a
frozen origin, running work, or preserved trigger settings still need that
history. Move the relevant reference or preserve the history; do not bypass
the refusal. Unused bakes belonging to explicitly pruned versions go too.

After an edit, run the relevant saved use cases and inspect their diffs. Report
what actually ran, what changed, why a result is acceptable or still wrong,
and the run colors. If you catch yourself calling an example good because its
run completed, write: "Wait. Read the result." Then inspect the evidence.
