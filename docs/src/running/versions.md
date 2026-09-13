# Versions, seeded runs and frozen examples

Every run records the code and parameters it used. You can reuse compatible
completed work while developing, or run the current code with a saved use case
and inspect how its answers changed.

## The tree

`weft tree` shows source versions and their runs. A color identifies one
execution. Freeze and diff also accept an unambiguous prefix of that color.
Each version records the project's source files by content hash.

| Command | Effect |
|---|---|
| `weft checkpoint [label]` | Record the current files without building or running, and move head to that version. |
| `weft branch <version\|label\|color>` | Restore that version's files and move head. A color also selects that run as the next seed. |
| `weft tree` | Show versions, runs and head. `--json` includes the version matching disk. |
| `weft checkpoint --root` / `weft run --root` | Record a source version without a parent. Refuses if that version already has a parent. On run, root disables seeding. |
| `weft prune <version>` | Remove the subtree and its runs after confirmation. |

Head is shared per project. Checkpoint clears head's selected run; a later seed
looks for a settled run on that version or its nearest ancestor.
Branch refuses to overwrite unkept edits; checkpoint them first.
`--discard` explicitly permits replacing them.

Prune refuses when head, an activated listener, running work or a frozen
example still needs the source history. Unused bakes belonging to explicitly
pruned versions are removed too. Source files still referenced by a registered
build or surviving history remain retained.

## Running one group, or one node onward

Run builds automatically. Keep the graph intact and select the work to exercise:

```bash
weft run --from classify='{"text":"the invoice is wrong"}' --target reply --save invoice --detach
weft run --group triage='{"text":"the invoice is wrong"}' --detach
weft run --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

| Flag | Meaning |
|---|---|
| `--from node='{"port":value}'` | Start at the node with backup inputs. A bare node supplies no backup. Repeat for several starts. |
| `--emit node='{"port":value}'` | Supply that node's outputs without executing its body, then continue downstream. |
| `--target node-or-group` | Include this endpoint and stop propagation beyond the cut. A group or loop name includes its whole body. Repeat for several endpoints. |
| `--before node-or-group` | Stop before this endpoint. A group or loop name excludes its whole body. Repeat for several endpoints. |
| `--group group='{"port":value}'` | Run a whole group or loop alone, using its input ports. An included file uses its group alias. |

Downstream work brings the other producers it needs. The upstream walk stops
at each `--from`, `--emit`, and trigger. For A feeding C and B also feeding C,
`--from A` includes B; `--from C` stops before both producers and uses C's
supplied backups or normal closed-input behaviour. Unrelated branches stay out.
A cut selecting no work or output evidence is refused before building an image.

`--group` is a complete selection: it cannot combine with from, emit,
target or before. `--from group=...` starts at the whole group and continues
downstream instead. Cuts inside ordinary groups stay at the named node.
Their `_should_flow` gates still apply, and a true gate dispatches only
selected work. Loops are indivisible: select the whole loop or move the cut
outside it. Trigger setup and infra preparation follow the same restrictions.

Supplied inputs are backups at the named starts. The runtime waits for real
producers first. A real value wins, including null; a clean closure without
a value permits the backup. An error or invalid real value remains an error.
Inputs at arbitrary interior nodes are not part of the run parameters.

For a generator output, `--emit batches='{"items":["first","second"]}'`
emits those items in order and closes the port. An empty array closes it
without an item. An ordinary list port receives its array as one value.
Read the port type before supplying it.

## Preparing and firing triggers

```bash
weft bake
weft run --fire incoming='{"event":"the trigger wake payload"}' --detach
weft run --emit incoming='{"message":"the emitted output value"}' --detach
```

Bake runs preparation and saves the resulting trigger settings without arming
listeners. A fire uses those settings and gives exactly one trigger its wake
payload. An emit supplies declared outputs without executing that trigger.
The same trigger cannot use both forms. Trigger inputs are prepared through
bake; a trigger cannot be a from start.

The bake must match the code and configuration being run. Bake again after
changes. Closed group gates can leave triggers unprepared; inspect preparation
events when fire refuses. `weft bake <project-id>` uses the registered build.
Builds include the whole catalog by default. If using `--referenced`, use it
for both bake and run so their code identities match.

`weft activate` prepares and arms listeners. Each real event starts its own
run from its one trigger, using the program that prepared that listener.

## Seeding: run only what changed

`weft run --seed` reuses eligible completed work from head's selected run.
When head names only a version, it finds a settled run there or on the nearest
ancestor. To select a particular older run, branch to its color first;
branch also restores its code, so checkpoint edits you want to keep.

`--seed-before node` permits reuse before that node.
`--seed-until node` also permits reusing the node itself. Both require
`--seed`. These flags bound reuse; target and before bound execution.

Changed implementations, inputs or dependencies invalidate affected work.
Failed work and live handles cannot be reused. Loops are reused whole.
The run reports what was inherited and what ran. A requested reuse boundary
does not make incompatible results eligible; read the warning and move the
cut earlier if the old values cannot supply it. A fully inherited run is valid.

`--from` chooses a boundary, not a forced rerun. With `--seed`, unchanged
work inside that cut can be reused, including identical used backups. Omit
`--seed` to run it again, or use the seed endpoints to limit reuse.

The unchanged standard library shares a finished worker image across projects.
Setup prepares it, and releases publish it so a project can pull it without
compiling. Project names, IDs and graph settings do not change that image.
Node edits, added nodes, custom build settings, or `--referenced` need their
own image. Building one compiles only what no earlier build on this machine
already compiled: every worker build shares one compile cache, and a node
whose files have not changed is taken from it. Once the standard library has
been compiled once, a project with one custom node compiles that node and
links. `weft clean --build-cache` throws that cache away.

Builds include the whole catalog by default. Adding an unchanged catalog node
or editing graph configuration needs no new image. `--referenced` opts into
compiling only the graph's node types. Implementation edits still rebuild.

## Freezing an accepted run

`--save name` saves starting parameters to `examples/name.json`.
`weft run name` runs the current program with those parameters, whether
the file contains only parameters or a frozen result.

```bash
weft run invoice --detach
weft events <color> --full
weft freeze invoice <color> --expect reply
# After changing the program:
weft run invoice --detach
weft diff <new-color> example:invoice --full
# After accepting the new result:
weft freeze invoice <new-color> --expect reply
```

Freeze requires a completed run. It preserves that run's starting parameters
and observed outputs. A seeded whole run preserves its original starting
inputs; a carved run preserves its cut and inputs entering it. Interior
reused results do not become hidden fixed inputs.

`expected` holds output history, including finite streams and closures.
Repeat `--expect node` to focus review on particular outputs.
Focus affects comparison, not execution. A focused output that disappeared
remains visible as a difference. Stored media compares by content hash.

Run the example without seed when reviewing the current computation.
Diff presents changed values for a person or AI to judge; differences do not
produce a failing exit status. Inspect execution status separately. Run and
diff leave the accepted file intact; freeze again only after accepting its
replacement. `weft examples` lists saved parameters and frozen examples.

## Repairing saved parameters after a graph change

Removed input ports are ignored with a warning. Missing starting nodes or
cut endpoints are errors, so move the cut explicitly:

```bash
weft run invoice --clear from --from new_classifier='{"text":"the invoice is wrong"}' --target reply --detach
weft run invoice --clear group --from triage='{"text":"the invoice is wrong"}' --before publish --detach
```

Explicit from, target, before, group and fire flags replace their corresponding
saved settings. Repeated new from flags form the replacement map.
Emit flags replace the named ports while retaining other saved emit entries.
`--clear from|emit|target|before|group|fire` clears a field before edits;
repeat it for several fields. Duplicate newly supplied ports are errors.
Use `--save another-name` to preserve revised parameters separately.

## When a run waits

Frozen examples retain human questions and answers and incoming caller
messages for inspection. They are not automatic replies. Compare the new
question with the recorded question and answer before answering the current
token. For a live connection, send messages through a new connection.

`weft wake <color> <node>` resolves a pure timer wait. A wait expecting a
value must receive that value.

## Reading the result

`weft executions` shows status. `weft logs <color>` shows failures and
node logs; `weft events <color> --node <id> --full` shows values.
Inherited history names its original run and is interpreted against its
original program. Historical costs are not new charges.
The editor can display the same run on its graph, including inherited work.

Completion establishes that execution finished. The output evidence tells
you whether the program answered the use case.
