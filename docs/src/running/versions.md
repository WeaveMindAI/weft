# Versions, seeds and frozen examples

Every run records the code it ran, so you can go back and look, run something
again against a change, and see what moved.

## A version

A snapshot of your project's files, taken whenever you run, activate, or type
`weft checkpoint`.

| In it | Not in it |
|---|---|
| Every `.weft` file, at any depth | Anything hidden at the top: `.git`, `.env`, `.weft` |
| `weft.toml` | `layouts/`, because dragging a box is not a change to your program |
| `nodes/**` | `nodes/base_catalog/`, which belongs to the installed weft. A version records which weft that was instead |
| `assets/**`, `prompts/**`, `scripts/**`, `sql/**` | `target/` at the root, and `node_modules` anywhere |
| `examples/**`, so going back restores the examples that existed then | |

A file identical to one an earlier version held costs nothing to store.

## head

**head** is where you are: the version your next checkpoint sits under, and the
run a `--seed` inherits from.

| Command | What it does |
|---|---|
| `weft tree` | The whole tree: every version with what changed against its parent, its runs beneath it, head marked |
| `weft checkpoint [<label>]` | Records the files as a version. No run, no build |
| `weft branch <version>` | Puts those files back and moves head |
| `weft branch <color>` | The same, and points head's run at that one, so the next seed inherits from it |

`weft branch` refuses on a dirty tree and names the files, rather than throwing
your work away:

```text
the tree has changes since head a1b2c3d:
  src/main.weft
`weft checkpoint` keeps them as a version, or `--discard` throws them away
```

## Seeding

```bash
weft run --seed
```

Reuse everything from head's run whose slice of the program did not change, and
run only the rest. That is the loop when you are iterating on step nine of a
twelve step program and steps one to eight cost real money.

### What will not be reused

| | Why |
|---|---|
| Anything whose code or dependencies changed | It is a different thing now. weft says so by name |
| Anything downstream of something that changed | Its input is different |
| Anything that never finished: failed, cancelled, still running, or parked on somebody | There is no result to reuse |
| Anything that emitted a live handle, like a bus | The handle belongs to a worker that is gone |
| A whole loop, if any iteration of it is invalid | A loop is reused whole or not at all |
| Anything downstream of an explicit `--emit` or `--fire` | You asked for a fresh event, so its consumers are fresh |
| Anything past `--seed-before` or `--seed-until` | You said where to stop |
| A starting value you supplied that differs from the one the earlier run used | Different input, different work |

The invalidation spreads: if a step is not reusable, nothing downstream of it
is either.

### The one that surprises people

A value you hand in at a start is a **backup**. It stands in only when nothing
upstream supplies that port.

Under a seed the upstream result is usually right there in history, so your
backup goes unused and the step is reused exactly as it was. The run reports
success and nothing ran, which is correct and invisible, so weft says it out
loud:

```text
'classify.text' keeps the value run 3f2a1111 gave it; the value supplied at
this start is a backup and only stands in when nothing upstream supplies the
port. Run without --seed to hand it in.
```

## Frozen examples

An example is a run you keep: its starting values, the answers people gave, and
the outputs you accept as right.

```bash
weft run --from classify='{"text":"..."}' --save angry-customer
weft freeze angry-customer
```

`--save` writes the starting parameters. `freeze` takes a run that completed
and records its accepted outputs alongside them.

Then, after a change:

```bash
weft run angry-customer
weft diff <new-color> example:angry-customer
```

`diff` shows what moved on the wires. You, or Tangle, judge whether the change
is acceptable. Freezing the new run replaces what is accepted.

`--expect <node>` marks the outputs that matter, so a later diff leads with
them rather than burying them.

| Command | What it does |
|---|---|
| `weft examples` | What is saved, which are frozen, and each one's latest run |
| `weft freeze <name> [<color>]` | Freeze a run. Without a color, head's run |
| `weft diff <left> <right>` | Compare two runs. A side is a color or `example:<name>` |
| `weft run <name>` | Run the current code with that example's parameters |

A freeze **replaces** the example whole, and says so. Only a completed run can
be frozen: a failed or cancelled one has nothing to accept.

## Cleaning up

```bash
weft prune <version>
```

Deletes that version, every version under it, and every run beneath them.

It refuses on head's version, on any version a frozen example came from, and
while any run underneath is still going.
