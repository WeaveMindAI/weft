# Versions, seeds and frozen examples

Every run records the code it ran, so you can go back and look, run something
again against a change, and see what moved.

## Your folder is a copy

weft builds and runs your code from its own copy, not from your folder. For
each place your project is edited, the install keeps a **working copy**, and it
keeps your folder and its working copy the same in both directions.

On an install on your machine, its runtime watches the folder. When you save,
the change reaches the install about a second after the folder goes quiet (a
burst of saves, or a `git checkout`, counts as one change). When a program
writes code into your project (a node calling `ctx.code()`, see [the
ctx](../nodes/ctx.md#a-projects-code)), or `weft branch` moves your folder, the
runtime writes those files into it.

Every command that builds (`run`, `build`, `bake`, `activate`, `resync`, `infra
start`, `infra upgrade`) or moves this folder in the tree (`checkpoint`,
`branch`, `resolve`) also syncs the folder before it does anything, and says
what it moved:

```text
sent your changes to the install: src/main.weft, prompts/tone.txt
brought the install's changes into this folder: nodes/greeter/mod.rs
```

If a command only reads (`tree`, `freeze`) or acts on another working copy
(`forget-copy`, `branch --copy`), it leaves your folder alone and syncs nothing.

On a cloud install nothing watches your folder, so that sync is the only one.

The first of those commands you run in a folder makes a working copy for it,
holding the files the folder has. That copy starts at the head (see below) of
whichever working copy of the project changed most recently.

If your project sits on a Windows drive under WSL (`/mnt/c/...`), an install on
your machine refuses to keep it in sync, because WSL tells Linux nothing about
changes made there:

```text
/mnt/c/Users/me/shop is on a Windows drive, and WSL tells Linux nothing of the changes made there, so weft cannot keep this folder in sync with its working copy. Move the project into the Linux filesystem (under ~/, for example) and run the command again from there
```

### When both sides changed one file

If you edited or deleted a file while the install's copy of it changed too (a
program wrote it, say), weft keeps what your folder holds and writes the
install's copy beside it, named after the file and the first eight characters
of that copy's content hash:

```text
warning: prompts/tone.txt changed both here and in the install: your file is kept, and the install's is written beside it as prompts/tone.weft-conflict-1a2b3c4d.txt; `weft resolve prompts/tone.txt --keep incoming` puts the install's in place, `weft resolve prompts/tone.txt --keep folder` keeps yours and drops the copy
```

Nothing waits on you: builds and runs go on with your file.

You see that warning when a command syncs the folder before it runs. On an
install on your machine you will usually not see it: the runtime syncs your folder about a second after
you save, so it finds the conflict first and only logs it (`the file changed in
the folder and in its working copy`, with both paths). To see it there, run
`weft daemon logs`.

A conflict copy is never part of a version, so even
`main.weft-conflict-1a2b3c4d.weft` is never compiled. To get rid of it, pick a
side with `weft resolve`:

- If you want the install's version, `weft resolve prompts/tone.txt --keep
  incoming` moves the conflict copy over your file.
- If you want to keep yours, `weft resolve prompts/tone.txt --keep folder`
  deletes the conflict copy.

If you had deleted the file, the warning says `prompts/tone.txt was removed here
and changed in the install: it stays removed`; `--keep incoming` brings the
install's version back and `--keep folder` keeps it deleted.

Either way, `resolve` then syncs the folder, so the working copy gets the
side you kept. `--keep` has no default. If you name a file, `resolve` settles
all its conflict copies; if you name one conflict copy, it settles only that
one; if you name nothing, it settles every one in the folder. A file can collect more than
one conflict copy, and then `--keep incoming` asks you to name the one to put
in place:

```text
prompts/tone.txt has 2 conflict copies (prompts/tone.weft-conflict-1a2b3c4d.txt, prompts/tone.weft-conflict-9e8f7a6b.txt); name the one to put in place
```

Name the copy you want (`weft resolve
prompts/tone.weft-conflict-1a2b3c4d.txt --keep incoming`); once it is in
place, `weft resolve prompts/tone.txt --keep folder` drops the rest.

If you would rather merge by hand, take what you want from the conflict copy,
then run `weft resolve <file> --keep folder` (or delete the copy yourself).
If you want to see which conflict copies are left, run `weft tree`.

If the install removed the file while you changed it, your file is kept and
nothing is written beside it, so there is nothing for `resolve` to settle. The
warning says so:

```text
warning: gone.weft changed here and was removed in the install: your file is kept; delete it if you want the removal
```

If you want the removal, delete the file; the next sync sends that to the
working copy.

## A version

A version is your project's files at one moment. A save never makes one.
Something you *do* with the code does: `weft run`, `weft build`, `weft
activate`, `weft bake`, `weft resync`, `weft infra start`, `weft infra
upgrade`, `weft checkpoint` or `weft branch`. Each of these first records the
folder's files as a version. Recording the same files again gives the version
they already are, so ten runs of unchanged code record one.

| In it | Not in it |
|---|---|
| Every `.weft` file, at any depth | Anything hidden at the top: `.git`, `.env`, `.weft` |
| `weft.toml` | `target/` at the root or beside a `Cargo.toml`, and `node_modules`, `__pycache__` and `.pytest_cache` anywhere |
| `src/**` and `nodes/**`, including a node that sits beside your code and the standard library in `nodes/base_catalog/`, edits and all | Any other file or folder at the top, such as `README.md` or `docs/`, and `front/`, which ships through your project's CI |
| `assets/**`, `prompts/**`, `scripts/**`, `sql/**` | Conflict copies |
| `examples/**`, so going back restores the examples that existed then | |
| `layouts/**`, so going back puts the boxes where they were | |

A file identical to one an earlier version held costs nothing to store, so
the standard library is stored once and every later version reuses it. An
install builds a version from exactly the files it holds and nothing of its
own: if you deleted `nodes/base_catalog/`, the version has no standard
library.

## head

Each working copy has a **head**: the version its next version is recorded under,
and the run a `--seed` from it inherits from. Your folder has its own, so a
second clone of the project does not move yours. A program that names no
working copy acts on the project's most recently changed folder: if that is
yours, any run, build, checkpoint or switch the program makes moves your head.

| Command | What it does |
|---|---|
| `weft tree` | Every working copy and where its head is, then every version with what changed against its parent and the runs started by hand beneath it (a trigger's runs are counted on the version's line) |
| `weft checkpoint [<label>]` | Records the folder's files as a version and moves head there. No run, no build |
| `weft branch <version>` | Moves this folder to that version and writes its files here |
| `weft branch <run-id>` | The same with the version that run ran, and makes it head's run, so the next seed inherits from it |

If you have changes no version holds yet, `weft branch` keeps them: it records
the folder as it is as a version first, then moves, and prints the way back.

```text
this folder is now at version 7c41e9a0
your files as they were are version 3f2a1b9c; `weft branch 3f2a1b9c` comes back to them
```

A file you edit while the switch is writing is kept, with the version's copy
written beside it as a conflict copy.

In the editor, the **Runs** view shows the same tree (**Flat list / by
version** on its title bar), and **Load this version** on a version or a run
does what `weft branch` does.

`weft tree` starts with one line per working copy: yours as `this folder`,
another folder as its path and machine, and `the program copy`, the one
programs write into when the project has no folder (every project that had a
head before the update that brought working copies has one). `changed since`
means its files changed after the version its head points at. Each head is marked on its version:

```text
this folder at version 7c41e9a0, changed since
/srv/shop on build-box at version 3f2a1b9c, changed since, conflict copies: src/main.weft-conflict-1a2b3c4d.weft
3f2a1b9c 2026-10-24 09:12:40 UTC (base) root <- HEAD of /srv/shop on build-box
  run 9b81d0a2 2026-10-24 09:13:02 UTC -> 2026-10-24 09:13:30 UTC completed
  7c41e9a0 2026-10-24 10:02:11 UTC ~src/main.weft <- HEAD of this folder, activated
```

`weft tree --json` prints the tree as the install answers it, with
`workingCopy` added: the id of this folder's working copy among
`workingCopies`.

## Seeding

```bash
weft run --seed
```

Reuse everything from head's run whose slice of the program did not change, and
run only the rest. That is the loop when you are iterating on node nine of a
twelve-node program and nodes one to eight cost real money.

If you want to reuse a different run, name it: `weft run --seed=<run-id>` takes
any finished run of the project, including one a trigger started or one that
failed. Keep the `=`: `weft run --seed <word>` reads the word as the name of
a saved example, not as a run. A run that is still going is refused until it
ends or you `weft stop` it.

If a node that runs again waited for somebody in that run, the answers they
gave it are replayed, so it carries on instead of asking again. That only
holds while the node's own code is unchanged; if it changed, the node asks
again and weft says so:

```text
'review' runs again and asks again: its code changed since the seed run answered it
```

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

The invalidation spreads: if a node is not reusable, nothing downstream of it
is either.

### When a reused node's file changed

A saved result that names a stored file says what the file held then. If a
node that runs again would read it, and the file has been edited since (a
later node added to it, another run changed it) or is gone, the run is refused
before anything starts. A run that reuses the node and runs nothing that reads
the file goes ahead:

```text
--seed would reuse what 'ask' produced on 'historyFile', but the file it names,
'conversation.json', has changed since (that run left it at version 4, it is at
version 6 now), so the saved result no longer says what that node made. Hand
the file in yourself (`--emit ask='{"historyFile": ...}'`), or run 'ask' again
by keeping it out of the seed (`--seed-before ask`).
```

### The one that surprises people

A `--from` start is reused like any other node. If its code and the values you hand it
are what they were in the seed run, it keeps that run's result and does not
run. The run reports success and nothing ran, which is correct and invisible,
so weft says it out loud:

```text
'classify' keeps the result run 3f2a1111-0c4e-4b1a-9d2e-5a7b8c9d0e1f gave it:
its code and what it is handed are as they were. Run without --seed to run it again.
```

## Frozen examples

An example is a run you keep: its starting values, the answers people gave, and
the outputs you accept as right.

```bash
weft run --from classify='{"text":"..."}' --save angry-customer
weft freeze angry-customer
```

`--save` writes the starting parameters. `freeze` takes a run that completed
and records its accepted outputs alongside them. If a saved file still uses a
spelling an older weft wrote (`frozen_from`, `gave_up`, `keep_for`), it still
reads, and reading it leaves the file as it is. It takes today's spelling the
next time weft writes it, with `--save` or `freeze`.

Then, after a change:

```bash
weft run angry-customer
weft diff <new-run-id> example:angry-customer
```

`diff` shows what moved on the wires. You, or Tangle, judge whether the change
is acceptable. Freezing the new run replaces what is accepted.

`--expect <node>` marks the outputs that matter, so a later diff leads with
them rather than burying them.

| Command | What it does |
|---|---|
| `weft examples` | What is saved, which are frozen, and each one's latest run |
| `weft freeze <name> [<run-id>]` | Freeze a run. Without a run id, head's run |
| `weft diff <left> <right>` | Compare two runs. A side is a run id or `example:<name>` |
| `weft run <name>` | Run the current code with that example's parameters |

A freeze **replaces** the example whole, and says so. Only a completed run can
be frozen: a failed or cancelled one has nothing to accept.

## Cleaning up

```bash
weft prune <version>
```

Deletes that version, every version under it, and every run beneath them.

It refuses, naming each reason, while any of those versions is a working
copy's head, is the one activated triggers run, is the one the project's
build runs, or is (or sits above) the version a frozen example came from, and
while any run underneath is queued or running. It also refuses while the
settings of a trigger that is on came from one of them, and while a run that
has not ended (one paused waiting for somebody or on a timer included) ran one
of them. Those last two come back as one sentence that names no version
(`these versions still supply trigger settings or running runs`), and only
once every other reason is cleared: `weft stop` the run, and `weft deactivate`
the trigger, which removes the settings the refusal means.

If the refusal names a working copy, you can move it out of the way or forget
it, from any folder of the project:

| Use it when | Command |
|---|---|
| The copy is still in use | `weft branch <version> --copy <copy>`, with a `<version>` outside the history you are pruning. It records what the copy held as a version first, moves it, and leaves this folder where it is |
| Nothing uses the copy any more (a folder that is gone, or the program copy) | `weft forget-copy <copy>`. It records the copy's files as a version, removes the copy, and prints that version with the `weft branch <id>` that brings its files to this folder |

Either way, the copy's files are kept as a version under its old head (a copy
that held no files records none), so
the prune deletes that version too, unless those exact files were already a
version elsewhere in the tree. If somebody still works in that folder, moving
it replaces their files there, so check with them first.

`<copy>` is what the refusal prints: a folder's absolute path, or `program`
for the program copy. The start of a working copy's id works too, and
`weft tree --json` lists every id under `workingCopies`. If two
machines have a folder at that path, weft says so and lists both ids, and you
pass the start of the one you mean.

If a folder you once ran a weft command in is gone, forget its copy: its head stays
where it was until you do. Forgetting a folder that is still there lasts only
until the next command in it that syncs the folder (one that builds, or
`weft checkpoint`, `branch` or `resolve`), which registers it again, so
`forget-copy` refuses this folder's own copy.

Updating to 0.2.274 gave each project that had a head a program copy on that
head, so if you prune the version it was on, or one above it, the refusal
names `program`. A folder starts on the head of the copy that changed last,
which right after the update is the program copy's, so the refusal names your
folder too: `weft branch <version>` in it first. If
the project has a folder, programs that name no working
copy write into that folder, so after `weft forget-copy program` only a
program that names the program copy by its id notices: its calls fail with
`this project has no working copy <id>`. If a program later acts on the
code of a project that has no working copy at all, weft makes a new program
copy then, on the newest version.

If `forget-copy` answers that the copy's files changed while it was being
forgotten, a save or a program wrote into it meanwhile: the copy is still
there, its files are already kept as a version, and running `forget-copy`
again finishes it.
