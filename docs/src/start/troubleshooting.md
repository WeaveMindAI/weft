# When something goes wrong

Before anything else: if you have Tangle, hand it the problem. `/weft-debug`
reads the failed run, follows it back to the step that broke, and tells you
what it found. That is usually faster than anything below.

If you would rather look yourself, start with what actually failed. The
installer, the compiler, or a run. Each has its own place to look.

## A step went red

Click it. The inspector shows the error, plus what went into that step, which
is normally where the answer is.

If a step is greyed out instead, it was skipped, and the inspector says why in
plain words: its `_should_flow` said no, a required input closed, or the group
it lives in never ran. Follow that back and you find the decision that
switched it off. A skip is often correct, since the branch of a `Switch` that
did not win is skipped exactly like this. For the rules behind that, read
[how a program runs](../language/mental-model.md#how-a-branch-stops-the-steps-after-it).

## The graph is showing you the wrong run

Check the pill at the top of the canvas. It says which run you are looking at,
and while it says **Pinned** it stays on that one no matter what else happens.
Click it to go back to following the newest, or use the **Executions** list in
the sidebar to pick another.

Old values do not update. A run is a record of what happened, so editing the
program afterwards does not change what it says.

## The compiler is complaining

Every complaint names a file, a place in it, and what rule it broke.
`type-mismatch` means the value on that arrow does not fit where you plugged
it in. `required-port-unmet` means a step needs an input that nothing is
supplying.

Open that spot and compare what the step wants with what you gave it.
`weft describe-nodes --node <Type> --compact` prints what a step takes and
gives back. Every code is listed in
[what the compiler refuses](../language/diagnostics.md).

## A run failed, or is stuck waiting

```bash
weft events <color>
```

The color is the run's id, from the terminal or the sidebar. That prints
everything that happened, in order, so you can find the error or the point
where it stopped.

A run waiting on a person stays waiting until somebody answers, and that is
working as intended. A run holding an HTTP caller open follows different
rules, in [talking to a live caller](../nodes/live-callers.md).

To stop a run you no longer want:

```bash
weft stop <color>
```

If the live updates in the editor stop arriving, that does not mean the run
stopped. Check with `weft executions`.

## The CLI cannot reach the runtime

```bash
weft daemon status
weft daemon logs
```

If you stopped it, `weft daemon start` brings it back. If it refuses to start,
read the error before you start reinstalling things: it usually names a
missing tool, a port already in use, or a database that needs a migration.

If something else already has port 9999, move weft to another one. Set both of
these wherever you run weft, and keep them set for later commands:

```bash
export WEFT_DISPATCHER_PORT=19999
export WEFT_DISPATCHER_URL=http://localhost:19999
weft daemon start
```

The editor has its own setting for that address, so change it there too. Where
these come from is in [the CLI](../running/cli.md#the-environment).

## It says the schema does not match

The message is `the canonical schema changed and this database does not hold
the shape it declares`, and it means the database and the code disagree about
a table.

Keep the whole message, because it names which one. If you were changing
weft's own database code, the fix is in
[working on the database](https://github.com/WeaveMindAI/weft/blob/mvp/CONTRIBUTING.md#working-on-the-database).
If it appeared during an ordinary update, that is a bug worth reporting, with
the versions you moved between. Do not wipe the database to make it go away.

## Ask us

Bring the command, the whole error, and the piece of program it points at to
[Discord](https://discord.com/invite/FGwNu6mDkU). For a broken run, add its
events. For a broken runtime, add the daemon logs. Take your credentials and
anything private out first.
