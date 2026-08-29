# When something goes wrong

The failures you will actually hit in the first hour, and what each one means.

## `dispatcher unreachable`

The runtime daemon is not up.

```bash
weft daemon status
weft daemon start
```

`weft daemon logs -f` tails it if it starts and then dies.

## Port 9999 is taken

That is the port the runtime is reachable on, and both the daemon and the CLI
have to agree on it:

```bash
export WEFT_DISPATCHER_PORT=19999
export WEFT_DISPATCHER_URL=http://localhost:19999
weft daemon start
```

Put **both** exports in your shell config. The first is the port the daemon
binds and the second is where the CLI looks, and a later `weft daemon stop` in
a shell missing the first one goes hunting for a daemon on 9999.

## `kind` not found on PATH

`weft daemon start` needs it, to make and reach the local cluster. Install
[kind](https://kind.sigs.k8s.io/docs/user/quick-start/) and re-run. A full
`./setup.sh` checks for it before it starts anything, so this usually only
turns up if you installed with `--cli` alone.

## The compiler refused something

Read the code in brackets. Every diagnostic has a stable slug like
`type-mismatch`, `required-port-unmet`, `graph-cycle`, and each one is listed
with its cause and its fix in
[What the compiler refuses](../language/diagnostics.md).

Every message names the fix. If you hit one that names the problem without the
fix, report it as a bug.

## A node ran and produced nothing

This is not an error. A node whose required input arrives **closed** is
skipped, and that skip cascades downstream. That is how branching works, and the graph
shows skipped nodes distinctly from failed ones.

If a whole branch is dark and you did not expect it, walk upstream to the
first node that closed a port. [How a weft program runs](../language/mental-model.md)
covers the rule in full.

## The editor's graph does not match the file

Both views come from the same compiler, so they should never disagree. Reload
the VS Code window: that respawns the parse server, which is usually a stale
one left over from an install.

## An execution is stuck

`weft events <color>` prints what each node did, in order, with what it
emitted. Read it to find the last node that fired and work out what the next
one was still waiting for.

`weft stop <color>` cancels a running execution.

## `the canonical schema changed with no migration to match`

This one only reaches you if you are changing weft itself. You edited a table's
`CREATE TABLE`, and the Postgres volume survives `./setup.sh` runs, so the
tables on disk are still the old shape and nothing was written to carry them
across.

`./setup.sh --migration <name>` writes the migration for what you changed, and
the next start applies it and keeps everything that was in the database.

The error also prints the SQL to drop just the tables it named, if you would
rather throw that data away than carry it across.

## Something else

If you do not know which side broke, look in two places, in this order:
`weft daemon logs` has the runtime's side and `weft events <color>` has the
execution's side. Between them almost everything
is visible, because the runtime writes down every event as it happens.

If you are stuck, the [Discord](https://discord.com/invite/FGwNu6mDkU) is the
fastest place to ask.

---

That is the tour. From here:

- [How a weft program runs](../language/mental-model.md) for the model
  everything rests on.
- [What a node is](../nodes/what-a-node-is.md) to start building vocabulary.
- [How connections work](../connections/overview.md) to talk to real services.
