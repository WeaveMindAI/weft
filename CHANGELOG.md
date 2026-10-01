# Changelog

What changed between versions of weft that you need to know about when you
update. To update, pull and run `./setup.sh` again.

## Unreleased: weft no longer needs Kubernetes

**Breaking: your run history does not come along.**

Weft used to run on a local Kubernetes cluster. It now runs as one process
next to Docker on your machine, and on Google Cloud as Cloud Run services plus
one small VM. The database behind it starts over with this version, so the old
one cannot be carried forward.

If you installed weft before this version, `./setup.sh` finds the old install
and stops to ask before it goes on:

```
an older weft (the one that ran on a local Kubernetes cluster) is on this machine,
and this version cannot carry it forward. It is going to be wiped: the projects on
your disk are not touched, but their run history, versions and stored connections are.
  Do you want to proceed? [Y/n]
```

If you say yes, it wipes the old install (`scripts/scrub-old-install.sh`) and
carries on with a normal install. If you say no, nothing is touched and nothing
is installed.

What goes:
- The old cluster.
- Every project's run history and versions.
- Your stored connections: you connect your accounts again the first time a
  program asks.
- Every container, volume and image the old weft made, your programs' own
  infrastructure (and its data) included.
- Every named install on the machine (a test cell, say), with its database.
  The question lists the ones it found.

What stays:
- Your project folders. `weft run` in each one registers it again.
- Whether you turned on a public address with `--public-url`.
- The assistant you picked for `weft new`.

If you would rather wipe first by hand, or `./setup.sh` runs without a
terminal to ask on, run `scripts/scrub-old-install.sh` yourself, then
`./setup.sh`. On a machine that never had the old weft, or that was already
wiped, `./setup.sh` is a normal install or update and asks nothing.

**"Member" is now "instance", everywhere.**

What used to be a program's members are now its instances: separate copies of
part of a program, each under its own id. One per person is one way to use
them, one per session another, several per person a third; which person owns
which instances is kept in your program's own database. Every name changed
with it, and the old ones are gone:
- `@per_member` is `@per_instance`, and `@member_filled` is `@instance_filled`.
  Change them in your `.weft` files.
- The `members` catalog package is `instances`, and its nodes follow:
  `CurrentInstance`, `MintInstanceToken`, `WipeInstance`, `ListInstances`,
  `ListInstanceInfra`, `InstanceInfraStatus`, `StartInstanceInfra`,
  `StopInstanceInfra`, `TerminateInstanceInfra`, `ActivateInstanceTriggers`,
  `DeactivateInstanceTriggers`, `GetInstanceValues`, `SetInstanceValues` and
  `InstanceCosts`.
- The headers are `Weft-Instance` and `Weft-Instance-Token`. If your backend or
  frontend sends the old ones, rename them there.
- The `--member` flag is `--instance` on every command, and
  `--all-members` is `--all-instances`.
- A member token is now an instance token: it acts inside one instance of the
  program.

The page is now [programs with instances](docs/src/running/instances.md).

The name "instance" used to mean a named install (a second weft on the same
machine, such as a test cell). That is now called an install: the field in
`~/.local/share/weft/config.json` is `install`.

**weft moves to new ports.**

Weft now answers at `http://127.0.0.1:14111` (it was 9999). Open the dashboard
there, and point anything you configured by hand at it. The VS Code extension
follows on its own unless you set `weft.dispatcherUrl` yourself (if yours
still says the old `http://localhost:9999`, it is ignored and the extension
offers to remove it). The browser
extension remembers the address with each token you saved, so remove those
tokens and add them again.

If you use a Cloudflare tunnel, point it at `127.0.0.1:14112` (it was 9998).

If you want weft on another port, set its variable once when you run
`./setup.sh` (`WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`, `WEFT_INTERNAL_PORT`
or `WEFT_POSTGRES_PORT`). Weft saves the ports it runs on in
`~/.local/share/weft/ports.json`, so the port stays moved on later runs and
the CLI and the VS Code extension find it there.
`WEFT_SEAWEED_PORT` moves the object store, and is read on every run.

**A step the worker was running is never run again by itself.**

If a worker went away while a step was running, the next worker used to run
that step again from the top, so an email, a post or a paid call could happen
twice. Now it fails the step instead, saying the worker went away and the step
was not run again. Check what the step did, then re-run from there with
`weft run --seed`, which reuses every step that completed. A step that was
waiting on an answer still carries on when the answer comes, as before.

**Nodes that catch their failures say so with `catchErrors`.**

If you wrote a node with an optional `error` output and `ctx.catch_into_error`,
change it: remove the `error` output from its `metadata.json`, add
`"catchErrors": true` under `features`, and let `run` return its errors as any
node does (`ctx.catch_into_error` is gone). Weft adds the `error` output
itself, and a failure goes there when a program wires it. A node with `catchErrors`
that still declares `error` is refused when it loads. Programs need no change.

**Conversations are kept in a file only.**

The AI nodes (`LlmInference`, `LlmStream`) and `ChatHistoryAppend` no longer
have a `history` input or output. If a program wires `history`, wire
`historyFile` instead: each call adds its turn to that one file and passes the
same file on. If a call gets no file and its `historyFile` output is wired, it
starts a new one. The graph shows each change to the file under "Files
edited" on the step that made it.

**A node changes a stored file with `edit`.**

`ctx.storage(scope).edit(&file, |old| ...)` changes a file in place without
losing another write's change. `replace` still overwrites.
`replace_stream` is gone. Every stored file value now carries a `version`. A
seeded run that would reuse a step whose file was edited since is refused,
naming the step, the port and the file, and offering `--emit` or
`--seed-before`.
