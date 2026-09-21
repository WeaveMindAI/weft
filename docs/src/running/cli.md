# The CLI

Every command takes `--dispatcher <url>` (or the `WEFT_DISPATCHER_URL`
environment variable) and `--json`, which prints one JSON object per line on
stdout while logs go to stderr. That is how the VS Code extension drives the
CLI.

This page is a reference, organized by what you are trying to do. For what each
verb means and what every refusal is telling you, the linked pages go deeper.

## Scaffold a project

| Command | What it does |
|---|---|
| `weft new <name>` | Scaffold a project: `weft.toml`, `src/main.weft`, `nodes/` with the standard library seeded in. |
| `weft new <name> --assistant <who>` | The same, plus Tangle, the AI builder persona, copied in for that assistant. The names: `cc` claude-code, `kc` kilo-code,
`cu` cursor, `cx` codex, `gh` github-copilot, `gc` gemini-cli, `cl` cline,
`oc` opencode, `dd` devin-desktop, `ju` junie. `--assistant none` installs
nothing, and an assistant weft has no row for gets the fallback Tangle (a
plain `AGENTS.md` plus its skills). Repeat the flag for several. See [Your first program](../start/first-program.md). |
| `weft tangle update [--assistant <who>]` | Re-copy this project's Tangle from the installed weft, the way `weft catalog update` re-copies the standard library. |

The `--assistant` choice is remembered for later `weft new` runs; `--assistant
none` stops it.

On `weft tangle update`: with no flag it refreshes the assistants the project
already has; with one it also installs an assistant that is not here yet. It
**replaces every file Tangle owns**, so your own edits to those files go with
them. Anything your assistant wrote beside them stays.

## Build and run

| Command | What it does |
|---|---|
| `weft build` | Build the worker image and register the project. Starts nothing. |
| `weft run` | Compile, register, fire one execution, and stream its events until completion, including across waits. |
| `weft run --detach` | Return as soon as the run has started. |
| `weft run --referenced` | Compile only the graph's node types instead of the whole catalog. |
| `weft run --target <node>` | Run those nodes and whatever they need. Repeatable; several targets run the union of what each needs. Any node can be a target. |
| `weft run --before <node>` | Run what that node needs and NOT the node. Repeatable. The mirror of `--target`. |
| `weft run --seed [--seed-before <node>] [--seed-until <node>]` | Reuse compatible completed work from head's run. See [Seeding](versions.md#seeding-run-only-what-changed). |
| `weft run [<example>]` | Run with a named example's saved starting parameters, compiled from the current code. |
| `weft follow <project>` | Live event stream for a project. |
| `weft stop <color>` | Cancel a running execution. |
| `weft ps` | List registered projects. |
| `weft status` | The runtime's overall state. If the project is not registered, it explains how to run it or activate its triggers. |

`weft build` also puts back code the dispatcher no longer holds; see
[A past run that shows nothing](the-journal.md#a-past-run-that-shows-nothing).
It does not add
a version to the tree; `weft checkpoint` does that.

Every run records the code it ran as a version. See
[Versions](versions.md).

![weft run following an execution to completion](../img/cli-run.gif)

### Choosing what part runs

The full form:

```text
weft run [<example>] [--referenced] [--seed] [--root]
    [--from <node>=<ports-json>]... [--emit <node>=<ports-json>]...
    [--target <id>]... [--before <id>]... [--group <id>=<ports-json>]
    [--fire <trigger>=<wake-json>] [--save <name>]
```

- `--from` supplies backup inputs at a start. `--emit` supplies outputs without
  executing that node. A real producer takes precedence over a backup.
- `--target` includes the endpoint. `--before` excludes it.
- `--group` selects a whole group or included file, with its input payload.
- `--fire` runs exactly one trigger, using a matching bake.
- `--seed-before` stops reuse short of that node; `--seed-until` reuses the
  node itself too. Changed code, inputs and dependencies still invalidate reuse.
- `--save <name>` saves the run's starting parameters as an example.

Ordinary groups can be cut precisely; loops stay whole. Clear and replacement
rules are in [Versions](versions.md).

### Prefix colors

Every command that takes a color also takes the first characters of one, as
long as they name a single run:

```bash
weft stop 3f2a
```

## Inspect a run

| Command | What it does |
|---|---|
| `weft executions [--limit N] [--project <id>] [--phase fire\|trigger_setup\|infra_setup]` | Recent executions, newest first: status, phase, local start time, entry node, and the tags the run put on itself. |
| `weft events <color> [--node <id>] [--kind <kind>] [--full]` | One execution's events in order, one line each: local time, kind, node, and a short summary of the value or error. |
| `weft logs [color]` | The run's log: the lines its nodes wrote, plus every failure the journal recorded about it. Last 1000 lines. |
| `weft clean [color]` | Purge journal data. See below. |

`--phase fire` hides the setup runs an activate or resync makes, so it answers
"has my trigger fired since the change".

`weft events` cuts values short so a long run stays readable. `--node` keeps
one node's events, spelled the same way [`wake`'s node argument is
spelled](#versions-and-examples), and only that use of it. `--kind` keeps one
kind (`node_failed`); a substring works, so `failed`
catches both failure kinds. `--full` prints the values whole, and `--json`
prints the replay rows the graph view reads.

`weft logs` records failures as `error` and `warn` lines naming the node they
are about: a node failing, a port refusing a value, the run failing or being
cancelled. `--limit` raises the 1000-line default, up to 20000, and a full page
says the run may have written more.

### weft clean

Purges journal data. Asks before deleting runs; pass `--yes` when scripting.

| Form | What it removes |
|---|---|
| `weft clean` | Runs older than `--keep-days` (default 30). |
| `weft clean <color>` | That one run. |
| `weft clean --project <id>` | That project's whole history. Runs outlive the project, so this is how a removed project's history is erased. |
| `weft clean --all` | Everything. |

It also reports and reclaims build output, which touches no journal rows:

- `--images` reclaims worker images nothing runs any more, scoped to the
  current project's images, and a global sweep of dangling untagged build
  leftovers rides along.
- `--images --all` takes every project's, the copies cached on the local
  cluster's node (the kind cluster), stale
  `weft-infra-*` tags, and old builder-base images. Whatever the dispatcher's
  referenced set covers survives.
- It prints the size of both compile caches on the machine: the node-test
  cache `weft test-node` builds into, and the cache every worker image build
  shares.
- `--build-cache` throws away the whole BuildKit cache, that compile cache
  included, so the next build seeds a fresh one from the builder base, which
  already holds every stock node compiled. It drops the node-test cache too,
  so the next `weft test-node` builds cold.

Both caches bound themselves. The node-test cache wipes itself past
`WEFT_TEST_CACHE_CAP_GB`, 6 by default. The worker-image build cache drops
compiled node packages and worker crates no build has linked for 30 days, and
`--all` also drops a whole cache no build has touched for 30 days (one is left
behind whenever the toolchain or the builder changes). `setup.sh` runs
`clean --images --all` after every daemon refresh.

## Versions and examples

| Command | What it does |
|---|---|
| `weft checkpoint [<label>] [--root]` | Record the files as a version under head. No run, no build. |
| `weft tree` | The version tree: every version, what changed in it against its parent, and its runs beneath it. |
| `weft branch <version\|label\|color> [--discard]` | Restore that version's files and move head there. |
| `weft diff <ref> <ref> [--full]` | Compare observed outputs for human or AI review, including frozen focus nodes. |
| `weft freeze <name> [<run>] [--expect <node>]...` | Save that run's starting parameters and observed outputs in `examples/<name>.json`. Default is head's run. |
| `weft examples` | List saved parameters and frozen examples. |
| `weft bake [project] [--referenced]` | Prepare trigger inputs without arming listeners. |
| `weft wake <color> <node>` | Resolve a pure time wait now. |
| `weft prune <version> [--yes]` | Delete a version, everything under it, and their runs. Asks first. |

Notes:

- `checkpoint` works as the first thing you do in a project: it tells the
  dispatcher the project exists, without building anything. It says `already at
  <id>` when nothing changed. `--root` (on `run` too) records a version with no
  parent and refuses if that same version already has a parent. On `run` it
  also means nothing is seeded.
- `tree` marks HEAD's version, the activated version, and HEAD's run, which is
  the one your next `--seed` inherits from. `--json` adds `disk_version`: the
  version your files on disk match right now, or null when they match none.
- `branch` refuses on unkept edits, naming the files. Restoring a version
  clears head's run, so the next `--seed` falls back to the newest finished or
  parked run on that version, or on the nearest ancestor version that has one.
  If you want a particular run as your seed, name its color instead of a
  version.
- A ref in `weft diff` is a color, its unambiguous prefix, or
  `example:<name>`. Differences are evidence and do not fail the command.
- `freeze --expect` marks nodes to focus on during review. Run and diff leave
  the accepted file intact; freeze again after accepting its replacement.
  Inspect examples with `weft examples`, rerun with `weft run <name>`, and
  compare with `weft diff`.
- A manual `--fire` requires a matching bake. Use `--referenced` on the bake
  when the run uses `--referenced`. Naming a project id runs the bake against
  that project's registered build instead of compiling. Activation also
  prepares and records a bake before arming.
- `wake`'s `<node>` is the waiting node's place, spelled the way the source
  reads: `triage.hold` for the `hold` inside the file the site `triage`
  includes. A file included twice holds one wait per site. The same spelling
  names a node to `weft events --node`, and an instance to the `weft infra`
  commands. A wait that expects
  a value is refused, naming its kind.
- `prune` refuses on head's version, the activated version, any version a
  frozen example was frozen from or any ancestor of one, and while a run in
  the subtree is running. It names each reason.

## Triggers

| Command | What it does |
|---|---|
| `weft activate [project]` | Register every trigger and mint its address. |
| `weft deactivate [project]` | Drop the registrations. |
| `weft cancel-activate` | Abort an activation in progress. |
| `weft cancel-build` | Abort a build in progress. |
| `weft cancel-running` | Cancel executions currently in flight. |
| `weft resync` | Re-register an active project's triggers against the current source. |

A parked or hibernated project refuses `resync`; `weft activate` brings it
back.

### Turning a project off

"Turn this project off" means different things depending on what is in flight,
so `deactivate` takes a `--mode` to say which you meant:

| If you want work in flight to be | Pass |
|---|---|
| thrown away | `--mode wipe`, the default |
| kept, and resumable when you turn the project back on | `--mode hibernate` |
| kept, and queued to run the moment you turn it back on | `--mode park` |

Every verb that turns triggers off takes this same flag: `deactivate`,
`resync`, and `infra stop` / `terminate` / `upgrade` on an active project. Ask
from a terminal and it offers you the three; from a script or an agent, with no
flag, it wipes. That default is deliberate. The two preserving modes carry
signals and suspended runs across a change to the program, and on a project you
are still building that is how a run ends up waiting for something nobody will
ever answer. Keeping the work in flight is the thing you say out loud.

Executions that are running right now are a separate question, answered by
`--running-policy`. With `--mode hibernate` or `--mode park` it defaults to
`wait`: running executions finish while new fires are held, and
`--drain-timeout <seconds>` caps that wait at 600 seconds by default, after
which the stragglers are cancelled. With `--mode wipe` it defaults to `cancel`
and `wait` is refused, because waiting before wiping is a contradiction.
`--running-policy cancel` kills the running executions at once. `--mode
hibernate` also takes `--grace <minutes>`, how long the hibernation window
lasts, 15 by default.

Under `--mode park` the running executions are left exactly as they are and the
command lands at once. Park's whole promise is that they stay alive with no
time limit, so a park that waited would end by cancelling the very runs it was
keeping. If you want them stopped under park, say so with `--running-policy
cancel`.

### Turning a project back on

Reactivating has to decide what happens to whatever the deactivation kept. On
a terminal `weft activate` prompts you. To answer up front, pass one of:

| `--reactivate-choice` | What happens |
|---|---|
| `execute_parked_keep_suspended` | queued work runs, paused work stays paused waiting for its answer |
| `keep_suspended_only` | paused work stays paused, queued work is dropped |
| `wipe_all` | both are dropped and the project starts clean |

## Infrastructure

| Command | What it does |
|---|---|
| `weft infra start` | Bring every unit up to spec, then wait until they are ready. |
| `weft infra stop` | Take units down per their stop behavior. |
| `weft infra upgrade` | Stop then start. |
| `weft infra terminate` | Delete every infra resource, disks included unless a node's spec preserves them. |
| `weft infra status` | Health and endpoint URL per infra instance. |
| `weft infra logs [node] [--tail N] [-f]` | What the infra containers wrote. |
| `weft infra cancel` | Abort an operation in progress. |
| `weft infra node-stop <node> [--force]` | Stop one instance. `--force` overrides a unit's stop behavior. |
| `weft infra node-terminate <node>` | Terminate one instance. |

Instance names use [the same source spelling as `wake`'s node
argument](#versions-and-examples): `db`, or `one.db` for the `db` inside the
file the site `one` includes. A file included twice
runs two instances, and both are listed. The same spelling is how you name an
instance to `node-stop` and `node-terminate`.

`weft infra logs` covers every infra node of the project, or one node's, each
line prefixed with its pod and container. It is the place a failure inside an
image is read from. On an active project, `infra stop` / `terminate` /
`upgrade` deactivate the triggers; `upgrade` **leaves them off**, so activate
again when you are ready.

See [Infrastructure nodes](../nodes/infrastructure.md) for what the verbs
actually do to a unit.

## The daemon

Aliased to `weft d`.

| Command | What it does |
|---|---|
| `weft daemon start` | Start the runtime. |
| `weft daemon stop` | Stop it. |
| `weft daemon status` | Is it up, and its public address if it has one. |
| `weft daemon restart` | The same reconcile as `daemon start` (an alias): apply what changed, roll what needs it. |
| `weft daemon logs [--tail N] [-f]` | Tail the runtime log. |
| `weft build-images [--push \| --push-suffix <s> \| --print]` | Make every shared image exist locally under its content-addressed ref. |
| `weft build-base` | Make just the worker builder-base image exist locally. |

`daemon start` flags:

- `--rebuild` re-makes the shared images under their existing tags and rolls
  everything onto the new bytes. Kind only; on a k8s cluster, publish with
  `weft build-images --push` instead.
- `--rebuild-cluster` deletes and recreates the kind cluster even when its
  shape did not change. A shape change, a port or a kind version, rebuilds on
  its own, saying so first. Every project's own database lives inside the node
  and is destroyed with it.
- `--public-url` / `--no-public-url`.

The shared images are the four system images, worker builder base and
full-library worker. `--push` publishes them to the registry (the release
workflow's verb); `--print` only prints the refs this tree resolves to,
without building images.

## Nodes and the catalog

| Command | What it does |
|---|---|
| `weft test-node [target]` | Run node self-tests: the basic and fake tiers, locally, no cluster. |
| `weft node-test-hash [target]` | The content hash of a package's test inputs. |
| `weft infra list-doors` | The pieces of this project's infrastructure a client on this machine can reach, and the address each answers on. |
| `weft catalog update` | Re-sync `nodes/base_catalog/` to the installed weft's standard library. |
| `weft describe-nodes [--stdlib] [--list \| --node <Type> [--compact]]` | Print the catalog. |

- `test-node --tier live` adds the real-credential tier, which **can spend
  money**, so it asks first (`--yes` to skip the prompt). See
  [Testing a node](../nodes/testing.md).
- `node-test-hash` answers whether anything a package's tests depend on has
  changed since they last passed.
- A door is part of what a node IS (its endpoint declares it, usually behind
  an input like `PostgresDatabase`'s `reachable`), so `list-doors` only
  reports: there is nothing here to open or close. The address is not in the
  source because the cluster allocates the port, which is why you ask.
- `catalog update` **wipes and recopies** `nodes/base_catalog/`, so copy
  anything you edited in there out first.
- `describe-nodes --list` is one line per node type (type, tags, one-line
  description): the cheap first look, and how you find a node.
  `--node <Type> --compact` is one node's wiring view (no labels, icons,
  connect recipes, or null knobs), the token-cheap thing a model reads before
  wiring it. Without `--compact` it is the full resolved metadata. The bare
  form prints the whole catalog as JSON for the editor's palette, and it is
  large.

## Compiler surfaces

| Command | What it does |
|---|---|
| `weft parse [--file]` | Parse leniently, print the project as JSON. |
| `weft validate [--file]` | Validate strictly, print diagnostics as JSON. |
| `weft parse-server` | A long-lived line-delimited JSON server, keeping the catalog warm. |

These three are how the editor gets live feedback. `parse` is lenient, so it
answers on incomplete source; `validate` is strict.

## Files and access

| Command | What it does |
|---|---|
| `weft connect` | The editor's Connect panel, in the terminal. |
| `weft files ls` | Stored runtime files. |
| `weft files inspect <key>` | One file's metadata. |
| `weft files download <key>` | Fetch it. |
| `weft files rm <key>` | Delete one file, or a whole space if the target ends in `/`. |
| `weft files usage` | How much is stored. |
| `weft token mint --name "..."` | Mint a browser-extension token. Prints the connect URL **once**. |
| `weft token ls` | List minted tokens. |
| `weft token revoke <id>` | Kill one. |
| `weft listener inspect` | What the listener tier is currently holding. |
| `weft rm [project]` | Remove a project. Asks first. |

`weft connect` picks a stored connection for an access node, connects a new
account (paste a key, browser sign-in, shared app), upgrades one, forgets one,
or disconnects the node. It sees access nodes inside `@include`d files too,
however deeply nested; a subgraph included in two places is one file, so one
pick connects every inclusion. It is interactive by default, and every choice
has a flag (`--help` lists them) so scripts never hang on a prompt. `--json`
works with the flag-driven actions (`--list`, `--grant`, `--disconnect`,
`--forget`); the walkthroughs print for a person.

`weft files rm` requires `--yes` non-interactively, since a prefix wipe can
take kept files with it.

`weft rm` wipes triggers, cancels runs, terminates infra, and reclaims stored
data; `--yes` answers the prompt (required when scripting). It also takes
`--journal`, `--local`, `--all`, `--force`.

`weft listener inspect` is what to reach for when a trigger stopped firing: if
what the listener holds disagrees with what the project registered, a cleanup
went wrong.

### Scoping a token

`weft token mint` also takes repeatable `--projects` and `--tags` flags that
narrow what a token can ever see, and `--display` / `--displays`, which GRANT
it what a node is showing (a token says nothing about displays and reads
none). `--display` takes a node the way you write it (`test.whatsapp`),
resolved against the project you are in:
[Scoping a token](browser-extension.md#scoping-a-token).

## The environment

| Variable | Default | Set it when |
|---|---|---|
| `WEFT_DISPATCHER_URL` | `http://localhost:9999` | the runtime is not on this machine, or you moved its port |
| `WEFT_DISPATCHER_PORT` | `9999` | something else already has 9999. The port is baked into the local cluster, so changing it makes the next `weft daemon start` rebuild the cluster; go and read [Port 9999 is taken](../start/troubleshooting.md#the-cli-cannot-reach-the-runtime-or-port-9999-is-taken) before you do |
| `CREDENTIAL_ENCRYPTION_KEY` | a development key, with a warning at first use | before you store a credential you care about. It seals them at rest, and changing it later strands every connection you had. |
| `WEFT_ACCESS_APPS_FILE` | | you are pointing weft at a [shared-credentials file](../connections/the-apps-file.md) |
| `WEFT_PUBLIC_TUNNEL_TOKEN` | | you want a [permanent public address](../connections/public-address.md) instead of the random one |
| `WEFT_PUBLIC_TUNNEL_HOSTNAME` | | the same, and both have to be set together |

A `.env` in the project is loaded automatically. A malformed one is fatal
rather than partially applied.
