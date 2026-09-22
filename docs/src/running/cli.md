# The CLI

Every command, grouped by what you are trying to do.

Two flags work everywhere. `--dispatcher <url>` points at a different runtime,
and also reads from `WEFT_DISPATCHER_URL`. `--json` prints machine-readable
output, which is how the VS Code extension drives all of this.

`--json` changes behaviour, not just formatting, in three places:

- `weft run --json` also detaches, so it starts the run and returns.
- `weft activate --json` refuses rather than asking, when the project has
  preserved state waiting.
- `weft connect --json` only works with `--list`, `--grant`, `--disconnect` and
  `--forget`. The walkthrough prints for a person.

Tab completes commands and flags in a new terminal after `./setup.sh`. For
zsh and bash, setup adds a short block to `~/.zshrc` or `~/.bashrc`, between
two `# weft tab completion` marker lines, and `./setup.sh --uninstall` takes it
out again. For fish it writes `~/.config/fish/completions/weft.fish`. Every Tab
asks the installed `weft` for the candidates, so new commands and flags show
up as soon as you update it.

## Start a project

| Command | What it does |
|---|---|
| `weft new <name> [--assistant <who>]` | Scaffolds `weft.toml`, `src/main.weft`, `nodes/`, a git repo and a `.gitignore`. `--assistant` copies Tangle in, is repeatable, and is remembered for next time |
| `weft catalog update` | Re-copies `nodes/base_catalog/` from the installed weft. Replaces the folder, so your edits in there go |
| `weft tangle update [--assistant <who>]` | Re-copies Tangle. Replaces every file the template owns; anything your assistant wrote beside them stays |

## Run something

| Command | What it does |
|---|---|
| `weft run` | Compiles, registers, records a version, starts a run, and follows it |
| `weft run <example>` | The same, with a saved example's starting values |
| `weft build` | Compiles and registers, starting nothing. Also how you put back code the dispatcher no longer holds |
| `weft bake` | Prepares every trigger's settings and starts no listeners. Takes the same `--running-policy` and `--drain-timeout` as `weft activate`, for the same reason: the setup runs on a worker, and one still up from an older build is replaced first |
| `weft stop <color>` | Cancels a run |
| `weft wake <color> <node>` | Resolves a pure time wait now. Refused for a wait that expects a value |
| `weft follow <project>` | Streams a project's events live |

Flags on `weft run`:

| Flag | What it does |
|---|---|
| `--detach` | Start and return rather than following |
| `--referenced` | Compile only the node types the graph uses, rather than the whole catalog |
| `--target <node>` | Run this node and what it needs. Repeatable |
| `--before <node>` | Run what it needs, and not it. Repeatable |
| `--from <node>=<json>` | Start here, with these values as backups. A real producer still wins |
| `--emit <node>=<json>` | Stand in for a node: say what it would have emitted, and do not run it |
| `--group <group>=<json>` | Run one group, or one included file, on its own |
| `--fire <trigger>=<json>` | Fire exactly one trigger with this event, using its bake. One per run |
| `--seed` | Reuse everything from head's run whose slice of the program did not change |
| `--seed-until <node>` / `--seed-before <node>` | Where reuse stops. Both need `--seed` |
| `--root` | Start a new version tree, parented on nothing |
| `--save <name>` | Write these settings to `examples/<name>.json` before running |
| `--clear <field>` | Clear a saved setting before applying flags: `from`, `emit`, `target`, `before`, `group`, `fire` |

## Look at a run

| Command | What it does |
|---|---|
| `weft executions` | Past runs, newest first. `--limit` (50), `--offset`, `--project`, `--phase`, `--node`, `--since 2h`, `--status` |
| `weft events <color>` | One run's events in order. `--node`, `--kind`, `--full` for whole values |
| `weft logs [<color>]` | What the nodes wrote, plus every failure. A run that wrote nothing lists what it skipped and why (under `skipped` with `--json`). `--limit` |
| `weft status` | The cwd project: registration, listener, infra per node, recent runs, what drifted, and what you can do next |
| `weft ps` | Every project the dispatcher knows |
| `weft listener inspect` | Every listener pod and the signals on it. An operator's view |

`--phase fire` hides the setup runs that an activate or a resync makes, so it
answers "has my trigger fired since I changed it".

## Versions and examples

| Command | What it does |
|---|---|
| `weft tree` | The version tree, with runs beneath each version and head marked |
| `weft checkpoint [<label>]` | Records the files as a version. No run, no build |
| `weft branch <ref> [--discard]` | Restores a version's files and moves head. Refuses if you have uncommitted changes, unless `--discard` |
| `weft diff <left> <right>` | Compares what two runs put on their wires. A ref is a color or `example:<name>`. `--full` for every wire |
| `weft freeze <name> [<color>]` | Writes `examples/<name>.json` from a run: its parameters, the answers people gave, and the outputs you accept. `--expect <node>` emphasises a node in later diffs |
| `weft examples` | Saved examples, whether each is frozen, and its latest run |
| `weft prune <version>` | Deletes a version, everything under it, and their runs |

## Triggers

| Command | What it does |
|---|---|
| `weft activate` | Sets up every trigger and starts the listeners. Builds and registers first if it has to. A worker still up from an older build is replaced on the way: `--running-policy cancel` (the default) cancels what it runs, `wait` lets that land first, up to `--drain-timeout` |
| `weft deactivate` | Stops the listening |
| `weft resync` | Deactivate and re-activate in one shot against your current program. Only on a project that is already active; an inactive one is `weft activate` |
| `weft cancel-activate` | Cancels an activate in flight |
| `weft cancel-build` | Cancels a build in flight. Only for a build running in the cluster; Ctrl+C handles a local one |
| `weft cancel-running` | Ends a drain early while a deactivate is waiting |

`deactivate`, `resync` and the infra verbs share four flags. `activate` and
`infra start` take the last two:

| Flag | What it does |
|---|---|
| `--mode wipe\|hibernate\|park` | What happens to work in flight. `wipe` drops it all, the other two keep it |
| `--grace <minutes>` | How long hibernate accepts late answers. 15 by default |
| `--running-policy cancel\|wait` | Cancel the running executions, or wait for them. `cancel` unless you say otherwise; `wipe` refuses `wait`. On `weft infra stop` and `terminate` it works on an inactive project too: no triggers to take down, but a run may still be using the infra |
| `--drain-timeout <seconds>` | Cap on a wait, so only beside `--running-policy wait`; passed with cancel it is refused. 60 by default. What is still running at the cap is cancelled |

## Infrastructure

| Command | What it does |
|---|---|
| `weft infra start` | Brings up whatever is down |
| `weft infra stop` | Scales to nothing, keeping the disk. The project's running executions are cancelled first unless you pass `--running-policy wait`, because they may be using this infra |
| `weft infra terminate` | Deletes it, disk included, unless the node asked for the disk to be kept. The same running-policy rule as stop |
| `weft infra upgrade` | Rebuilds against your current source. Leaves the project deactivated |
| `weft infra status` | Where each piece stands, with its address |
| `weft infra list-doors` | Which pieces you can reach from this machine, and at what address |
| `weft infra logs [<node>]` | What the containers printed. `--tail` (200), `-f` |
| `weft infra cancel` | Stops waiting on work in flight. Halts between steps rather than undoing |
| `weft infra node-stop <node> [--force]` | One piece. `--force` takes down units that would normally stay up. Cancels every running execution of the project unless you pass `--running-policy wait`, since nothing records which of them use this piece |
| `weft infra node-terminate <node>` | One piece, deleted. The same running-policy rule |

## Connections and tokens

| Command | What it does |
|---|---|
| `weft connect` | Walks you through connecting an account |
| `weft connect --list` | Prints what is stored, changing nothing. Works outside a project |
| `weft connect --node <id>` | Go straight to one access node |
| `weft connect --grant <id>` | Pick a stored connection for the node |
| `weft connect --disconnect` | Clear the node's pick, leaving the connection stored |
| `weft connect --forget <id>` | Delete a stored connection for good |
| `weft connect --door shared\|own` | Which door to connect through, without being asked |
| `weft connect --set NAME=VALUE` | Fill a credential field. The value is visible to other processes, so prefer the prompt or `--set-env NAME=ENV_VAR` |
| `weft token mint` | Mint a signal token. `--name`, `--projects`, `--tags`, `--display <node>`, `--displays`. Printed once and never again |
| `weft token ls` | The tokens you have, by their recognizer prefix |
| `weft token revoke <id>` | Revoke one |

## Stored files

| Command | What it does |
|---|---|
| `weft files ls [<prefix>]` | Everything stored, grouped by space |
| `weft files inspect <key>` | One file's full metadata |
| `weft files download <key> [-o <path>]` | Streams it out. A failed download never leaves a half file behind |
| `weft files rm <key>` | Removes one file |
| `weft files rm <space>/` | Removes a whole space, kept files included |
| `weft files usage` | How much is stored, and how many files |

## The daemon

| Command | What it does |
|---|---|
| `weft daemon start` | Brings the runtime to the state the code describes. Idempotent, so it is both first boot and refresh. `restart` is the same thing |
| `weft daemon stop` | Scales the dispatcher to nothing. The cluster and your data stay |
| `weft daemon status` | Whether it is reachable, and the public address if one is open |
| `weft daemon logs` | The dispatcher's log. `--tail` (100), `-f` |

Flags on `weft daemon start`: `--rebuild` forces every shared image to rebuild,
`--rebuild-cluster` rebuilds the kind node, `--public-url` and `--no-public-url`
open and close the public trigger surface, and `--clear-access-apps` empties the
shared credentials when your checkout has no `access-apps.json`.

## Testing nodes

| Command | What it does |
|---|---|
| `weft test-node [<node or package>]` | Runs node tests. Without `--tier`, the basic and fake tiers, which need no cluster |
| `weft test-node --tier live` | Calls the real provider with a real credential, and can spend money |
| `weft node-test-hash [<target>]` | The content hash naming a package's test inputs |

Other flags: `--test <name>` for one test, `--key <service>` and
`--connection <service>=<id>` for live credentials, `--parallel [N]`, and
`--yes` to skip the live confirmation.

## What the editor drives

| Command | What it does |
|---|---|
| `weft describe-nodes --list` | One line per node type: its name, its tags, the first sentence of what it does. The cheap first look |
| `weft describe-nodes --node <Type> --compact` | One node's wiring view: ports, types and rules, with the presentation stripped out. A long pick list shows its first eight entries and how many more there are; the plain `--node <Type>` has them all |
| `weft parse` | Parses source from stdin and prints the project, the catalog and the diagnostics as JSON. Lenient: unknown types become placeholders |
| `weft validate` | The same input, strict, including the runtime rules like missing credentials |
| `weft parse-server` | A long-lived parse server, one JSON request per line, catalog held warm |

## Cleaning up

| Command | What it does |
|---|---|
| `weft clean` | Deletes runs older than `--keep-days`, 30 by default |
| `weft clean <color>` | Deletes one run |
| `weft clean --all` | Deletes every run |
| `weft clean --project <id>` | Deletes that project's whole history, which outlives the project |
| `weft clean --images` | Reclaims worker images nothing references. `--all` spans every project |
| `weft clean --build-cache` | Drops the docker build cache and the node-test cache. The next build compiles cold |
| `weft rm` | Unregisters a project: signals wiped, runs cancelled, infra terminated, stored data reclaimed |
| `weft rm --journal` | Also drops its runs and logs |
| `weft rm --local` | Also wipes its build artifacts here |
| `weft rm --all` | Both of those |

## Everything that asks you a question

Every command runs without a terminal. When the deciding flag is missing, weft
asks a person if there is one, and otherwise fails naming the flag rather than
hanging on an answer that will never come.

| Command | What it asks | The flag that answers it |
|---|---|---|
| `weft prune` | Confirm the count of versions and runs | `--yes` |
| `weft rm` | Confirm what will be removed | `--yes` |
| `weft clean` | Confirm the runs to delete | `--yes` |
| `weft files rm` | Confirm, naming how many files are kept ones | `--yes` |
| `weft connect --forget` | Confirm | `--yes` |
| `weft connect` | Which node, which door, which app, which permissions, the credential fields | `--node`, `--door`, `--shared-app`, `--permissions`, `--set` |
| `weft activate` | What to do with preserved state | `--reactivate-choice` |
| `weft deactivate`, `weft resync`, the infra verbs | Which preservation mode, and the hibernate grace | `--mode`, `--grace` |
| `weft test-node --tier live` | Confirm that this spends money | `--yes` |

One asymmetry worth knowing: `deactivate`, `resync` and the infra verbs are the
only ones that pick a default off a terminal rather than refusing. They pick
`wipe`, with running executions cancelled.

## Everything destructive, and what it takes

| Command | What goes |
|---|---|
| `weft rm` | The project's registration, its signals, its running executions, its infra **including disks**, its stored data |
| `weft rm --journal` | Also every run and log row |
| `weft rm --local` | Also `.weft/target/` and its slice of the node-test cache |
| `weft prune <version>` | That version, every version under it, and every run beneath them |
| `weft clean <color>` | One run, for good |
| `weft clean --all` | Every run |
| `weft clean --project <id>` | That project's entire history |
| `weft clean --build-cache` | The docker build cache and the node-test cache |
| `weft files rm <space>/` | A whole space, **including files you marked to keep** |
| `weft deactivate --mode wipe` | Every waiting signal, and suspended runs are cancelled |
| `weft activate --reactivate-choice wipe_all` | Every parked signal and every pending suspension |
| `weft branch --discard` | Your uncommitted changes |
| `weft catalog update` | Anything you edited under `nodes/base_catalog/` |
| `weft tangle update` | Your edits to the Tangle files the template owns |
| `weft freeze <name>` | The previous `examples/<name>.json`, replaced whole |
| `weft infra terminate` | Every infra resource, **disks included**, unless the node preserves them |
| `weft daemon start --rebuild-cluster` | The kind node, and with it **every project's own database**. The runtime's own database lives on the host and survives |
| `weft connect --forget <id>` | A stored connection. Other projects using it then fail at run time asking for a reconnect |

Worker images are shared between projects, so no level of `weft rm` reclaims
them. `weft clean --images` is what does.
