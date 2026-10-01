# The CLI

Every command, grouped by what you are trying to do.

Three flags work everywhere. `--on <target>` acts on an install named in
`weft.toml` instead of the one on your machine
([deploying to your cloud](cloud.md)). If you want to reach a runtime by its address
rather than a target name, pass `--dispatcher <url>` or set
`WEFT_DISPATCHER_URL`; you cannot combine it with `--on`. If you are scripting weft, pass `--json` for machine-readable
output.

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
| `weft new <name> [--assistant <who>] [--remember] [--ci <cloud>]` | Scaffolds `weft.toml`, `src/main.weft`, `nodes/`, a git repo and a `.gitignore`. `--assistant` copies Tangle into this project only, and is repeatable. `--remember` also makes that choice the default for every later `weft new` run without `--assistant`; `--assistant none --remember` clears the default, and `--assistant none` alone installs nothing and leaves it as it was. `--ci gcp` also writes the deploy workflow. If any step fails, nothing is written |
| `weft catalog update` | Re-copies `nodes/base_catalog/` from the installed weft. Replaces the folder, so your edits in there go |
| `weft tangle update [--assistant <who>]` | Re-copies Tangle. Replaces every file the template owns; anything your assistant wrote beside them stays |

## Run something

| Command | What it does |
|---|---|
| `weft run` | Compiles, registers, records a version, starts a run, and follows it |
| `weft run <example>` | The same, with a saved example's starting values |
| `weft build` | Compiles and registers, starting nothing. Also how you put back code the dispatcher no longer holds |
| `weft bake` | Prepares every trigger's settings and starts no listeners. Takes the same `--running-policy` and `--drain-timeout` as `weft activate`, for the same reason: the setup runs on a worker, and one still up from an older build is replaced first |
| `weft stop <execution-id>` | Cancels a run |
| `weft wake <execution-id> <node>` | Resolves a pure time wait now. Refused for a wait that expects a value |
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
| `--group <group>=<json>` | Run one group, or one included file, on its own, with the values you hand it. A port a step inside needs that you do not hand is refused before the run starts |
| `--feed <start>` | Also run what feeds this start: for each input you did not hand a value, the node that feeds it (through any group or include doors on the way) and nothing above it. Names a `--from` or `--group` start. Repeatable |
| `--fire <trigger>=<json>` | Fire exactly one trigger with this event, using its bake. One per run |
| `--seed` | Reuse everything from head's run whose slice of the program did not change |
| `--seed-until <node>` / `--seed-before <node>` | Where reuse stops. Both need `--seed` |
| `--root` | Start a new version tree, parented on nothing |
| `--save <name>` | Write these settings to `examples/<name>.json` before running |
| `--clear <field>` | Clear a saved setting before applying flags: `from`, `emit`, `target`, `before`, `group`, `feed`, `fire`, `instance`, `long` |
| `--long` | Gives the run a worker of its own, for a run that may take longer than an hour on a cloud install (Cloud Run cuts an ordinary request at an hour). A trigger's `longRuns` field does the same for every run it starts |
| `--instance <id>` | Run for this instance of the program. Needed when the run reaches a step that exists once per instance (see [programs with instances](instances.md)) |

If you want to hand a run a file its real source cannot give you here (a
WhatsApp voice note needs a paired phone), write it in any `--from`,
`--emit`, `--group` or `--fire` value, or in a saved example, the way you
would write it in source. `"@asset(\"samples/hello.ogg\", Audio)"` uploads
the file into the project's storage and hands the port the file value its
type names (`Image`, `Video`, `Audio` or `Blob`); `"@file(\"prompts/x.md\")"`
hands it the file's text. Both mean exactly what they mean in a `.weft` (see
[files and reuse](../language/files-and-reuse.md)), and a path is relative to
the project root:

```bash
weft run --emit receive='{"messageType":"audio","file":"@asset(\"samples/hello.ogg\", Audio)","seconds":3}' --target transcribe
```

A missing file stops the run before it starts, and so does a file on a port
whose type does not take one. A saved example keeps the marker, so each run
reads the file again.

## Look at a run

| Command | What it does |
|---|---|
| `weft executions` | Past runs, newest first. `--limit` (50), `--offset`, `--project`, `--phase`, `--node`, `--since 2h`, `--status`, `--instance`, `--tag`. A run of a `Route` with `recorded: false` shows only if it failed |
| `weft events <execution-id>` | One run's events in order. `--node`, `--kind`, `--full` for whole values. If you want one time round a loop, `--iteration 3` keeps the fourth (they count from 0), and `3.0` the first time round a loop inside it |
| `weft logs [<execution-id>]` | What the nodes wrote, plus every failure. A run that wrote nothing lists what it skipped and why (under `skipped` with `--json`). `--limit` |
| `weft status` | The cwd project: registration, listener, every infra node the program declares (one never started says `not started`, a `@per_instance` one how many instances have a copy), each trigger (with the events an instance's trigger holds until a field is filled), recent runs, what drifted, and what you can do next |
| `weft ps` | Every project the dispatcher knows |

`--phase fire` hides the setup runs that an activate or a resync makes, so it
answers "has my trigger fired since I changed it".

## Versions and examples

| Command | What it does |
|---|---|
| `weft tree` | The version tree, with runs beneath each version and head marked |
| `weft checkpoint [<label>]` | Records the files as a version. No run, no build |
| `weft branch <ref> [--discard]` | Restores a version's files and moves head. Refuses if you have uncommitted changes, unless `--discard` |
| `weft diff <left> <right>` | Compares what two runs put on their wires. A ref is an execution id or `example:<name>`. `--full` for every wire |
| `weft freeze <name> [<execution-id>]` | Writes `examples/<name>.json` from a run: its parameters, the answers people gave, and the outputs you accept. `--expect <node>` emphasises a node in later diffs |
| `weft examples` | Saved examples, whether each is frozen, and its latest run |
| `weft prune <version>` | Deletes a version, everything under it, and their runs |

## Triggers

| Command | What it does |
|---|---|
| `weft activate` | Sets up every shared trigger and starts the listeners. Builds and registers first if it has to. A trigger that runs once per instance is left off and named in a warning, with the way to switch it on: `--instance <id>`, or an `ActivateInstanceTriggers` node in the program. It refuses while a connection the program needs is not picked on this install, naming each step. A worker still up from an older build is replaced on the way: `--running-policy cancel` (the default) cancels what it runs, `wait` lets that land first, up to `--drain-timeout`. If the triggers are already on and you changed the source, it tells you to run `weft resync` |
| `weft deactivate` | Stops the program's own triggers listening, and tells you how many instances still have theirs on. `--instance <id>` stops one instance's, `--all-instances` stops every instance's and leaves the program's own alone |
| `weft resync` | Deactivate and re-activate in one shot against your current program. With no flag it does every trigger that is on, the program's own and every instance's, and prints whose it did. It only touches triggers that are on; for ones that are off, use `weft activate` |
| `weft cancel-activate` | Cancels an activate in flight |
| `weft cancel-build` | Cancels a build in flight. The install stops the build, and the command that was building fails saying it was cancelled |
| `weft cancel-running` | Ends a drain early while a deactivate is waiting |

`deactivate`, `resync` and the infra verbs share four flags. `activate` and
`infra start` take the last two:

| Flag | What it does |
|---|---|
| `--mode wipe\|hibernate\|park` | What happens to work in flight. `wipe` drops it all, the other two keep it |
| `--grace <minutes>` | How long hibernate accepts late answers. 15 by default |
| `--running-policy cancel\|wait` | Cancel the running executions, or wait for them. `cancel` unless you say otherwise; `wipe` refuses `wait`. On `weft infra stop` and `terminate` it works on an inactive project too: no triggers to take down, but a run may still be using the infra |
| `--drain-timeout <seconds>` | Cap on a wait, so only beside `--running-policy wait`; passed with cancel it is refused. 60 by default. What is still running at the cap is cancelled |

Every trigger verb (`activate`, `deactivate`, `resync`, `bake`,
`cancel-activate`, `cancel-running`) also takes which triggers it acts on:

| Flag | What it does |
|---|---|
| `--trigger <name>` | Only this trigger. Repeat it for several. Without it, every shared trigger (a plain `resync` does every instance's too) |
| `--instance <id>` | That instance's copies of the triggers that exist once per instance (see [programs with instances](instances.md)) |
| `--all-instances` | On `weft deactivate` only: every instance whose triggers are on, each with the same `--mode`. The program's own triggers stay as they are |

## Infrastructure

| Command | What it does |
|---|---|
| `weft infra start` | Brings up whatever is down |
| `weft infra stop` | Scales to nothing, keeping the disk. The project's running executions are cancelled first unless you pass `--running-policy wait`, because they may be using this infra |
| `weft infra terminate` | Deletes it, disk included, unless the node asked for the disk to be kept. Asks first, and off a terminal or with `--json` it needs `--yes`. The same running-policy rule as stop |
| `weft infra upgrade` | Rebuilds against your current source. Leaves the project deactivated. When a build changes an infra node's image, every command that builds says so: a copy started from then on gets the new image, and one already running keeps its own until this |
| `weft infra status` | Where each copy that exists stands, with its address. A start or stop on its way shows as `provisioning` or `stopping` at once |
| `weft infra list-doors` | Which pieces you can reach from this machine, and at what address |
| `weft infra logs [<node>]` | What the containers printed. `--tail` (200), `-f` |
| `weft infra cancel` | Stops waiting on work in flight. Halts between steps rather than undoing |
| `weft infra node-stop <node> [--force]` | One piece. `--force` takes down units that would normally stay up. Cancels every running execution of the project unless you pass `--running-policy wait`, since nothing records which of them use this piece |
| `weft infra node-terminate <node>` | One piece, deleted. Asks first like `terminate`, and needs `--yes` off a terminal or with `--json`. The same running-policy rule |
| `weft infra show <node>` | The node's card: every item (label, kind, value) and every button, with the action to hand `press` and what it asks before pressing. A `secret` item's value is never printed, `--json` included |
| `weft infra press <node> <action>` | Presses the card's button with that action, without asking: naming the action is the choice. Prints what the node answered, and the button's warning if it has one |
| `weft infra env <node> --into <file> --set NAME=Label ...` | Writes what the node's card shows into an env file, every name in one go: `--set DATABASE_USER=User --set DATABASE_PASSWORD=Password` takes each item by its label as `show` prints it, and `--as <NAME>` is the short form for the card's only secret. A secret's value is never printed; the other values are, in the line confirming what it wrote. A new file is yours only; an existing one gets those lines set and keeps the rest. If a secret was handed over already, it refuses, quoting what the card says there: press the card's button with `weft infra press`, then run it again |

If you want to act on one instance's copies of the nodes marked
`@per_instance` instead of the shared infra, `start`, `stop`, `terminate`,
`upgrade`, `node-stop`, `node-terminate`, `cancel`, `show`, `press` and `env`
all take `--instance <id>`.

## Connections and tokens

| Command | What it does |
|---|---|
| `weft connect` | Walks you through connecting an account |
| `weft connect --list` | Prints what is stored, changing nothing. Works outside a project |
| `weft connect --node <id>` | Go straight to one access node |
| `weft connect --grant <id>` | Pick a stored connection for the node |
| `weft connect --disconnect` | Clear the node's pick, leaving the connection stored |
| `weft connect --forget <id>` | Delete a stored connection for good |
| `weft connect --instance <id>` | Act inside that instance of the program on an access node whose connection is `@instance_filled`: `--list`, `--grant`, `--disconnect` and connecting a new account all work on its connections and its value, exactly as its settings page would |
| `weft instance-values --instance <id>` | Lists what the program asks that instance to fill and what it was given; `--set node.field=value` and `--clear node.field` (both repeatable) change it in one go, re-arming its live triggers that read a changed value |
| `weft connect --door shared\|own` | Which door to connect through, without being asked |
| `weft connect --set NAME=VALUE` | Fill a credential field. The value is visible to other processes, so prefer the prompt or `--set-env NAME=ENV_VAR` |
| `weft token mint` | Mint a signal token. `--name`, `--projects`, `--tags`, `--display <node>`, `--displays`, and `--expires <duration>` (`30m`, `12h`, `7d`; without it the token works until revoked). Printed once and never again. `--operator` mints an operator key instead, which administers the whole install and takes no scope |
| `weft token mint --instance <id> --expires <duration>` | An instance token for the project you are in: it acts inside that one instance and nothing else. `--expires` (`30m`, `12h`, `7d`) is required |
| `weft token ls` | The tokens you have, by their recognizer prefix |
| `weft token revoke <id>` | Revoke one |
| `weft options <step> <field> [--search <text>]` | Lists the choices a field with a searchable list offers (a model, a spreadsheet, a channel), the same list the editor shows when you search that field, read through the connection picked for it. One `id  label` line each; the id is what you write in the field |
| `weft connect-lib` | Copies weft's connect library into the project's frontend (`front/src/lib/weft-connect`, or the folder `--into` names), so its pages show the editor's connection pickers and list fields, and each instance gets its own settings page. The folder is the library's: each run replaces it, so run it again after updating weft and keep your own code outside it |

## Cloud installs

| Command | What it does |
|---|---|
| `weft target add <name> <url>` | Names an install in `weft.toml`. Replaces the address when the name exists |
| `weft target list` | The targets, and whether you are logged in to each |
| `weft target remove <name>` | Takes one out of `weft.toml` |
| `weft login <name>` | Stores your operator key for it, after checking the install accepts it. `--key-stdin` for a script |
| `weft logout <name>` | Forgets your key for it |
| `weft running-source <folder>` | Writes the program an install is running into a new folder, without touching your own files. Add `--on <target>` to get what a cloud install runs |
| `weft ci add --cloud gcp` | Writes `.github/workflows/deploy.yml`. Refuses to replace one somebody edited |
| `weft target export <name> [--github]` | Mints a CI operator key and a frontend token on the install. With `--github` it sets them, and the workflow's other settings, on the repository through `gh`; without it, it prints the variables and writes the secrets to a file only you can read, under the install's `exports/` folder |
| `weft domain add <name>` | Makes the install answer at a domain you own: prints the DNS record to set and waits until it points here. `--for api` serves the project's routes there instead, `--for frontend --to <address>` its frontend. `--no-wait` returns after printing the record |
| `weft domain list` | Every domain, with its DNS record |
| `weft domain rm <name>` | Stops answering at it |
| `weft workers` | The project's worker settings: copies kept warm, the most copies, runs per copy, CPU, memory. It shows which the project sets and which come from the install |
| `weft workers set --min-instances 1 ...` | Changes settings for this project; the change reaches its running workers at once |
| `weft workers reset [<setting>...]` | Puts settings back to the install's values |

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
| `weft daemon stop` | Stops the runtime and the workers it started. The database, your infrastructure and your data stay |
| `weft daemon status` | Whether it is reachable, and the public address if one is open |
| `weft daemon logs` | The runtime's log. `--tail` (100), `-f` |
| `weft daemon remove` | Takes a named install (`WEFT_INSTALL`) off this machine, everything of it included. The default install is removed by `./setup.sh --uninstall` |

Flags on `weft daemon start`: `--rebuild` forces every shared image to rebuild.
`--public-url` and `--no-public-url` open and close
[the public address](../build/public-address.md).

## Testing nodes

| Command | What it does |
|---|---|
| `weft test-node [<node or package>]` | Runs node tests. Without `--tier`, the basic and fake tiers, which need no install |
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
| `weft validate` | The same input, strict, including the runtime rules like missing credentials. Also warns about every folder under `nodes/` it had to leave out, and why; that alone never fails it |
| `weft parse-server` | A long-lived parse server, one JSON request per line, catalog held warm |

## Cleaning up

| Command | What it does |
|---|---|
| `weft clean` | Deletes runs older than `--keep-days`, 30 by default |
| `weft clean <execution-id>` | Deletes one run |
| `weft clean --all` | Deletes every run |
| `weft clean --project <id>` | Deletes that project's whole history, which outlives the project |
| `weft clean --instance <id>`, `--tag <tag>`, `--status <how>`, `--node <node>` | Deletes only the runs these name, across the projects you name (or every one). Naming an instance or a tag takes all their runs; add `--keep-days` to spare recent ones. A run still going is left to finish unless you pass `--cancel-running` |
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
| `weft infra terminate`, `weft infra node-terminate` | Confirm what will be deleted | `--yes` |
| `weft connect` | Which node, which door, which app, which permissions, the credential fields | `--node`, `--door`, `--shared-app`, `--permissions`, `--set` |
| `weft activate` | What to do with preserved state | `--reactivate-choice` |
| `weft deactivate`, `weft resync`, the infra verbs | Which preservation mode, and the hibernate grace | `--mode`, `--grace` |
| `weft test-node --tier live` | Confirm that this spends money | `--yes` |

One asymmetry worth knowing: `deactivate` and `resync` are the only ones that
pick a default off a terminal rather than refusing. They pick `wipe`, with
running executions cancelled. The infra verbs ask only when a trigger reading
that infra is on (the program's or an instance's), and off a terminal, or with
`--json`, they stop and name the flags to pass, for example `--mode park
--running-policy wait`. If you pass `--mode` or `--grace` up front, it is used
when needed and ignored otherwise.

## Everything destructive, and what it takes

| Command | What goes |
|---|---|
| `weft rm` | The project's registration, its signals, its running executions, its infra **including disks**, its stored data |
| `weft rm --journal` | Also every run and log row |
| `weft rm --local` | Also `.weft/target/` and its slice of the node-test cache |
| `weft prune <version>` | That version, every version under it, and every run beneath them |
| `weft clean <execution-id>` | One run, for good |
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
| `weft connect --forget <id>` | A stored connection. Other projects using it then fail at run time asking for a reconnect |

Worker images are shared between projects, so no level of `weft rm` reclaims
them. `weft clean --images` is what does.
