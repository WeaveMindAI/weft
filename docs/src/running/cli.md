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
| `--clear <field>` | Clear a saved setting before applying flags: `from`, `emit`, `target`, `before`, `group`, `feed`, `fire`, `instance`, `keeping` (what `--durable` or `--fast` saved), `keep_for`, `hold_secs` |
| `--durable` | If the run has to carry on after its worker dies: another worker picks it up where it stopped, and a step caught halfway is failed rather than run twice. For what that costs in speed,, go and read [when a run waits for its writes](the-journal.md#when-a-run-waits-for-its-writes) |
| `--fast` | If the run fires a durable trigger and you want it fast anyway: it lives in its worker's memory and ends if that worker dies |
| `--keep-for <duration>` | If you want this run kept longer or shorter after it ends than its trigger or project says: `30m`, `12h`, `7d`, or `forever`. For the project and trigger settings, go and read [how long a run is kept](the-journal.md#how-long-a-run-is-kept) |
| `--hold-secs <seconds>` | If a wait in this run should hold longer or shorter than its trigger's `holdSecs` while the run cannot pause (a bus between its nodes is open): `0` fails such a wait at once, at most 30 days. For when a run cannot pause, go and read [triggers and routes](../language/triggers-and-routes.md#when-a-run-cannot-pause) |
| `--instance <id>` | Run for this instance of the program. Needed when the run reaches a step that exists once per instance (see [programs with instances](instances.md)) |

A run that fires a trigger (`--fire`) follows the trigger's `durable`,
`keepRunsFor` and `holdSecs`, and `--durable`, `--fast`, `--keep-for` and
`--hold-secs` override them for that run; a run
that fires nothing is fast unless a flag says otherwise. A run you start by
hand is always recorded, even when its trigger has `recorded` off, so you can
always find it in `weft executions`. For what each of these does when you set it on a
trigger, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept).

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
| `weft executions` | Past runs, newest first. `--limit` (50), `--offset`, `--project`, `--phase`, `--node` (the node that started the run), `--since 2h`, `--status`, `--instance`, `--tag`. `--through <node>` keeps the runs in which that node fired, wherever it sits. `--search <words>` keeps the finished runs that carried every one of those words somewhere in what they recorded (the trigger's input, what a node sent on, an error, a log line): an email, an order id, a phrase in quotes. `--through` and `--search` only find recorded runs that have ended: a run shows up in them a few seconds after it ends, or longer while the install is busy. On a cloud install whose dispatcher had scaled to zero, the runs that ended meanwhile are found a few seconds after it wakes, so the first search after a quiet stretch can miss them. In the plain list, a run that a trigger with `recorded: false` started shows only if weft wrote it down after all (for when, go and read [how a run is kept](../language/triggers-and-routes.md#how-a-run-is-kept)), and `--through` and `--search` never find it. A run you start by hand is always recorded, so it always shows |
| `weft events <execution-id>` | One run's events in order. `--node`, `--kind`, `--full` for whole values. If you want one time round a loop, `--iteration 3` keeps the fourth (they count from 0), and `3.0` the first time round a loop inside it |
| `weft logs [<execution-id>]` | What the nodes wrote, plus every failure. A run that wrote nothing lists what it skipped and why (under `skipped` with `--json`). `--limit` |
| `weft status` | The cwd project: registration, listener, every infra node the program declares (one never started says `not started`, a `@per_instance` one how many instances have a copy), each trigger and the version of the source it fires (with the events an instance's trigger holds until a field is filled), recent runs, how many runs each trigger started in the last minute or two and how many of those failed (for a trigger with `recorded: false`, often the only sign its runs happen), what drifted, and what you can do next. It also shows the project's own address (its routes at the root), or why that address is unavailable. While the install builds, it names each image building and where its log is; while infra starts, how long it has been going and what it waits on |
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
| `weft activate` | Sets up every shared trigger and starts the listeners, and prints the project's own address, where its routes answer at the root (on your machine a local port, on a cloud install its Cloud Run address). On your machine, if you want that address on a port of your choosing, `--port <n>` opens it there and the project keeps it for later activates. If another program or another of your projects holds that port, the command fails and nothing is activated. Without `--port`, the project keeps the port it has, or takes a free one when it has none or another program took it. Builds and registers first if it has to. A trigger that runs once per instance is left off and named in a warning, with the way to switch it on: `--instance <id>`, or an `ActivateInstanceTriggers` node in the program. It refuses while a connection the program needs is not picked on this install, naming each step, and while infra a trigger reads is not running (it never starts infra: `weft infra start` does). A worker still up from an older build is replaced on the way: `--running-policy cancel` (the default) cancels what it runs, `wait` lets that land first, up to `--drain-timeout`. If some triggers are on and others off (an infra stop took down the ones reading it), it turns on the ones that are off and leaves the rest as they are; if every trigger is on and you changed the source, it tells you to run `weft resync` |
| `weft deactivate` | Stops the program's own triggers listening, and tells you how many instances still have theirs on. `--instance <id>` stops one instance's, `--all-instances` stops every instance's and leaves the program's own alone |
| `weft resync` | Deactivate and re-activate in one shot against your current program. With no flag it does every trigger that is on, the program's own and every instance's, and prints whose it did. It only touches triggers that are on; for ones that are off, use `weft activate` |
| `weft cancel-activate` | Cancels an activate in flight |
| `weft cancel-build` | Cancels a build in flight. The install stops the build, and the command that was building fails saying it was cancelled |
| `weft cancel-running` | Ends a drain early while a deactivate is waiting |

`deactivate`, `resync` and the infra verbs share four flags. `activate` and
`infra start` take the last two:

| Flag | What it does |
|---|---|
| `--mode wipe\|hibernate\|park` | What happens to work that arrives while the triggers are down. `park` keeps all of it and runs it once they are back, `hibernate` does that for a grace window, `wipe` drops it ([the full rules](lifecycles.md#choosing-what-happens-to-work-in-flight)) |
| `--grace <minutes>` | How long hibernate keeps taking work. 15 by default |
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
| `weft infra start` | Brings up whatever is down, and turns back on the triggers that came down with it (never one you switched off yourself) |
| `weft infra stop` | Scales to nothing, keeping the disk. The project's running executions are cancelled first unless you pass `--running-policy wait`, because they may be using this infra |
| `weft infra terminate` | Deletes it, disk included, unless the node asked for the disk to be kept. Asks first, and off a terminal or with `--json` it needs `--yes`. The same running-policy rule as stop |
| `weft infra upgrade` | Rebuilds against your current source; the triggers it took down come back once the new copies are up. When a build changes an infra node's image, every command that builds says so: a copy started from then on gets the new image, and one already running keeps its own until this |
| `weft infra status` | Where each copy that exists stands, with its address. A start or stop on its way shows as `provisioning` or `stopping` at once |
| `weft infra list-doors` | Which pieces you can reach from this machine, and at what address |
| `weft infra logs [<node>]` | What the containers printed. `--tail` (200), `-f` |
| `weft infra cancel` | Stops waiting on work in flight. Halts between steps rather than undoing |
| `weft infra node-stop <node> [--force]` | One piece. `--force` takes down units that would normally stay up. Cancels every running execution of the project unless you pass `--running-policy wait`, since nothing records which of them use this piece |
| `weft infra node-terminate <node>` | One piece, deleted. Asks first like `terminate`, and needs `--yes` off a terminal or with `--json`. The same running-policy rule |
| `weft infra show <node>` | The node's card: every item (label, kind, value) and every button, with the action to hand `press` and what it asks before pressing. A `secret` item's value is never printed, `--json` included |
| `weft infra press <node> <action>` | Presses the card's button with that action, without asking: naming the action is the choice. Prints what the node answered, and the button's warning if it has one |
| `weft infra rebake <node>` | If an infra node's baked outputs no longer match the real thing (say its password was reset outside weft), this applies the node again, leaving alone anything already running the way the node asks, and saves its [baked outputs](../nodes/infrastructure.md#outputs-saved-with-the-infrastructure-baking) anew. `--instance <id>` rebakes one instance's copy of a `@per_instance` node. If a `weft infra start`, `upgrade` or another rebake is already working on the same copies (the shared ones, or that instance's), it waits for that to finish first |
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
| `weft target export <name> [--github] [--frontend <name>]` | Mints a CI operator key on the install, and gives the frontend the install hosts for this repository a new token along with its service's name (the old token works until the workflow's next run deploys the new one and retires it). With `--github` it sets them, and the workflow's other settings, on the repository through `gh`, and lists what it set; without it, it prints the variables and writes the secrets to a file only you can read, under the install's `exports/` folder |
| `weft target show <name>` | Where a target's install lives: its address, and on a cloud the project and region it runs in, and the weft it runs |
| `weft frontend add <name> [--repo owner/name]` | Makes one of the project's frontends. With `--repo` (read through `gh`), a cloud install makes it a Cloud Run service and lets that repository deploy to it, and its token comes from the deploy workflow (`weft target export`); without, it runs wherever you run it, and its token is written to a file only you can read |
| `weft frontend ls` | The project's frontends, and where each runs |
| `weft frontend rm <name> [--force]` | Removes one: its tokens stop working, and a service the install made for it is deleted. `--force` forgets it even when the service cannot be removed, naming what stays |
| `weft frontend token <name> [--done <id>]` | A new token, beside the one it has, with its id; `--done <id>` once that token is in place retires every other token of the frontend |
| `weft domain add <name>` | Makes a cloud install answer at a domain you own: prints the DNS record to set and waits until it points there. `--for api` serves the project's routes there instead, `--for frontend --to <address>` its frontend. `--no-wait` returns after printing the record. The first domain makes a load balancer that is billed while any domain exists, so it needs `--accept-cost` ([your own domain](cloud.md#your-own-domain)) |
| `weft domain list` | Every domain, with its DNS record |
| `weft domain rm <name>` | Stops answering at it; the last one takes the load balancer down |
| `weft workers` | The project's worker settings: copies kept warm, the most copies, how many calls Cloud Run sends one copy at once (`concurrency`), CPU, memory, how long a copy holds on to things its runs share, such as a database pool, after nothing uses them (`shared_idle_seconds`), how many calls and events one copy takes at once (`max_runs_at_once`) and how long a call waits when it is full (`max_queue_wait_seconds`; for both, go and read [when a worker is full](architecture.md#when-a-worker-is-full)). It shows which the project sets and which come from the install. CPU and memory only apply on a cloud install, where an unset CPU means one CPU; on your machine no worker is capped |
| `weft workers set --min-instances 1 ...` | Changes settings for this project. Calls after the change go to workers that have them. On a cloud install, a worker started before finishes what it runs and then stops. On your machine, the worker at the project's own address is stopped and the new one takes its port. A run that can pause moves to the new worker, a run that cannot keeps running on the old one, and whatever is still running after 10 seconds is killed. For the full rule, go and read [how a run is kept](../language/triggers-and-routes.md#how-a-run-is-kept) |
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
| `weft clean --project <id>` | Deletes that project's whole history |
| `weft clean --instance <id>`, `--tag <tag>`, `--status <how>`, `--node <node>` | Deletes only the runs these name, across the projects you name (or every one). Naming an instance or a tag takes all their runs; add `--keep-days` to spare recent ones. A run still going is left to finish unless you pass `--cancel-running` |
| `weft clean --images` | Reclaims worker images nothing references. `--all` spans every project |
| `weft clean --build-cache` | Drops the docker build cache and the node-test cache. The next build compiles cold |
| `weft rm` | Unregisters a project: signals wiped, runs cancelled and erased, infra terminated, stored data reclaimed |
| `weft rm --journal` | Erases its runs one by one before unregistering it; `weft rm` erases them on its own too |
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
| `weft rm` | The project's registration, its signals, its runs, its infra **including disks**, its stored data |
| `weft rm --journal` | The same runs, erased before the registration goes |
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
them. `weft clean --images` is what does, and `weft clean --images --all` also
removes every older builder base, runtime and standard worker image left by
earlier weft versions. You only need it by hand for those: the runtime already
reclaims the images no project uses when it starts and every six hours. A
local install also holds the docker build cache to 20 GB at the same times
(and `./setup.sh` does on every install), dropping what was used least
recently first.
