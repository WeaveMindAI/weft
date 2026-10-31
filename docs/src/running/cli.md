# The CLI

Every command, grouped by what you are trying to do.

Three flags work everywhere. `--on <target>` acts on an install named in
`weft.toml` instead of the one on your machine
([deploying to your cloud](cloud.md)). If you want to reach a runtime by its address
rather than a target name, pass `--dispatcher <url>` or set
`WEFT_DISPATCHER_URL`; you cannot combine it with `--on`. If you are scripting weft, pass `--json` for machine-readable
output.

You never log in to the install on your own machine: every command sends it
the key it made for itself when it was set up. If you want to call its API
from a script, or wonder where that key lives, go and read [the key the
install's API asks for](architecture.md#the-key-the-installs-api-asks-for).

`--json` changes behaviour, not just formatting, in three places:

- `weft run --json` also detaches, so it starts the run and returns.
- `weft infra terminate --json` never asks, so it needs `--yes`.
- `weft connect --json` only works with `--list`, `--grant`, `--disconnect`,
  `--forget` and `--move`. The walkthrough prints for a person.

Tab completes commands and flags in a new terminal after `./setup.sh`. For
zsh and bash, setup adds a short block to `~/.zshrc` or `~/.bashrc`, between
two `# weft tab completion` marker lines, and `./setup.sh --uninstall` takes it
out again. For fish it writes `~/.config/fish/completions/weft.fish`. Every Tab
asks the installed `weft` for the candidates, so new commands and flags show
up as soon as you update it.

## When your files change, or part of them is broken

Every command that builds (`run`, `build`, `bake`, `activate`, `resync`,
`infra start` and `infra upgrade`) first syncs this folder with the install's
copy of it, then has the install record those files as a version and build
that version. The command compiles nothing itself: the install compiles the
version, so a compile error it prints is about the files in that version. A
file you change after that goes into the next command. For how your folder
and the install stay in step, go and read [your folder is a
copy](versions.md#your-folder-is-a-copy).

A part of the program that does not compile (a `.weft` file that does not
parse, an include that does not resolve, a node type weft cannot find, a wire
between two ports that cannot meet) only stops what needs it: everything else
goes on, and every command that builds warns, with the file and line:

```text
warning: does not compile, so nothing that needs it runs: <what is wrong, with its file and line>
```

A run that needs a broken part is refused before it starts, and so is
`weft activate` for a trigger whose runs would reach one. While a broken part
hides nodes (an include that does not compile, a node type that did not
load), weft takes nothing down for being missing from your source; for why,
go and read [infra you deleted from the
source](lifecycles.md#infra-you-deleted-from-the-source).

A node package whose Rust does not compile is left out of the worker rather
than sinking it, and the build says so:

```text
warning: the worker was built without it, so nothing that uses its nodes runs: package 'whisper' didn't compile: <its first error>; its full errors are in the build log: <address>
```

Runs that never use its nodes run; one that does is refused before it starts:

```text
the run cannot start:
it needs parts of the program that do not compile:
  transcribe: package 'whisper' didn't compile: <its first error>; its full errors are in the build log: <address>
```

The package is built again once its files change, so fix it and run the command again.

## Start a project

| Command | What it does |
|---|---|
| `weft new <name> [--assistant <who>] [--remember] [--ci <cloud>]` | Scaffolds `weft.toml`, `src/main.weft`, `nodes/`, a git repo and a `.gitignore`. `--assistant` copies Tangle into this project only, and is repeatable. If you leave it out, the default `--remember` saved is used, and with no default weft asks you to pick from a list, the assistants it found on your machine first. With nobody to ask (no terminal, or `--json`) it stops and lists the values instead. `--assistant none` is how you make a project without Tangle; `--assistant none --remember` also clears the default, so the next `weft new` asks again. It ends by printing the command that opens the project in your assistant. `--ci gcp` also writes the deploy workflow. If any step fails, nothing is written |
| `weft catalog update` | Re-copies `nodes/base_catalog/` from the installed weft. Replaces the folder, so your edits in there go |
| `weft tangle update [--assistant <who>]` | Re-copies Tangle. Replaces every file the template owns; anything your assistant wrote beside them stays |

## Run something

| Command | What it does |
|---|---|
| `weft run` | Has the install record your files as a version and build it, then starts a run and follows it |
| `weft run <example>` | The same, with a saved example's starting values |
| `weft build` | Has the install record your files as a version, build it and register it, starting nothing. Also how you put back code the dispatcher no longer holds |
| `weft bake` | Prepares every trigger's settings and starts no listeners |
| `weft stop <run-id>` | Cancels a run |
| `weft wait <run-id>` | Waits until a run ends, then says how in one line and in its exit code (below) |
| `weft wake <run-id> <node>` | Resolves a pure time wait now. Refused for a wait that expects a value. If the node sits in a loop and waits in several iterations at once, `--iteration 3` says which one, and without it the refusal lists them |
| `weft follow <project>` | Streams a project's events live |

Flags on `weft run`:

| Flag | What it does |
|---|---|
| `--detach` | Start and return rather than following |
| `--referenced` | Compile only the node types the graph uses, rather than the whole catalog |
| `--target <node>` | Run this node and what it needs. Repeatable |
| `--before <node>` | Run what it needs, and not it. Repeatable |
| `--from <node>=<json>` | Start here, handing these ports their values. A port you hand a value keeps it: nothing a wire brings replaces it |
| `--emit <node>=<json>` | Stand in for a node: say what it would have emitted, and do not run it |
| `--group <group>=<json>` | Run one group, or one included file, on its own, with the values you hand it. A port a node inside needs that you do not hand is refused before the run starts |
| `--feed <start>` | Also run what feeds this start: for each input you did not hand a value, the node that feeds it (through any groups and included files on the way) and nothing above it. Names a `--from` or `--group` start. Repeatable |
| `--fire <trigger>=<json>` | Fire exactly one trigger with this event, using its bake. One per run |
| `--seed[=<run>]` | Reuse, from head's run or the run you name, everything whose slice of the program did not change (any finished run of the project, one a trigger started or one that failed included). A node that runs again and waited for an answer there gets that answer replayed, if its code did not change |
| `--seed-until <node>` / `--seed-before <node>` | Where reuse stops. Both need `--seed` |
| `--root` | Start a new version tree, parented on nothing. Refused beside `--seed`, since a new tree has nothing before it to reuse |
| `--check` | Only check that the run could start, building and running nothing: it prints `the run can start` and the infra the run reads, and exits 0, or prints the refusal and exits 1. With `--json`, the infra is the `infraRead` list, and with several targets `targets` says what stops each one if it ran alone |
| `--save <name>` | Write these settings to `examples/<name>.json` before running |
| `--clear <field>` | Clear a saved setting before applying flags: `from`, `emit`, `target`, `before`, `group`, `feed`, `fire`, `instance`, `keeping` (what `--durable` or `--fast` saved), `keep_for`, `hold_secs` |
| `--durable` | If the run has to carry on after its worker dies: another worker picks it up where it stopped, and a node caught halfway is failed rather than run twice. For what that costs in speed, go and read [when a run waits for its writes](the-journal.md#when-a-run-waits-for-its-writes) |
| `--fast` | If the run fires a durable trigger and you want it fast anyway: it lives in its worker's memory and ends if that worker dies |
| `--keep-for <duration>` | If you want this run kept longer or shorter after it ends than its trigger or project says: `30m`, `12h`, `7d`, or `forever`. For the project and trigger settings, go and read [how long a run is kept](the-journal.md#how-long-a-run-is-kept) |
| `--hold-secs <seconds>` | If a wait in this run should hold longer or shorter than its trigger's `holdSecs` while the run cannot pause (a bus between its nodes is open): `0` fails such a wait at once, at most 30 days. For when a run cannot pause, go and read [triggers and routes](../language/triggers-and-routes.md#when-a-run-cannot-pause) |
| `--instance <id>` | Run for this instance of the program. Needed when the run reaches a node that exists once per instance (see [programs with instances](instances.md)) |

A run that fires a trigger (`--fire`) follows the trigger's `durable`,
`keepRunsFor` and `holdSecs`, and `--durable`, `--fast`, `--keep-for` and
`--hold-secs` override them for that run; a run
that fires nothing is fast unless a flag says otherwise. A run you start by
hand is always recorded, even when its trigger has `recorded` off, so you can
always find it in `weft runs`. For what each of these does when you set it on a
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

If you run part of a program and it needs a value that only a trigger the run
does not fire would give, the run is refused before it starts, naming both
ways through:

```text
the run cannot start:
summarize.text gets nothing in this run: it only comes from the trigger inbox, which this run does not fire. Fire it (--fire inbox='<event>'), or hand the value at a start (--from summarize='{"text": ...}').
```

If the trigger it does not fire only feeds nodes that can do without it, you
get a warning instead, naming the nodes that skip unless something else feeds
them. For how a partial run is cut out of the program, go and read [how a
program runs](../language/how-a-program-runs.md#what-happens-when-you-hit-run).

### Waiting on a run

If you started a run with `--detach`, or a trigger started it, `weft wait
<run-id>` waits for it to end and says how, in one line and in its exit code.
If you are scripting weft, read the exit code rather than the line:

| Exit code | The run |
|---|---|
| `0` | completed |
| `10` | failed, or was cancelled |
| `11` | is waiting for input (only with `--waiting-for-input`, which also returns as soon as the run parks on a person or a service) |
| `12` | is still going when the timeout runs out |
| `1` | the wait itself failed: no such run, or the install unreachable |

`--timeout` takes a whole number and a unit (`30s`, `10m`), from one second
to 10 minutes, and defaults to 10 minutes. A timeout only stops the waiting: the run carries on, and `weft wait` again
picks it back up.

`weft run`, when it follows its run to the end, exits with the same codes: 0
when the run completed, 10 when it failed or was cancelled. With `--detach` or
`--json` it only starts the run and exits 0.

## What nodes need from your machine

Some nodes start a program that has to be on your machine, like `ExecPython`
and its `python3`. If your program uses such a node and what the node needs
is missing, `weft run`, `weft activate` and `weft resync` refuse before
anything starts, and say what to run:

```text
this program uses nodes that need things the machine its workers run on doesn't have:
  ExecPython (node 'greet') needs python3
install them with:  weft deps-install ExecPython
```

Those three check only what a node needs while it runs. What a node's
package needs to compile is not checked: if it is missing, the package does
not compile and the build leaves it out, naming it (for what that looks
like, go and read [when a package does not
compile](../nodes/packaging.md#when-a-package-does-not-compile)). `weft deps-install` installs both. If what a node needs
goes missing after the check (somebody uninstalled it), `ExecPython` fails
with those same lines, naming only itself, and so does any node that reports
it through `weft::error::need_missing`.

`weft deps-install` prints the install command for your system (`sudo
apt-get install -y python3`, `brew install python`), runs it once you say
yes, then checks again and tells you what is still missing. It installs only
what the machine lacks, with apt (Debian, Ubuntu), dnf (Fedora, RHEL,
CentOS, Rocky, AlmaLinux), apk (Alpine) or Homebrew (macOS). When a node
names no package for your system, `weft deps-install` says so, and you
install that one by hand. On a system none of those four serves, it installs
nothing, and you install everything by hand.

If `weft deps-install` says that what it installed is still missing, it is
not on the `PATH` of the shell you ran it from: put it there, then run
`./setup.sh` again from that shell. If it says everything is there but `weft
run` still refuses, the program is on your shell's `PATH` but not on the one
the install checks with, which is the `PATH` of the shell that last ran
`./setup.sh`: run `./setup.sh` again. For what else a worker's environment
holds, go and read [the worker](architecture.md#the-worker).

| Command | What it does |
|---|---|
| `weft deps-install` | Installs what the nodes of the program in this folder need and the machine lacks. A node type the program does not use is never asked about |
| `weft deps-install <NodeType>...` | The same for those node types, whether the program uses them or not |
| `weft deps-install --all` | The same for every node type of the project's catalog |

If you are scripting it, `--yes` runs the install command without asking,
and `--print` only prints it. `--stage run` keeps only what nodes start
while they run, and `--stage build` only what their packages need to
compile. If you write a node that needs something, go and read [what a node
needs from the machine](../nodes/packaging.md#what-a-node-needs-from-the-machine).

## Look at a run

| Command | What it does |
|---|---|
| `weft runs` | Past runs, newest first. `--limit` (50), `--offset`, `--project`, `--phase`, `--node` (the node that started the run), `--since 2h`, `--status`, `--instance`, `--tag`. `--through <node>` keeps the runs in which that node fired, wherever it sits. `--search <words>` keeps the finished runs that carried every one of those words somewhere in what they recorded (the trigger's input, what a node sent on, an error, a log line): an email, an order id, a phrase in quotes. `--through` and `--search` only find recorded runs that have ended, and they find one as soon as it ends. weft builds a run's search entry the first time anyone searches after it ended, so the first search after many runs ended takes longer. In the plain list, a run that a trigger with `recorded: false` started shows only if weft wrote it down after all (for when, go and read [how a run is kept](../language/triggers-and-routes.md#how-a-run-is-kept)), and `--through` and `--search` never find it. A run you start by hand is always recorded, so it always shows |
| `weft events <run-id>` | One run's events in order. `--node`, `--kind`, `--full` for whole values. Nodes are named the way the program that run ran names them, so if you moved or renamed a node since, you find it on an old run by the name it had there. If you want one time round a loop, add `--iteration 3` to a `--node` that names the loop or a node inside it: you get that node's rows from the fourth time round (they count from 0, outermost loop first, the same numbers `weft wake --iteration` takes), and `3.0` the first time round a loop inside that. `--iteration` needs `--node`, because on its own `3` would match the fourth time round every loop at once. A caller row that names messages the caller never received carries `dropped=<offsets> (<why>)` first |
| `weft logs [<run-id>]` | What the nodes wrote, plus every failure. A run that wrote nothing lists what it skipped and why (under `skipped` with `--json`). `--limit` |
| `weft status` | The cwd project: registration, listener, every infra node the program declares (one never started says `not started`, a `@per_instance` one how many instances have a copy), each trigger, whether it is on and behind the source, what infra it reads, and the version of the source it fires (with what it holds while it is down, and the events an instance's trigger holds until a field is filled), recent runs, how many runs each trigger started in the last minute or two and how many of those failed (for a trigger with `recorded: false`, often the only sign its runs happen), what drifted, and what you can do next. It also shows the project's own address (its routes at the root), or why that address is unavailable. If requests waited for a database connection in the last 15 minutes, a `database: busy` line says how often, what the database takes and each copy's share of it, and how to give the install more (see [how many connections the install takes](cloud.md#how-many-connections-the-install-takes)). While the install builds, it names each image building and where its log is; while infra starts or a trigger turns on, how long it has been going and what it waits on. If something weft does for the project keeps failing (handing runs to its workers, a worker that has answered busy for longer than the install's alert delay included, acting on its infra, finishing an infra command it then gave up, repairing work left behind by a worker or a weft process that died, getting the listener to let go of triggers whose door stopped taking work, removing stored files from the bucket, deleting what the infra host still holds of removed projects, a resync that stopped with triggers down, and on a cloud install also waking one of weft's services or running one of its background jobs), it lists it under `waiting on weft:` as `<what> (failing for <time>, last tried <time> ago; <what weft does next>): <error>`, and keeps it there until it works again. The lines about weft's own parts and jobs, the bucket, the install's news and removed projects are install-wide, so every project shows them. The triggers a resync you gave up with `weft cancel-activate` left down show there too, as `a given-up resync left these triggers down: 'feed', 'door'`, until they are on again or the next resync begins. A trigger taken down with its infra or by the health check says so, and one that cannot come back because other triggers are on another build says `held back`, with the way through. Under `triggers:`, a trigger whose queued events wait on something says what they wait on: the instance's values changing, its infra running again, connections being picked, or a failed start that holds the queue until `weft activate`, a new build or a change of the worker settings |
| `weft ps` | Every project the dispatcher knows |
| `weft instances` | The project's instances, one line each: its triggers on, its infra copies up, and its last run |

`--phase fire` hides the setup runs that an activate or a resync makes, so it
answers "has my trigger fired since I changed it".

## Versions and examples

| Command | What it does |
|---|---|
| `weft tree` | Every working copy of the project and where its head is, then the version tree with the runs started by hand beneath each version. `--json` adds `workingCopy`, the id of this folder's working copy |
| `weft checkpoint [<label>]` | Records this folder's files as a version and moves its head there. No run, no build |
| `weft branch <ref>` | Records this folder's files as a version, moves the folder to the version (or run) you name and writes its files here, then prints the `weft branch` that comes back. `--copy <copy>` moves another working copy instead (`program`, a folder's absolute path, or the start of a working copy's id) and leaves this folder where it is |
| `weft forget-copy <copy>` | Records another working copy's files as a version and removes the copy, for a folder that is gone or the program copy, when its head keeps `weft prune` from deleting a version. `<copy>` is named as for `--copy`. A folder that still exists registers again at its next command that syncs it, which is why this folder's own copy is refused |
| `weft resolve [<file>...] --keep folder\|incoming` | Settles the conflict copies a sync wrote beside files that changed both here and in the install. `--keep folder` keeps this folder's files and deletes the copies; `--keep incoming` moves each one over its file. Then it syncs, so the working copy gets the result. Name a file to settle its copies, a conflict copy to settle that one, or nothing to settle every one in the folder. `--keep` has no default, and `--keep incoming` refuses a file with several conflict copies until you name one. `--json` answers `keep`, `resolved` (the files settled) and `copy` (the working copy after the sync) |
| `weft diff <left> <right>` | Compares what two runs put on their wires. A ref is a run id or `example:<name>`. `--full` for every wire |
| `weft freeze <name> [<run-id>]` | Writes `examples/<name>.json` from a run: its parameters, the answers people gave, and the outputs you accept. `--expect <node>` emphasises a node in later diffs |
| `weft examples` | Saved examples, whether each is frozen, and its latest run |
| `weft prune <version>` | Deletes a version, everything under it, and their runs |

## Triggers

| Command | What it does |
|---|---|
| `weft activate` | Turns on every one of the program's own triggers that is off, and leaves the ones that are on alone; with nothing off it says so and succeeds. Builds and registers first if it has to, and prints the project's own address, where its routes answer at the root (on your machine a local port, on a cloud install its Cloud Run address). It never starts infra and never touches a run going now: it refuses a trigger whose setup reads infra that is not running (a trigger whose runs alone use it is turned on with a note naming it), and refuses while a connection the program needs is not picked on this install, naming each node. A trigger that runs once per instance is left off and named in a warning, with the way to switch it on. `--held run\|drop` decides what happens to the work the triggers held while they were down: run it (the default, at half each trigger's limits) or drop it. The activation runs on the install, so Ctrl+C, a closed terminal or a dropped connection stops only the following: `weft status` shows each trigger turning on, for how long and what it waits on (usually its setup run), and `weft cancel-activate` stops it |
| `weft activate --port <n>` | On your machine, opens the project's own address on that port, and the project keeps it for later activates. If another program or another of your projects holds it, nothing is activated. Without it, the project keeps the port it has, or takes a free one |
| `weft deactivate` | Takes the program's own triggers down, and tells you how many instances still have theirs on |
| `weft resync` | Builds, then brings every trigger that is on and behind the build onto it, and leaves every other trigger alone. With nothing behind, it says `no trigger that is on is behind this build; nothing to resync`. It turns on nothing you switched off. It does first bring back, on the build it just made, what an update's `weft maintenance off` could not bring back in this project, and says what came back and what is still down. Any other trigger of the new build that is off is named with the `weft activate` that would. Before it takes anything down, it checks everything that could refuse it, so a refusal leaves your triggers as they were. While it waits for runs on the old version to end, it names them (`the new version takes over once 2 runs on the old one end: ...`), with `weft stop <run>` to end one now and `weft cancel-activate` to give the resync up. `weft resync --resume` takes up again a resync that stopped with triggers down, and takes no other flag. While a resync runs, or stands stopped, a build you start is refused with `cannot build now` |
| `weft cancel-activate` | Cancels an activate in flight, one still waiting to start included. With no `--trigger` or `--instance`, it also gives up a resync, running or stopped, and names the triggers it leaves down |
| `weft cancel-build` | Cancels a build in flight. The install stops the build, and the command that was building fails saying it was cancelled |

For what each of these does to work in flight, a resync that stops, and
everything else about turning triggers on and off, go and read [starting and
stopping things](lifecycles.md#triggers).

`deactivate`, `resync` and the infra verbs that take infra down (`stop`,
`terminate`, `upgrade`) take `--door take|hide|refuse`,
`--door-for <minutes>`, `--held keep|drop` and `--running wait|cancel`, each
with a default that loses nothing (`take`, `keep`, `wait`). For what each answer
does, go and read [taking triggers
down](lifecycles.md#taking-triggers-down-three-questions).

Every trigger verb also takes which triggers it acts on:

| Flag | What it does |
|---|---|
| `--trigger <name>` | Only this trigger, spelled the way the program writes it (`receive`, `one.receive`). Repeat it for several |
| `--instance <id>` | That instance's triggers, the ones that exist once per instance. Repeat it for several (see [programs with instances](instances.md)) |
| `--all-instances` | Every instance's, and not the program's own |
| `--all` | The program's own and every instance's, in one command |

With none of the last three, a verb acts on the program's own triggers only.

## Infrastructure

| Command | What it does |
|---|---|
| `weft infra start` | Builds, then starts what is not running and nothing else. The triggers a stop of that infra took down come back on by themselves once it runs; no other trigger is touched |
| `weft infra stop` | Scales to nothing, keeping the disk. The triggers that depend on it go down first, and come back on by themselves once it runs again |
| `weft infra terminate` | Deletes it, disk included, except the disks the node lists in `keepOnTerminate`. Takes down what `stop` takes down. Asks first, and off a terminal or with `--json` it needs `--yes` |
| `weft infra upgrade` | Builds, then replaces only what is running on another build: it starts nothing that is stopped and makes nothing that is missing. The triggers that depend on what it replaces go down for it and come back once it runs again |
| `weft infra status` | Where each copy that exists stands, with its address. A start or stop on its way shows as `provisioning` or `stopping` at once |
| `weft infra list-doors` | Which pieces you can reach from this machine, and at what address |
| `weft infra logs [<node>]` | What the containers printed. `--tail` (200), `-f` |
| `weft infra cancel` | Calls off the infra work in flight. Halts between steps rather than undoing |
| `weft infra look` | If `weft status` says the supervisor could not get through the project's infra and you have fixed the cause, this asks it to go through the project again (on a cloud install, with a fresh round of three tries). A health rule that gave up tries again too. For what the supervisor retries by itself, go and read [the supervisor](architecture.md#the-supervisor) |
| `weft infra show <node>` | The node's card: every item (label, kind, value) and every button, with the action to hand `press` and what it asks before pressing. A `secret` item's value is never printed, `--json` included |
| `weft infra press <node> <action>` | Presses the card's button with that action, without asking: naming the action is the choice. Prints what the node answered, and the button's warning if it has one |
| `weft infra rebake <node>` | If an infra node's baked outputs no longer match the real thing (say its password was reset outside weft), this applies the node again, leaving alone anything already running the way the node asks, and saves its [baked outputs](../nodes/infrastructure.md#outputs-saved-with-the-infrastructure-baking) anew. If a `weft infra start`, `upgrade` or another rebake is already working on the same copies (the shared ones, or that instance's), it waits for that to finish first |
| `weft infra env <node> --into <file> --set NAME=Label ...` | Writes what the node's card shows into an env file, every name in one go: `--set DATABASE_USER=User --set DATABASE_PASSWORD=Password` takes each item by its label as `show` prints it, `--as <NAME>` is the short form for the card's only secret, and `--door DATABASE_ADDRESS=pg` writes where one of its doors answers (`host:port`, by the endpoint `list-doors` prints). A secret's value is never printed; the other values are, in the line confirming what it wrote. A new file is yours only; an existing one gets those lines set and keeps the rest. A door's port may change when the infra restarts or is upgraded, so run it again then. If a secret was handed over already, it refuses, quoting what the card says there: press the card's button with `weft infra press`, then run it again |

If a build changes an infra node's image, every command that builds says so.
A copy started after that gets the new image; one already running keeps the
old one until `weft infra upgrade`.

`start`, `stop`, `terminate` and `upgrade` act on every piece of the
program's own infra. `--infra <node>` (repeatable) names only some, spelled as
`weft infra status` lists them (`db`, or `one.db` for the `db` inside the file
included as `one`); `start --infra db` also starts what `db` needs
that is not running, and `stop --infra` still reaches a node the program no
longer declares. `stop`, `terminate` and `upgrade` take `--door`,
`--door-for`, `--held` and `--running` as above: with `--running wait`, the infra stays up until every
run reaching it has ended, even when no trigger reading it is on. `stop
--force` also brings down the units a node keeps up on a normal stop, so you
can update them on the next start.

If you want an instance's copies of the nodes marked `@per_instance` instead
of the shared infra, `start`, `stop`, `terminate` and `upgrade` take
`--instance <id>` (repeatable), `--all-instances` and `--all` like the trigger
verbs, and `cancel`, `show`, `press`, `rebake` and `env` take `--instance
<id>`. Naming a `@per_instance` node with `--infra` and no instance is refused,
naming those flags.

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
| `weft connect --label <name>` | Name the new connection, so two of one service read apart in the list. Without it the connection is named after the service, or after the app it signs in through |
| `weft token mint` | Mint a signal token. `--name`, `--projects`, `--tags`, `--display <node>`, `--displays`, and `--expires <duration>` (`30m`, `12h`, `7d`; without it the token works until revoked). Printed once and never again. `--operator` mints an operator key instead, which administers the whole install and takes no scope |
| `weft token mint --instance <id> --expires <duration>` | An instance token for the project you are in: it acts inside that one instance and nothing else. `--expires` (`30m`, `12h`, `7d`) is required |
| `weft token ls` | The tokens you have, by their recognizer prefix |
| `weft token revoke <id>` | Revoke one |
| `weft options <node> <field> [--search <text>]` | Lists the choices a field with a searchable list offers (a model, a spreadsheet, a channel), the same list the editor shows when you search that field, read through the connection picked for that node. One `id  label` line each; the id is what you write in the field. `<node>` is the node's name as written in your source (`path/to/file.weft:name` when two files use that name), and `<field>` is its input's name |
| `weft connect-lib` | Copies weft's connect library into the project's frontend (`front/src/lib/weft-connect`, or the folder `--into` names), so its pages show the editor's connection pickers and list fields, and each instance gets its own settings page. The folder is the library's: each run replaces it, so run it again after updating weft and keep your own code outside it |

## Cloud installs

| Command | What it does |
|---|---|
| `weft target add <name> <url>` | Names an install in `weft.toml`. Replaces the address when the name exists |
| `weft target list` | The targets, and whether you are logged in to each |
| `weft target remove <name>` | Takes one out of `weft.toml` |
| `weft login <name>` | Stores your operator key for it, after checking the install accepts it. If you are scripting it, `--key-stdin` reads the key from standard input. Refused for an install on this machine (the `local` target, or any target at its address), whose own key every command already sends |
| `weft logout <name>` | Forgets your key for it |
| `weft target key` | Prints, as JSON (`url`, `operatorKey`), the address and operator key a command with the same `--on` (or `--dispatcher`) would use, or with neither, those of this machine's install. The editor calls it to reach its install |
| `weft running-source <folder>` | Writes the program an install is running into a new folder, without touching your own files. Add `--on <target>` to get what a cloud install runs |
| `weft ci add --cloud gcp` | Writes `.github/workflows/deploy.yml`. Refuses to replace one somebody edited |
| `weft target export <name> [--github] [--frontend <name>]` | Mints a CI operator key on the install, and gives the frontend the install hosts for this repository a new token along with its service's name (the old token works until the workflow's next run deploys the new one and retires it). With `--github` it sets them, and the workflow's other settings, on the repository through `gh`, and lists what it set; without it, it prints the variables and writes the secrets to a file only you can read, under the install's `exports/` folder |
| `weft target show <name>` | Where a target's install lives: its address, and on a cloud the project and region it runs in, and the weft it runs |
| `weft frontend add <name> [--repo owner/name]` | Makes one of the project's frontends. With `--repo` (read through `gh`), a cloud install makes it a Cloud Run service and lets that repository deploy to it, and its token comes from the deploy workflow (`weft target export`); without, it runs wherever you run it, and its token is written to a file only you can read |
| `weft frontend ls` | The project's frontends, and where each runs |
| `weft frontend visitor-address <name> <url\|off>` | Sets where that frontend's server passes weft's doors on to its visitors (its `weftPassThrough` mount, like `https://shop.example/weft`), so links to the project's stored files go through your site; `off` clears it. One frontend of a project has one at most ([your own site in front](../build/public-address.md)) |
| `weft frontend rm <name> [--force]` | Removes one: its tokens stop working, and a service the install made for it is deleted. `--force` forgets it even when the service cannot be removed, naming what stays |
| `weft frontend token <name> [--done <id>]` | A new token, beside the one it has, with its id; `--done <id>` once that token is in place retires every other token of the frontend |
| `weft domain add <name>` | Makes a cloud install answer at a domain you own: prints the DNS record to set and waits until it points there. `--for api` serves the project's routes there instead, `--for frontend --to <address>` its frontend. `--no-wait` returns after printing the record. The first domain makes a load balancer that is billed while any domain exists, so it needs `--accept-cost` ([your own domain](cloud.md#your-own-domain)) |
| `weft domain list` | Every domain, with its DNS record and where its certificate stands, the address the install's links use, and the sign-in callback to give your providers once a domain is that address |
| `weft domain rm <name>` | Stops answering at it; the last one takes the load balancer down |
| `weft workers` | The project's worker settings: copies kept warm, the most copies, how many calls Cloud Run sends one copy at once (`concurrency`), CPU, memory, how long a copy holds on to things its runs share, such as a database pool, after nothing uses them (`shared_idle_seconds`), how many calls and events one copy takes at once (`max_runs_at_once`) and how long a call waits when it is full (`max_queue_wait_seconds`; for both, go and read [when a worker is full](architecture.md#when-a-worker-is-full)). It shows which the project sets and which come from the install. CPU and memory only apply on a cloud install, where an unset CPU means one CPU; on your machine no worker is capped |
| `weft workers set --min-instances 1 ...` | Changes settings for this project. On a cloud install, calls after the change go to workers that have them, and a worker started before finishes what it runs and then stops. On your machine, the worker at the project's own address keeps its port, and keeps answering there, until it has nothing left running; then it is stopped and a worker with the new settings takes the port |
| `weft workers reset [<setting>...]` | Puts settings back to the install's values |

## Stored files

| Command | What it does |
|---|---|
| `weft migrate connections --to <target>` | Copies saved connections to another install, their secrets sealed to it. Name what: `--all`, `--id`, `--since`/`--until`, `--service`, `--project`; `--dry-run` copies nothing ([moving to the cloud](cloud.md#moving-from-your-machine-to-the-cloud)) |
| `weft migrate files --to <target>` | Copies stored files to another install, keeping each one's key, version and lifetime. Name what: `--all`, `--id`, `--since`/`--until`, `--prefix`; `--dry-run` copies nothing |
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
| `weft daemon remove` | Takes a named install off this machine, everything of it included (its images and compile caches too): the install of the branch you run it from, or the one `WEFT_INSTALL` names. The default install is removed by `./setup.sh --uninstall` |
| `weft daemon which` | Which install a command run here acts on, and where its files are. `--json` for scripts |

If the runtime image or the standard worker program is broken or was changed by hand, `weft daemon start --rebuild` rebuilds both even when they are there, compiling the standard worker on your machine (so it needs Rust), and `./setup.sh --rebuild` passes it on. To open or close [the public
address](../build/public-address.md), `weft daemon start` takes
`--public-url` and `--no-public-url`.

## Updating weft

An update replaces the weft that runs your install, and an older and a newer
weft never run side by side. So if you want to update, you switch everything
off first, update, and switch it back on: `weft maintenance on`, then
`./setup.sh` (or the cloud's install workflow), then `weft maintenance off`.
Add `--on <target>` for a cloud install. Both the installer and the workflow
check first, and refuse to replace anything while something is on.

| Command | What it does |
|---|---|
| `weft maintenance on` | Takes down every trigger that is on, in every project, with its door refusing what arrives meanwhile (what it held already is kept), and stops every infra copy that runs. Every unit of a copy comes down, even one its node keeps up on a plain stop; disks are kept. Then it waits for the runs going to finish and the copies to stop, however long that takes. `--running cancel` cancels the runs instead. Until `off`, nothing can be turned on: an activate, an infra start, a resync or a `weft run` is refused, naming `off`, and nothing weft brings back on its own comes back. A trigger it took down takes no work, as with any door that refuses: an event for it is dropped, and a live call is answered `503` with a `Retry-After`. You can run it again: it takes down whatever is on by then and forgets nothing it took down before |
| `weft maintenance off` | Builds every project `on` took something down in again on the new weft, from the same files and settings its last build used, then starts the infra copies `on` stopped on that build, waits until they run, and brings back the triggers `on` took down on it, with what they held before `on`. A project last built by a weft older than this command has nothing stored to build again from: it comes back on the build it has, and `off` says to run `weft resync` in its folder. Only those: a trigger you took down yourself during the maintenance stays off, and a copy you terminated is skipped. If something in a project does not come back, `off` says what and why, and ends the maintenance anyway: everything else comes back, and what did not stays down until that project's next `weft resync`. If the message says the project's copy of the standard nodes is older than this weft, run `weft catalog update` in its folder; otherwise fix what it says (usually something in the project's folder). Then `weft resync` there builds it, first brings back what `on` took down in it and `off` could not bring back, and says what came back. If you want some of those triggers left off, `weft deactivate` them before the resync. Running `off` again also tries what is left and says again what does not come back, but it builds from the same files as before, so it does not pick up a change you made in the folder. Projects come back side by side, but across all of them at most four setups run at once (a setup is the infra, or the triggers, of a program or of one of its instances), so on an install with many instances the rest wait their turn. While it runs it shows a line per project with its step and how long that step has taken; for each worker it compiles and each image it builds it prints where the build's log is, and on a local install the line also shows the log's latest line. It runs on the install, so closing the terminal does not stop it |
| `weft maintenance` | Whether the install is in maintenance and since when, what `off` brings back, and what is on right now. `status` is the same |
| `weft maintenance check` | Fails while something is on (a trigger, an infra copy, a run going). It is what the installer and the workflow run before they replace anything. An install that is not set up or not running passes, and so does one whose weft is older than this command, saying it cannot tell |

`weft status` says when the install is in maintenance.

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
| `weft describe-nodes --list` | One line per node type: its name, its tags, the first sentence of what it does. The cheap first look. After the types, every `.weft` file under `nodes/` a program can `@include`, spelled the way you write it (`__catalog/...`), with its first-line comment |
| `weft describe-nodes --node <Type> --compact` | One node's wiring view: ports, types and rules, with the presentation stripped out. A long pick list shows its first eight entries and how many more there are; the plain `--node <Type>` has them all |
| `weft parse` | Parses source from stdin and prints the project, the catalog and the diagnostics as JSON. Lenient: unknown types become placeholders |
| `weft validate` | The same input, strict, including the runtime rules like a provider with no model named. Also warns about every node under `nodes/` or `src/` it had to leave out, and every name a node's `metadata.json` spells off the weft way; neither fails it |
| `weft parse-server` | A long-lived parse server, one JSON request per line, catalog held warm |

## Cleaning up

| Command | What it does |
|---|---|
| `weft clean` | Deletes runs older than `--keep-days`, 30 by default |
| `weft clean <run-id>` | Deletes one run |
| `weft clean --all` | Deletes every run |
| `weft clean --project <id>` | Deletes that project's whole history |
| `weft clean --instance <id>`, `--tag <tag>`, `--status <how>`, `--node <node>` | Deletes only the runs these name, across the projects you name (or every one). Naming an instance or a tag takes all their runs; add `--keep-days` to spare recent ones. A run still going is left to finish unless you pass `--cancel-running` |
| `weft clean --images` | Reclaims the workers and infra images nothing references (on your machine a worker is a program under `~/.local/share/weft/workers/`, on a cloud install an image), then prints how much the worker compile caches (`~/.local/share/weft/worker-target/`) and the crates your machine compiles workers from (`worker-crates/`) take. `--all` spans every project, and also removes what installs no longer on this machine left behind (below) |
| `weft clean --build-cache` | Drops the node-test cache, waits for the builds compiling right now to end, then drops the worker compile caches and the crates your machine compiles workers from (`~/.local/share/weft/worker-target/` and `worker-crates/`), then Docker's build cache. The next build of a project with its own nodes then compiles the whole standard library again |
| `weft rm` | Unregisters a project: its triggers taken down (the door refusing, what they held dropped, the runs going cancelled), infra terminated, runs erased, stored data reclaimed. Before it erases anything, it waits until the last calls its cancelled runs queued (an outside job told it was cancelled, say) have gone out, following that in short looks: Ctrl+C stops following, and running `weft rm` again picks it back up |
| `weft rm --journal` | Waits for every run of the project to end, then erases its runs one by one before unregistering it; `weft rm` erases them on its own too |
| `weft rm --local` | Also wipes its build artifacts here |
| `weft rm --all` | Both of those |
| `weft rm --force` | Doesn't wait for the infra to confirm it stopped, and goes on past leftovers on the cloud, naming them. A worker it can't kill still stops it: nothing is erased while one runs, and the error says how to stop that worker by hand |

If `weft rm` fails partway, the project stays marked as being removed, and
the way to finish is to run `weft rm` again in the project's folder. Until
you do, no new run of it starts, and `weft ps` lists it with ``the project is
being removed; run `weft rm` again to finish removing it``. `weft status`
still answers, with ``WARNING: the project is being removed; run `weft rm`
again to finish removing it`` above what is left of it. `weft run`, `weft
activate`, `weft resync`, the `weft infra` commands and every build of it
refuse with that same line. The editor shows it as a banner over the project
and offers no button, `weft rm` included, so finishing it is a terminal job.

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
| `weft infra terminate` | Confirm what will be deleted | `--yes` |
| `weft connect` | Which node, which door, which app, which permissions, the credential fields, the connection's name | `--node`, `--door`, `--shared-app`, `--permissions`, `--set`, `--label` |
| `weft test-node --tier live` | Confirm that this spends money | `--yes` |
| `weft login` | The operator key, at a hidden prompt | `--key-stdin` |
| `weft deps-install` | Whether to run the install command it printed | `--yes`, or `--print` to only print it |

`deactivate`, `resync`, `activate` and the infra verbs never ask how to take
triggers down or bring them back: every flag you leave out takes its
default.

## Everything destructive, and what it takes

| Command | What goes |
|---|---|
| `weft rm` | The project's registration, its signals, its runs, its infra **including disks**, its stored data |
| `weft rm --journal` | The same runs, erased before the registration goes |
| `weft rm --local` | Also `.weft/target/` and its slice of the node-test cache |
| `weft prune <version>` | That version, every version under it, and every run beneath them |
| `weft clean <run-id>` | One run, for good |
| `weft clean --all` | Every run |
| `weft clean --project <id>` | That project's entire history |
| `weft clean --build-cache` | The worker compile caches and the crates your machine compiles workers from, the node-test cache and Docker's build cache |
| `weft files rm <space>/` | A whole space, **including files you marked to keep** |
| `weft deactivate --held drop` (or `resync`, or an infra verb) | Everything the triggers held from an earlier time down, and the runs they started that are paused on a wait are cancelled |
| `weft deactivate --running cancel` (or `resync`, or an infra verb) | The runs going now, cancelled |
| `weft maintenance on --running cancel` | Every run going now on the install, cancelled |
| `weft activate --held drop` | Everything the triggers held while they were down, answers people sent to waiting runs included, and the runs paused on them are cancelled |
| `weft catalog update` | Anything you edited under `nodes/base_catalog/` |
| `weft tangle update` | Your edits to the Tangle files the template owns |
| `weft freeze <name>` | The previous `examples/<name>.json`, replaced whole |
| `weft infra terminate` | Every infra resource, **disks included**, unless the node preserves them |
| `weft connect --forget <id>` | A stored connection. Other projects using it then fail at run time asking for a reconnect |

The runtime reclaims the workers and images no project uses each time a
build registers a version and each time a project is removed, and on your
machine also when an infra container stops or is replaced. `weft clean
--images` does the same by hand. `weft clean --images --all` also removes
what no install on this machine uses: runtime images, the builder bases,
node-test images, standard worker images and image compile caches an earlier
weft or a `weft build-images` run left, and the worker programs (an earlier
weft's standard worker among them). It also removes the crates your machine
compiles workers from that no worker program on it was built from
(`~/.local/share/weft/worker-crates/`), and the images and build caches of
installs no longer on your machine, along with the caches an older weft made
before each install had its own (`weft daemon remove` drops an install's
images and caches as it goes). A local install also holds the docker build
cache to 20 GB when it starts and every six hours (and `./setup.sh` does on
every install), dropping what was used least recently first.
