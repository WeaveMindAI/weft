---
name: weft-running
description: "Read when running or activating a program: the CLI command map, the daemon (never yours to restart), running a command so it never asks and never sits quiet (with how long each normally takes), waiting on a run, what a command does when the files change under it or part of the program is broken, the build and run flow, turning triggers on and off (the three answers every take-down gives, the defaults, instances, one build per project), the infra lifecycle and its health check, and what each kind of trigger does across a restart. A run that failed or did the wrong thing is the weft-debugging skill."
---

# Running

The `weft` CLI is a thin client of [the daemon] plus the front end for
building. [the daemon] is the dispatcher process that owns projects,
runs, triggers, and infra. It listens on `http://127.0.0.1:14111`
unless the user started it on another port, and the port it really runs
on is the `public` field of `~/.local/share/weft/ports.json`.
Every command talks to that one unless it is given `--on <target>`, a cloud
install named in `weft.toml` (the `weft-deploying` skill says what you do
there yourself and what you ask the user first). The user's install (`setup.sh`) starts it, once per
machine.
`weft daemon start`, `stop` and `restart` re-run that install, and a restart
from a project has wiped shared keys before, so you never run them. A `.env`
near the project auto-loads.

Each run has its own id (a UUID minted when it starts). Everything in it
is journaled, node by node, with the values on the wires. Every command that
takes a run id also takes its first characters (`weft events 3f2a`), at
least four, as long as they name a single run.

## How you run a command

No command you run may ask you anything. These are the only ones that do,
with the answer already in them:

| Instead of | type |
|---|---|
| `weft rm`, `weft clean`, `weft prune`, `weft infra terminate` | the same with `--yes`, once the user has said yes |
| a long `weft run` | `weft run --detach`, then `weft wait <run-id> --waiting-for-input --timeout <its cap>` |

A command that asks anyway gets killed and reported with what it asked.

A command that changes the deployed code and the thing you do next go in ONE
line, joined by `&&`: `weft resync && curl ...`. Run them separately and a
resync that failed leaves the next command talking to the old code, which
reads as a bug in what you just wrote. Nothing goes between the weft command
and the `&&`: no `| tail`, no `| grep`, no `2>&1 | head`. A pipe's exit
status is its last command's, so `weft resync 2>&1 | tail -1 && curl ...`
runs the `curl` after a resync that failed, because `tail` succeeded. If the
output is long, let it be long and read it. (A resync whose step keeps
failing after taking its triggers down stops with them down, says why, and
refuses builds until it is settled. If it stopped on infra its triggers read
that was not running, or on an activation still landing, it goes on by
itself once that infra runs or that activation has landed. Otherwise `weft
resync --resume` picks it up where it stopped (a plain `weft resync` is
refused meanwhile), and a plain `weft cancel-activate` gives it up, leaves
those triggers down and prints which; with `--trigger` or `--instance` it is
refused, since the resync covers every trigger of every owner.)

**You never sit on a quiet command.** If a command can take more than a few
seconds, you start it in the background (a long `weft run` gets
`--detach`), and every wait on it has a cap: the time the table below says
that command normally takes. Inside that cap you still look for movement at
least every thirty seconds: new lines in its output or log, a changed `weft
status --json`, a new line in `weft daemon logs` or `weft infra logs`.
Nothing weft does is silent for thirty seconds when it is healthy, so
silence is the finding. At the cap you look and decide. If it is still
moving, it gets one more expected period at most. If it is quiet or not
moving, it is stuck: you stop it and find out why (`weft daemon logs`,
`weft infra logs`, `weft status --json`). You never just wait longer.

| Command | Normally takes | Your first cap |
|---|---|---|
| Reads: `weft status`, `runs`, `events`, `logs`, `describe-nodes`, `parse`, `validate` | under 5 seconds | 15 seconds |
| `weft run` of a program whose nodes are all built already | a few seconds | 30 seconds; `--detach` for anything that waits on a person, a timer or a slow node |
| The first `weft run` or `weft activate` that compiles a node of the project's own | 1 to 3 minutes | 3 minutes, looking every 30 seconds |
| `weft activate`, `resync` or `deactivate` with nothing to build | under 30 seconds, plus however long the runs still going take to end, since `resync` and `deactivate` wait for them by default | 30 seconds, counted from when `weft runs --project <id> --status running` lists none of the runs it waits on (a deactivate does not wait on runs started by hand) |
| `weft test-node`, basic and fake tiers | 1 to 3 minutes on the first compile, under 30 seconds after | 3 minutes the first time, 30 seconds after |
| `weft infra start` with its image already there | under 1 minute | 1 minute |
| `weft infra start` that builds or pulls an image | 2 to 5 minutes | 5 minutes, looking as it goes |
| `weft infra start --on <cloud>` the first time: it builds the program on the install's builder, then boots a machine for each piece | about 2 minutes of build, then 2 to 3 minutes of boot | 6 minutes, looking as it goes |
| A stopped cloud copy started again, its disk kept | 1 to 2 minutes | 3 minutes |
| `weft frontend add --repo` on a cloud install | about 10 seconds | 1 minute |
| The deploy workflow `weft ci add` writes, run from GitHub | about a minute and a half, plus the frontend's build | 5 minutes, watching its log |
| A frontend dev server, or `pnpm install` | runs in the background | checked by its port or its log, never waited on in the foreground |

Nothing in weft normally runs for thirty minutes, so a cap of thirty
minutes is always wrong. Check what unit your own tool wants before you
type a number. Some take seconds and some take milliseconds, and getting
it backwards is the difference between thirty seconds and two days.

Getting out is cheap and safe: Ctrl+C on the CLI (nothing weft was doing
is left half done; the daemon finishes or rolls back on its own). Then read
`weft daemon logs`, which is where a wait says what it is waiting on. You
raise a number only once you can say what is taking the extra time, and a
run that is longer than its shape says is something you report.

The one wait that is long on purpose is a take-down waiting for runs (a
`resync`, a `deactivate`, an infra `stop`, `terminate` or `upgrade`): by
default it lets the runs going finish first, however long they take, and
says so in a line as it starts. If you want those runs ended, `weft stop
<run>` ends one now (`weft runs --project <id> --status running` lists
them); to have the next take-down end them all at once, pass it
`--running cancel` (Taking
triggers down, below, says when that is yours to pass). The CLI follows a resync, a
deactivate and a `weft rm` in short looks, so Ctrl+C only stops following:
the resync goes on without you, a deactivate's triggers stay down, and a
removal waits, project down, until `weft rm` runs again and picks it up.

**Waiting on a run is `weft wait <run-id>`, never a loop.** It returns when
the run ends and says how in one line and in its exit code:

| Exit | The run |
|---|---|
| `0` | completed |
| `10` | failed, or was cancelled |
| `11` | waits for input: every node still open waits on a form, a timer or an outside job (only with `--waiting-for-input`) |
| `12` | still going when `--timeout` ran out (`30s`, `10m`; 10 minutes at most, and the default) |
| `1` | the wait itself failed: no such run, the install unreachable |

Give it the cap from the table above as its timeout (`weft wait <run-id>
--timeout 30s` for a run of nodes already built), so a run that went quiet
comes back to you to look at. A timeout stops only the waiting: the run
carries on, and `weft wait` again picks it back up. A run parked on a person or a timer never ends by itself,
so pass `--waiting-for-input` whenever the run can park, or the wait sits
until its timeout. `weft run` that follows its run to the end exits with
the same `0` or `10`; with `--detach` or `--json` it only starts the run and
exits `0`. For a build or [the daemon] coming up, one `weft status --json`
per look, between other work.

You never write `sleep`, `until` or `while` around a weft command. A loop
you write has no idea what it waits on, and a run parked on a person makes
an uncapped one hang until somebody kills it, which has happened.

**A `pkill` or `pgrep` pattern matches your own command line too.** The
shell running it has the pattern in its arguments, so a broad pattern kills
the shell mid-command: you get an exit code in the 140s, no output, and
nothing saying what happened. Match on the executable instead of a substring
of the whole line (`pkill -x <name>`, or `pgrep -f` on a path that cannot
appear in your own invocation), and prefer the tool that owns the process:
weft's own commands stop what weft started, and [the daemon] is never yours
to kill anyway.

`--json` is a global flag with two meanings. The long commands (`build`,
`bake`, `run`, `activate`, `deactivate`, `resync`, `infra`, `rm`, the
cancels) stream progress as one JSON object per line. The readers (`status`,
`ps`, `runs`, `events`, `logs`, `files`, `token`,
`stop`, `connect`, `tree`, `examples`, `diff`, `checkpoint`, `branch`,
`freeze`, `prune`, `wake`) print what [the daemon] answered, which you read
with `jq` instead of parsing the human columns; `test-node` prints its
reports as one JSON array. `new`, `follow`, `daemon`, `catalog`, `clean` and
`update` ignore it. Every field name weft writes in that JSON is camelCase
(`runId`, `entryNode`, `startedAt`), so that is what your `jq` reads
(`weft runs --json | jq -r '.runs[0].runId'`). A value picked from a fixed set stays snake_case: a
status (`waiting_for_input`), an event kind (`node_failed`), a phase
(`trigger_setup`).

## Naming a node

Every command that takes a node takes its whole path from the entry file,
dot-joined, the way the source reads: `classify` for a node written in
`src/main.weft`, `review.classify` for one inside the group `review`,
`triage.classify` for one inside the file the site `triage` includes
(`triage = @include("triage.weft")`), and `triage.review.classify` when it
sits in a group inside that file. Sites, groups and nodes are one tree, and a
name is the walk down it from the top. There is no short form: a bare
`classify` names nothing once the node sits inside a group or an included
file, and the refusal spells the name that works. The same file included
twice is two places with two names (`triage.classify` and `again.classify`),
each with its own runs, waits and display. This is what `--from`, `--emit`,
`--target`, `--before`, `--seed-until`, `--seed-before`, `--group`, `--fire`,
`--trigger`, `--infra`, `weft events --node`, `weft node-state`, `weft
wake`, `weft freeze --expect`, `weft infra logs` and `weft token mint
--display` take, and what `weft events`, `weft runs`, `weft infra status`
and the graph print back.

## The command map

| Command | What it does |
|---|---|
| `weft build` | record this folder's files as a version, then have the install compile that version, resolve its assets, build the worker (content-addressed) and register it with [the daemon]; the CLI compiles nothing itself. Starts nothing. You never need it while building a program: `weft run`, `weft activate` and `weft resync` build on their own, so running `weft build` before them only builds twice. Its one use is the final deployment, `weft build --referenced`, which ships only the node types the program uses. Also the repair when [the daemon] no longer holds the code a past run ran: registering records the compiled program under its own hash, the one the run names, so unchanged files make the run readable again. Like every command that builds, it records a version, and unchanged files record nothing new |
| `weft validate --file src/main.weft < src/main.weft` | strict compile + validate, diagnostics as JSON, nothing runs |
| `weft run [<example>] [--referenced] [--seed[=<run-id>]] [--root] [--from <node>=<ports-json>]... [--emit <node>=<ports-json>]... [--target <id>]... [--before <id>]... [--group <id>=<ports-json>] [--feed <start>]... [--fire <trigger>=<wake-json>] [--instance <id>] [--check] [--save <name>] [--durable\|--fast] [--keep-for <duration>] [--hold-secs <seconds>]` | build and start one run; `--detach` returns its run id, and without it the command follows the run and exits `0` when it completed, `10` when it failed or was cancelled (`weft wait` has the codes). On a cloud install one stretch of a run lasts an hour at most; a run that pauses whole starts a fresh hour, so split longer work with a pause. `--durable` writes down what the run did before each node that is not `pure` starts, so if its worker dies another one carries the run on, with the node that was running failed, unless it is pure, had sent nothing on and was not reading a stream; `--fast` turns a trigger's `durable` back off. A run started by hand is always recorded, even when its trigger has `recorded` off. `--keep-for` (`30m`, `12h`, `7d`, `forever`) is how long the run is kept once it ends, over the trigger's `keepRunsFor` and the project's `[runs] keep_for`. A run that fires a trigger (`--fire`) is durable or fast the way the trigger's `durable` says and kept as long as its `keepRunsFor` says, and a run that fires nothing is fast; `--durable`, `--fast` and `--keep-for` override either, and `--hold-secs` how long a wait holds while the run cannot pause (a bus between its nodes open) over the trigger's `holdSecs`. `--seed[=<run-id>]` reuses finished work and `--check` only tests whether the run could start; both, with the starts, ends, stand-ins and the refusals they draw, are in the `weft-sdp` skill |
| `weft checkpoint [<label>]` | record this folder's files as a version under its head and move the head there, no run, no build; `already at <id>` when identical |
| `weft branch <version\|label\|run-id>` | put this folder on that version: its head moves there and the version's files are written into the folder (a checkpoint label names its version; a run id means its version, and makes that run the one the next `--seed` reuses from). It no longer refuses a folder with changes: it first records the folder's files as a version, then prints `this folder is now at version <id>` (`at run <run> on version <id>` for a run), any `conflict copies to look at: ...`, and `your files as they were are version <id>` with the `weft branch <id>` that comes back to them. `--copy <copy>` moves another working copy instead and leaves this folder where it is: `program` for the program copy, a folder's absolute path, or the start of a working copy's id |
| `weft resolve [<file>...] --keep folder\|incoming` | settle the conflict copies in this folder once the user has picked a side: `--keep folder` deletes them and keeps the folder's files, `--keep incoming` moves each one over its file; then it syncs, so the working copy gets the result. Name a file to settle its copies, a conflict copy to settle only that one, or nothing to settle every one in the folder. `--keep` has no default; `--keep incoming` refuses a file with several conflict copies until one is named. `--json` answers `{keep, resolved, copy}` |
| `weft forget-copy <copy>` | record another working copy's files as a version and remove the copy (named as for `--copy`), and print that version; for a folder that is gone, or the program copy (with a folder present, programs that name no working copy write into the folder). The version it records sits under the copy's old head, so the prune it clears the way for deletes it too (unless those exact files were already a version elsewhere). Refuses this folder's own copy, and a folder that still exists registers again at its next command that syncs it (a build, `weft tree`, `checkpoint`, `branch`, `freeze`) |
| `weft tree` | every working copy of the project (this folder, the project's other folders, the copy programs write into) with its head, whether its files changed since and its conflict copies; then the version tree: versions with what changed, their runs beneath, each head marked (`--json` adds `workingCopy`, this folder's working copy id) |
| `weft diff <ref> <ref> [--full]` | compare observed outputs for human or AI review, including frozen focus nodes. A ref is a run id, its unambiguous prefix or `example:<name>`. Differences are evidence and do not fail the command |
| `weft freeze <name> [<run>] [--expect <node>]...` | save that run's starting parameters and observed outputs in `examples/<name>.json`; default is head's run. `--expect` marks nodes to focus on during review. Run and diff leave the accepted file intact; freeze again after accepting its replacement |
| `weft examples` | list saved parameters and frozen examples; inspect them, rerun one with `weft run <name>`, then compare with `weft diff` |
| `weft bake [--referenced] [--trigger <node>]...` | prepare trigger inputs without arming listeners. A manual `--fire` requires a matching bake; use `--referenced` here when the run uses `--referenced`. Activation also prepares and records a bake before arming |
| `weft wake <run-id> <node>` | resolve a pure time wait now; refused for a wait expecting a value, naming its kind |
| `weft prune <version>` | delete a version, everything under it, and their runs; asks first, `--yes` for scripts. Refuses while a working copy's head is inside it (the refusal names the copy, a folder's path or `program`, and both ways out: `weft branch <version> --copy <copy>` to a version outside it, or `weft forget-copy <copy>`; after the update that brought working copies, each project's old head is a program copy, so a prune of the history holding it names `program`), while an activated version, the version the project's build runs or a frozen example's origin is inside it, while a version in it still supplies an armed trigger's settings, and while a run in the subtree is running or paused |
| `weft stop <run-id>` | cancel a run |
| `weft wait <run-id> [--timeout <duration>] [--waiting-for-input]` | wait for a run to end and say how, in a line and in the exit code (How you run a command, above) |
| `weft node-state <run-id> <node>` | where one node of a run stands: `pending`, `running`, `waiting` (with the last answer its wait read, such as an outside job's status), `completed`, `failed` (with its error), `skipped` or `cancelled`. A node inside a loop is refused: name the loop. `GetNodeState` asks the same from a program |
| `weft workers [set --min-instances N ...\| reset [lever]...]` | the project's own worker levers (copies kept warm, the most copies, how many calls Cloud Run sends one copy at once, CPU, memory, how many runs one copy takes at once, how long a call waits for room, and `--shared-idle-seconds`: how long a copy holds a pool or a process its runs share once nothing uses it, 300 by default); CPU and memory apply only on a cloud, where an unset CPU means one CPU, and on this machine no worker is capped; what it leaves unset follows the install, and calls after a change go to workers that have it |
| `weft status` | registration, build state, listener, infra, drift, how many runs each trigger started in the last minute or two and how many failed, and the project's own address (or why it is unavailable) |
| `weft ps` | every registered project |
| `weft runs [--limit N] [--project <id>] [--phase fire] [--status <status>] [--node <node>] [--since <duration>] [--instance <id>] [--tag <tag>]` | past runs, newest first; `--node` (the node that started it) and `--since 10m` find yours among many, `--status failed` the ones that broke (see Reading a run, in the `weft-debugging` skill) |
| `weft events <run-id> [--node <id>] [--kind <kind>] [--iteration <n>] [--full] [--json]` | a run's events in order, one compact line each (see Reading a run, in the `weft-debugging` skill) |
| `weft logs [run-id]` | a run's log (no argument: the latest run of the project in the current directory; see Reading a run, in the `weft-debugging` skill) |
| `weft follow <project>` | live events for a project |
| `weft activate [--held run\|drop] [--port <n>]` | build, then turn on every trigger in its reach that is off (with no flag the shared ones; Whose triggers, below), new ones included, and leave the ones that are on alone (with nothing off it says so and succeeds). It prints the project's own address, where its routes answer at the root (a local port here, the Cloud Run address on a cloud). It never starts [infra] and never touches a run going now. `--held` is what happens to the work the triggers held while they were down (Taking triggers down, below) |
| `weft deactivate [--door ...] [--held ...] [--running ...]` | take triggers down, answering the three questions under Taking triggers down |
| `weft instances` | the project's instances: each one weft holds anything for, how many of its triggers are on, how many of its infra copies are up, and its last run |
| `weft resync [--door ...] [--held ...] [--running ...] [--resume]` | build, then move every trigger that is on and behind the build onto it: each goes down, the runs going on the old version finish (or are cancelled), and it comes back on the new version. A trigger that is off is never turned on: the answer names it with the `weft activate --trigger` that would. With nothing behind, it says so and succeeds. Which triggers it reaches is under Taking triggers down |
| `weft infra start` / `status` / `stop` / `upgrade` / `terminate` / `cancel` / `look` / `logs` / `show` / `press` / `env` / `list-doors` / `rebake` | the project's [infra] (see The infra verbs) |
| `weft connect-lib` | copy weft's connect library into the frontend (`front/src/lib/weft-connect`, `--into <dir>` for another folder) so its pages show the editor's connection pickers; the `weft-frontend` skill has when and how. Replaces that folder each run |
| `weft token mint` / `ls` / `revoke` | signal tokens: scoped access for an outside listener such as the browser extension. `mint` prints the connect URL, then the bare token on its own line for a script |
| `weft daemon start` / `status` / `logs` | [the daemon]; only `status` and `logs` are yours |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's stdlib |
| `weft options <node> <field> --search <text>` | the choices a field with a searchable list offers (a model id, a spreadsheet), the same list the editor shows, through the node's picked connection; write the id it prints. Before you write such a value as a default (`@instance_filled("...")` included), check it here: nothing else checks it before a run fails |
| `weft describe-nodes --list` | one line per node type; how you find one |
| `weft describe-nodes --node <Type> --compact` | one node's wiring view; read it before wiring. With no flags you get the whole catalog as JSON, which is large |
| `weft test-node <target>` | run node self-tests (`--tier live` spends money, asks first) |
| `weft connect` | the editor's Connect panel as a CLI verb: `--list` the stored connections, `--node <id> --grant <id>` to pick one for a node, connect new accounts through both doors, `--upgrade`, `--forget`, `--disconnect`; `--instance <id>` does the same for one instance of the program on a node whose connection is `@instance_filled`, the way its settings page would |
| `weft instance-values --instance <id> [--set node.field=value]... [--clear node.field]...` | what the program asks that instance to fill and what was given; with `--set` / `--clear`, changes it in one go (its live triggers reading a changed value are set up again). Through an instance token it mints and revokes |
| `weft rm [--journal] [--local] [--all] --yes` | remove the project: it takes the project down first (every trigger off with nothing kept, every run cancelled), follows that in short looks until it is over, then unregisters it, terminates its [infra] and reclaims its data. `--journal` also drops its run history, after waiting for every run to end; `--local` its build artifacts; `--all` implies every flag. Asks first; pass `--yes`, and only after the user confirmed. Ctrl+C stops following, leaving the project down, and `weft rm` again picks the removal up. If it fails partway, the project stays marked as being removed until `weft rm` runs again in the project folder, which is the only way to finish it: the editor shows ``the project is being removed; run `weft rm` again to finish removing it`` as a banner and offers no button, that one included. Until then no new run starts and `weft ps` lists it that way. `weft status` still answers, with that line as a `WARNING:` above what is left of the project; run, activate, resync, the infra commands and every build refuse with it |
| `weft clean --yes` | journal and image cleanup, per subject, and naming a subject takes all of it: a run id takes that one run, `--project <id>` takes a whole project's history (removing a project leaves its runs behind, so this is how you erase them), no subject takes everything older than `--keep-days` (30 by default), `--all` takes the lot. A version the deletion left bare (no runs, nothing under it, no checkpoint name, not head) goes with the runs; a named checkpoint never does. `--images` and `--build-cache` touch no journal rows. Deleting runs asks first; pass `--yes`, and only after the user confirmed |

How a run picks which nodes run is in the `weft-language` skill. A
trigger fires on its own event only once the project is activated; to try a
trigger's program before activating, `weft bake`, then `weft run --fire
'<trigger>=<wake-json>'` (the `weft-sdp` skill).

## The build and run flow

`weft run` compiles, registers the project with [the daemon], builds the
worker if sources changed, then fires. The install does the building, never
you: on this machine it compiles the worker with the machine's own Rust
toolchain, and a project whose nodes are all the standard library's,
unchanged, compiles nothing in a full build (they share the standard
worker; `--referenced` compiles). A build that fails saying `cargo` is not
on the PATH means the machine has no Rust: the user installs it from
rustup.rs and runs `./setup.sh` again. Compile failures print `compile
failed:` then `line:column message` lines. HTTP errors surface [the
daemon]'s own message verbatim.

On this machine the worker is a plain process running in the project's
folder, as the user, so a node reaches their files and the programs on the
`PATH` the install had when `./setup.sh` last ran. If a node the program uses needs a program the machine lacks,
`weft run`, `weft activate` and `weft resync` refuse before anything starts
with `this program uses nodes that need things the machine its workers run on doesn't have:`,
the node and what it needs, then `install them with:  weft deps-install
<NodeType>`. Only what a node needs while it runs is checked there; what its
package needs to compile matters only while it compiles, and `weft
deps-install` installs both. `weft test-node --tier live` checks the tested
nodes' run needs the same way before it starts. That command prints the system's install command (`sudo
apt-get install -y ...`, `brew install ...`) and asks before running it;
`--print` only prints it, `--yes` runs it without asking.

`weft build` skips [the runtime tier] (a connection not yet picked), so a
half-wired program still builds. A run is refused before it starts when a
node it reaches needs a connection nobody picked on this install, naming
each one with the command that picks it (`'slack' has no slack connection
picked on this install; pick one with weft connect --node slack`). `weft
validate --file src/main.weft < src/main.weft` reports the same gaps as
`rule-runtime` findings in seconds, before you run. It also warns about
every catalog folder it left out and why (a broken node folder, say), in its
JSON's `warnings` and on stderr; those never fail validation, so read them
when a node you expected is missing. The fix is a picked
connection (`weft connect --node <id> --grant <id>`, or the user on the
node's Connect button), never a source edit.

Activate and resync refuse the same way while any connection the program
picks on the install is missing. They also name picks left under an old
address when a node moved (into a group, into another file): `weft connect
--move OLD NEW` moves the install's picks and every instance's values to the
new address.

A build that changed an infra node's image says so: a copy started from then
on gets the new image, and a copy already running keeps its own until `weft
infra upgrade` (`--instance <id>` for one instance's copy).

Nothing starts [infra] but `weft infra start`. On a program with [infra] it
is the very first command, since it is also what builds the node's images.
A run that reaches [infra] that is not running is refused, from the CLI and
from the editor, naming the command that starts it. `weft activate`, `weft
resync` and `weft bake` refuse the same way when a trigger's setup reads
that [infra] (`these triggers' infra is not running: <node>. Start it with
weft infra start, then run this again.`). When only its fires use it,
`weft activate` turns the trigger on and warns `infra not running: <node>.
Activating does not start it; if anything uses it, run weft infra start`,
and a fire that needs that [infra] waits until it runs. All of that is the
order being kept, not a bug or a gap in weft, so none of it goes in a report
as friction.

### When files change, or part of the program does not compile

The project folder is a working copy: the install holds its own copy of
the project's files, and the two are kept the same both ways. On a local
install the runtime does this as you go: a save reaches the install about a
second after the folder goes quiet, and a file a program writes into the
project's code appears on disk. On a cloud install nothing watches the
folder. Either way, every command that builds, and `weft tree`, `weft
checkpoint`, `weft branch` and `weft freeze`, first syncs the folder and says
what moved (`sent your changes to the install: src/triage.weft`, `brought the install's
changes into this folder: ...`).

A save never records a version. A version is recorded when something is
done with the code: `weft run`, `weft build`, `weft activate`, `weft bake`,
`weft resync`, `weft infra start` and `weft infra upgrade` record the folder's files as a version and
build that, and so do `weft checkpoint` and `weft branch` (without the
build). Files that did not change are the version they already were, so
nothing new is recorded. A file you or a specialist save while a command
builds is not in that build; the next command takes it.

If a file changed both in the folder and in the install, the folder's file
stays and the install's is written beside it as
`<stem>.weft-conflict-<8 hex>.<ext>` (`prompts/tone.weft-conflict-1a2b3c4d.txt`),
with the warning `<path> changed both here and in the install: your file is
kept, and the install's is written beside it as <copy>`, followed by the two
`weft resolve` commands that settle it. A conflict copy is never compiled and
never part of a version, and nothing waits on it: builds and runs go on with
the folder's file. Which side stays is the user's call, so tell them the
conflict copy is there and what differs, and ask. Once they pick, run `weft
resolve <path> --keep incoming` to put the install's in place or `weft
resolve <path> --keep folder` to keep the user's file and delete the copy. If the user had deleted the
file, `--keep incoming` brings it back and `--keep folder` keeps it deleted.

If the install removed a file the folder changed, the warning is `<path>
changed here and was removed in the install: your file is kept; delete it if
you want the removal`. No conflict copy is written, so `resolve` has nothing
to settle; if the user wants the removal, delete the file.

On a local install, a project folder on a Windows drive under WSL
(`/mnt/c/...`) is refused, because WSL tells Linux nothing of the changes
made there, so weft cannot keep it in sync. The fix is to move the project
into the Linux filesystem (under `~/`) and run the command again from there.

A part of the program that does not compile stops only what needs it. A
part is an included file that does not parse or whose path leads nowhere, a
node whose type no folder declares, a node whose `@file` is missing, or a
wire whose types do not fit. A command whose run, triggers or [infra] never
reach the part does its work and then prints one warning per part: `does
not compile, so nothing that needs it runs: src/report.weft:12:3 <message>`.
A command that does reach it is refused, naming each file and line: `it
needs parts of the program that do not compile:` and the list. What still stops everything is `src/main.weft` itself not parsing,
and the few errors no part can be blamed for: the command then fails with
`compile failed:` and the list. `weft validate` stays strict and reports every
error in the program, wherever it sits.

`weft activate` refuses a trigger whose FIRES would reach a broken part,
even when its setup would not: `a fire of 'post' cannot run: it needs parts
of the program that do not compile: ...`. A resync bringing a trigger back
checks the same.

While anything is broken, weft takes nothing down for being missing from
the source: [infra] declared inside a file that does not compile keeps
running, and `weft status` says `live infra exists whose node the source
does not declare (removed from it, or inside a part that does not
compile)`. Read that line as "hidden behind a broken file", never as
"gone": fixing the file brings the node back, and a broken include is never
a way to remove [infra] (`weft infra stop` and `terminate` are).

A node package whose Rust does not compile is left out of the worker, and
the rest of the program builds and runs. The build warns `the worker was
built without it, so nothing that uses its nodes runs: package 'shout'
didn't compile: <first error>; its full errors are in the build log: <url>`,
and a run, fire or setup that reaches one of its nodes is refused with that
same reason. The package is compiled again once its source changes.

## Taking triggers down, and bringing them back

A program with triggers listens only after `weft activate`. Every command
that takes triggers down (`weft deactivate`, `weft resync`, `weft infra
stop`, `terminate` and `upgrade`) answers the same three questions, in this
order, each with a default, and prints what it did. Nothing is asked:

| Flag | Decides | Default | The other answers |
|---|---|---|---|
| `--door take\|hide\|refuse` | the work arriving while the trigger is down: an event, a form sent, an answer to a waiting run, a schedule's tick | `take`: held, and run once the trigger is back, on the version it comes back with; a question stays listed for the people who answer it | `hide`: held, out of sight of the people who answer questions. `refuse`: turned away at once. With `take` or `hide`, `--door-for <minutes>` refuses once that long has passed |
| `--held keep\|drop` | the work already waiting: what the trigger held from an earlier take-down, and the runs it fired that are paused on a person or a timer | `keep` | `drop`: deletes the held work and cancels the paused runs |
| `--running wait\|cancel` | the runs going right now | `wait`: they finish on the version they started on, and the command goes on only once they have, however long that takes | `cancel`: they stop now |

A caller on a route is never held: while its trigger is down or still coming
on, it is answered `503` with `Retry-After: 5` (for what a client does with
that, go and read the `weft-api` skill).

The defaults lose nothing, which is right for a program people use. While
you are building, add `--running cancel` when the runs still going are your
own tries, so the command does not wait on them. `--held drop` and `--door
refuse` throw away work, which may be real people's: pass them only when
the user said so, or when `weft runs` shows nothing waiting but your own
tests.

**Whose triggers.** With no flag, a command acts on the program's own
triggers, the shared ones. `--instance <id>` (repeatable) names instances,
`--all-instances` takes every instance's, and `--all` takes the program's
own and every instance's in one command; `--trigger <node>` (repeatable)
narrows any of them. That holds for `activate`, `deactivate`, `resync`,
`bake` and `cancel-activate`, and the same three instance flags work on the
infra verbs. A plain `deactivate` says how many instances
still have triggers on. A `deactivate` reaches only runs its triggers
fired: a run you started by hand is left alone.

**A change meant for the instances too is `weft resync --all`.** A plain
`weft resync` reaches only the program's own triggers: when instances have
triggers on and the change rebuilt the code, it is refused (next paragraph);
when only the graph changed, it goes through and leaves every instance on
the old version. `weft resync` moves only the triggers that are on and
behind the build. A trigger is behind when the part of the program its
fires run, or that part's code, or anything in front of it (a node feeding
its inputs or its gate, which runs at its setup) changed since it was turned
on. With none
behind it says so and succeeds,
and it never turns on a trigger that is off: it names the `weft activate
--trigger <node>` that would.

**One build answers a project's routes.** So `weft activate` and `weft
resync` refuse to leave the triggers that are on split across two builds:
`one binary answers a project's routes, and these triggers are on another
build than the registered one: <list>. weft resync --all moves every trigger
that is on to the new build; then run this again`. After you change the
program and want a new trigger on, that is `weft resync --all`, then `weft
activate`. A trigger the health check or an infra command took down, which
cannot come back for that same reason, reads `held back` in `weft status`
and in the editor, naming the others, and `weft resync --all` brings it
back at once.

**Bringing them back.** `weft activate` turns on what is off and releases
what the triggers held: `--held run` (the default) runs it, paced at half
each trigger's own limits so new arrivals still get through; `--held drop`
deletes the held arrivals and cancels the runs paused on them (here `--held`
takes `run` or `drop`; on a take-down it takes `keep` or `drop`). Before you
run `weft activate` on a program people use, read `weft status`: if a
trigger shows `holding ...`, that work is people's, so tell the user how
much waits and ask whether to run it or drop it. A trigger an infra command or
the health check took down comes back by itself once its [infra] runs and
is healthy; one taken down with `weft deactivate` comes back only with
`weft activate`.

`weft status` says how each trigger stands: `on`, `turning on`, `on, behind the source`,
`off, its door set to take` (or `hide`, `refuse`), followed after a
take-down by `taken down with its infra (it comes back once that infra runs
again)` or `taken down by the health check (...)`, then `held back: <why>`
when one cannot come back yet, and `holding ...` when it holds work. `weft status --json` lists every trigger with its instance under
`activations`; a trigger being turned on reads `activating` from the moment
the activate is taken, and an instance's trigger holding events until a
field is filled carries `waiting` (`fires` and the `reason` naming the
field).

## The infra verbs

[infra] is a container the runtime keeps running for the program (an infra
node: `PostgresDatabase`, `BaileyBridge`), with a disk that survives
restarts. With no flag a verb acts on every shared infra node; `--infra
<node>` (repeatable) names some, `--instance <id>` or `--all-instances`
acts on the copies of a node marked `@per_instance`, and `--all` on the
shared ones and every instance's (what a deploy runs). You pick the verb by
what you want to keep:

- **If the [infra] is not running yet**, or was stopped: `weft infra start`.
  It starts what is not running and nothing else (with `--infra <node>`,
  that node and whatever it needs upstream), and waits until it is ready.
  The triggers that came down with the [infra] come back on by themselves
  once it is up; one switched off with `weft deactivate` stays off.
- **If you changed an infra node's spec** (image, env, volumes) and want it
  live: `weft infra upgrade`. It builds first, then replaces only the units
  running on an older build, keeping every disk; it starts nothing that is
  stopped and makes nothing missing (`weft infra start` does both).
- **If you want it off for a while and the data kept** (a paired WhatsApp
  session, a database's rows): `weft infra stop`. The containers go, the
  disks stay, and `weft infra start` brings it back with everything in it.
  `--force` also brings down the units whose spec keeps them up through a
  stop.
- **If you want it gone** (the database and its rows, the bridge and its
  pairing): `weft infra terminate`. Every resource is deleted, disks included
  unless the node's own spec keeps them. There is no undo: a database
  comes back empty on the next start, and a bridge needs its QR scanned
  again. It asks for confirmation on a terminal; from your commands (no
  terminal, or `--json`) it needs `--yes`, which you pass only after the user
  confirmed.
- **If a verb is stuck mid-way**: `weft infra cancel` stops it between
  steps; whatever it already did stays done.
- **If a baked output went stale** (one marked `"baked": true`, which weft makes when the infra is applied and saves; it has nothing to do with `weft bake`), say a password reset whose report to weft never arrived: `weft infra rebake <node>` runs the node's infra setup again and saves those outputs. A run that reads nothing else from the node uses the saved values instead of running it, and a node that fails using a connection an infra node made names this command. If you changed what the node's code emits there, runs that skip the node keep getting the old saved value until it bakes again. If you want that without restarting anything, run `weft build` (or any `weft run`), then `weft infra rebake <node>`, which runs what was built last. `weft infra upgrade` also bakes again, but it replaces the running units to do it. For a `@per_instance` node, add `--instance <id>` for each copy.
- **If you want to see what an infra node shows**: `weft infra show <node>`.
  A node's card can hold items (a status, an address, a secret) and buttons.
  It prints every item and every button, with the action you hand to `weft
  infra press`, and never prints a secret's value.
- **If something outside the program needs values the [infra] shows** (a
  frontend's database user and password): `weft infra env <node> --into
  <file> --set DATABASE_USER=User --set DATABASE_PASSWORD=Password` writes
  them all into that env file in one go, each `NAME=Label` taking the card
  item under that label as `weft infra show` prints it. `--as <NAME>` is the
  short form for the card's only secret. A new file is readable by the user
  only; an existing one has only those lines changed. A secret's value is
  never printed, the other values are. Nobody edits the file by hand for a
  value the card shows. If the secret is still on the card, that is all you
  need.
- **If the secret was already handed over**, `env` refuses and says what
  the card shows instead. The card usually has a button that issues a new one:
  `weft infra press <node> <action>` presses it (any card button, in fact),
  then run `env` again. A button's warning (`weft infra show` prints it) says what the press breaks
  (a new password cuts off everything holding the old one), so read it, and
  ask the user before pressing a button that breaks something in use, unless
  they already told you to.

`stop`, `terminate` and `upgrade` first take down the triggers whose fires
reach that [infra], with the three answers under Taking triggers down, and
those triggers come back by themselves once it runs again. A trigger whose
fires never reach it keeps listening throughout. The runs that reach it,
including runs you started by hand, are waited for (`--running wait`, the
default, which keeps the [infra] up until they end) or stopped (`--running
cancel`, your answer when it has to go now). Shared [infra] reaches every
trigger and run that uses it, instances' included; an instance's copy only
that instance's.

**The health check.** Each infra node carries health rules (its own, or two
built-in ones), and weft applies them per copy. By default, when a copy's
units turn flaky, the triggers whose fires reach it go down, and they come
back once its units are healthy again (the rules themselves are in the
`weft-node-authoring` skill). `weft status` then says `taken down by the health check` on
those triggers. A rule that keeps failing shows under `waiting on weft:` (the
`weft-debugging` skill), and `weft infra look` asks the supervisor to look
at the project's [infra] again, giving a rule that gave up another try.

`weft infra status` says per copy that exists whether it is running and its
endpoint; `weft status` lists under `infra:` every infra node the program
declares, one never started as `not started`, so an empty list there means no
node needs [infra]. Every status field (a display's, `InstanceInfraStatus`)
reads `none` for a copy never started or terminated. A start or stop on its way reads `provisioning` or
`stopping` in every reader at once, before the containers move.
On a local install, a unit that asks for a GPU gets every GPU on the machine
(it cannot pick one by kind): `weft infra start`, `upgrade` and `status` warn
about it, and the status JSON carries it in `nodes[].notes`.
`weft infra logs <node>` (or no node, for all) prints what the containers
wrote, `--tail N` and `-f` as for a run: a failure inside a service is read
there.

## Triggers and public reachability

A trigger is any node carrying `isTrigger: true`, and how it reaches its
events decides what a restart does to it. Three behaviours, and the node's
own description says which one it is only sometimes, so the shapes matter
more than any list of names:

- It POLLS, asking the service what is new. It keeps a cursor, starts from
  activation time, and never replays history (Telegram, Gmail, sheets,
  notion, airtable and RSS all work this way).
- It HOLDS a connection open, or recomputes its next moment on every fire.
  No cursor either way, so anything that happened while it was down is
  gone: `ReceiveEmail` watches over IMAP IDLE, and `Cron` works out its
  next tick from now, so missed ticks do not fire late.
- It waits to be PUSHED at, which needs the runtime reachable from the
  internet. The user opens that with `./setup.sh --public-url` (the
  `weft-public-address` skill walks them through it), and the trigger's
  live feed in the graph then shows its real URL. Signal tokens
  (`weft token mint`) scope external listeners like the browser extension (the `weft-people-in-the-loop` skill).
