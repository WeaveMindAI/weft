---
name: weft-running
description: "Read when running or activating a program: the CLI command map, the daemon (never yours to restart), running a command so it never asks and never sits quiet (with how long each normally takes), the build and run flow, trigger activation and the three deactivate modes, the infra lifecycle, and what each kind of trigger does across a restart. A run that failed or did the wrong thing is the weft-debugging skill."
---

# Running

The `weft` CLI is a thin client of [the daemon] plus the front end for
building. [the daemon] is the dispatcher process that owns projects,
executions, triggers, and infra. It listens on `http://127.0.0.1:14111`
unless the user started it on another port, and the port it really runs
on is the `public` field of `~/.local/share/weft/ports.json`.
Every command talks to that one unless it is given `--on <target>`, a cloud
install named in `weft.toml`, which only the `deployer` does (the
`weft-deploying` skill). The user's install (`setup.sh`) starts it, once per
machine.
`weft daemon start`, `stop` and `restart` re-run that install, and a restart
from a project has wiped shared keys before, so you never run them. A `.env`
near the project auto-loads.

Each run is one execution, with its own id (a UUID minted when it starts). Everything in it
is journaled, node by node, with the values on the wires. Every command that
takes an execution id also takes its first characters (`weft events 3f2a`), at
least four, as long as they name a single run.

## How you run a command

Every command you run cannot ask you anything. These are the ones that ask,
with the answer already in them. While you are building, you type them as
they are written here:

| Instead of | type |
|---|---|
| `weft resync` | `weft resync --mode wipe` |
| `weft deactivate` | `weft deactivate --mode wipe` |
| `weft rm`, `weft clean`, `weft prune` | the same with `--yes`, once the user has said yes |
| a long `weft run` | `weft run --detach` |

`--mode wipe` is the answer while you are building; `hibernate` and `park`
are for a program people are using, and the three modes below say when. A
command that asks anyway gets killed and reported with what it asked.

A command that changes the deployed code and the thing you do next go in ONE
line, joined by `&&`: `weft resync --mode wipe && curl ...`. Run them
separately and a resync that failed leaves the next command talking to the
old code, which reads as a bug in what you just wrote.

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
| Reads: `weft status`, `executions`, `events`, `logs`, `describe-nodes`, `parse`, `validate` | under 5 seconds | 15 seconds |
| `weft run` of a program whose nodes are all built already | a few seconds | 30 seconds; `--detach` for anything that waits on a person, a timer or a long step |
| The first `weft run` or `weft activate` that compiles a node of the project's own | 1 to 3 minutes | 3 minutes, looking every 30 seconds |
| `weft activate`, `resync` or `deactivate` with nothing to build | under 30 seconds | 30 seconds |
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
`weft daemon logs`, which is where a wait says what it is waiting on. The
one wait that used to hide here was `activate` sitting behind a worker
from an older build; it no longer waits by default (see `--running-policy`
under the command map), and `weft status` reports an activation still in
flight, which `weft cancel-activate` ends before you run the verb again.
You raise a number only once you can say what is taking the extra time,
and a run that is longer than its shape says is something you report.

If you want to wait on something long (a build, [the daemon] coming up, a run
settling), start it detached and check its state between other steps instead
of writing a loop: `weft run --detach` hands you the execution id, then `weft
executions --json` for a run and `weft status --json` for a build or [the
daemon].

If you do write a loop, it is this line and no other, cap included:

```bash
timeout 30 bash -c 'until <check>; do sleep 5; done'
```

When the cap trips you read the state and say what it is waiting on. A run
parked on a timer or a person never finishes on its own, so an uncapped
`until` on one hangs until somebody kills it, which has happened.

Before you write any loop, say what will make the check true, and check that
it is not you. A loop waiting for the open runs to reach zero while you keep
starting runs never ends. When nothing outside the loop can make the check
true, do not write the loop: do the thing that ends the wait.

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
`ps`, `executions`, `events`, `logs`, `files`, `token`,
`stop`, `connect`, `tree`, `examples`, `diff`, `checkpoint`, `branch`,
`freeze`, `prune`, `wake`) print what [the daemon] answered, which you read
with `jq` instead of parsing the human columns; `test-node` prints its
reports as one JSON array. `new`, `follow`, `daemon`, `catalog`, `clean` and
`update` ignore it.

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
`weft events --node`, `weft wake`, `weft freeze --expect`, `weft infra
node-stop`, `weft infra node-terminate`, `weft infra logs` and `weft token
mint --display` take, and what `weft events`, `weft executions`, `weft infra
status` and the graph print back.

## The command map

| Command | What it does |
|---|---|
| `weft build` | compile, resolve assets, build the worker image (content-addressed) and register the project with [the daemon]. Starts nothing. You never need it while building a program: `weft run`, `weft activate` and `weft resync` build on their own, so running `weft build` before them only builds twice. Its one use is the final deployment, `weft build --referenced`, which ships only the node types the program uses. Also the repair when [the daemon] no longer holds the code a past run ran: registering records the compiled program under its own hash, the one the run names, so unchanged files make the run readable again. Adds nothing to the version tree; `weft checkpoint` does that |
| `weft validate --file src/main.weft < src/main.weft` | strict compile + validate, diagnostics as JSON, nothing runs |
| `weft run [<example>] [--referenced] [--seed] [--root] [--from <node>=<ports-json>]... [--emit <node>=<ports-json>]... [--target <id>]... [--before <id>]... [--group <id>=<ports-json>] [--fire <trigger>=<wake-json>] [--save <name>] [--durable|--fast] [--keep-for <duration>] [--hold-secs <seconds>]` | build and start one execution; `--detach` returns its execution id. On a cloud install one stretch of a run lasts an hour at most; a run that pauses whole starts a fresh hour, so split longer work with a pause. `--durable` writes down what the run did before each step of a node that is not `pure`, so if its worker dies another one carries the run on, with the step that was running failed, unless its node is pure, had sent nothing on and was not reading a stream; `--fast` turns a trigger's `durable` back off. A run started by hand is always recorded, even when its trigger has `recorded` off. `--keep-for` (`30m`, `12h`, `7d`, `forever`) is how long the run is kept once it ends, over the trigger's `keepRunsFor` and the project's `[runs] keep_for`. A run that fires a trigger (`--fire`) is durable or fast the way the trigger's `durable` says and kept as long as its `keepRunsFor` says, and a run that fires nothing is fast; `--durable`, `--fast` and `--keep-for` override either, and `--hold-secs` how long a wait holds while the run cannot pause (a bus between its nodes open) over the trigger's `holdSecs`. `--from` supplies backup inputs at a start, `--emit` supplies outputs without executing that node. Real producers take precedence over backups, and under `--seed` the earlier run's result IS a real producer: a `--from` value at a node history already feeds goes unused (the run warns). To hand a new value in, run without `--seed`, or `--emit` the upstream output. `--target` includes the endpoint; `--before` excludes it. `--group` selects a whole group or included file, with its input payload. `--fire` runs exactly one trigger, using the settings a matching `weft bake` saved. Ordinary groups can be cut precisely; loops stay whole, and a cut inside one is refused on purpose ("select the whole loop"). If you want to try one node that lives inside a loop, copy it to a scratch spot outside the loop (or a scratch project), run it there with the inputs you want, then put it back. `--seed-before` / `--seed-until` bound compatible reuse. A named example supplies saved starting parameters; current code runs. Clear and replacement rules are in `weft-sdp` |
| `weft checkpoint [<label>]` | record the files as a version under head, no run, no build; `already at <id>` when identical |
| `weft branch <version\|label\|execution-id>` | restore that version's files and move head (a checkpoint label names its version; an execution id makes that run the next seed). Refuses on a dirty tree naming the files; `--discard` overrides |
| `weft tree` | the version tree: versions with what changed, their runs beneath, head marked (`--json` adds `disk_version`) |
| `weft diff <ref> <ref> [--full]` | compare observed outputs for human or AI review, including frozen focus nodes. A ref is an execution id, its unambiguous prefix or `example:<name>`. Differences are evidence and do not fail the command |
| `weft freeze <name> [<run>] [--expect <node>]...` | save that run's starting parameters and observed outputs in `examples/<name>.json`; default is head's run. `--expect` marks nodes to focus on during review. Run and diff leave the accepted file intact; freeze again after accepting its replacement |
| `weft examples` | list saved parameters and frozen examples; inspect them, rerun one with `weft run <name>`, then compare with `weft diff` |
| `weft bake [--referenced]` | prepare trigger inputs without arming listeners. A manual `--fire` requires a matching bake; use `--referenced` here when the run uses `--referenced`. Activation also prepares and records a bake before arming. Takes `--running-policy` like `activate` (the setup runs on a worker, and a stale one is replaced first) |
| `weft wake <execution-id> <node>` | resolve a pure time wait now; refused for a wait expecting a value, naming its kind |
| `weft prune <version>` | delete a version, everything under it, and their runs; asks first, `--yes` for scripts. Refuses on head's version, under a frozen example's origin, and while a run in the subtree is running |
| `weft stop <execution-id>` | cancel an execution |
| `weft workers [set --min-instances N ...\| reset [lever]...]` | the project's own worker levers (copies kept warm, the most copies, how many calls Cloud Run sends one copy at once, CPU, memory, how many runs one copy takes at once, how long a call waits for room, and `--shared-idle-seconds`: how long a copy holds a pool or a process its runs share once nothing uses it, 300 by default); CPU and memory apply only on a cloud, where an unset CPU means one CPU, and on this machine no worker is capped; what it leaves unset follows the install, and calls after a change go to workers that have it |
| `weft status` | registration, build state, listener, infra, drift, how many runs each trigger started in the last minute or two and how many failed, and the project's own address (or why it is unavailable) |
| `weft ps` | every registered project |
| `weft executions [--limit N] [--project <id>] [--phase fire]` | past executions, newest first (see Reading a run, in the `weft-debugging` skill) |
| `weft events <execution-id> [--node <id>] [--kind <kind>] [--iteration <n>] [--full] [--json]` | a run's events in order, one compact line each (see Reading a run, in the `weft-debugging` skill) |
| `weft logs [execution-id]` | a run's log (no argument: the latest execution of the project in the current directory; see Reading a run, in the `weft-debugging` skill) |
| `weft follow <project>` | live events for a project |
| `weft activate` / `weft deactivate` | turn triggers on / off; `activate` prints the project's own address, where its routes answer at the root (a local port here, the Cloud Run address on a cloud). `deactivate` on an active project takes `--mode <wipe\|hibernate\|park>` (a [mode], defined under The three modes), `wipe` when you leave it out. Both take `--running-policy <cancel\|wait>`, default `cancel`: on `activate` it says what happens to a worker still up from an older build (cancel what it runs and replace it now, or `wait` for its executions to land, up to `--drain-timeout` seconds); on `deactivate` the same for the project's running executions |
| `weft resync` | deactivate + activate against a fresh build, after editing a trigger subgraph. Only while some trigger is on, the program's or an instance's (with none on it refuses: `weft activate` first), and it takes the same `--mode` as `deactivate`, `wipe` when you leave it out |
| `weft infra start` / `status` / `stop` / `upgrade` / `terminate` / `cancel` / `logs` / `show` / `press` / `env` / `rebake` | the project's [infra] (see The infra verbs) |
| `weft connect-lib` | copy weft's connect library into the frontend (`front/src/lib/weft-connect`, `--into <dir>` for another folder) so its pages show the editor's connection pickers; the `weft-frontend` skill has when and how. Replaces that folder each run |
| `weft token mint` / `ls` / `revoke` | signal tokens: scoped access for an outside listener such as the browser extension. `mint` prints the connect URL, then the bare token on its own line for a script |
| `weft daemon start` / `status` / `logs` | [the daemon]; only `status` and `logs` are yours |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's stdlib |
| `weft options <step> <field> --search <text>` | the choices a field with a searchable list offers (a model id, a spreadsheet), the same list the editor shows, through the step's picked connection; write the id it prints. Before you write such a value as a default (`@instance_filled("...")` included), check it here: nothing else checks it before a run fails |
| `weft describe-nodes --list` | one line per node type; how you find one |
| `weft describe-nodes --node <Type> --compact` | one node's wiring view; read it before wiring. With no flags you get the whole catalog as JSON, which is large |
| `weft test-node <target>` | run node self-tests (`--tier live` spends money, asks first) |
| `weft connect` | the editor's Connect panel as a CLI verb: `--list` the stored connections, `--node <id> --grant <id>` to pick one for a node, connect new accounts through both doors, `--upgrade`, `--forget`, `--disconnect`; `--instance <id>` does the same for one instance of the program on a node whose connection is `@instance_filled`, the way its settings page would |
| `weft instance-values --instance <id> [--set node.field=value]... [--clear node.field]...` | what the program asks that instance to fill and what was given; with `--set` / `--clear`, changes it in one go (its live triggers reading a changed value are set up again). Through an instance token it mints and revokes |
| `weft rm [--journal] [--local] [--all] --yes` | unregister the project, terminate [infra], reclaim data. `--journal` also drops its run history, `--local` its build artifacts, `--all` implies every flag. Asks first; pass `--yes`, and only after the user confirmed |
| `weft clean --yes` | journal and image cleanup, per subject, and naming a subject takes all of it: an execution id takes that one run, `--project <id>` takes a whole project's history (removing a project leaves its runs behind, so this is how you erase them), no subject takes everything older than `--keep-days` (30 by default), `--all` takes the lot. A version the deletion left bare (no runs, nothing under it, no checkpoint name, not head) goes with the runs; a named checkpoint never does. `--images` and `--build-cache` touch no journal rows. Deleting runs asks first; pass `--yes`, and only after the user confirmed |

How a run picks which nodes execute is in the `weft-language` skill. A
trigger fires on its own event only once the project is activated; to try a
trigger's program before activating, `weft bake`, then `weft run --fire
'<trigger>=<wake-json>'` (the `weft-sdp` skill).

## The build and run flow

`weft run` compiles, registers the project with [the daemon], builds the
worker image if sources changed (Cargo runs inside Docker, never on the
host), then fires. Compile failures print `compile failed:` then
`line:column message` lines. HTTP errors surface [the daemon]'s own message
verbatim.

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

A program with triggers listens only after `weft activate`. Each trigger is
turned on and off on its own: `--trigger <name>` on `activate`, `deactivate`,
`resync`, `bake` and the cancel verbs acts on that trigger alone, and without
it they act on every shared trigger. A deactivate drains or cancels only the
runs its triggers fired; a run started by hand is left alone. A program with
instances (the `weft-instances` skill) adds `--instance <id>`: a plain `weft
activate` turns on the shared triggers and names the per-instance ones it
left off. Activating triggers that are already on says so and points at
`weft resync`, which is what a source change needs.

Activate and resync refuse the same way while any connection the program
picks on the install is missing. They also name picks left under an old
address when a node moved (into a group, into another file): `weft connect
--move OLD NEW` moves the install's picks and every instance's values to the
new address.

A build that changed an infra node's image says so: a copy started from then
on gets the new image, and a copy already running keeps its own until `weft
infra upgrade` (`--instance <id>` for one instance's copy). An edit to a
trigger's subgraph takes effect only after `weft resync`, and a plain `weft
resync` brings every trigger that is on up to the new code, each instance's
included. A plain `weft deactivate` leaves instances' triggers on and says whose
are still on; `weft deactivate --all-instances --mode <mode>` takes theirs down.
`weft status --json` lists every trigger with its instance under `activations`;
a trigger being turned on reads `activating` from the moment the activate is
taken, and an instance's trigger holding events until a field is filled carries
`waiting` (`fires` and the `reason` naming the field).

`project files changed while building; run the command again` means a file of
the project was written while the build was reading it, which is almost always
a helper of yours writing into `nodes/` at the same time. Nothing is broken:
run the command again. What matters is that the deployment did NOT change, so
the live program is still the previous one. Anything you call before a resync
succeeds is exercising the OLD code, and results from it tell you nothing
about the edit you just made.

A run that touches
[infra] is refused, from the CLI and from the editor's Run button, until that
[infra] is running: `weft infra status`, then `weft infra start`. Activating
is refused the same way, so on a program with [infra] the very first command
is `weft infra start`, not `weft activate`: it is what builds the node's
images, and nothing else does. That order is the design: `weft activate`
never starts [infra], you start it with `weft infra start` and then activate.
A refusal naming [infra] that is not running is that order being enforced,
not a bug or a gap in weft, so it does not go in a report as friction.

### The three modes

A [mode] is what happens to the work that arrives while the triggers are
down, and to the runs waiting on a person or a timer (`deactivate`, `resync`
on an active project, the [infra] verbs that deactivate on the way):

- `wipe`: everything waiting is dropped, the questions deleted, and the
  runs waiting on the triggers end cancelled; new work is refused. You pass
  it only when nothing is in flight (`weft executions` shows no suspended
  run of the project) or the user said to drop the waiting work.
- `park`: nothing is dropped. A call from outside, an answer to a waiting
  run, and every fire a trigger makes by itself (a schedule's tick, a
  message on a connection it holds) wait, and run once the triggers are
  back, on the version they come back with; someone calling a route is kept
  on the line until then. The questions stay listed for the people who
  answer them. Your pick when the user is editing and people are
  mid-conversation. A schedule parked for two hours has two hours of ticks
  waiting when it comes back.
- `hibernate`: the same as `park` for a grace window (`--grace <minutes>`,
  15 unless set), with the questions hidden (someone who already had one
  open can still send their answer). Past the window, new work is refused
  and the triggers stop listening; the waiting runs and the project survive
  (`wipe` is the [mode] that drops them).

**`wipe` is what you pass while you are building.** Nothing waiting on the
program is anyone's conversation yet, so dropping it costs nothing and the
command lands at once. `hibernate` and `park` exist for a program people are
actually using: pass one when the user says so, or when `weft executions`
shows a run parked on a person you would be throwing away.

`--running-policy cancel` (the default) stops the executions already running
now; `wait` lets them finish first, new fires held meanwhile, and you pass
it only when the user asked for the running work to land. A `wait` waits
under `hibernate` and `park` alike (the mode says what happens to the parked
work, the policy what happens to the running work: two separate answers),
and it ends by cancelling whatever is still running at its cap
(`--drain-timeout`, 60 seconds unless set); under `wipe` waiting is refused.
With no `--mode` and no terminal (which is every command you run),
`deactivate` and `resync` use `wipe`. That is the right answer while you are
building, so you rarely type it; you type `--mode hibernate` or `--mode park`
when the program is one people are using and the work in flight has to
survive. The [infra] verbs never pick for you: when a trigger reads the
[infra], they stop and name `--mode` and `--grace`, and you run them again
with one.

## The infra verbs

[infra] is a container the runtime keeps running for the program (an infra
node: `PostgresDatabase`, `BaileyBridge`), with a disk that survives
restarts. You pick the verb by what you want to keep:

- **If the [infra] is not running yet**, or was stopped: `weft infra start`.
  It brings every unit up to its spec and waits until ready. Running it again
  does nothing for units already up. The triggers that came down with the
  [infra] (a stop, a terminate) come back on by themselves once it is up; one
  switched off with `weft deactivate` stays off.
- **If you changed an infra node's spec** (image, env, volumes) and want it
  live: `weft infra upgrade`. Each unit whose spec changed goes down and
  comes back on the new spec, the others are left alone, every disk is kept.
  The triggers it took down come back on once the [infra] reports ready.
- **If you want it off for a while and the data kept** (a paired WhatsApp
  session, a database's rows): `weft infra stop`. The containers go, the
  disks stay, and `weft infra start` brings it back with everything in it.
  Stop and terminate cancel the project's running executions first, because
  they may be using this infra; `--running-policy wait` lets them land
  instead, up to `--drain-timeout` seconds, on an active project and an
  inactive one alike.
- **If you want it gone** (the database and its rows, the bridge and its
  pairing): `weft infra terminate`. Every resource is deleted, disks included
  unless the node's own spec preserves them. There is no undo: a database
  comes back empty on the next start, and a bridge needs its QR scanned
  again. It asks for confirmation on a terminal; from your commands (no
  terminal, or `--json`) it needs `--yes`, which you pass only after the user
  confirmed.
- **If a verb is stuck mid-way**: `weft infra cancel` stops it between
  steps; whatever it already did stays done.
- **If a baked output went stale** (one marked `"baked": true`, which weft makes when the infra is applied and saves; it has nothing to do with `weft bake`), say a password reset whose report to weft never arrived: `weft infra rebake <node>` runs the node's infra setup again and saves those outputs. A run that reads nothing else from the node uses the saved values instead of running it, and a step that fails using a connection an infra node made names this command. If you changed what the node's code emits there, runs that skip the node keep getting the old saved value until it bakes again. If you want that without restarting anything, run `weft build` (or any `weft run`), then `weft infra rebake <node>`, which runs what was built last. `weft infra upgrade` also bakes again, but it stops the infra and starts it again. For a `@per_instance` node, add `--instance <id>` for each copy.
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

`stop`, `terminate` and `upgrade` take down first the triggers that read
the [infra] going away (nothing can fire at it) and leave them deactivated,
so each ends with `weft activate` when you want them listening again. A
trigger that reads none of it keeps listening throughout.

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
