---
name: weft-running
description: "Read when running, activating or debugging a program: the CLI command map, the daemon (never yours to restart), the build and run flow, trigger activation and the three deactivate modes, the infra lifecycle, sizing a command's timeout, journal inspection (executions, events, logs), and the debugging playbook."
---

# Running and debugging

The `weft` CLI is a thin client of [the daemon] plus the front end for
building. [the daemon] is the dispatcher process that owns projects,
executions, triggers, and infra, and listens on `http://localhost:9999`
(override: `--dispatcher <url>`, `WEFT_DISPATCHER_URL`, or `[dispatcher] url`
in `weft.toml`). The user's install (`setup.sh`) starts it, once per machine.
`weft daemon start`, `stop` and `restart` re-run that install, and a restart
from a project has wiped shared keys before, so you never run them. A `.env`
near the project auto-loads.

A [color] is one execution: a UUID minted when a run starts. Everything in it
is journaled, node by node, with the values on the wires. Every command that
takes a [color] also takes its first characters (`weft events 3f2a`), at
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

Every command you run carries a timeout, and you copy it from this table
rather than estimating one.

| If you are running | wait at most |
|---|---|
| `status`, `executions`, `events`, `logs`, `connect`, `describe-nodes` | 15 seconds |
| `weft validate` | 30 seconds |
| a build, warm | 45 seconds |
| a build, the first time in this project | 3 minutes |
| `weft test-node` | 1 minute |
| `activate`, `deactivate`, `resync`, `infra start` | 2 minutes |
| a frontend `pnpm install` | 3 minutes |
| a frontend build or type check | 1 minute |
| `weft run` | its own expected length, or `--detach` and check later |
| a frontend dev server | nothing: start it in the background |

Check what unit your own tool wants before you type one of these. Some take
seconds and some take milliseconds, and getting it backwards is the
difference between three minutes and two days.

Nothing on this table is over three minutes, and neither is anything you
type. If the number you are about to write is longer than its row, the row is
right.

A timeout that trips is a finding, never a number to raise. Read what the
command printed before it was killed, work out why this run is longer than
its shape says, and report that. You raise the number only once you can say
what is taking the extra time.

If you want to wait on something long (a build, [the daemon] coming up, a run
settling), start it detached and check its state between other steps instead
of writing a loop: `weft run --detach` hands you the [color], then `weft
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
`ps`, `executions`, `events`, `logs`, `files`, `listener inspect`, `token`,
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
| `weft run [<example>] [--referenced] [--seed] [--root] [--from <node>=<ports-json>]... [--emit <node>=<ports-json>]... [--target <id>]... [--before <id>]... [--group <id>=<ports-json>] [--fire <trigger>=<wake-json>] [--save <name>]` | build and start one execution; `--detach` returns its [color]. `--from` supplies backup inputs at a start, `--emit` supplies outputs without executing that node. Real producers take precedence over backups, and under `--seed` the earlier run's result IS a real producer: a `--from` value at a node history already feeds goes unused (the run warns). To hand a new value in, run without `--seed`, or `--emit` the upstream output. `--target` includes the endpoint; `--before` excludes it. `--group` selects a whole group or included file, with its input payload. `--fire` runs exactly one trigger using a matching bake. Ordinary groups can be cut precisely; loops stay whole. `--seed-before` / `--seed-until` bound compatible reuse. A named example supplies saved starting parameters; current code runs. Clear and replacement rules are in `weft-sdp` |
| `weft checkpoint [<label>]` | record the files as a version under head, no run, no build; `already at <id>` when identical |
| `weft branch <version\|label\|color>` | restore that version's files and move head (a checkpoint label names its version; a [color] makes that run the next seed). Refuses on a dirty tree naming the files; `--discard` overrides |
| `weft tree` | the version tree: versions with what changed, their runs beneath, head marked (`--json` adds `disk_version`) |
| `weft diff <ref> <ref> [--full]` | compare observed outputs for human or AI review, including frozen focus nodes. A ref is a [color], its unambiguous prefix or `example:<name>`. Differences are evidence and do not fail the command |
| `weft freeze <name> [<run>] [--expect <node>]...` | save that run's starting parameters and observed outputs in `examples/<name>.json`; default is head's run. `--expect` marks nodes to focus on during review. Run and diff leave the accepted file intact; freeze again after accepting its replacement |
| `weft examples` | list saved parameters and frozen examples; inspect them, rerun one with `weft run <name>`, then compare with `weft diff` |
| `weft bake [--referenced]` | prepare trigger inputs without arming listeners. A manual `--fire` requires a matching bake; use `--referenced` here when the run uses `--referenced`. Activation also prepares and records a bake before arming |
| `weft wake <color> <node>` | resolve a pure time wait now; refused for a wait expecting a value, naming its kind |
| `weft prune <version>` | delete a version, everything under it, and their runs; asks first, `--yes` for scripts. Refuses on head's version, under a frozen example's origin, and while a run in the subtree is running |
| `weft stop <color>` | cancel an execution |
| `weft status` | registration, build state, listener, infra, drift |
| `weft ps` | every registered project |
| `weft executions [--limit N] [--project <id>] [--phase fire]` | past executions, newest first (see Reading a run) |
| `weft events <color> [--node <id>] [--kind <kind>] [--full] [--json]` | a run's events in order, one compact line each (see Reading a run) |
| `weft logs [color]` | a run's log (no argument: the latest execution of the project in the current directory; see Reading a run) |
| `weft follow <project>` | live events for a project |
| `weft activate` / `weft deactivate` | turn triggers on / off. `deactivate` on an active project needs `--mode <wipe\|hibernate\|park>` (a [mode], defined under The three modes) and takes `--running-policy <wait\|cancel>`, default `wait` |
| `weft resync` | deactivate + activate against a fresh build, after editing a trigger subgraph. Only for an ACTIVE project (a parked or hibernated one refuses: `weft activate` first), and it needs the same `--mode` answer as `deactivate`; without it, it stops and asks |
| `weft infra start` / `status` / `stop` / `upgrade` / `terminate` / `cancel` / `logs` | the project's [infra] (see The infra verbs) |
| `weft token mint` / `ls` / `revoke` | signal tokens: scoped access for an outside listener such as the browser extension. `mint` prints the connect URL, then the bare token on its own line for a script |
| `weft daemon start` / `status` / `logs` | [the daemon]; only `status` and `logs` are yours |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's stdlib |
| `weft describe-nodes --list` | one line per node type; how you find one |
| `weft describe-nodes --node <Type> --compact` | one node's wiring view; read it before wiring. With no flags you get the whole catalog as JSON, which is large |
| `weft test-node <target>` | run node self-tests (`--tier live` spends money, asks first) |
| `weft connect` | the editor's Connect panel as a CLI verb: `--list` the stored connections, `--node <id> --grant <id>` to pick one for a node, connect new accounts through both doors, `--upgrade`, `--forget`, `--disconnect` |
| `weft rm [--journal] [--local] [--all] --yes` | unregister the project, terminate [infra], reclaim data. `--journal` also drops its run history, `--local` its build artifacts, `--all` implies every flag. Asks first; pass `--yes`, and only after the user confirmed |
| `weft clean --yes` | journal and image cleanup, per subject, and naming a subject takes all of it: a [color] takes that one run, `--project <id>` takes a whole project's history (removing a project leaves its runs behind, so this is how you erase them), no subject takes everything older than `--keep-days` (30 by default), `--all` takes the lot. A version the deletion left bare (no runs, nothing under it, no checkpoint name, not head) goes with the runs; a named checkpoint never does. `--images` and `--build-cache` touch no journal rows. Deleting runs asks first; pass `--yes`, and only after the user confirmed |

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
half-wired program still builds and a CLI-started run still starts; the node
fails at execution, in the journal, naming the service and what to do
("no telegram connection picked; connect one on the node"). What `weft
validate` and the editor print before a run is the same fact in fuller
words, naming the node too.
The editor's Run, Activate and Resync buttons refuse until every
connection is picked; the CLI does not, so before a run you check with `weft
validate --file src/main.weft < src/main.weft`, which reports the
`rule-runtime` findings in seconds. The fix is a picked connection (`weft
connect --node <id> --grant <id>`, or the user on the node's Connect button),
never a source edit.

A program with triggers listens only after `weft activate`. An edit to a
trigger's subgraph takes effect only after `weft resync`.

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
images, and nothing else does.

### The three modes

A [mode] is what happens to the runs parked on a person or a timer when the
triggers go down (`deactivate`, `resync` on an active project, the [infra]
verbs that deactivate on the way):

- `wipe`: their forms and timers are dropped and the runs end cancelled. You
  pass it only when nothing is in flight (`weft executions` shows no
  suspended run of the project) or the user said to drop the waiting work.
- `hibernate`: the runs stay alive for a grace window (`--grace <minutes>`,
  15 unless set); a fire arriving inside it is held and delivered when the
  project comes back. Past the window new fires are refused; the waiting runs
  and the project survive (`wipe` is the [mode] that drops them).
- `park`: the runs stay alive with no time limit; every fire is held until
  the project is reactivated. Your pick when the user is editing and people
  are mid-conversation.

**`wipe` is what you pass while you are building.** Nothing waiting on the
program is anyone's conversation yet, so dropping it costs nothing and the
command lands at once. `hibernate` and `park` exist for a program people are
actually using: pass one when the user says so, or when `weft executions`
shows a run parked on a person you would be throwing away.

`--running-policy wait` (the default) lets executions already running finish
first, new fires held meanwhile; `cancel` stops them now, and `wipe` implies
it. `wait` only ever waits under `hibernate`, and it ends by cancelling
whatever is still running at its cap: under `park` the running executions are
left exactly as they are and the command lands at once, because park promises
they stay alive. With no `--mode` and no terminal (which is every command you run) the mode
is `wipe`. That is the right answer while you are building, so you rarely
type it; you type `--mode hibernate` or `--mode park` when the program is one
people are using and the work in flight has to survive.

## The infra verbs

[infra] is a container the runtime keeps running for the program (an infra
node: `PostgresDatabase`, `BaileyBridge`), with a disk that survives
restarts. You pick the verb by what you want to keep:

- **If the [infra] is not running yet**, or was stopped: `weft infra start`.
  It brings every unit up to its spec and waits until ready. Running it again
  does nothing for units already up. It leaves activation alone.
- **If you changed an infra node's spec** (image, env, volumes) and want it
  live: `weft infra upgrade`. Each unit whose spec changed goes down and
  comes back on the new spec, the others are left alone, every disk is kept.
  Once the [infra] reports ready, `weft activate`.
- **If you want it off for a while and the data kept** (a paired WhatsApp
  session, a database's rows): `weft infra stop`. The containers go, the
  disks stay, and `weft infra start` brings it back with everything in it.
- **If you want it gone** (the database and its rows, the bridge and its
  pairing): `weft infra terminate`. Every resource is deleted, disks included
  unless the node's own spec preserves them. There is no undo: a database
  comes back empty on the next start, and a bridge needs its QR scanned
  again.
- **If a verb is stuck mid-way**: `weft infra cancel` stops it between
  steps; whatever it already did stays done.

`stop`, `terminate` and `upgrade` take the project's triggers down first
(nothing can fire at [infra] that is going away) and leave it deactivated, so
each ends with `weft activate` when you want it listening again.

`weft infra status` says per node whether it is running and its endpoint.
`weft infra logs <node>` (or no node, for all) prints what the containers
wrote, `--tail N` and `-f` as for a run: a failure inside a service is read
there, with no kubectl.

## Reading a run

Each verb below prints one compact line per run or per event and has a flag
that opens the part you want, so a forty-node run reads without loading the
whole journal. You read in this order and stop when you hold the failing
node and the wrong value.

- **If you want to know what ran, or whether your trigger fired since the
  change**: `weft executions --limit 10 --phase fire`. One line per run:
  [color], status (`running`, `completed`, `failed`, `cancelled`), phase, the
  local start time, the entry node (the trigger that fired), the tags. An
  activate, a resync or an infra start creates setup runs, phases
  `trigger_setup` and `infra_setup`; `--phase fire` hides them. `--project
  <id>` narrows to one project.
- **If a run failed and you want the reason**: `weft logs <color>`. It
  prints what the run's nodes wrote and every failure the journal recorded,
  as `error` and `warn` lines. A line about one node names it (and the loop
  iteration, `llm#3:`); a line about the run itself (the run failing, a
  cancel) names none:
  `[2026-09-02 21:36:47] error llm: node failed: the service answered 401 ...`. It is
  the last 1000 lines and says so when the run wrote more; `--limit` raises
  that up to 20000, higher is refused. For most failures this is enough and
  you never open the events. `(no logs: ...)` means the run wrote nothing
  and recorded no failure: it did not fail, so you check its status.
- **If you want the values on the wires**: `weft events <color>`. One line
  per event: local time, kind, node, then everything the row carries as
  `key=value`, each cut to a screen's width (`input=` on `node_started`,
  `output=` on `node_completed`, `error=` on `node_failed`, `reason=` on a
  skip or a cancel, `token=` on a suspension, and so on). You narrow before
  you read: `--kind failed` for the failures (a substring matches, so this
  catches `node_failed` and `execution_failed`), `--kind node_skipped` for
  what did not run and why, `--node <id>` for every event on one node.
- **If a value on one of those lines is cut off**: it was cut to fit the
  screen, not stored short. `weft events <color> --node <id> --full` prints
  it whole, and `--json` gives you the replay rows for `grep` or `jq`. No
  value a run recorded is unreadable, so you never report one as unreadable.
- **If a node did not run**: its `node_skipped` line carries the reason.
  `did_not_flow`: the node's `_should_flow` said no. `flow_closed`: nothing
  ever answered its `_should_flow`, so walk to whatever drives that wire.
  `required_input_closed` names the input that arrived closed, so walk to
  that node's line. `every_input_closed` and `one_of_group_closed` are the
  same story for a node with no required inputs and for a `@require_one_of`
  group. `scope_skipped` names the group or loop whose `_should_flow` said
  no, taking this node with it: walk to that container. (A group input
  arriving closed is not this: it passes through to the nodes inside that
  read it, and they carry their own reason.)
- **If a `Debug` shows `output=` empty**: correct, a `Debug` has no
  outputs. Its value is on its `node_started` line as `input=`, or `weft
  events <color> --node <debug id>`.
- **If a past run shows no values at all**: a run's rows say what happened,
  not what it meant; the editor works out every input and output by
  replaying those rows against the code the run ran, so a run whose code
  [the daemon] no longer holds shows every node with no values, and the run
  itself says why. The empty graph is not "the node produced nothing" and
  not a viewer bug. The code is kept as long as any run points at it, so
  this is rare. If the files are still on disk, `weft build` puts the
  program back under the hash the run names and the run reads as it did. If
  not, the run is readable only as its shape, and `weft clean <color>`
  removes it.
- In VS Code with the weft extension: the Executions view, "View in Graph"
  replays the run in the graph, values on every wire; `Debug` nodes render
  their latest value inline. For the full editor surface (inspector, action
  bar, targets, everything clickable), go and read the `weft-editor` skill.

## The debugging playbook

1. **It does not compile.** Read the diagnostics: `line:column message`, a
   stable slug, and the message names the fix. You fix what it names; you
   never route around a diagnostic. `weft validate --file src/main.weft <
   src/main.weft` re-checks without building. A diagnostic naming the
   catalog (an enrichment error, an unknown field, "a stale base_catalog
   copy") means the stdlib copy lags the installed weft: `weft catalog
   update`, then re-check, before anything else.
2. **A run failed.** `weft logs <color>` names the node and the error. If
   that is not enough, `weft events <color> --node <that node>` shows the
   values that reached it, on its `node_started` line. For the compiler
   error slugs, go and read the `weft-language` skill. A node's own error
   text is written by the node's author. A node error saying it has no
   connection picked is [the runtime tier] firing at execution,
   not a source bug: you pick a stored connection yourself (`weft connect
   --node <id> --grant <grant>`) or send the user to the node's Connect
   button / `weft connect` in their terminal, then run again.
3. **A value is wrong, not failed.** Work upstream from the output: open the
   run, look at the top-level groups, find the first whose output is already
   wrong, descend, repeat. When you hold the concrete failing case, iterate
   on that one step (build, run, look) and feed the earlier stages' passing
   examples through again.
4. **Suspended.** Expected for human-in-the-loop: nothing is wrong. A
   suspended run is alive and costs nothing: the `HumanQuery` parks, the
   worker exits, and the answer resumes it. The person answers in the
   browser extension (built with `./setup.sh --browser --no-sign`). If
   nobody should have been asked, the bug is the `_should_flow` that routed
   into the person.
5. **Cancelled.** Read the reason on `execution_cancelled`. `Cancelled by
   user` is a person: the Stop button, `weft stop`, a project deactivate or
   wipe, or a cancel through a signal token. `Stopped by execution <color>
   (tag <tag>)` means another run asked the engine to stop everything
   carrying that tag: look at **that** run, and if no sibling was supposed
   to stop this one, the bug is in whichever node tagged and stopped it
   (`weft executions` shows each run's tags, so you
   see which runs shared it). `Caller disconnected` means the live caller
   this run was answering dropped its connection. Anything else is the
   runtime's own reason, printed as words (a worker pod shutting down).
6. **Stuck.** The engine proved nothing can proceed; that is a graph-shape
   bug (a wire the compiler could not catch). The `execution_failed` line
   names every firing left holding a pulse and the wired ports it never
   received (`theirs has value, still waiting on go`): walk to whatever
   should have driven the missing port.
7. **Nothing ran.** Nothing reached the branch: check that the trigger that
   should have fired is activated, then walk from it down to the first
   `_should_flow` or required input that closed.
8. **Too much is running.** The one failure nobody reports, because nothing
   errors: runs accumulate and the only symptom is a list that keeps
   growing. Every live connection is its own run for as long as the caller
   holds it, so a page anyone can open, or a client that reconnects by
   itself, can leave one behind per visit. `weft executions` is the check,
   and what you look at is the STANDING count of runs still going, not
   whether the one you started finished. Drive the thing, close it, look
   again: the count comes back down, or you have found a leak. Several runs
   of one trigger that never end is a finding you report, with the count and
   how long they have stood.
9. **[the daemon] unreachable** (connection refused, an editor button that
   does nothing). You read, never restart: `weft daemon status` and `weft
   daemon logs --tail 50` say what [the daemon] is doing. Then you tell the
   user the symptom (the exact refusal, and what you were running) and stop;
   bringing [the daemon] back is their install's job. If you catch yourself
   typing `weft daemon start`, `stop` or `restart`, stop and write: "Wait.
   The daemon is not mine to restart." Then report the symptom.

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
  internet. The user opens that with `./setup.sh --public-url`, and the
  trigger's live feed in the graph then shows its real URL. Signal tokens
  (`weft token mint`) scope external listeners like the browser extension.
