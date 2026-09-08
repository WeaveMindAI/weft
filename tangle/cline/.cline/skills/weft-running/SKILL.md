---
name: weft-running
description: "Running, activating, and debugging a weft project. Read when executing a program or chasing a failure: the CLI command map, the daemon, build and run flow, trigger activation, infra lifecycle, journal inspection (executions, events, logs), and the debugging playbook."
---

# Running and debugging

The `weft` CLI is a thin client of the dispatcher daemon plus the front end
for building. The daemon (`weft daemon start`, once per machine, needs Docker
and the local kind cluster from `setup.sh`) owns projects, executions,
triggers, and infra, and listens on `http://localhost:9999` (override:
`--dispatcher <url>`, `WEFT_DISPATCHER_URL`, or `[dispatcher] url` in
`weft.toml`). A `.env` near the project auto-loads.

One run is one execution, called a **color** (a UUID). Everything that
happens in it is journaled, node by node, with the values on the wires.

## The command map

| Command | What it does |
|---|---|
| `weft build` | compile, resolve assets, build the worker image (content-addressed) |
| `weft validate --file main.weft < main.weft` | strict compile + validate, diagnostics as JSON, nothing runs |
| `weft run [--target <id>]...` | register and fire one execution; follows it live unless `--detach`. With targets, only those nodes and what they need run (the union when you name several, independent branches included); nothing past a target and no sibling branch, even one sharing a root like the database. Without targets, every root fires and triggers close their outputs |
| `weft stop <color>` | cancel an execution |
| `weft status` | registration, build state, listener, infra, drift |
| `weft ps` | every registered project |
| `weft executions [--limit N] [--project <id>] [--phase fire]` | past executions, newest first (see Reading a run) |
| `weft events <color> [--node <id>] [--kind <kind>] [--full] [--json]` | a run's events in order, one compact line each (see Reading a run). Every command that takes a color also takes the first characters of one (`weft events 3f2a`), at least four, as long as they name a single run |
| `weft logs [color]` | a run's log (no argument: the latest execution of the project in the current directory; see Reading a run) |
| `weft follow <project>` | live events for a project |
| `weft activate` / `weft deactivate` | turn triggers on / off. They ask on a terminal; you have no terminal, so pass the answer: `weft deactivate --mode <wipe\|hibernate\|park>` (required without a terminal on an active project; `--running-policy <wait\|cancel>` defaults to `wait`) (see The three modes below) |
| `weft resync` | deactivate + activate against a fresh build, after editing a trigger subgraph. On an active project it needs the same `--mode` answer as `deactivate`; without it, it stops and asks |
| `weft infra start` / `status` / `stop` / `upgrade` / `terminate` / `cancel` | the project's long-running infra (Postgres, bridges) |
| `weft token mint` / `ls` / `revoke` | signal tokens: scoped access for an outside listener such as the browser extension. `mint` prints the connect URL, then the bare token on its own line for a script |
| `weft daemon start` / `status` / `logs` | the local runtime |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's stdlib |
| `weft describe-nodes --list` | one line per node type, which is how you find one |
| `weft describe-nodes --node <Type> --compact` | one node's wiring view, which is what you read before wiring it. With no flags you get the whole catalog as JSON, which is large |
| `weft test-node <target>` | run node self-tests (`--tier live` spends money, asks first) |
| `weft connect` | the editor's Connect panel as a CLI verb: `--list` the stored connections, `--node <id> --grant <id>` to pick one for a node, connect new accounts through both doors, `--upgrade`, `--forget`, `--disconnect` |
| `weft rm [--all] [--force] --yes` | unregister the project, terminate infra, reclaim data. Asks first; you have no terminal, so pass `--yes`, and only after the user confirmed |
| `weft clean --yes` | journal and image cleanup. Deleting runs asks first; same rule, `--yes` after the user confirmed |

How a run picks which nodes execute is in the `weft-language` skill. A
trigger only fires on its event once the project is activated. `--json` is a
global flag, and it means two things. The long commands (`build`, `run`,
`activate`, `deactivate`, `resync`, `infra`, `rm`, the cancels) stream
progress as one JSON object per line. The readers (`status`, `ps`,
`executions`, `events`, `logs`, `files`, `listener inspect`, `token`,
`stop`, `connect`) print what the dispatcher answered, which is what you
want for `jq` instead of parsing the human columns, and `test-node`
prints its reports as one JSON array. `new`, `follow`, `daemon`,
`catalog`, `clean` and `update` ignore it.

When you wait on something long (a build, the daemon coming up, a run
settling), never sit in a loop you cannot leave. Start the long thing
detached (`weft run --detach`, the build in the background), then check
its state yourself between other steps: `weft executions --json` for a
run, `weft status --json` for a build or the daemon. A run parked on a
timer or a person is not going to finish on its own, and an open-ended
`until` loop on it hangs you until somebody kills it, which has happened.
If you do loop on a check, cap it: `timeout 30 bash -c 'until <check>; do
sleep 5; done'`, and when the cap trips, read the state and say what it
is waiting on rather than looping again.

## The build and run flow

`weft run` compiles, registers the project with the dispatcher, builds the
worker image if sources changed (Cargo runs inside Docker, never on the
host), then fires. Compile failures print `compile failed:` then
`line:column message` lines. HTTP errors surface the dispatcher's own
message verbatim.

The compiler's tiers on this path: `weft build` refuses structural errors
and deliberately skips the runtime rules, so a half-wired program (a
connection not yet picked) still builds and a CLI-started run still starts;
the runtime rules fire at execution and the node fails loudly in the journal
(a provider node's "no connection picked; pick one on the node"). The
editor's Run, Activate and Resync buttons check the runtime rules before
sending (the bar's banner lists every finding), and the CLI does not. So
when you are the one running it, check first: `weft validate` reports the
`rule-runtime` findings in seconds. The fix is a picked connection, never a
source edit: pick a stored one yourself with
`weft connect --node <id> --grant <grant>`, or send the user to the node's
Connect button in the editor or `weft connect` in their terminal (the
`weft-connections` skill).

A program with triggers needs `weft activate` before its triggers listen.
Editing a trigger's subgraph needs `weft resync` to take effect. Infra nodes
must be running for a run that touches them: `weft infra status`, then
`weft infra start`.

### The three modes

Taking triggers down (`deactivate`, `resync` on an active project, the
infra verbs that deactivate on the way) asks what happens to the runs
parked on a person or a timer:

- `wipe`: their forms and timers are dropped and the runs end cancelled.
  Pass it only when nothing is in flight (`weft executions` shows no
  suspended run of the project) or the user said to drop the waiting work.
- `hibernate`: the runs stay alive for a grace window (`--grace <minutes>`,
  15 unless set); a fire arriving inside it is held and delivered when the
  project comes back. Past the window new fires are refused (the waiting
  runs and the project survive; wiping is the mode that drops them).
- `park`: the runs stay alive with no time limit; every fire is held until
  the project is reactivated. The default choice when the user is editing
  and people are mid-conversation.

`--running-policy wait` (the default) lets executions already running
finish first (new fires are held meanwhile); `cancel` stops them now, and
`wipe` takes it on its own. Without `--mode` on a run with no terminal the
command refuses and names the flag, so a script never hangs on a prompt.

## The infra verbs

An infra node (`PostgresDatabase`, `BaileyBridge`) is a container the runtime
keeps running, with a disk that survives restarts. Which verb you reach for
depends on what you want to keep:

- **If the infra is not running yet**, or was stopped: `weft infra start`.
  It brings every unit up to its spec and waits until it is ready. Running it
  again does nothing for units that are already up.
- **If you changed an infra node's spec** (its image, its env, its volumes)
  and want the change live: `weft infra upgrade`. Each unit whose spec
  changed goes down and comes back up on the new spec, the others are left
  alone, and every disk is kept. Once the infra reports ready, run
  `weft activate`.

  Stop, terminate and upgrade all take the project's triggers down first
  (nothing can fire at infra that is going away) and leave it deactivated,
  so each of them ends with `weft activate` when you want it listening
  again. Start does not: it brings the infra up and leaves activation
  alone.
- **If you want it off for a while and the data kept** (a paired WhatsApp
  session, a database's rows): `weft infra stop`. The containers go, the
  disks stay, and `weft infra start` brings it back with everything in it.
- **If you want it gone** (the database and its rows, the bridge and its
  pairing): `weft infra terminate`. Every resource is deleted, disks included
  unless the node's own spec preserves them. There is no undo. A database
  comes back empty on the next start, and a bridge needs its QR scanned
  again.
- **If a verb is stuck mid-way**: `weft infra cancel` stops it between
  steps; whatever it already did stays done.

`weft infra status` says per node whether it is running and what its
endpoint is. `weft infra logs <node>` (or no node, for all of them) prints
what the containers wrote, `--tail N` and `-f` as for a run: that is where
a failure inside a service is read from, and it needs no kubectl. A run that touches infra is refused until that infra is
running, both from the CLI and from the editor's Run button.

## Reading a run

Each verb below prints one compact line per run or per event and has a flag
that opens the part you want, so you can read a forty-node run without
loading the whole journal. Read in this order and stop when you hold the
failing node and the wrong value.

- **If you want to know what ran, or whether your trigger fired since the
  change**: `weft executions --limit 10 --phase fire`. One line per run:
  color, status (`running`, `completed`, `failed`, `cancelled`), phase, the
  local start time, the entry node (the trigger that fired), the tags. An
  activate, a resync or an infra start creates setup runs, whose phases are
  `trigger_setup` and `infra_setup`; `--phase fire` hides them.
  `--project <id>` narrows to one project.
- **If a run failed and you want the reason**: `weft logs <color>`. It prints
  what the run's nodes wrote, and every failure the journal recorded, as
  `error` and `warn` lines. A line about one node names it (and the loop
  iteration it was in, `llm#3:`); a line about the run itself (the run
  failing, a cancel) names none:
  `[2026-09-02 21:36:47] error llm: node failed: the service answered 401 ...`. It is
  the last 1000 lines and says so when the run wrote more; `--limit` raises
  that up to 20000, and anything higher is refused. For most failures this
  is enough, and you never open the events. `(no logs: ...)`
  means the run wrote nothing and recorded no failure. It did not fail, so
  check its status to see what it did.
- **If you want the values on the wires**: `weft events <color>`. One line
  per event: local time, kind, node, then everything that row carries as
  `key=value`, each cut to a screen's width (`input=` on `node_started`,
  `output=` on `node_completed`, `error=` on `node_failed`, `reason=` on a
  skip or a cancel, `token=` on a suspension, and so on). Narrow before you
  read: `--kind failed` for the failures
  (a substring matches, so this catches `node_failed` and
  `execution_failed`), `--kind node_skipped` for what did not run and why,
  `--node <id>` for every event on one node. `--full` prints the values
  uncut, once you know which line you want; `--json` prints the replay rows
  for `grep` or `jq`.
- **If a node did not run**: its `node_skipped` line carries the reason.
  `did_not_flow` means the node's `_should_flow` said no. `flow_closed` means
  nothing ever answered its `_should_flow`, so walk to whatever drives that
  wire. `required_input_closed` names the input that arrived closed, so walk
  to that node's line. `every_input_closed` and `one_of_group_closed` are the
  same story for a node with no required inputs and for a `@require_one_of`
  group. `scope_skipped` names the group or loop whose `_should_flow` said
  no, taking this node with it: walk to that container. (A group input
  arriving closed is not this: it passes through to the nodes inside that
  read it, and they carry their own reason.)
- **If a `Debug` shows `output=` empty**: that is correct, because a
  `Debug` has no outputs. Its value is on its `node_started` line as
  `input=`, or `weft events <color> --node <debug id>`.
- In VS Code with the weft extension: the Executions view, "View in Graph"
  replays the run in the graph, values on every wire; `Debug` nodes render
  their latest value inline. For the full editor surface (inspector,
  action bar, targets, everything clickable), go and read the `weft-editor`
  skill.

## The debugging playbook

1. **It does not compile.** Read the diagnostics: `line:column message`, a
   stable slug, and the message names the fix. Fix what it names; never
   route around a diagnostic. `weft validate` re-checks without building.
   A diagnostic naming the catalog (an enrichment error, an unknown field,
   "a stale base_catalog copy") means the stdlib copy lags the installed
   weft: run `weft catalog update`, then re-check, before anything else.
2. **A run failed.** `weft logs <color>` names the node and the error. If
   the error alone does not say enough, `weft events <color> --node <that
   node>` shows the values that reached it, on its `node_started` line. For the
   compiler error slugs, go and read the `weft-language` skill. A node's own
   error text is written by the node's author. A node error like "no connection picked; pick one on
   the node" is the runtime tier firing at execution, not a source bug:
   pick a stored connection yourself (`weft connect --node <id> --grant
   <grant>`) or send the user to the node's Connect button / `weft
   connect` in their terminal, then run again.
3. **A value is wrong, not failed.** Work upstream from the output: open the run,
   look at the top-level groups, find the first whose output is already
   wrong, descend, repeat. You are hunting the concrete failing case; when
   you have it, iterate on that one step (build, run, look) and feed the
   earlier stages' passing examples through again.
4. **Suspended.** Expected for human-in-the-loop: nothing is wrong. A
   suspended run is alive and costs nothing: the `HumanQuery` parks, the
   worker exits, and the answer resumes it. The person answers in the browser
   extension (built with `./setup.sh --browser --no-sign`). If nobody should
   have been asked, the bug is the `_should_flow` that routed into the person.
5. **Cancelled.** Read the reason on `execution_cancelled`. `Cancelled by
   user` is a person: the Stop button, `weft stop`, a project deactivate or
   wipe, or a cancel through a signal token. `Stopped by execution <color>
   (tag <tag>)` is a sibling run's `StopTagged`: look at **that** run for
   the answer, and if no sibling was supposed to stop this one, the bug is
   in whichever node tagged and stopped it (`weft executions` shows each
   run's tags, so you can see which runs shared the tag). `Caller
   disconnected` means the live caller this run was answering dropped its
   connection. Anything else is the runtime's own reason, printed as words
   (a worker pod shutting down, a build superseding a queued run's image).
6. **Stuck.** The engine proved nothing can proceed; that is a graph-shape
   bug (usually a wire the compiler could not catch). The `execution_failed`
   line names every firing left holding a pulse and the wired ports it never
   received (`theirs has value, still waiting on go`): walk to whatever
   should have driven the missing port.
7. **Nothing ran.** Nothing reached the branch: check that the trigger that
   should have fired is activated, then walk from it down to the first
   `_should_flow` or required input that closed.
8. **Dispatcher unreachable.** `weft daemon start` (idempotent reconcile),
   then `weft daemon logs --tail 50` if it still refuses.

## Triggers and public reachability

Polling triggers (Telegram, Gmail, sheets, notion, airtable, RSS) checkpoint
across restarts and start from activation time; history never replays. A
held-connection trigger (`ReceiveEmail` over IMAP IDLE) and a scheduled one
(`Cron`) keep no cursor: cron recomputes its next tick from now on every
fire. Push triggers (webhooks, socket mode, the live nodes) need a public
address: `weft daemon start --public-url` tunnels it. Signal tokens
(`weft token mint`) scope external listeners like the browser extension.
