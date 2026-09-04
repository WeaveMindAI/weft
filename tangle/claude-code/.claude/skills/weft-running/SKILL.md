---
name: weft-running
description: Running, activating, and debugging a weft project. Read when executing a program or chasing a failure: the CLI command map, the daemon, build and run flow, trigger activation, infra lifecycle, journal inspection (executions, events, logs), and the debugging playbook.
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
| `weft run [--target <id>]...` | register and fire one execution; follows it live unless `--detach` |
| `weft stop <color>` | cancel an execution |
| `weft status` | registration, build state, listener, infra, drift |
| `weft ps` | every registered project |
| `weft executions [--limit N] [--project <id>] [--phase fire]` | past executions, newest first (see Reading a run) |
| `weft events <color> [--node <id>] [--kind <kind>] [--full] [--json]` | a run's events in order, one compact line each (see Reading a run) |
| `weft logs [color]` | a run's log (no argument: the latest execution; see Reading a run) |
| `weft follow <project>` | live events for a project |
| `weft activate` / `weft deactivate` | turn triggers on / off (deactivate default wipes; `--mode hibernate` or `park` preserves in-flight human work) |
| `weft resync` | deactivate + activate against a fresh build, after editing a trigger subgraph |
| `weft infra start` / `status` / `stop` / `upgrade` / `terminate` / `cancel` | the project's long-running infra (Postgres, bridges); the verbs are explained below |
| `weft daemon start` / `status` / `logs` | the local runtime |
| `weft catalog update` | re-sync `nodes/base_catalog/` to the installed weft's stdlib |
| `weft describe-nodes --list` | one line per node type, which is how you find one |
| `weft describe-nodes --node <Type> --compact` | one node's wiring view, which is what you read before wiring it. With no flags you get the whole catalog as JSON, which is large |
| `weft test-node <target>` | run node self-tests (`--tier live` spends money, asks first) |
| `weft connect` | the editor's Connect panel as a CLI verb: `--list` access nodes, pick a stored connection (`--node`, `--grant`), connect new accounts through both doors, `--upgrade`, `--forget`, `--disconnect` |
| `weft rm [--all] [--force]` | unregister the project, terminate infra, reclaim data |
| `weft clean` | journal and image cleanup |

A manual run starts from every output node and walks upstream; `--target`
narrows it (targets must be output nodes). A trigger only fires on its event
once the project is activated.

When you wait on something long (a build, the daemon coming up, a run
settling), wait on the condition, never a timer: loop on the actual check
(`until <check>; do sleep 5; done`, run with a generous timeout) so you
return the moment it flips. A bare `sleep <N>` followed by a check is the
wrong shape: too short and you churn, too long and you idle.

## The build and run flow

`weft run` compiles, registers the project with the dispatcher, builds the
worker image if sources changed (Cargo runs inside Docker, never on the
host), then fires. Compile failures print `compile failed:` then
`line:column message` lines. HTTP errors surface the dispatcher's own
message verbatim.

The compiler's tiers on this path: `weft build` refuses structural errors
and deliberately skips the runtime rules, so a program still being wired
up (a connection not yet picked) still builds, and a CLI-started run
starts too: the runtime tier fires at execution, the node failing loudly
in the journal (e.g. a provider node's "no connection picked; pick one on
the node"). The editor's Run, Activate, and Resync buttons gate on the
runtime rules before sending (the bar's banner lists every finding); the
CLI path does not, so the cheap moment when you drive is before the run:
`weft validate` reports the `rule-runtime` findings in seconds. The fix is
a picked connection, never a hand edit: pick a stored one yourself with
`weft connect --node <id> --grant <grant>`, or send the user to the node's
Connect button in the editor or `weft connect` in their terminal (the
`weft-connections` skill).

A program with triggers needs `weft activate` before its triggers listen.
Editing a trigger's subgraph needs `weft resync` to take effect. Infra nodes
must be running for a run that touches them: `weft infra status`, then
`weft infra start`.

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
endpoint is. A run that touches infra is refused until that infra is
running, both from the CLI and from the editor's Run button.

Suspended executions are alive and cost nothing: a `HumanQuery` waiting on a
person parks, the worker exits, and the answer resumes it. The browser
extension (built with `./setup.sh --browser --no-sign`) is where people
answer.

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
  the last 1000 lines (`--limit` raises that) and says so when a run wrote more. For most
  failures this is enough, and you never open the events. `(no logs: ...)`
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
  `did_not_flow` means the node's `_should_flow` said no. `flow_closed` or
  `required_input_closed` means whatever should have fed it never fired, so
  walk to that node's line. `outside_this_run` means this run was aimed at
  some output nodes and none of them depends on this node; usually that is a
  missing `_is_output`.
- **If a `Debug` shows `output=` empty**: that is correct, because a
  `Debug` has no outputs. Its value is on its `node_started` line as
  `input=`, or `weft events <color> --node <debug id>`.
- In VS Code with the weft extension: the Executions view, "View in Graph"
  replays the run in the graph, values on every wire; `Debug` nodes render
  their latest value inline. The full editor surface (inspector, action
  bar, targets, everything clickable) is in the `weft-editor` skill.

## The debugging playbook

1. **It does not compile.** Read the diagnostics: `line:column message`, a
   stable slug, and the message names the fix. Fix what it names; never
   route around a diagnostic. `weft validate` re-checks without building.
   A diagnostic naming the catalog (an enrichment error, an unknown field,
   "a stale base_catalog copy") means the stdlib copy lags the installed
   weft: run `weft catalog update`, then re-check, before anything else.
2. **A run failed.** `weft logs <color>` names the node and the error. If
   the error alone does not say enough, `weft events <color> --node <that
   node>` shows the values that reached it, on its `node_started` line. The
   slug
   catalogue is in the `weft-language` skill; node errors are the node
   author's message. A node error like "no connection picked; pick one on
   the node" is the runtime tier firing at execution, not a source bug:
   pick a stored connection yourself (`weft connect --node <id> --grant
   <grant>`) or send the user to the node's Connect button / `weft
   connect` in their terminal, then run again.
3. **A value is wrong, not failed.** The same motion backwards: open the run,
   look at the top-level groups, find the first whose output is already
   wrong, descend, repeat. You are hunting the concrete failing case; when
   you have it, iterate on that one step (build, run, look) and feed the
   earlier stages' passing examples through again.
4. **Suspended.** Expected for human-in-the-loop: nothing is wrong. The
   person answers in the extension and the run resumes. If nobody should have
   been asked, the bug is the `_should_flow` that routed into the person.
5. **Cancelled.** Read the reason on `execution_cancelled`. `Cancelled by
   user` is a person: the Stop button, `weft stop`, a project deactivate or
   wipe, or a cancel through a signal token. `Stopped by execution <color>
   (tag <tag>)` is a sibling run's `ctx.stop_tagged`: look at **that** run for
   the answer, and if no sibling was supposed to stop this one, the bug is
   in whichever node tagged and stopped it (`weft executions` shows each
   run's tags, so you can see which runs shared the tag). `Caller
   disconnected` means the live caller this run was answering dropped its
   connection. Anything else is the runtime's own reason, printed as words
   (a worker pod shutting down, a build superseding a queued run's image).
6. **Stuck.** The engine proved nothing can proceed; that is a graph-shape
   bug (usually a wire the compiler could not catch), and the message names
   the nodes waiting on each other.
7. **Nothing ran.** A branch wired to no output never executes: check
   `_is_output` on the deliverable, and check that the trigger that should
   have fired is activated.
8. **Dispatcher unreachable.** `weft daemon start` (idempotent reconcile),
   then `weft daemon logs --tail 50` if it still refuses.

## Triggers and identity

Polling triggers (Telegram, email, sheets, notion, airtable, RSS, cron)
checkpoint across restarts and start from activation time; history never
replays. Push triggers (webhooks, socket mode, the live nodes) need a public
address: `weft daemon start --public-url` tunnels it. Signal tokens
(`weft token mint`) scope external listeners like the browser extension.
