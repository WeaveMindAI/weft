---
name: weft-debugging
description: "Read when a run failed, did the wrong thing, never started, got stuck or was cancelled, or when you need to read what a past run did: the order to read a run in (executions, logs, events), what each skip and cancel reason means, and the debugging playbook, from a compile error to a daemon that does not answer."
---

# Debugging a run

## Reading a run

Each verb below prints one compact line per run or per event and has a flag
that opens the part you want, so a forty-node run reads without loading the
whole journal. You read in this order and stop when you hold the failing
node and the wrong value.

- **If you want to know what ran, or whether your trigger fired since the
  change**: `weft executions --limit 10 --phase fire`. One line per run:
  execution id, status (`running`, `completed`, `failed`, `cancelled`), phase, the
  local start time, the entry node (the trigger that fired), the tags. An
  activate, a resync or an infra start creates setup runs, phases
  `trigger_setup` and `infra_setup`; `--phase fire` hides them. `--project
  <id>` narrows to one project.
- **If a run failed and you want the reason**: `weft logs <execution-id>`. It
  prints what the run's nodes wrote and every failure the journal recorded,
  as `error` and `warn` lines. A line about one node names it (and the loop
  iteration, `llm#3:`); a line about the run itself (the run failing, a
  cancel) names none:
  `[2026-09-02 21:36:47] error llm: node failed: the service answered 401 ...`. It is
  the last 1000 lines and says so when the run wrote more; `--limit` raises
  that up to 20000, higher is refused. For most failures this is enough and
  you never open the events. `(no logs: ...)` means the run wrote nothing
  and recorded no failure: it did not fail, so you check its status.
- **If you want the values on the wires**: `weft events <execution-id>`. One line
  per event: UTC time, kind, node, then everything the row carries as
  `key=value`, each cut to a screen's width (`input=` on `node_started`,
  `output=` on `node_completed`, `error=` on `node_failed`, `reason=` on a
  skip or a cancel, `token=` on a suspension, and so on). You narrow before
  you read: `--kind failed` for the failures (a substring matches, so this
  catches `node_failed` and `execution_failed`), `--kind node_skipped` for
  what did not run and why, `--node <id>` for every event on one node,
  `--iteration 3` for the fourth time round a loop (counted from 0; `3.0`
  is the first iteration of a loop inside it, outermost first).
- **If a value on one of those lines is cut off**: it was cut to fit the
  screen, not stored short. `weft events <execution-id> --node <id> --full` prints
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
  events <execution-id> --node <debug id>`.
- **If a past run shows no values at all**: a run's rows say what happened,
  not what it meant; the editor works out every input and output by
  replaying those rows against the code the run ran, so a run whose code
  [the daemon] no longer holds shows every node with no values, and the run
  itself says why. The empty graph is not "the node produced nothing" and
  not a viewer bug. The code is kept as long as any run points at it, so
  this is rare. If the files are still on disk, `weft build` puts the
  program back under the hash the run names and the run reads as it did. If
  not, the run is readable only as its shape, and `weft clean <execution-id>`
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
2. **A run failed.** `weft logs <execution-id>` names the node and the error. If
   that is not enough, `weft events <execution-id> --node <that node>` shows the
   values that reached it, on its `node_started` line. For the compiler
   error slugs, go and read the `weft-language` skill. A node's own error
   text is written by the node's author. A node error saying it has no
   connection picked is [the runtime tier] firing at execution,
   not a source bug: you pick a stored connection yourself (`weft connect
   --node <id> --grant <grant>`) or send the user to the node's Connect
   button / `weft connect` in their terminal, then run again. A failure saying
   `the worker running '<node>' went away while it was running` means the
   worker died mid-step: weft does not run a step again once its start is on
   record, because the step may have partly happened. Check what it did outside (the email, the
   row, the post), then `weft run --seed`, which reuses what completed and
   runs that step again.
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
   wipe, or a cancel through a signal token. `Stopped by execution <execution-id>
   (tag <tag>)` means another run asked the engine to stop everything
   carrying that tag: look at **that** run, and if no sibling was supposed
   to stop this one, the bug is in whichever node tagged and stopped it
   (`weft executions` shows each run's tags, so you
   see which runs shared it). `Caller disconnected` means the live caller
   this run was answering dropped its connection. Anything else is the
   runtime's own reason, printed as words (a worker shutting down).
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
9. **A timer or a wake that never arrived.** A wake the receiver refuses
   for good is dropped once, with one error in `weft daemon logs` naming it,
   the status and the answer (on a local install: any `4xx` except `408`
   and `429`; the listener answers a wake it cannot read `200` with
   `dropped` and logs it). A failure that may pass (a `5xx`, `408`, `429`,
   no answer) is sent again later, waiting longer each time. So a wait that
   never woke is read in `weft daemon logs`.
10. **[the daemon] unreachable** (connection refused, an editor button that
   does nothing). You read, never restart: `weft daemon status` and `weft
   daemon logs --tail 50` say what [the daemon] is doing. Then you tell the
   user the symptom (the exact refusal, and what you were running) and stop;
   bringing [the daemon] back is their install's job. If you catch yourself
   typing `weft daemon start`, `stop` or `restart`, stop and write: "Wait.
   The daemon is not mine to restart." Then report the symptom.
