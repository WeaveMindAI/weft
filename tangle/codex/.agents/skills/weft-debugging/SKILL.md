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
  execution id, status (`running`, `waiting_for_input`, `completed`, `failed`, `cancelled`), phase, the
  start time in UTC, the project id, the entry node (the trigger that fired), the tags. An
  activate, a resync or an infra start creates setup runs, phases
  `trigger_setup` and `infra_setup`; `--phase fire` hides them. `--project <id>` narrows to one project. A run whose trigger has `recorded: false` is listed only if it failed (with the step that failed and why, nothing before it) or reported a cost or asked weft for something on its behalf, so a route that answered and left nothing here may simply be unrecorded: `weft status` shows how many runs each trigger started in the last minute or two and how many failed, which is how you confirm its runs happen.
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
  read it, and they carry their own reason.) `wait_skipped`: the node was
  waiting on a person (a form, a question) and they skipped it, so it
  ends with nothing sent on and what reads it skips: not a bug to chase.
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
   text is written by the node's author. A run, an activate or a resync is
   refused before it starts while a connection it needs is not picked,
   naming the `weft connect` that picks it, and a node error saying it has
   no connection picked is the same [the runtime tier] gap, not a source bug: you pick a stored connection yourself (`weft connect
   --node <id> --grant <grant>`) or send the user to the node's Connect
   button / `weft connect` in their terminal, then run again. A failure saying `the worker running '<node>' went away while it was running` means the worker of a durable run died mid-step: weft does not run a step again once its start is on
   record, because the step may have partly happened. Check what it did outside (the email, the row, the post), then, if it has to happen again, start a run at that step by hand: `weft run --from <node>=<inputs>`, with the inputs from `weft events <execution-id> --node <node> --full`. If the message says `went away while it was reading a stream` instead, the items it read are gone from the stream, so `--from` cannot hand them back: check what it did, then run the whole execution again.
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
   see which runs shared it). `Caller disconnected` means the live caller this run was answering went away: it hung up, closed its socket (or never opened it), missed its heartbeat, or sent a message the connection refused. `the worker running this run went away; a fast run lives in its worker's memory, so it is not run again` means a fast run lost its worker (a crash, a machine gone): check what it did outside, then start it again (let the trigger fire again, or fire it by hand: `weft bake`, then `weft run --fire <trigger>=<payload>`, with the `payload` on the trigger's `node_kicked` line in `weft events <execution-id>`), and if the trigger's runs have to carry on after a worker dies, set `durable: true` on it. A durable run whose worker stops or dies is never cancelled: another worker picks it up from its record, and only the step that was running fails (item 2). `the run had to leave the copy of the program running it (...), and it cannot be suspended: <why>` means its worker was stopping and this run could not move to another one; the `<why>` names the reason: its trigger has `recorded: false`, its route does not have `outlivesCaller`, or a bus between its nodes was open. Check what it did outside, then start it again the same way as a fast run that lost its worker. A run cut by a platform limit on a cloud install (its hour, or its worker's connection to weft about to be cut while every run on it only waited) says so in words, starting `execution <id> stopped ... before the ... minute limit this platform puts on ...`, and the rest of the sentence says how to stay under it (a pause, or a client that reconnects). `weft tree` marks it `(a time limit: <detail>)`, and a caller still connected was answered `504`. A run whose caller's session reached the route's or socket's `maxSessionSecs` is a time limit too, on any install: `execution <id> stopped when its caller's session reached the <n> second limit its trigger sets (maxSessionSecs), and the caller was told it was a time limit (a 504, or close code 1011 on a socket). A session meant to last longer raises maxSessionSecs, or sets it to 0 for no limit`. An older run cut by a time limit shows the same words without `a time limit:` in front. `its worker refused it 3 times: <error>`, `the platform refused weft's calls for it 3 times: <error>` and `its project's worker settings kept it from any worker 3 times: <error>` mean a queued run (a `weft run`, a resume) never got going: it got a no three times, 5 s and then 20 s apart, the reason names who said the last one, and the `<error>` is that no. Only an answer counts as a no: from the worker, any error but its busy `503`; from Cloud Run answering in its place, a `4xx` other than `404`, `408` or `429`; from Google Cloud asked to create the worker, a `400`, `401` or `403`, whose words are in the `<error>`; and from weft itself, worker settings that together are a shape no platform could run. A `404`, `408`, `429` or `5xx` from Cloud Run, and a `429`, `5xx` or timeout from Google Cloud, count as no answer. A worker answering busy makes the run wait without spending a try, for as long as the workers answer busy, each wait as long as it has been turned away so far (at least 5 s, at most a minute): once that lasts longer than the install's alert delay (`alert_after_minutes`, 5 minutes by default), `weft status` lists it under `waiting on weft:` (item 11), ending in ``this project's worker has answered busy for <time> (memory full); raise its memory (`weft workers set --memory <size>`)``, and a Google Cloud install emails an alert. A worker still starting makes the run wait for it, and a call that got no answer at all is tried again 5 s and then 20 s later; after a third with no answer the run is not ended but waits, queued, until a worker of its project comes alive or a build of the project registers, and then its tries start over. If the error points at the worker settings, fix them with `weft workers set`: that hands every run of the project still queued to the workers again at once, while this one stays ended, so start it again. `weft status` shows the same under `waiting on weft:` (item 11). Anything else is the runtime's own reason, printed as words.
6. **Stuck.** The engine proved nothing can proceed; that is a graph-shape
   bug (a wire the compiler could not catch). The `execution_failed` line
   names every firing left holding a pulse and the wired ports it never
   received (`theirs has value, still waiting on go`): walk to whatever
   should have driven the missing port.
7. **Nothing ran.** Nothing reached the branch: check that the trigger that
   should have fired is activated, then walk from it down to the first
   `_should_flow` or required input that closed. If the trigger fired and
   no run came of it, `weft status` says what holds it. Under `triggers:`,
   a trigger whose next event is stuck gets a line saying on what:
   `2 fires wait until the infra the runs read is running again: <reason>`
   (or `until the instance's values change`, `until the install picks the
   connections the runs need`), and they run as soon as that changes.
   `1 fire stopped after failing to start a run, and holds the trigger's
   queue` means handing it to a worker failed three times (5 s, then 20 s
   apart), and every event behind it waits in order until `weft activate`,
   a new build or a change of the worker settings tries again. A fire that
   waits for room (a limit reached) or for its parked trigger gets no line
   here. One waiting for room tries again by itself, waiting longer each
   time, up to 5 minutes; one waiting for its trigger is handed over when
   the trigger is activated again.
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
   the status and the answer (on a local install: a `400`, `413`, `415`
   or `422`; the listener answers a wake it cannot read `200` with
   `dropped` and logs it). Any other failure (another `4xx`, a `5xx`, no
   answer) is sent again later, waiting longer each time. So a wait that
   never woke is read in `weft daemon logs`. A trigger's own timer,
   schedule, poll or subscription renewal whose wake keeps failing gets 20
   failed deliveries the listener answered (`wake_attempts` on a cloud
   install). A delivery that got no answer (the listener was down) or a
   5xx (from the listener or from something in front of it) does not
   count, so an outage spends none of them. On the user's own machine the 20 take about an
   hour when the listener answered every one, and up to about 95 minutes
   when an outage had already stretched the wait between tries to its
   longest, 5 minutes. Then the trigger is given up and stops being woken,
   and its node shows `its wake failed each of the 20 times the listener
   handled it, the last with: <error>`. If the wake did its work and only
   setting the next one failed, the node shows `its wake was handled, but
   the wake after it could not be set on the last of the 20 times the
   listener handled it, so nothing wakes it again: <error>` instead. One whose
   connection only a person can fix (it is gone, needs reconnecting, or
   lacks a permission: the broker answers 400, 404, 409 or 410) is given up
   at once. A trigger that keeps a connection open (a socket, a stream, an
   SSE subscription, a service's events) retries by itself while its live
   panel shows `reconnecting since <time>: <error>`, and stops on a failure
   only a person can fix (those same answers, a credential refused right
   after it was fetched, a setup error that fails the same way every time):
   its live panel shows `stopped: <why>`, and the reason stays on its node.
   A service that is busy or limiting calls is still retried.
   To bring either kind back: if its connection was the trouble,
   reconnecting that connection is enough, and weft brings back every
   trigger that uses it; for anything else, a setup error say, fix it and
   run `weft resync`. `weft activate` leaves it as it is, since the trigger
   still counts as on.
10. **[the daemon] unreachable** (connection refused, an editor button that
   does nothing). You read, never restart: `weft daemon status` and `weft
   daemon logs --tail 50` say what [the daemon] is doing. Then you tell the
   user the symptom (the exact refusal, and what you were running) and stop;
   bringing [the daemon] back is their install's job. If you catch yourself
   typing `weft daemon start`, `stop` or `restart`, stop and write: "Wait.
   The daemon is not mine to restart." Then report the symptom.
11. **A part of weft keeps failing.** Under `waiting on weft:`, `weft
   status` lists each of weft's own calls that keeps failing, as `<what>
   (failing for <span>, last tried <span> ago; <then>): <error>`, and the
   line stays until the problem is solved:
   - `weft's supervisor, which starts and stops infra, cannot be reached`
     (or the dispatcher, or another of weft's parts): weft rings it again
     on the next change that needs it.
   - `this project's workers, which runs are handed to, cannot be
     reached or cannot take them`, followed by `a queued run refused 3
     times ends, and one its workers could not be reached for 3 times waits
     until a worker of the project comes up or a build of it registers; one
     they keep answering busy (which spends no try) is tried again after a
     pause that grows to a minute, for as long as they do; changing the
     project's worker settings (more memory, for a worker that stays busy)
     tries every queued run afresh`: the run that ends is cancelled (item 5). It clears once
     none of the project's queued runs still waits on its workers (answered
     busy, refused, or not reached) and a run of the project has since left
     the queue: the last one that waited (it reached a worker, ended, or was
     cancelled), or one started after the failure. Changing the project's
     worker settings clears it too.
   - `weft's <part> could not <job>` (say, `hand queued runs to workers`),
     followed by `weft tried a few times and stopped; it tries again on
     the next change that concerns it`: this row appears only on a cloud
     install. A pass of one of weft's background jobs failed, was tried
     again 1 s, 5 s and 20 s later, and failed every time, so it stopped
     and wrote its error here; Google Cloud's queue does not keep calling
     it. It runs again on the next change that concerns it, and the row
     stays until a pass of that job goes through.
   - `weft's supervisor could not get through this project's infra`: the
     error says what the supervisor could not do (look at the project's
     health, delete what the host still holds for copies that are gone, or
     write down how a command ended). An error starting `could not list
     what the host holds to delete what its gone copies left` or `could not
     read what it owns to look at its health` failed the whole pass, so
     every project the supervisor holds counts a failed pass. On the user's
     own machine the error ends `(tried again at the supervisor's next
     look)` and the supervisor never gives up. On a cloud install it ends
     `(failure <n> in a row; tried again in <s> s)`, 5 s after the first
     failure and 10 s after the second, or, at the third, ``(failed 3 times
     in a row, so the supervisor stopped trying; `weft infra look` tries
     again)``. A pass that goes through starts the count again from zero. A health protocol
     (restarting an unhealthy unit, taking down the triggers that read
     it) that failed 3 times shows here too, as `its health protocol
     '<name>' failed 3 times: <error>`. Fix what the error names, then run
     `weft infra look`: it starts a fresh round of three tries and gives
     every health protocol that gave up another go. A protocol that gave
     up also runs again once the units report a change, all turn healthy,
     or you start, stop, upgrade or terminate the infra. The row clears
     once a pass goes through.
   - `weft's supervisor could not delete what the infra host still holds
     of removed projects (their disks included)`: a removed project has no
     status of its own, so this one shows in every project. The error
     names each removed project, what went wrong and what happens next: on
     the user's own machine `(tried again at the supervisor's next look)`;
     on a cloud install it is tried again 5 s, then 10 s later, and the
     third failure ends `(failed 3 times in a row, so the supervisor
     stopped trying; its next pass tries again)`. From then on every pass
     of the supervisor tries again, and `weft infra look` in any project
     starts one. The row clears once the host holds nothing of any removed
     project.
   - `weft's supervisor gave up infra commands of this project it could
     never finish`: a command (a start, a stop, an upgrade) was taken up 3
     times and never ended, because its run or the write of how it ended
     kept failing. The error names the command and ends `so it gave the
     command up; issue it again`. Fix the cause, then issue the command
     again; the row clears once a later infra command of the project ends.
   - `weft could not repair work of this project whose holder went away`:
     it tries again twice within half a minute, then whenever it next looks
     at lapsed work, and the error says what fixes it by hand.
   - `the listener still serves triggers of this project whose
     hibernation ended, and what they pick up is refused`: the error names
     each node and why it was kept. weft tells the listener to let go of
     them again at the next change of a trigger.
   - `a resync of this project stopped with triggers it took down still
     down; builds wait for it`: the resync failed three times in one go
     (it tried again 5 s, then 20 s later), and the error is the last failure. Builds are
     refused with `cannot build now` until it is settled, and one already
     compiling is not registered. If it stopped on infra those triggers
     read that was not running, or on an activation still landing, it goes
     on by itself once that infra runs or that activation has landed. Otherwise, once the cause is fixed,
     `weft resync --resume` takes it up again where it stopped (a plain
     `weft resync` is refused while it is held); a plain `weft
     cancel-activate` gives it up, leaves those triggers down for a `weft
     activate` later, and prints which ones. With `--trigger` or
     `--instance` it is refused while the resync holds the project, since
     the resync covers every trigger of every owner. The editor's action bar shows the same as
     "Resync stopped", with Resync and Give up.
   - `a given-up resync left these triggers down: <whose triggers>
     (<names>)`: a resync was given up with `weft cancel-activate`, which
     printed the same triggers, and these are the ones still not on. `weft
     activate` turns them on, and the row clears once they are on again or
     the next resync begins.
   - `weft's broker could not remove stored files from the bucket`: the
     error counts the files and names up to five, which hold their
     tenant's quota until removed. They are tried again at the expiry
     loop's next pass, a wipe of their files, or the tenant's next upload.
   - `weft could not tell the install's news (a cancel or an answer
     reaching a run a worker drives); those runs hear of it late`: weft
     tried a few times, then marked that news refused. New news always
     goes out first, on its own; the refused news is tried again one piece
     at a time when news is next written, or when a weft process starts,
     and the row clears once none is left.
   The rows naming one of weft's parts or jobs, and the bucket and news rows, are
   about the whole install
   (every project's `weft status` shows them): report them to the user as
   printed, as for item 10.
