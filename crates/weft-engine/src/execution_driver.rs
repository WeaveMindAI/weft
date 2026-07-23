//! Drives one execution (one color) from boot to a terminal
//! outcome: completion, failure, stall, or stuck. The pulse loop
//! lives here.
//!
//! Shape:
//! - Boot: fold the journal for this color to recover pulses,
//!   executions, kicked roots, and pending deliveries. If the
//!   journal is empty we wait briefly for the producer to write
//!   `ExecutionStarted` + `NodeKicked`, then re-fold.
//! - Dispatch: ready nodes go into a `JoinSet` as tokio tasks.
//!   Each task runs the node's async `execute` and reports back
//!   through an mpsc channel; the main loop applies results to
//!   `pulses` and `executions` (single-writer invariant).
//! - Suspend: a node calling `ctx.await_signal(...)` returns
//!   `WeftError::Suspended { token }` from the spawned task; the
//!   loop's `apply_results` records the token in `waiting`. The
//!   fold at boot seeds any already-resolved suspensions in
//!   `awaited_sequences`; bodies pop entries in call_index order.
//!   When nothing is making progress and at least one firing is
//!   waiting, the loop returns `Stalled`.
//! - Stall / Stuck: when drive() runs out of work but pulses or
//!   waiting suspensions remain, `run_one_execution` re-fetches
//!   the journal and re-folds. New SuspensionResolved rows that
//!   landed during drive() get picked up; the loop drives again.
//!   Only after the journal has stabilized does the worker
//!   actually exit (Stalled = waiting on more fires; Stuck =
//!   graph-shape bug).
//! - Completion: no ready nodes, no in-flight tasks, nothing
//!   waiting. Journal a terminal event and return.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use weft_core::exec::{
    check_completion, find_ready_nodes, postprocess::{close_unmentioned_downstream, postprocess_output},
    NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use weft_core::node::NodeOutput;
use weft_core::primitive::ExecutionSnapshot;
use weft_core::project::EdgeIndex;
use weft_core::pulse::PulseTable;
use weft_core::cancellation::CancellationFlag;
use weft_core::{Color, ExecutionContext, NodeCatalog, ProjectDefinition};

use weft_journal::JournalClient;

use crate::context::{
    ship_node_completed, ship_node_failed, ship_node_lifecycle, ship_node_skipped,
    ship_node_suspended, EngineClients, NodeTaskOutcome, RunnerHandle, TaskMsg,
};
use crate::now_unix;


/// How long shutdown waits for the bus-journal pump to drain every
/// live bus before declaring the journal client wedged and panicking.
/// The shutdown loop is notify-driven (it wakes on every drain pass,
/// not on a polling interval), so the only thing the deadline bounds
/// is "the pump itself is making no progress at all" (the journal
/// client wedged or the pump task panicked silently). 10s is loose
/// enough to absorb a slow journal client without hiding a real
/// wedge: a healthy drain pass writes one row per entry, single-
/// digit ms each, and a chatty execution at shutdown might still
/// have several hundred entries across all buses to flush.
const BUS_PUMP_SHUTDOWN_DEADLINE_SECS: u64 = 10;

/// How often a bus-held worker re-checks the journal for a resolved
/// suspension while it can't exit (a live bus keeps `in_flight`
/// non-empty so the outer re-fetch loop never runs). Only polls in
/// that exact state; the common path never touches it. 250ms keeps
/// resume latency low without hammering the journal: a bus-held
/// worker waiting on human input would poll ~4x/sec, cheap against a
/// single indexed read.
const RESUME_POLL_INTERVAL_MS: u64 = 250;

/// Outcome the loop reports back to the binary wrapper.
#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    Completed { outputs: Value },
    Failed { error: String },
    /// The execution was cancelled (the cancellation flag tripped).
    /// A distinct variant rather than `Failed { error: "cancelled" }`:
    /// the "this exact string means cancelled" contract was invisible
    /// to the type system and decoded by string equality at the
    /// terminal-match and journal sites, so a reworded sentinel at one
    /// producer would silently flip a cancel into a generic failure.
    Cancelled,
    /// Worker stalled: at least one firing is waiting for a signal.
    /// Worker should exit; the next fire's `register_signal` task
    /// will resume by re-folding the journal.
    Stalled,
    /// Scheduler ran to quiescence but pulses remain pending and
    /// nothing is waiting. Treat as a graph-shape bug.
    Stuck,
}

/// Run one execution to a terminal state or a stall. Each call folds
/// the journal once on entry and, after Stalled/Stuck, re-folds for
/// as long as the journal keeps growing. The natural termination is
/// "no new rows since the last fetch": at that point another drive()
/// would see the same snapshot and reach the same conclusion. No
/// magic iteration cap; the absent-new-rows invariant is sharper.
/// `pod_name` stamps every journal write so the fencing trigger can
/// reject writes from a Pod whose row is no longer alive.
pub async fn run_one_execution(
    project: Arc<ProjectDefinition>,
    catalog: Arc<dyn NodeCatalog>,
    color: Color,
    clients: EngineClients,
    pod_name: String,
    tenant_id: String,
    // namespace: project namespace this worker pod runs in. Used to
    // build the InfraProvisionContext passed to infra nodes during
    // InfraSetup.
    namespace: String,
    cancellation: Arc<CancellationFlag>,
    // The live caller connection for this execution, if any. `Some` only
    // on the worker that received a `live_connection` request; threaded
    // into every firing's `RunnerHandle` (so `ctx.caller()` resolves) and
    // into the loop's keep-warm decision (an attached caller under a
    // `keep_alive` reconcile holds the worker warm like a live bus does).
    caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
) -> anyhow::Result<ExecutionOutcome> {
    let project = &*project;
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    // Kicked roots (entry points of the active execution: firing
    // trigger, manual-run roots, infra-setup roots). Folded from
    // `ExecEvent::NodeKicked`. The scheduler dispatches each not-yet-
    // dispatched kick once at frames=[]; the payload for the firing
    // trigger threads through to the `ctx.wake` bag.
    let mut kicked: HashMap<String, weft_core::primitive::KickedNode> = HashMap::new();
    // Per-(node, frames) ordered list of past `await_signal` calls.
    // Pre-loaded from the journal fold; consumed by the body's
    // `await_signal` calls in call_index order. Replaces the
    // single-token `expected_tokens` HashMap from the
    // single-await-per-body world.
    let mut awaited_sequences: HashMap<
        (String, weft_core::frames::LoopFrames),
        Vec<weft_core::primitive::AwaitedEntry>,
    > = HashMap::new();

    // Fold the journal: this is the source of truth. If the log is
    // non-empty (resume case), apply it to seed pulses, executions,
    // and pending deliveries. If empty, the producer just journaled
    // ExecutionStarted + NodeKicked; wait briefly for the rows.
    //
    // The drive's journal client is wrapped so a failed lifecycle
    // write poisons the drive (the loop checks the flag every
    // iteration and exits the worker; see `PoisonOnWriteFailure`).
    // The bus pump below keeps the UNwrapped client: bus-row failures
    // degrade per-bus without killing the worker.
    let pump_journal = clients.journal.clone();
    let (wrapped_journal, journal_poisoned) =
        crate::context::PoisonOnWriteFailure::wrap(clients.journal.clone());
    let clients = EngineClients { journal: wrapped_journal, ..clients };
    let journal = clients.journal.clone();
    let mut events = fetch_events(journal.as_ref(), color).await?;
    if events.is_empty() {
        // Wait up to 6s (30 * 200ms) for the producer to commit. The
        // sleep yields to cancellation: a cancel landing mid-wait
        // breaks us out instead of forcing the worker to sit idle.
        // Driven by `clients.clock` so layer-3 tests can fast-forward.
        for _ in 0..30 {
            tokio::select! {
                _ = clients.clock.sleep(std::time::Duration::from_millis(200)) => {}
                _ = cancellation.cancelled() => {
                    return Ok(ExecutionOutcome::Cancelled);
                }
            }
            let evs = fetch_events(journal.as_ref(), color).await?;
            if !evs.is_empty() {
                events = evs;
                break;
            }
        }
    }
    // The dispatcher's contract is "ExecutionStarted is journaled
    // before the worker boots." If we sat through the full wait
    // and the journal is STILL empty, that contract is broken: bail
    // loudly instead of silently proceeding with phase=Fire (which
    // would bypass phase_scope for what might have been a
    // TriggerSetup execution).
    if events.is_empty() {
        anyhow::bail!(
            "worker booted for color {color} but no ExecutionStarted \
             arrived within 6s; the dispatcher contract is broken"
        );
    }
    // Phase derives from the ExecutionStarted event we now have. No
    // unwrap_or fallback: if events is non-empty but contains no
    // ExecutionStarted, the journal is malformed and we fail loud.
    let phase = events
        .iter()
        .find_map(|e| match e {
            weft_journal::ExecEvent::ExecutionStarted { phase, .. } => Some(*phase),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!(
            "color {color} has journal events but no ExecutionStarted; \
             journal is malformed"
        ))?;
    let snap = weft_journal::fold_to_snapshot(color, &events);
    let mut loop_runtime = rehydrate_loop_runtime(&project, &snap.loop_instances)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    apply_snapshot(snap, &mut pulses, &mut executions, &mut kicked, &mut awaited_sequences);

    let exec_id = uuid::Uuid::new_v4().to_string();
    // Per-execution bus coordinator: shared with every RunnerHandle so
    // `create_bus` can register its bus, and read by the loop's idle-
    // wait stuck-check (loop stuck + any bus live -> close every bus;
    // every cursor wakes with None, every wait wakes with Closed).
    //
    // Spawn the one-task bus-journal pump alongside: every bus append
    // pings the coordinator's `journal_pump_notify`; the pump walks
    // every live bus, drains its unjournaled tail, and ships the
    // entries to the journal so the inspector can replay the
    // conversation. The pump holds a `Weak<BusCoordinator>` plus an
    // owned `Arc<Notify>`. At shutdown the coordinator (1) closes
    // every bus, (2) waits notify-driven for the pump to drain, (3)
    // releases its `Arc<BusInner>` pins, (4) sets the explicit
    // `pump_should_exit` flag and wakes the pump. The pump's next
    // iteration reads the flag, runs one final (empty) drain pass,
    // and exits. The `Weak<BusCoordinator>::upgrade()` failure path
    // is a backstop for the case where the coordinator is dropped
    // without shutdown (panic unwind); the explicit flag is the
    // primary exit signal.
    let bus_coordinator = crate::context::BusCoordinator::new();
    let bus_journal_task = tokio::spawn(crate::context::run_bus_journal_task(
        Arc::downgrade(&bus_coordinator),
        color,
        pump_journal,
        pod_name.to_string(),
    ));
    // Drive in a re-fetch loop. drive() folds the journal once at
    // boot and works off that snapshot; SuspensionResolved rows that
    // arrive while drive() is running are invisible to it. When
    // drive() returns Stalled/Stuck, refetch the journal: if new
    // deliveries arrived, fold the new state on top and re-drive.
    //
    // The natural termination is "no new rows since the last fetch"
    // (`fresh.len() == event_count_before`): a drive() that ends
    // Stalled/Stuck and finds no new events in the journal can't make
    // progress no matter how many times we re-loop. That gives us a
    // sharper invariant than a magic iteration cap and lets a chatty
    // journal (long burst of deliveries) keep absorbing rows.
    //
    // A wall-clock safety net guards against a pathological producer
    // (a buggy node that keeps emitting indefinitely, an external
    // writer flooding the color despite pod fencing): if the refetch
    // loop has been spinning for more than this deadline without
    // reaching a terminal outcome, exit Stuck and surface the
    // pathology rather than pin the pod's CPU forever. The deadline
    // is generous so a legitimate burst of deliveries (say, a 10s
    // wave of webhook fires) completes naturally.
    const REFETCH_WALL_CLOCK_DEADLINE_SECS: u64 = 60;
    let refetch_deadline =
        std::time::Duration::from_secs(REFETCH_WALL_CLOCK_DEADLINE_SECS);
    let refetch_start = clients.clock.now();
    let mut event_count_before = events.len();
    let mut outcome;
    loop {
        outcome = drive(
            &project,
            &edge_idx,
            catalog.as_ref(),
            &exec_id,
            color,
            &clients,
            &journal_poisoned,
            &pod_name,
            &tenant_id,
            &namespace,
            &cancellation,
            &bus_coordinator,
            caller.as_ref(),
            &mut pulses,
            &mut executions,
            &mut kicked,
            std::mem::take(&mut awaited_sequences),
            &mut loop_runtime,
            phase,
            event_count_before,
        )
        .await?;
        if !matches!(outcome, ExecutionOutcome::Stalled | ExecutionOutcome::Stuck) {
            break;
        }
        // The worker has stalled: every branch is parked or done. THIS is
        // the true suspension point, the one place we reconcile the live
        // caller against a durable wait (never per-await, since other
        // branches may have still been running and talking to the caller).
        //
        // A caller-tied run (`can_suspend = false`) HOLDS the worker warm
        // here instead of exiting: it keeps the connection and polls the
        // journal in-process for the resolving signal, up to the resolved
        // hold time. The same warmth a live bus gives, expressed at the
        // resume loop because an `await_signal` node (unlike a bus node)
        // ends its task. On hold expiry (or caller drop), a tied run cannot
        // degrade into a background job, so it is KILLED (cancelled), not
        // cleanly suspended. A suspendable run (`can_suspend = true`) does
        // not hold: it falls through to the normal clean exit and resumes
        // later caller-less.
        let caller_warm = match caller.as_ref() {
            Some(conn) => !conn.config().suspend.can_suspend && conn.is_connected(),
            None => false,
        };
        // The hold bound for a warm tied run = the run's default hold time
        // (per-call override plumbing is a follow-on; the trigger default
        // is the bound today). A non-warm stall uses the normal short
        // refetch deadline.
        let effective_deadline = match (caller_warm, caller.as_ref()) {
            (true, Some(conn)) => {
                std::time::Duration::from_secs(conn.config().suspend.default_hold_secs)
            }
            _ => refetch_deadline,
        };
        if clients.clock.now().saturating_duration_since(refetch_start) > effective_deadline {
            if caller_warm {
                // Tied run, hold expired with the caller still attached and
                // no resolving signal: it cannot make progress and must not
                // become a background job. Kill it (cancel the color); the
                // connection layer surfaces the clear disconnect message.
                tracing::warn!(
                    target: "weft_engine::resume",
                    color = %color,
                    hold_secs = effective_deadline.as_secs(),
                    "caller-tied run held past its hold time with no resolving signal; \
                     cancelling (a tied run cannot degrade into a background job)"
                );
                cancellation.cancel();
                outcome = ExecutionOutcome::Cancelled;
                break;
            }
            // Suspendable (or no caller): a Stalled drive that ran out of
            // refetch budget is STILL Stalled (the worker exits cleanly,
            // dispatcher respawns on the next fire). Don't relabel.
            tracing::warn!(
                target: "weft_engine::resume",
                color = %color,
                deadline_secs = effective_deadline.as_secs(),
                outcome = ?outcome,
                "refetch loop hit deadline; exiting with last drive outcome"
            );
            break;
        }
        let fresh = fetch_events(journal.as_ref(), color).await?;
        // Append-only journal: fresh.len() can only grow or stay equal.
        // No new events since the last fetch means we're parked behind
        // a signal the dispatcher hasn't resolved yet.
        debug_assert!(fresh.len() >= event_count_before, "journal shrank under us");
        if fresh.len() == event_count_before {
            // No resolving signal yet. A caller-tied warm run holds (sleep
            // a poll interval and re-fetch, keeping the connection alive
            // until the signal lands, the caller drops, or the hold expires
            // above). A suspendable run exits cleanly and respawns on the
            // fire.
            if caller_warm {
                clients
                    .clock
                    .sleep(std::time::Duration::from_millis(RESUME_POLL_INTERVAL_MS))
                    .await;
                continue;
            }
            break;
        }
        event_count_before = fresh.len();
        let snap = weft_journal::fold_to_snapshot(color, &fresh);
        loop_runtime = rehydrate_loop_runtime(&project, &snap.loop_instances)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        apply_snapshot(snap, &mut pulses, &mut executions, &mut kicked, &mut awaited_sequences);
        tracing::info!(
            target: "weft_engine::resume",
            color = %color,
            "re-fetched journal after stall/stuck; re-driving"
        );
    }

    // Journal the terminal event based on what the worker actually
    // did. The pump shutdown happens AFTER so a pump abort surfaces
    // via tracing without corrupting the terminal payload (the round-1
    // override of outcome made cancellation+pump_abort write a
    // Failed{"pump aborted"} terminal after NodeCancelled events, a
    // self-contradictory journal). The caller (run_pod) discards the
    // outcome variant via `.map(|_| ())`, so there is no return-value
    // path that needs the override either.
    match &outcome {
        // Cancellation: walk the in-memory snapshot and journal a
        // NodeCancelled per non-terminal node so the UI's per-node
        // tally flips to cancelled (not stuck-running). Only the
        // worker knows which nodes are non-terminal AT the moment
        // we exit, so this can't live in the dispatcher's cancel
        // path.
        ExecutionOutcome::Cancelled => {
            journal_node_cancellations(journal.as_ref(), color, &pod_name).await;
            journal_terminal(journal.as_ref(), clients.clock.as_ref(), color, &pod_name, &outcome).await;
        }
        ExecutionOutcome::Completed { .. } | ExecutionOutcome::Failed { .. } | ExecutionOutcome::Stuck => {
            journal_terminal(journal.as_ref(), clients.clock.as_ref(), color, &pod_name, &outcome).await;
            // No worker-side storage cleanup here: the dispatcher's durable
            // terminate sweep owns the run's un-kept exec files. It reaps
            // crashed uploads and grants completed files a short post-run
            // linger (so the user can still download a run's output), then
            // the broker's expiry sweep deletes them. A worker-side eager
            // delete would defeat that linger.
        }
        ExecutionOutcome::Stalled => {
            // Worker exits cleanly without writing a terminal event.
            // Resume happens on the next fire: dispatcher writes a
            // SuspensionResolved row + enqueues a fresh `resume`
            // task (the prior task is `complete` so dedup lets a
            // new one through), and a worker spawns to fold the
            // updated journal. Nothing extra to journal here.
        }
    }

    // Shut down the bus-journal pump AFTER the terminal write. Append
    // `Closed` to every live bus, wait (notify-driven) for the pump to
    // drain, drop the coordinator's pinned `Arc<BusInner>` refs, then
    // await the pump's JoinHandle. A pump abort means bus events
    // written during the drive never reached the journal: replay is
    // degraded for this execution. Surface it loudly via tracing.
    bus_coordinator
        .shutdown(std::time::Duration::from_secs(BUS_PUMP_SHUTDOWN_DEADLINE_SECS))
        .await;
    drop(bus_coordinator);
    if let Err(e) = bus_journal_task.await {
        tracing::error!(
            target: "weft_engine::execution_driver",
            color = %color,
            error = %e,
            "bus journal task ended abnormally; bus replay for this execution is degraded"
        );
    }
    Ok(outcome)
}

/// Rebuild the `LoopRuntime` from a journal-fold snapshot. The full
/// `LoopConfig` (over / carry / trim_on_mismatch) rides on the
/// snapshot itself, so the project definition is only consulted to
/// recover the LoopOut node's declared gather port names (needed to
/// assemble outward emits for ports no iteration touched). A missing
/// LoopOut on resume is corruption: the project drifted between
/// suspend and resume, OR the worker is folding a journal that
/// doesn't match its project_definition fetch. Either way, silent
/// recovery would mask the bug; bail loud.
fn rehydrate_loop_runtime(
    project: &ProjectDefinition,
    snapshot: &HashMap<
        weft_core::primitive::LoopInstanceKey,
        weft_core::primitive::LoopInstanceSnapshot,
    >,
) -> Result<crate::loop_runtime::LoopRuntime, String> {
    let mut rt = crate::loop_runtime::LoopRuntime::new();
    for (key, snap) in snapshot {
        let gather_ports = loop_gather_ports(project, &key.group_id, &snap.carry)
            .ok_or_else(|| {
                format!(
                    "rehydrate: snapshot has loop instance {} at parent_frames={:?} \
                     but the project has no LoopOut node '{}__out'; the snapshot was \
                     written against a different project shape",
                    key.group_id, key.parent_frames, key.group_id,
                )
            })?;
        let inst = crate::loop_runtime::LoopInstance::from_snapshot(
            key.clone(),
            snap,
            gather_ports,
        );
        rt.insert(inst);
    }
    Ok(rt)
}

/// The declared gather port names for a loop: the matching LoopOut
/// node's outward outputs minus the carry-named ones. Captured at
/// instantiation (and on rehydrate) so the outward emit assembles a
/// list for EVERY declared gather port, even ones no iteration writes
/// to (which would otherwise produce no pulse and deadlock downstream
/// consumers). `None` when the project has no `{group_id}__out` node,
/// which every caller treats as corruption.
fn loop_gather_ports(
    project: &ProjectDefinition,
    group_id: &str,
    carry: &[String],
) -> Option<Vec<String>> {
    let loop_out_id = format!("{group_id}__out");
    project.nodes.iter().find(|n| n.id == loop_out_id).map(|n| {
        n.outputs
            .iter()
            .filter(|p| !carry.contains(&p.name))
            .map(|p| p.name.clone())
            .collect()
    })
}

/// A `(node_id, frames)` location: identifies one firing of a node at a
/// specific loop frame stack.
type FiringLocation = (String, weft_core::frames::LoopFrames);

/// The set of parked nodes whose CURRENT suspension is now resolved. A
/// `WaitingForInput` exec resumes ONLY if the token it is parked on (its
/// `callback_id`) appears as a resolved `Await` in its folded sequence.
///
/// Checking "ANY resolved entry" instead of the current token would
/// livelock a multi-await body: after the first await resolves, the
/// sequence permanently contains a resolved entry, so every re-fold would
/// re-dispatch the node, the body would replay and re-suspend on the next
/// await, shipping fresh NodeResumed + NodeSuspended rows that the
/// refetch loop treats as progress, spinning until its deadline. Scoping
/// to the current token is what both resume paths (boot-time
/// `apply_snapshot` and mid-drive `resume_resolved_suspensions_in_place`)
/// depend on, so it lives here once.
fn resolved_waiting_locations(
    executions: &NodeExecutionTable,
    awaited_sequences: &HashMap<FiringLocation, Vec<weft_core::primitive::AwaitedEntry>>,
) -> std::collections::HashSet<FiringLocation> {
    executions
        .values()
        .flat_map(|v| v.iter())
        .filter(|e| e.status == NodeExecutionStatus::WaitingForInput)
        .filter_map(|e| {
            let token = e.callback_id.as_deref()?;
            let seq = awaited_sequences.get(&(e.node_id.clone(), e.frames.clone()))?;
            seq.iter()
                .any(|entry| matches!(
                    &entry.kind,
                    weft_core::primitive::AwaitedEntryKind::Await { token: t, resolved: Some(_) }
                        if t.as_str() == token
                ))
                .then(|| (e.node_id.clone(), e.frames.clone()))
        })
        .collect()
}

/// Mark a set of firing locations for re-dispatch as resumes. For each
/// location: a kicked entry node (no inbound pulses) gets `dispatched`
/// reset so the kick-synthesis path re-fires it; every other location
/// gets the exact pulse IDs its dispatch consumed (`pulses_absorbed`)
/// flipped Absorbed → Pending so the dispatch loop re-fires it. In both
/// cases the non-terminal exec record stays, so the dispatch loop ships
/// `NodeResumed` (not a duplicate `NodeStarted`). One un-absorb mechanic,
/// shared by both resume paths.
///
/// Pulse IDs are looked up directly (not by count): a count-based
/// un-absorb would restore the wrong firing's pulses if two firings at
/// different frame stacks shared a node and one needs re-dispatch.
fn redispatch_locations(
    to_un_absorb: &std::collections::HashSet<FiringLocation>,
    pulses: &mut PulseTable,
    executions: &NodeExecutionTable,
    kicked: &mut HashMap<String, weft_core::primitive::KickedNode>,
) {
    for (node_id, info) in kicked.iter_mut() {
        if info.dispatched && to_un_absorb.contains(&(node_id.clone(), Vec::new())) {
            info.dispatched = false;
        }
    }

    let mut un_absorb_ids: HashMap<String, std::collections::HashSet<uuid::Uuid>> = HashMap::new();
    for execs in executions.values() {
        for e in execs {
            let key = (e.node_id.clone(), e.frames.clone());
            if to_un_absorb.contains(&key) && !e.status.is_terminal() {
                un_absorb_ids
                    .entry(e.node_id.clone())
                    .or_default()
                    .extend(e.pulses_absorbed.iter().copied());
            }
        }
    }

    for (node_id, ids) in un_absorb_ids {
        if let Some(bucket) = pulses.get_mut(&node_id) {
            for p in bucket.iter_mut() {
                if p.status == weft_core::pulse::PulseStatus::Absorbed && ids.contains(&p.id) {
                    p.status = weft_core::pulse::PulseStatus::Pending;
                }
            }
        }
    }
}

fn apply_snapshot(
    snap: ExecutionSnapshot,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<String, weft_core::primitive::KickedNode>,
    awaited_sequences: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        Vec<weft_core::primitive::AwaitedEntry>,
    >,
) {
    *pulses = snap.pulses;
    *executions = snap.executions;
    *kicked = snap.kicked;
    *awaited_sequences = snap.awaited_sequences;
    // `snap.corruptions` is intentionally not consumed here. The
    // engine cannot do anything with a list of unparseable rows it
    // already skipped during fold. The same fold runs when the
    // inspector calls `/replay`, and THAT path forwards
    // each corruption as a `DispatcherEvent::JournalCorruption`
    // (the user-facing surface). For ops, `report_corruption`
    // already wrote each row at `error!` level when the fold ran.

    // A WaitingForInput exec re-dispatches ONLY if the suspension it
    // is CURRENTLY parked on has been resolved by a fire AND the exec
    // is still non-terminal (`resolved_waiting_locations`). Without
    // this scoping, every fresh worker spawn would re-dispatch every
    // still-suspended sibling, re-run the body, hit the first await,
    // find no delivery yet, re-suspend, churning the journal with
    // spurious NodeStarted/Suspended cycles per fresh worker.
    let resume_locations = resolved_waiting_locations(executions, awaited_sequences);

    // Crashed-worker recovery: a `Running` firing with no terminal in
    // the journal is assumed to belong to a worker that DIED mid-node;
    // we un-absorb its pulses and re-run it. This is only safe because
    // at most ONE worker exists per color at a time: the dispatcher's
    // spawn path dedups (enqueue-dedup key + the partial unique index
    // on worker_pod + a NOT-EXISTS-live-pod check in cold_start), so a
    // fresh worker can't re-fold and re-run a node body while the prior
    // worker is still alive and about to ship its own NodeCompleted. If
    // that one-worker-per-color invariant ever broke, this re-run would
    // double-execute the node (double LLM spend / double side-effects).
    // This crashed-Running set is unique to the boot-time path: mid-
    // drive (`resume_resolved_suspensions_in_place`) a Running exec is a
    // live in-flight task, not a dead one, so that path omits it.
    let crashed_running: std::collections::HashSet<FiringLocation> = executions
        .values()
        .flat_map(|v| v.iter())
        .filter(|e| e.status == NodeExecutionStatus::Running)
        .map(|e| (e.node_id.clone(), e.frames.clone()))
        .collect();

    let to_un_absorb: std::collections::HashSet<FiringLocation> =
        resume_locations.union(&crashed_running).cloned().collect();

    redispatch_locations(&to_un_absorb, pulses, executions, kicked);
}

/// Surgically resume the parked nodes whose CURRENT suspension just
/// resolved, IN PLACE, without re-folding the whole execution. Used by
/// the bus-held mid-drive resume poll: a live bus keeps unrelated nodes
/// genuinely Running in-flight, so a full `apply_snapshot` would
/// re-dispatch them (double-run). This touches ONLY the resolved
/// waiters: it folds the journal solely to recover the resolved
/// `awaited_sequences` entries, then for each WaitingForInput exec
/// whose `callback_id` token is now resolved it (a) installs that
/// node's fresh await sequence into the live map and (b) un-absorbs the
/// pulses the original dispatch consumed, so the next drain re-fires it
/// as a resume (NodeResumed). Returns how many nodes it resumed.
///
/// Mirrors the `resume_locations` half of `apply_snapshot` (the
/// crashed-Running half is deliberately omitted: mid-flight a Running
/// exec is a live task, not a dead one).
fn resume_resolved_suspensions_in_place(
    color: Color,
    events: &[weft_journal::ExecEvent],
    executions: &NodeExecutionTable,
    pulses: &mut PulseTable,
    kicked: &mut HashMap<String, weft_core::primitive::KickedNode>,
    awaited_sequences: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        Vec<weft_core::primitive::AwaitedEntry>,
    >,
) -> usize {
    let snap = weft_journal::fold_to_snapshot(color, events);

    // Which parked nodes have their CURRENT suspension resolved now?
    // Computed against the FRESHLY-FOLDED sequences (the live map is
    // stale mid-drive, which is exactly why this path exists), NOT the
    // crashed-Running set apply_snapshot also folds in: mid-flight a
    // Running exec is a live in-flight task, not a dead one.
    let resume_locations = resolved_waiting_locations(executions, &snap.awaited_sequences);

    if resume_locations.is_empty() {
        return 0;
    }

    // Install the resolved await sequences for exactly those nodes so the
    // body's replay pops the resolved value.
    for key in &resume_locations {
        if let Some(seq) = snap.awaited_sequences.get(key) {
            awaited_sequences.insert(key.clone(), seq.clone());
        }
    }

    // Re-dispatch them as resumes (kicked-reset + pulse un-absorb), the
    // same mechanic the boot-time path uses.
    redispatch_locations(&resume_locations, pulses, executions, kicked);

    resume_locations.len()
}

// ---------- Main drive loop ----------

/// Internal loop body called once per execution by `run_one_execution`.
#[allow(clippy::too_many_arguments)]
async fn drive(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    catalog: &dyn NodeCatalog,
    exec_id: &str,
    color: Color,
    clients: &EngineClients,
    journal_poisoned: &std::sync::atomic::AtomicBool,
    pod_name: &str,
    tenant_id: &str,
    // namespace: project namespace this worker is running in. Used
    // to populate InfraProvisionContext when dispatching infra nodes.
    namespace: &str,
    cancellation: &Arc<CancellationFlag>,
    bus_coordinator: &Arc<crate::context::BusCoordinator>,
    caller: Option<&Arc<dyn weft_core::caller::CallerConnection>>,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    kicked: &mut HashMap<String, weft_core::primitive::KickedNode>,
    mut awaited_sequences: HashMap<
        (String, weft_core::frames::LoopFrames),
        Vec<weft_core::primitive::AwaitedEntry>,
    >,
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
    phase: weft_core::context::Phase,
    // Number of journal events the caller already folded into the
    // snapshot it handed us. The bus-held resume poll compares against
    // this to detect newly-landed rows without a redundant re-fetch.
    journaled_baseline: usize,
) -> anyhow::Result<ExecutionOutcome> {
    let journal = clients.journal.as_ref();
    // ONE ordered channel from node tasks to the loop. A node sends
    // `TaskMsg::Emission` zero or more times while it runs (each
    // `pulse_downstream` / `close_port`, applied without closing the
    // emitting node), then exactly one `TaskMsg::Terminal` when its body
    // returns. FIFO ordering on this single channel is load-bearing: the
    // loop always observes a node's emissions BEFORE its terminal, so the
    // close-unmentioned-ports sweep at the terminal sees the complete set
    // of emitted ports. (Two separate channels left a window where a
    // terminal could be read before a just-sent emission, closing a port
    // that was actually emitted and skipping its consumer until a
    // re-dispatch, an emit-then-immediately-return race.)
    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<crate::context::TaskMsg>();
    // Per-(node, frames) set of OUTPUT PORTS this firing has mentioned
    // across all its `pulse_downstream` and `close_port` calls. On the
    // firing's terminal event (Completed / Failed / Skipped / Cancelled),
    // the loop emits CLOSURE markers on every output port NOT in this
    // set so downstream consumers learn no value is coming. A firing
    // that never emitted at all is the empty-set case: every output
    // port is closed.
    let mut emitted_ports: HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    > = HashMap::new();
    let mut in_flight: JoinSet<()> = JoinSet::new();
    // Maps each spawned node task's `tokio::task::Id` to the firing it
    // runs, so a task that PANICS (which never sends a NodeTaskResult on
    // `result_tx`) can still be turned into a terminal `NodeFailed` for
    // the right (node, frames). Without this, a panicked task surfaces as
    // an anonymous JoinError, its exec record stays `Running` forever,
    // and the crashed-Running refold path re-dispatches it on every
    // respawn: an infinite re-run until the refetch wall-clock deadline.
    let mut task_firings: HashMap<tokio::task::Id, (String, weft_core::frames::LoopFrames)> =
        HashMap::new();
    // Nodes that called `await_signal` and returned `Suspended`.
    // Keyed by token; value is (node_id, frames). When the loop finds
    // no active work to run and this map is non-empty, we stall:
    // the worker tells the dispatcher "I'm just waiting, please
    // kill me" and exits.
    let mut waiting: HashMap<String, (String, weft_core::frames::LoopFrames)> = HashMap::new();

    // Phase-scoped subgraph. In TriggerSetup we only want the
    // triggers (features.is_trigger) and their upstream closure to
    // execute; in InfraSetup we only want infra nodes
    // (requires_infra) and their upstream closure. Any node outside
    // the set is auto-skipped when a pulse lands on it, otherwise
    // downstream nodes block forever waiting for inputs that the
    // setup phase never produces.
    //
    // None (no scoping) is used for Phase::Fire (normal execution
    // graph reaches everything via pulses).
    let phase_scope: Option<std::collections::HashSet<String>> =
        match phase {
            weft_core::context::Phase::TriggerSetup => {
                Some(compute_trigger_setup_scope(project, edge_idx))
            }
            weft_core::context::Phase::InfraSetup => {
                Some(compute_infra_setup_scope(project, edge_idx))
            }
            weft_core::context::Phase::Fire => None,
        };
    if let Some(scope) = phase_scope.as_ref() {
        drop_out_of_scope_pulses(pulses, scope);
    }

    // Two-pass stuck detector: a single no-progress drain can mean the
    // runtime just hasn't polled spawned tasks yet. Only after the loop
    // idle-waits AND wakes AND re-drains AND still finds nothing to do
    // do we declare stuck. The flag flips true when the select returns,
    // false on real progress and after a stuck-close.
    let mut idled_since_progress = false;

    // In-flight resume baseline. A live bus keeps `in_flight` non-empty,
    // so a node parked on `await_signal` would otherwise wait for its
    // fire FOREVER inside this loop: the outer re-fetch loop only runs
    // after drive() RETURNS, and a bus never lets it return. So while a
    // bus holds the worker AND a suspension is pending, poll the journal
    // in the idle path; when its `SuspensionResolved` row lands we
    // re-fold and re-dispatch the parked node IN PROCESS, on this live
    // worker, with the open bus untouched (the fold reconstructs node
    // state only; bus state lives entirely in `BusCoordinator`). The
    // bus is thus transparent to wait-for-input: same resume as any
    // other live worker, the bus just prevents the worker from dying.
    // Without a bus, a parked node empties `in_flight`, drive() returns
    // Stalled, and the normal die-then-respawn path handles the resume.
    // The caller already folded the journal to seed the snapshot, so it
    // passes the event count in rather than us re-fetching it here.
    let mut journaled_count = journaled_baseline;
    loop {
        // Poison checkpoint: a journal write failed somewhere since
        // the last iteration. The journal is now a strict prefix of
        // the live state; driving further would compound the
        // divergence (every later refold rebuilds a different world).
        // Exit the worker; the respawn refolds from the consistent
        // prefix and re-runs the lost suffix (same at-least-once
        // semantics as a crash).
        if journal_poisoned.load(std::sync::atomic::Ordering::Acquire) {
            anyhow::bail!(
                "a journal write failed mid-drive for color {color}; worker exiting so a \
                 respawned worker resumes from the journal's consistent prefix"
            );
        }

        // Cancellation checkpoint. Checked at the TOP of every
        // iteration regardless of whether the previous iteration
        // made progress. The flag is persistent (AtomicBool), so
        // there's no race between cancel() landing and the next
        // check.
        //
        // Before returning Cancelled, `shutdown().await`
        // the in-flight JoinSet: simply dropping the JoinSet aborts
        // its tasks at their next yield point, but a task that's
        // mid-journal-write may finish writing (e.g. NodeCompleted)
        // AFTER the cancel path wrote NodeCancelled for the same
        // (node, frames). The fold is last-write-wins, so the final
        // state would flip to Completed and downstream nodes would
        // receive a fake output. Awaiting shutdown drives every
        // task to its abort point deterministically before we
        // declare the execution cancelled.
        if cancellation.is_cancelled() {
            tracing::info!(
                target: "weft_engine::execution_driver",
                color = %color,
                in_flight = in_flight.len(),
                "cancellation observed at loop top; draining in-flight tasks"
            );
            cancel_cleanup(
                &mut in_flight,
                &mut task_rx,
                &mut waiting,
                executions,
                &mut emitted_ports,
                color,
                project,
                edge_idx,
                pulses,
                journal,
                pod_name,
                phase_scope.as_ref(),
                loop_runtime,
            )
            .await;
            return Ok(ExecutionOutcome::Cancelled);
        }

        // At Fire, wires INTO a trigger are inert: a trigger's ports
        // replay its setup-time snapshot, so an upstream node that ran
        // for the output path must not re-dispatch the trigger with a
        // live value. Drop those
        // pulses before groups form; the trigger's one dispatch is its
        // kick.
        if matches!(phase, weft_core::context::Phase::Fire) {
            for trigger_id in project.nodes.iter().filter(|n| n.features.is_trigger) {
                pulses.remove(&trigger_id.id);
            }
        }

        let mut ready = find_ready_nodes(project, pulses, edge_idx);
        // HOLD pulses that arrive at a node already PARKED on an
        // unresolved suspension. A WaitingForInput record is waiting for
        // its token to RESOLVE (a signal), not for input pulses; a fresh
        // pulse at the same (node, frames) must not dispatch it. If it
        // did, the body would re-run as a bogus "resume" with an EMPTY
        // await sequence (the live map entry was consumed at first
        // dispatch and is only reinstalled by a real resume), register a
        // NEW suspension at call_index 0 colliding with the already-
        // journaled one, and corrupt replay. So drop those groups from
        // this batch and leave their pulses Pending: the genuine resume
        // (token resolution) re-fires the node and absorbs them then.
        // `find_ready_nodes` re-produces the group only on the next wake,
        // by which point the resume has flipped the record, so this does
        // not spin. A record that is Running (crashed/mid-flight) or
        // WaitingForInput-with-token-RESOLVED is a real resume and stays.
        let resolved_waiters = resolved_waiting_locations(executions, &awaited_sequences);
        ready.retain(|(node_id, group)| {
            let loc = (node_id.clone(), group.frames.clone());
            // A WaitingForInput record whose token is NOT resolved is
            // parked-and-unresolved: hold this group. `resolved_waiting_
            // locations` is the single source of "this parked node's
            // current token resolved" (also used by apply_snapshot), so
            // we reuse it rather than re-deriving the predicate.
            let parked_unresolved = executions
                .get(node_id)
                .map(|recs| {
                    recs.iter().any(|e| {
                        e.frames == group.frames
                            && e.status == NodeExecutionStatus::WaitingForInput
                    })
                })
                .unwrap_or(false)
                && !resolved_waiters.contains(&loc);
            !parked_unresolved
        });
        // The kicked map drives two things this turn:
        //
        // 1. Wake payloads: every dispatch of a kicked node at frames=[]
        //    needs to see the wake event's payload. This INCLUDES
        //    resumes (e.g. a worker crashed mid-Fire of a webhook; the
        //    fresh worker re-dispatches the trigger via the
        //    non-terminal-exec resume path, and the body's `ctx.wake`
        //    bag MUST still hold the body the
        //    listener delivered). So populate `kick_payloads` for
        //    EVERY kicked node, not just first-dispatch.
        //
        // 2. First-dispatch synthesis: a not-yet-dispatched kick that
        //    has no pulse-driven ReadyGroup at frames=[] gets a
        //    synthesized one so the scheduler picks it up. After the
        //    synthesis (or after we observe a pulse-driven group at
        //    the same key), flip `dispatched=true` so the next tick
        //    doesn't double-fire.
        let mut kick_payloads: HashMap<(String, weft_core::frames::LoopFrames), Value> = HashMap::new();
        for (node_id, info) in kicked.iter_mut() {
            // The FIRING trigger always gets a wake delivery, even when
            // the fire's body was empty (a bare ping journals `null`);
            // a non-firing kick only carries one when a manual-run mock
            // set a real payload on a plain root.
            if info.firing {
                kick_payloads
                    .insert((node_id.clone(), Vec::new()), info.payload.clone().unwrap_or(Value::Null));
            } else if let Some(payload) = info.payload.clone() {
                kick_payloads.insert((node_id.clone(), Vec::new()), payload);
            }
            if info.dispatched {
                continue;
            }
            // Not yet dispatched. If a pulse-driven ReadyGroup at
            // frames=[] already covers this node (unusual: the entry
            // node also received a regular pulse, possible in test
            // setups), let the pulse-driven dispatch run; we just
            // flip `dispatched`. Either way, the payload is in
            // `kick_payloads` so the dispatch sees it.
            let already_in_ready = ready
                .iter()
                .any(|(rid, g)| rid == node_id && g.frames.is_empty());
            if !already_in_ready {
                // Synthesize a ReadyGroup at frames=[] with input
                // populated from the node's config. Configurable input
                // ports (`Range.to: 10`, etc.) reach the firing as
                // input values; without this the kick path saw an empty
                // bag and the node failed with
                // `missing input on port: <name>` despite the source
                // setting the value.
                let input = match project.nodes.iter().find(|n| n.id == *node_id) {
                    Some(def) => weft_core::exec::ready::build_kicked_input(
                        def,
                        info.port_snapshot.as_ref(),
                    ),
                    None => Value::Object(serde_json::Map::new()),
                };
                ready.push((
                    node_id.clone(),
                    weft_core::exec::ready::ReadyGroup {
                        frames: Vec::new(),
                        color,
                        input,
                        closed_ports: Vec::new(),
                        should_skip: false,
                        pulse_ids: Vec::new(),
                        error: None,
                    },
                ));
            }
            info.dispatched = true;
        }
        if !ready.is_empty() {
            let ids: Vec<&str> = ready.iter().map(|(id, _)| id.as_str()).collect();
            tracing::info!(
                target: "weft_engine::execution_driver",
                color = %color,
                ready_ids = ?ids,
                "ready batch"
            );
            // Dispatching new work counts as progress: the just-spawned
            // tasks haven't been polled by the runtime yet, so the next
            // no-progress drain must NOT immediately declare stuck.
            idled_since_progress = false;
        }

        // Dispatch every ready group that isn't already Running for
        // this (node_id, color, frames). Each dispatch either short-
        // circuits (skip/failure) or spawns a task.
        for (node_id, mut group) in ready {
            tracing::info!(
                target: "weft_engine::execution_driver",
                node = %node_id,
                color = %group.color,
                frames = ?group.frames,
                "dispatching ready group"
            );
            let Some(node_def) = project.nodes.iter().find(|n| n.id == node_id) else {
                // Unreachable by construction: pulse-driven groups come
                // from `project.nodes` itself and kicks are synthesized
                // from the same definition set. If it ever fires, a
                // silent skip would park this group's pending pulses
                // forever; fail the drive loudly instead.
                return Err(anyhow::anyhow!(
                    "dispatch: ready group references node '{node_id}' that is not in the \
                     project definition; corrupt compiled project shape"
                ));
            };
            // TriggerSetup scoping: if this node isn't in the
            // trigger-upstream closure, skip it. The pulses that got
            // it into `ready` still need to be absorbed, and
            // downstream should still see null pulses so cascading
            // skips terminate cleanly.
            if let Some(scope) = phase_scope.as_ref() {
                if !scope.contains(&node_id) {
                    group.should_skip = true;
                }
            }
            // Absorb input pulses for this dispatch.
            if let Some(bucket) = pulses.get_mut(&node_id) {
                for p in bucket.iter_mut() {
                    if group.pulse_ids.contains(&p.id) && p.status == weft_core::pulse::PulseStatus::Pending {
                        p.absorb();
                    }
                }
            }

            // Resume detection: if a non-terminal exec already
            // exists at this (node, frames), this dispatch continues
            // that record (state Suspended → Running). Otherwise
            // it's a first dispatch and we open a new record.
            let existing = executions
                .get(&node_id)
                .and_then(|v| v.iter().rposition(|e| e.frames == group.frames && !e.status.is_terminal()));
            let is_resume = existing.is_some();
            // For NodeResumed event reporting: the token + resolved value
            // of the await this dispatch is actually resuming on. Scope it
            // to the existing record's CURRENT parked token
            // (`callback_id`), NOT just "the last resolved await in the
            // sequence". A crashed-Running recovery has the record Running
            // with `callback_id = None` (it is not parked on anything), so
            // this is None and the dispatch ships a crash re-run, not a
            // fresh delivery. Picking the last resolved await instead would
            // make a multi-await body whose EARLIER await resolved in a
            // prior life ship a NodeResumed carrying that stale token/value,
            // rendering a crash re-run as a fresh signal delivery in the
            // inspector.
            let parked_token: Option<String> = existing
                .and_then(|idx| executions.get(&node_id).and_then(|v| v.get(idx)))
                .and_then(|e| e.callback_id.clone());
            let resume_token_value: Option<(String, serde_json::Value)> = parked_token
                .as_ref()
                .and_then(|token| {
                    awaited_sequences
                        .get(&(node_id.clone(), group.frames.clone()))
                        .and_then(|seq| {
                            seq.iter().rev().find_map(|e| match &e.kind {
                                weft_core::primitive::AwaitedEntryKind::Await {
                                    token: t,
                                    resolved: Some(value),
                                } if t == token => Some((token.clone(), value.clone())),
                                _ => None,
                            })
                        })
                });

            if is_resume {
                if let Some(idx) = existing {
                    if let Some(record) = executions.get_mut(&node_id).and_then(|v| v.get_mut(idx)) {
                        record.status = NodeExecutionStatus::Running;
                        // Keep the original input: the fold always
                        // recorded it (`NodeStarted.input` is non-null),
                        // and a resume replays from the journal sequence
                        // rather than re-deriving inputs.
                        //
                        // Extend `pulses_absorbed` with this resume
                        // dispatch's newly-absorbed pulses, exactly as the
                        // fold does on `NodeResumed` (events.rs). The
                        // dispatch site below ships those same ids via
                        // `NodeResumed.pulses_absorbed`, so a full refold
                        // lands them here too. This keeps the live record
                        // equal to what a refold produces (RAM == refold),
                        // the invariant the in-place resume path
                        // (`resume_resolved_suspensions_in_place`) relies
                        // on when it reads `pulses_absorbed` straight from
                        // the live record. The matching fold-side flip
                        // (events.rs `NodeResumed` arm marks these pulses
                        // Absorbed in the pulse table) is what makes a
                        // refold-after-stall NOT re-fire this node: without
                        // it the pulses came back Pending at a terminal
                        // record and `find_ready_nodes` double-executed the
                        // node. Both sides must stay in lockstep.
                        for id in &group.pulse_ids {
                            if !record.pulses_absorbed.contains(id) {
                                record.pulses_absorbed.push(*id);
                            }
                        }
                        // Clear the suspension token, exactly as the fold
                        // does on `NodeResumed` (events.rs). The record is
                        // now Running, not parked, so it no longer holds a
                        // pending callback; leaving the old token here is a
                        // RAM-vs-refold divergence (a refold would show
                        // None) and a trap for any future reader of
                        // `callback_id` on a Running record.
                        record.callback_id = None;
                    }
                }
            } else {
                let dispatch_pulse_id = uuid::Uuid::new_v4();
                let record = NodeExecution {
                    id: uuid::Uuid::new_v4(),
                    node_id: node_id.clone(),
                    status: NodeExecutionStatus::Running,
                    pulses_absorbed: group.pulse_ids.clone(),
                    dispatch_pulse: dispatch_pulse_id,
                    error: group.error.clone(),
                    callback_id: None,
                    started_at: now_unix(),
                    completed_at: None,
                    input: Some(group.input.clone()),
                    output: None,
                    cost_usd: 0.0,
                    logs: Vec::new(),
                    port_warnings: Vec::new(),
                    color: group.color,
                    frames: group.frames.clone(),
                };
                executions.entry(node_id.clone()).or_default().push(record);
            }

            // Ship the lifecycle event NOW, before any branch. Every
            // dispatch that reaches here has already absorbed
            // `group.pulse_ids` and created (or resumed) an exec record;
            // the journal MUST carry a matching NodeStarted/NodeResumed so
            // a refold reconstructs that record and re-absorbs those
            // pulses. Shipping it after the skip/error/passthrough/loop
            // branches (the old placement) meant a skipped or failed
            // firing absorbed its pulses in RAM but journaled no absorbing
            // event: on refold the pulses came back Pending with no record,
            // and `find_ready_nodes` (which keys purely on Pending pulses)
            // re-fired the node, replaying the whole skip/failure cascade
            // on every respawn. A skipped/failed firing is still a firing
            // the fold must reconstruct.
            ship_node_lifecycle(
                journal, pod_name, color, &node_id, &group.frames,
                &group.input, &group.closed_ports, &group.pulse_ids,
                resume_token_value.as_ref(), is_resume,
            ).await;

            if group.should_skip {
                handle_node_skip(
                    &node_id, group.color, &group.frames, &group.closed_ports,
                    project, edge_idx, pulses, executions, journal, pod_name,
                    phase_scope.as_ref(),
                )
                .await;
                continue;
            }

            if let Some(err) = &group.error {
                // Pre-dispatch failure: the body never ran, so no port
                // was ever emitted. Empty mentioned set → close every
                // declared output port.
                let mentioned = std::collections::HashSet::new();
                handle_node_failure(
                    &node_id, &mentioned, group.color, &group.frames, err,
                    project, edge_idx, pulses, executions, journal, pod_name,
                    phase_scope.as_ref(),
                )
                .await;
                continue;
            }

            // Group boundary nodes (Passthrough) are built-in firings
            // handled inline, like the loop boundaries below: a plain
            // Group's __in/__out forwards every input port verbatim to
            // the same-named output at the same frames. There is no
            // catalog impl and no async body to spawn. Closed inputs
            // are absent from the input bag, so the unmentioned sweep
            // closes their same-named outputs and skips cascade
            // through (and out of) the group.
            if node_def.node_type == "Passthrough" {
                let forwarded = group.input.clone();
                let mut emissions = Vec::new();
                match weft_core::exec::postprocess_output(
                    &node_id, &forwarded, color, &group.frames,
                    project, pulses, edge_idx, &mut emissions,
                ) {
                    Ok(mentioned) => {
                        // Passthrough is SYNCHRONOUS: it forwards its
                        // inputs verbatim and has no async body, so a
                        // crash between shipping the forwarded values and
                        // NodeCompleted would re-fire it on resume and
                        // re-emit with fresh ids (duplicate pulses
                        // downstream). Carry BOTH the forwarded values AND
                        // the unmentioned-port closures INSIDE NodeCompleted
                        // so the whole emission set folds atomically with
                        // the terminal marker (push_pulse dedup makes a
                        // replayed row idempotent).
                        let mut all_emissions = emissions;
                        all_emissions.extend(build_unmentioned_closures_and_prune(
                            &node_id, &mentioned, color, &group.frames,
                            project, edge_idx, pulses, phase_scope.as_ref(),
                        ));
                        mark_completed(executions, &node_id, color, &group.frames);
                        ship_node_completed(journal, pod_name, color, &node_id, &group.frames, &forwarded, all_emissions).await;
                    }
                    Err(e) => {
                        let mentioned = std::collections::HashSet::new();
                        handle_node_failure(
                            &node_id, &mentioned, color, &group.frames, &e.to_string(),
                            project, edge_idx, pulses, executions, journal, pod_name,
                            phase_scope.as_ref(),
                        ).await;
                    }
                }
                continue;
            }

            // Loop boundary nodes (LoopIn/LoopOut) are NOT in the
            // catalog: they're built-in firings handled inline by the
            // engine + `LoopRuntime`. Intercept BEFORE the catalog
            // lookup so the dispatch produces per-iteration body
            // pulses (LoopIn) or records LoopOut state and emits
            // outward at termination (LoopOut). NodeStarted was shipped
            // by the common path above; these ship NodeCompleted here
            // after the inline handler returns, and run synchronously
            // (there is no async body to spawn).
            if matches!(node_def.node_type.as_str(), "LoopIn" | "LoopOut") {
                let outcome = handle_loop_boundary_firing(
                    node_def,
                    &group,
                    project,
                    edge_idx,
                    pulses,
                    journal,
                    pod_name,
                    loop_runtime,
                )
                .await;
                match outcome {
                    Ok(()) => {
                        mark_completed(executions, &node_id, color, &group.frames);
                        // Loop boundary: closures are the loop machinery's
                        // job, not the generic sweep; no closures to carry.
                        ship_node_completed(journal, pod_name, color, &node_id, &group.frames, &serde_json::Value::Object(serde_json::Map::new()), Vec::new()).await;
                    }
                    Err(err) => {
                        handle_loop_boundary_failure(
                            node_def, color, &group.frames, &err,
                            project, edge_idx, pulses, executions, journal, pod_name,
                            phase_scope.as_ref(), loop_runtime,
                        )
                        .await;
                    }
                }
                continue;
            }

            let node_impl = match catalog.lookup(&node_def.node_type) {
                Some(n) => n,
                None => {
                    let err = format!("unknown node type: {}", node_def.node_type);
                    let mentioned = std::collections::HashSet::new();
                    handle_node_failure(
                        &node_id, &mentioned, group.color, &group.frames, &err,
                        project, edge_idx, pulses, executions, journal, pod_name,
                        phase_scope.as_ref(),
                    )
                    .await;
                    continue;
                }
            };

            // The lifecycle event (NodeStarted / NodeResumed) was already
            // shipped above, right after the record was created, so every
            // dispatch path (skip, fail, passthrough, loop, body) carries
            // it. Don't ship a second one here.
            //
            // The node's ONE input bag: everything the ready paths
            // delivered (wired pulses + body literals), the remaining
            // braces config values, and declared defaults for whatever
            // is still absent (a closed wire is never defaulted).
            let inputs = weft_core::context::node_input_bag(
                node_def,
                group.input.as_object().cloned().unwrap_or_default(),
                &group.closed_ports,
            );
            // Hand the per-(node, frames) await sequence to the
            // handle. The body's `await_signal` calls pop entries
            // in call_index order: resolved entries replay
            // instantly, the pending tail re-suspends, and an
            // exhausted sequence (or fresh node) registers a new
            // await with the next call_index.
            let sequence = awaited_sequences
                .remove(&(node_id.clone(), group.frames.clone()))
                .unwrap_or_default();

            let declared_outputs: std::collections::HashMap<String, weft_core::weft_type::WeftType> =
                node_def
                    .outputs
                    .iter()
                    .map(|p| (p.name.clone(), p.port_type.clone()))
                    .collect();
            let wake_payload = kick_payloads.remove(&(node_id.clone(), group.frames.clone()));
            let mut runner = RunnerHandle::new(
                exec_id.to_string(),
                project.id.to_string(),
                group.color,
                node_id.clone(),
                node_def.node_type.clone(),
                group.frames.clone(),
                clients.clone(),
                pod_name.to_string(),
                tenant_id.to_string(),
                cancellation.clone(),
                bus_coordinator.clone(),
                declared_outputs,
            )
            .with_awaited_sequence(sequence)
            .with_emit_channel(task_tx.clone())
            .with_caller_connection(caller.cloned());
            let has_wake = wake_payload.is_some();
            if let Some(payload) = wake_payload {
                runner = runner.with_wake_payload(payload);
            }
            // A concrete clone survives next to the ctx so the spawn can
            // give back the firing's provider accesses after the body ends.
            let runner = Arc::new(runner);
            let runner_for_close = runner.clone();
            let handle = runner as Arc<dyn weft_core::context::ContextHandle>;

            let ctx = ExecutionContext::new(
                exec_id.to_string(),
                project.id.to_string(),
                node_id.clone(),
                node_def.node_type.clone(),
                node_def.label.clone(),
                group.color,
                group.frames.clone(),
                inputs,
                handle,
            );

            // The lifecycle event (NodeStarted or NodeResumed) was
            // already shipped earlier in this loop body, before
            // the spawn. Don't ship a second one here.

            // Spawn the node's body as a task. For infra nodes in
            // `Phase::InfraSetup` the body runs in two stages:
            //   1. `node_impl.provision_infra(infra_ctx, input)` returns
            //      an InfraSpec. Failure here = node fails with stage
            //      "provision"; downstream cascade-skips.
            //   2. Engine compiles spec locally, asks broker for prior
            //      applied state, picks skip / fresh / replace, and
            //      (when not skip) enqueues an Apply lifecycle command
            //      via the broker. The tenant's supervisor pod claims
            //      the command and runs kubectl. Failure here = node
            //      fails with stage "apply".
            //   3. `node_impl.run(ctx)` runs as usual, with
            //      `ctx.endpoint_url(name)` now resolving against the
            //      freshly-applied infra_node row. Failure here = node
            //      fails with stage "run"; the infra stays up
            //      (provisioned-but-run-failed sub-state).
            //
            // Otherwise, the engine picks the node body from the phase
            // plus the manifest (`node_body_for`): a trigger's
            // `setup_trigger` at TriggerSetup; at Fire, `run` for THE
            // firing trigger (its dispatch carries the wake payload)
            // while every other trigger terminates as Completed so its
            // ports close; nothing at InfraSetup either. Nodes never
            // see the phase. The task sends its terminal back on the
            // shared `task_tx` (after any emissions it sent on the same
            // channel); the main loop applies the effect on
            // `pulses`/`executions`.
            let body = node_body_for(phase, node_def.features.is_trigger, has_wake);
            let tx = task_tx.clone();
            let node_id_task = node_id.clone();
            let color_task = group.color;
            let frames_task = group.frames.clone();
            // node_impl is &'static dyn Node (see NodeCatalog::lookup
            // contract). No allocation or unsafe needed.
            let is_infra_setup_provision =
                matches!(phase, weft_core::context::Phase::InfraSetup) && node_def.requires_infra;
            let provision_project_id = project.id.to_string();
            let provision_node_id = node_id.clone();
            let provision_tenant_id = tenant_id.to_string();
            let provision_namespace = namespace.to_string();
            let provision_clients = clients.clone();
            // Input values for provisioning (the same one-bag build the
            // run-time dispatch uses, so provision bodies read the same
            // view a `run` body would).
            let provision_input = weft_core::context::node_input_bag(
                node_def,
                group.input.as_object().cloned().unwrap_or_default(),
                &group.closed_ports,
            );
            let abort_handle = in_flight.spawn(async move {
                if is_infra_setup_provision {
                    // 1. Call the node's provision body.
                    let infra_ctx = weft_core::infra::InfraProvisionContext::new(
                        provision_project_id.clone(),
                        provision_node_id.clone(),
                        provision_namespace.clone(),
                        provision_tenant_id.clone(),
                    );
                    let spec = match node_impl.provision_infra(infra_ctx, provision_input).await {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = tx.send(TaskMsg::Terminal {
                                node_id: node_id_task,
                                color: color_task,
                                frames: frames_task,
                                outcome: NodeTaskOutcome::Failed(format!("provision: {e}")),
                            });
                            return;
                        }
                    };

                    // 2. Enqueue an Apply lifecycle command and wait.
                    //
                    // The worker does NOT compile or hash the spec.
                    // Compile requires the per-(project, node) image
                    // tag map; only the supervisor has the role +
                    // RBAC to read it. More importantly, the
                    // supervisor mints the real `instance_id` at
                    // apply time, so any worker-side hash would be
                    // computed against a placeholder and would never
                    // match the supervisor's hash anyway.
                    //
                    // Single source of compile + hash: the
                    // supervisor reads the prior `infra_node` row,
                    // compiles the new spec with the real instance
                    // id + image tags, hashes, decides skip / fresh
                    // / replace, and executes. The worker just polls
                    // the command row for terminal state.
                    //
                    // After this returns Ok the supervisor has
                    // written `infra_node` to Running (or short-
                    // circuited on Skip), so the subsequent execute
                    // can call `ctx.endpoint_url` and get a live URL.
                    if let Err(e) = crate::context::apply_via_supervisor(
                        provision_clients.infra_state.as_ref(),
                        provision_clients.clock.as_ref(),
                        &provision_project_id,
                        &provision_node_id,
                        &spec,
                    )
                    .await
                    {
                        let _ = tx.send(TaskMsg::Terminal {
                            node_id: node_id_task,
                            color: color_task,
                            frames: frames_task,
                            outcome: NodeTaskOutcome::Failed(format!("apply: {e}")),
                        });
                        return;
                    }
                    // 3. Fall through to run.
                }

                // The node body returns `()`: it fires downstream only via
                // `ctx.pulse_downstream` (emissions ride the SAME task
                // channel, applied by the loop while the task runs, and
                // always BEFORE this terminal by FIFO ordering). The
                // return just signals terminal outcome.
                //
                // The guard gives back every runtime-granted provider
                // access the body opened, on EVERY exit: awaited inline on
                // normal completion, spawned detached when the task is
                // ABORTED mid-body (a cancel), where no code after the
                // body ever runs. Runtime plumbing, not the node's job.
                let access_guard = AccessCloseGuard(Some(runner_for_close));
                let result = match body {
                    NodeBody::Run => node_impl.run(ctx).await,
                    NodeBody::SetupTrigger => node_impl.setup_trigger(ctx).await,
                    // A trigger has no infra-phase work: terminate as
                    // Completed without invoking the node, so its ports
                    // close and downstream learns nothing is coming.
                    NodeBody::SkipTrigger => Ok(()),
                };
                access_guard.close_now().await;
                let outcome = match result {
                    Ok(()) => NodeTaskOutcome::Completed,
                    Err(weft_core::error::WeftError::Suspended { token }) => {
                        NodeTaskOutcome::Waiting(token)
                    }
                    Err(e) => NodeTaskOutcome::Failed(format!("{e}")),
                };
                let _ = tx.send(TaskMsg::Terminal {
                    node_id: node_id_task,
                    color: color_task,
                    frames: frames_task,
                    outcome,
                });
            });
            task_firings.insert(abort_handle.id(), (node_id.clone(), group.frames.clone()));
        }

        // Drain the task channel in FIFO order: each `Emission`
        // (a still-running node's `pulse_downstream` / `close_port`)
        // postprocesses into downstream pulses and records the mentioned
        // port, keeping the emitting node Running; each `Terminal` closes
        // the firing's record and closes every UNmentioned output port.
        // Because emissions and the terminal share this one ordered
        // channel, a node's emissions are always drained before its
        // terminal, so the close-unmentioned sweep sees the complete
        // mentioned set (no emit-then-return race). Non-blocking so we
        // keep dispatching newly-ready nodes next iteration.
        let progress = apply_task_msgs(
            &mut task_rx,
            color,
            project,
            edge_idx,
            pulses,
            executions,
            journal,
            pod_name,
            &mut waiting,
            &mut emitted_ports,
            phase_scope.as_ref(),
            /* is_cancel = */ false,
        )
        .await;

        if progress {
            idled_since_progress = false;
            continue;
        }

        // No progress from draining. Check: is anything still in flight?
        if in_flight.is_empty() {
            return terminate(pulses, executions, &waiting).await;
        }

        // Stuck-check: we drained twice without progress, with at least
        // one idle-wait in between. The first no-progress pass might just
        // mean "tasks were scheduled but not yet polled by the runtime";
        // the second no-progress pass AFTER the select woke us means the
        // wake came from somewhere (a task finishing, an emission landing,
        // a cancellation), we re-drained, and there is STILL nothing for
        // the loop to do but the in-flight tasks are alive. That is the
        // definition of stuck: the only way out is to close every live
        // bus (every waiting `wait_for` / cursor wakes with `Closed` /
        // `None`, bodies fail or recover, tasks complete, the next
        // iteration drains them and either dispatches more work or
        // terminates).
        //
        // Persisted external suspensions (executions-table
        // `WaitingForInput` records, via `waiting_count`) keep us out of
        // stuck: those waits fire from outside the worker.
        // Stuck only when EVERY in-flight task is a node execution
        // PARKED and CAUGHT UP on a bus, AND a bus exists to be the wait.
        // `deadlock_provable(in_flight.len())` evaluates this under ONE
        // liveness-map-lock snapshot: liveness is keyed by node execution
        // `(node_id, frames)`, so a node holding several bus
        // registrations is ONE entry and counts ONCE. The close fires
        // only when the count of parked-and-caught-up nodes EQUALS
        // `in_flight.len()`: every live task accounted for as a parked
        // node, none off computing.
        //
        // Why per-node liveness is sound where a per-waiter count was
        // not: the old shape counted loose cursor waits and required
        // "waiters >= in_flight", which assumed one waiter per task and
        // broke the moment a node held two concurrent waits (e.g. a
        // `select!` over two cursors): a live node could be closed under.
        // Keying on the node execution removes that assumption: a node's
        // several concurrent waits collapse to one entry that counts as
        // parked only when EVERY wait is parked.
        //
        // ASSUMPTION (holds for every catalog node today): all of a node
        // execution's bus waits run on that node's single dispatched
        // task. A node body that `tokio::spawn`ed a detached task holding
        // a bus cursor would attribute that helper's wait to the node
        // while the helper is invisible to `in_flight`, breaking the
        // count. No node does this; if a future pattern needs detached
        // bus work, enforce "bus waits run on the node's own task" loudly
        // (compare `tokio::task::id()` in `enter_wait`) rather than
        // letting the count silently skew.
        //
        // GROUND-TRUTH, not a scheduler race. A node parked on a bus is
        // counted only when it has observed that bus's CURRENT settled
        // append generation (read under the bus's log lock inside
        // `deadlock_provable`). A receiver woken by a send but still
        // unpolled in another worker thread's queue has NOT re-evaluated
        // since the send bumped the generation, so it reads as behind
        // and is excluded; a receiver mid-evaluation (observed recorded,
        // search not yet returned) reads as not-parked and is excluded;
        // a receiver that RESOLVED but has not yet acted has left its
        // wait, dropping the parked count below `in_flight`. Every case
        // keeps the count short of `in_flight`, suppressing the close BY
        // CONSTRUCTION (the generation recorded BEFORE the evaluation,
        // the parked flag set only AFTER every pre-park re-check failed),
        // not by betting the runtime polled the woken peer in time. So
        // the per-node generation check alone is exact at the check
        // instant: no across-park generation comparison and no grace
        // timer are needed. Every liveness transition (`enter_wait` /
        // `exit_wait` / `observed` / `parked` / `on_append`) re-wakes
        // this loop, so once a lagging node re-checks (consuming the
        // message or re-parking caught-up) we re-evaluate promptly.
        //
        // A genuine deadlock (every in-flight task a node parked having
        // observed its bus's final generation) still closes; a live
        // send-then-park fails the count or the generation, so a
        // conversation is never torn down under a peer not yet scheduled.
        if idled_since_progress
            && waiting_count(executions) == 0
            && bus_coordinator.has_live_buses()
            && bus_coordinator.deadlock_provable(in_flight.len())
        {
            tracing::warn!(
                target: "weft_engine::execution_driver",
                color = %color,
                in_flight = in_flight.len(),
                parked_nodes = bus_coordinator.parked_nodes_count(),
                "every in-flight task is a node parked on a bus with no unconsumed \
                 activity; closing all buses to unwind"
            );
            bus_coordinator.close_all();
            // Don't `continue`: the waiting tasks wake in other tokio
            // tasks; their results arrive on `result_rx` shortly.
            // Falling through to the idle-wait yields to the runtime and
            // wakes on the first real event. The select below re-sets
            // `idled_since_progress = true` on return; the next
            // iteration drains the unwound state.
        }

        // At least one in-flight task. Block until something happens:
        // a task terminates, a still-running node emits (a live bus
        // node sits in its loop and only emits; its task never ends
        // until the bus closes, so we MUST also wake on an emission
        // or we'd hang), a task entered a bus wait (so the next
        // stuck-check can fire if everything else is silent), or
        // cancellation.
        //
        // Arm `on_bus_wait` BEFORE entering the select. `Notify::notified`
        // only registers a waiter the first time the future is polled,
        // and `notify_waiters` stores no permit; a wait that fires
        // between future creation and first poll would be lost without
        // pin + `enable()`.
        //
        // DO NOT poll `result_rx` here: `recv().await` would consume
        // the message and drop it. Same reason we don't drain emit_rx
        // here; we just need the wakeup.
        // Poll the journal for a resume ONLY when a bus is holding the
        // worker alive AND a suspension is pending. In that state the
        // worker can't exit (bus tasks in-flight) so the outer re-fetch
        // loop never runs; this in-loop poll is the only way an arriving
        // `SuspensionResolved` reaches the parked node. Disabled
        // otherwise (a never-resolving sleep) so the common no-bus /
        // no-suspension path doesn't poll the journal at all.
        let resume_poll_active =
            bus_coordinator.has_live_buses() && waiting_count(executions) > 0;
        let resume_poll = async {
            if resume_poll_active {
                clients
                    .clock
                    .sleep(std::time::Duration::from_millis(RESUME_POLL_INTERVAL_MS))
                    .await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(resume_poll);

        let on_bus_wait = bus_coordinator.wait_notified();
        tokio::pin!(on_bus_wait);
        on_bus_wait.as_mut().enable();
        tokio::select! {
            _ = resume_poll.as_mut() => {
                // Bus-held worker with a pending suspension. Re-fetch the
                // journal; if a new row landed, SURGICALLY resume only the
                // parked nodes whose current suspension just resolved. We
                // do NOT `apply_snapshot` (a full re-fold): mid-flight the
                // in-RAM `executions`/`pulses` are AHEAD of the journal
                // for the live bus tasks (Running execs that are genuinely
                // in-flight, not crashed), and a full re-fold would
                // re-dispatch them (double-run) and reset their state. The
                // surgical path touches only the resolved waiters; the bus
                // tasks and their state are left exactly as they are.
                let fresh = fetch_events(journal, color).await?;
                debug_assert!(fresh.len() >= journaled_count, "journal shrank under us");
                if fresh.len() > journaled_count {
                    journaled_count = fresh.len();
                    let resumed = resume_resolved_suspensions_in_place(
                        color, &fresh, executions, pulses, kicked, &mut awaited_sequences,
                    );
                    if resumed > 0 {
                        tracing::info!(
                            target: "weft_engine::resume",
                            color = %color,
                            resumed,
                            "bus-held worker resumed suspension(s) in process; bus untouched"
                        );
                    }
                }
            }
            joined = in_flight.join_next_with_id() => {
                // A spawned node task ended. A JoinError here means a
                // panic: cancellation-aborted tasks are drained inside
                // `JoinSet::shutdown().await` (see cancel_cleanup) and
                // never surface in this idle-wait arm. The panicked task
                // never sent its NodeTaskResult, so we must turn the
                // panic into a terminal NodeFailed for the right firing
                // ourselves (looked up via the task id); otherwise its
                // exec record stays Running, the crashed-Running refold
                // path re-dispatches it on every respawn, and the node
                // panics in an infinite re-run loop. A successful task
                // already reported via `result_tx`; we just drop its id.
                match joined {
                    Some(Ok((task_id, ()))) => {
                        task_firings.remove(&task_id);
                    }
                    Some(Err(join_err)) => {
                        let task_id = join_err.id();
                        match task_firings.remove(&task_id) {
                            Some((node_id, frames)) => {
                                let err = format!("node task panicked: {join_err}");
                                tracing::error!(
                                    target: "weft_engine::execution_driver",
                                    color = %color,
                                    node = %node_id,
                                    frames = ?frames,
                                    error = %err,
                                    "in-flight node task panicked; failing the node"
                                );
                                // Route the panic through the SAME failure
                                // path a body-returned error takes: send a
                                // synthetic Failed terminal and loop. The
                                // next iteration drains the task channel in
                                // FIFO order, so any pulses the node emitted
                                // before panicking are applied first, then
                                // this Terminal fails the node with the
                                // correct `mentioned` set from
                                // `emitted_ports` (keeping already-emitted
                                // ports' values, closing only the rest) and
                                // cleans up its `emitted_ports` entry.
                                // Handling it inline with an empty mentioned
                                // set would double-pulse already-emitted
                                // ports (value + closure on one edge) and
                                // leak the emitted_ports entry.
                                let _ = task_tx.send(TaskMsg::Terminal {
                                    node_id,
                                    color,
                                    frames,
                                    outcome: NodeTaskOutcome::Failed(err),
                                });
                            }
                            None => {
                                // No identity recorded: a panic from a
                                // task we don't own (should be impossible).
                                // Fail loud rather than silently drop it.
                                tracing::error!(
                                    target: "weft_engine::execution_driver",
                                    color = %color,
                                    error = %join_err,
                                    "in-flight task panicked with no recorded firing; \
                                     engine invariant violated"
                                );
                            }
                        }
                    }
                    None => {}
                }
            }
            task_msg = task_rx.recv() => {
                // Apply this message immediately (we consumed it from the
                // channel, so we can't let it drop). It may be an Emission
                // OR a Terminal; both must be handled here, not just
                // emissions. Then loop. Live path, not cancel: a bad-shape
                // emission journals NodeFailed.
                if let Some(msg) = task_msg {
                    apply_one_task_msg(
                        msg, color, project, edge_idx, pulses, executions, journal, pod_name,
                        &mut waiting, &mut emitted_ports, phase_scope.as_ref(),
                        /* is_cancel = */ false,
                    )
                    .await;
                }
            }
            _ = on_bus_wait.as_mut() => {
                // A task just entered a bus wait. The next drain will
                // re-check stuck.
            }
            _ = cancellation.cancelled() => {
                tracing::info!(
                    target: "weft_engine::execution_driver",
                    color = %color,
                    "cancellation observed at idle wait; exiting Failed(cancelled)"
                );
                cancel_cleanup(
                    &mut in_flight,
                    &mut task_rx,
                    &mut waiting,
                    executions,
                    &mut emitted_ports,
                    color,
                    project,
                    edge_idx,
                    pulses,
                    journal,
                    pod_name,
                    phase_scope.as_ref(),
                    loop_runtime,
                )
                .await;
                return Ok(ExecutionOutcome::Cancelled);
            }
        }
        // We just unblocked from the idle-wait. The next no-progress
        // drain is allowed to declare stuck.
        idled_since_progress = true;
    }
}

/// Tear down a terminated firing downstream: emit CLOSURE markers
/// on every output port the firing did NOT mention, ship those
/// mutations, then prune out-of-scope pulses. The SHARED tail of
/// every node-down path (success-with-unmentioned-ports, failure,
/// skip). The prune is here, not at the call sites, so a new
/// node-down path can't forget it (during a setup phase
/// `close_unmentioned_downstream` can emit onto out-of-scope nodes,
/// which would otherwise be dispatched, auto-skipped, and churn the
/// loop).
///
/// A closure is structural: it tells the consumer "this port is dead
/// at this frame stack". A user-emitted null is data; these are different
/// signals and the consumer-side `skip` distinguishes them.
#[allow(clippy::too_many_arguments)]
/// Build (and apply to `pulses`) the CLOSURE pulses on every output port
/// the firing did NOT mention, then prune out-of-scope pulses. RETURNS
/// the emissions so the caller folds them into its terminal row
/// (NodeCompleted / NodeFailed / NodeSkipped) so the marker and the
/// closures it implies are ONE atomic write: a crash between two separate
/// writes would lose the closures and leave downstream consumers neither
/// firing nor skipping (refold Stuck). Every terminal-shipping path uses
/// this; the standalone shipping wrapper below exists only for the
/// cancel-cleanup walk, which has no terminal row to carry them.
fn build_unmentioned_closures_and_prune(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    phase_scope: Option<&std::collections::HashSet<String>>,
) -> Vec<weft_core::exec::PulseEmission> {
    // Loop boundary nodes (LoopIn, LoopOut) fire many times during a
    // loop's lifetime and their outward output ports must NOT
    // auto-close on per-iteration firings. The engine closes them at
    // the loop's outward emit moment. This is the single chokepoint
    // for the language layer's "close unmentioned" semantics, so the
    // skip lives here, NOT in `exec::postprocess` (which stays
    // generic over node types).
    if is_loop_boundary_node(project, node_id) {
        return Vec::new();
    }
    let mut emissions = Vec::new();
    // Teardown cannot propagate (this is the shared tail of failure /
    // skip / completion paths), so a sweep error (node or port missing
    // from the project: corrupt compiled shape) is logged at error
    // level here, the single chokepoint. Partial emissions still ship
    // so whatever closed before the error reaches downstream.
    if let Err(e) = close_unmentioned_downstream(
        node_id, mentioned, color, frames, project, pulses, edge_idx, &mut emissions,
    ) {
        tracing::error!(
            target: "weft_engine::execution_driver",
            node = node_id,
            error = %e,
            "closure sweep failed; downstream consumers of this node's unclosed ports \
             will neither fire nor skip"
        );
    }
    if let Some(scope) = phase_scope {
        drop_out_of_scope_pulses(pulses, scope);
    }
    emissions
}

/// Shipping wrapper that journals the closures as standalone
/// `PulseEmitted` rows. Sole caller is the cancel-cleanup walk, which
/// suppresses the per-node terminal row (the dispatcher's NodeCancelled
/// is the status truth), so there is no terminal row to carry them.
/// Every other path folds the closures into its terminal event instead.
async fn close_unmentioned_downstream_and_prune(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    let emissions = build_unmentioned_closures_and_prune(
        node_id, mentioned, color, frames, project, edge_idx, pulses, phase_scope,
    );
    crate::context::ship_pulse_emissions(journal, pod_name, emissions).await;
}

/// Whether `node_id` is a `LoopIn` or `LoopOut` boundary node. Lives
/// in the engine, not in `weft-core`, so the language layer stays
/// generic over node types.
fn is_loop_boundary_node(project: &ProjectDefinition, node_id: &str) -> bool {
    project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .map(|n| n.node_type == "LoopIn" || n.node_type == "LoopOut")
        .unwrap_or(false)
}

/// LoopIn / LoopOut firing handler. The engine treats these two
/// boundary node types as built-in: no catalog impl, no spawned task.
/// LoopIn instantiates / lookups the `LoopInstance`, validates zip
/// lengths, computes the effective iteration count, and emits per-
/// iteration pulses on its inside outputs at the body's frame stack.
/// LoopOut records per-iteration writes via `LoopRuntime` and, on
/// termination, emits assembled gather lists + final carry values on
/// its outer outputs at the parent frame stack.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_loop_boundary_firing(
    node_def: &weft_core::project::NodeDefinition,
    group: &weft_core::exec::ready::ReadyGroup,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
) -> Result<(), String> {
    use crate::loop_runtime::{LoopAdvance, LoopConfig};
    use weft_core::primitive::{LoopInstanceKey, LoopTerminationReason};

    let group_id = node_def
        .group_boundary
        .as_ref()
        .ok_or_else(|| format!("{} '{}' missing group_boundary", node_def.node_type, node_def.id))?
        .group_id
        .clone();
    let parent_frames =
        crate::loop_runtime::boundary_parent_frames(&node_def.node_type, &group.frames);
    let key = LoopInstanceKey {
        group_id: group_id.clone(),
        parent_frames,
        color: group.color,
    };
    let input_obj = group.input.as_object().cloned().unwrap_or_default();

    // A boundary firing over an instance that already terminated FAILED
    // must route to the FAILURE path, not the idle-complete path:
    // completing it would journal NodeCompleted and silently lose the
    // loop's failure status (the execution would fold with zero Failed
    // nodes). Two reachable cases: a crash-resume where LoopTerminated
    // {Failed} landed but the boundary's NodeFailed did not, AND a live
    // same-worker-life straggler (a sibling iteration's LoopOut still
    // queued after another iteration's failure marked the instance
    // Failed). Both route to `handle_loop_boundary_failure`, whose
    // already-terminated branch ships NodeFailed with no closures (they
    // already rode LoopTerminated) and no second LoopTerminated. Only
    // FAILED is gated: idle replays after OverExhausted / DoneVoted /
    // MaxItersReached / Cancelled are legitimate and stay Ok.
    if matches!(
        loop_runtime.get(&key).and_then(|inst| inst.terminated),
        Some(LoopTerminationReason::Failed)
    ) {
        return Err(format!(
            "loop '{group_id}' already terminated Failed; boundary {} firing routed to the \
             failure path",
            node_def.id
        ));
    }

    if node_def.node_type == "LoopIn" {
        // LoopConfig lives on LoopIn ONLY (the compiler emits the
        // minimal `{"parentId": ...}` on LoopOut). Parse here so a
        // LoopOut firing doesn't hard-error on the missing parallel
        // field; LoopOut reads the config from the runtime instance
        // it shares with LoopIn.
        let config = LoopConfig::from_node_config(&node_def.config)
            .map_err(|e| format!("LoopIn '{}': {}", node_def.id, e))?;
        let iter_count = compute_loop_iter_count(&config, &input_obj)?;
        let gather_ports = loop_gather_ports(project, &group_id, &config.carry)
            .ok_or_else(|| {
                format!(
                    "LoopIn '{}': project has no LoopOut node '{}__out'; \
                     corrupt compiled project shape",
                    node_def.id, group_id,
                )
            })?;
        // Initial carry seeds come from the loop's same-named inputs.
        // A carry port that is wired and carries a real value seeds from
        // it; an unwired (or null) carry seeds from its declared type's
        // ZERO VALUE (Number -> 0, String -> "", List -> [], etc), so a
        // loop can accumulate from a clean default without the author
        // wiring an explicit seed. The port type lives on the LoopIn's
        // input def. Seeded BEFORE `ensure` so a failure cannot leave a
        // RAM instance behind with no LoopInstantiated row.
        let mut seed_carry: Vec<(String, serde_json::Value)> = Vec::new();
        for carry_port in &config.carry {
            let v = match input_obj.get(carry_port) {
                Some(v) if !v.is_null() => v.clone(),
                _ => {
                    let port = node_def
                        .inputs
                        .iter()
                        .find(|p| &p.name == carry_port)
                        .ok_or_else(|| {
                            format!(
                                "loop '{}': carry port '{}' has no input definition on the \
                                 LoopIn node; corrupt compiled project shape",
                                group_id, carry_port,
                            )
                        })?;
                    port.port_type.zero_value()
                }
            };
            seed_carry.push((carry_port.clone(), v));
        }
        let first_instantiation = loop_runtime.ensure(
            key.clone(),
            config.clone(),
            iter_count,
            gather_ports.clone(),
        );
        if first_instantiation {
            // Stash the outer input bag and seed initial carry values
            // BEFORE journaling, so the journal's LoopInstantiated event
            // carries the bag for resume.
            if let Some(inst) = loop_runtime.get_mut(&key) {
                let input_map: HashMap<String, serde_json::Value> =
                    input_obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                inst.outer_input = input_map;
                for (port, v) in seed_carry {
                    inst.carry_values.insert(port, v);
                }
            }
            let (outer_input_map, initial_carry_map) = loop_runtime
                .get(&key)
                .map(|inst| (inst.outer_input.clone(), inst.carry_values.clone()))
                .unwrap_or_default();
            crate::context::record_from_pod(
                journal,
                weft_journal::ExecEvent::LoopInstantiated {
                    color: group.color,
                    group_id: group_id.clone(),
                    parent_frames: group.frames.clone(),
                    iter_count,
                    parallel: config.parallel,
                    max_iters: config.max_iters,
                    over: config.over.clone(),
                    carry: config.carry.clone(),
                    trim_on_mismatch: config.trim_on_mismatch,
                    outer_input: outer_input_map,
                    initial_carry: initial_carry_map,
                    at_unix: now_unix(),
                },
                pod_name,
            )
            .await;
        }
        let inst_carry: Vec<(String, serde_json::Value)> = loop_runtime
            .get(&key)
            .map(|inst| inst.carry_values.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        // Decide which iterations to launch now. Launches derive from
        // the instance's (journal-backed) `launched` set, NEVER from
        // `first_instantiation`: on crash-resume the LoopIn re-fires
        // with first_instantiation=false while the rehydrated instance
        // may have launched only a subset (or none) of its iterations,
        // and deciding from first_instantiation would silently skip
        // the rest (the loop "completes" without running). The inverse
        // hazard is covered too: `LoopIterationLaunched` CARRIES the
        // iteration's body pulses in the same journal row, so an index
        // in `launched` has its body pulses durably journaled and
        // re-launching it would duplicate them.
        let (already_launched, inst_terminated) = loop_runtime
            .get(&key)
            .map(|inst| (inst.launched.clone(), inst.terminated.is_some()))
            .unwrap_or((Vec::new(), false));
        let to_launch: Vec<u32> = if inst_terminated {
            Vec::new()
        } else if config.parallel {
            (0..iter_count).filter(|i| !already_launched.contains(i)).collect()
        } else if already_launched.is_empty() && iter_count > 0 {
            vec![0]
        } else {
            // Sequential with a launch already recorded: subsequent
            // launches are handled by LoopOut's
            // `LoopAdvance::LaunchNext` path, not LoopIn.
            Vec::new()
        };
        if iter_count == 0 {
            // Zero-iteration loop: terminate immediately. The runtime
            // assembles the outward payload (a length-0 list for EVERY
            // declared gather port, initial carry values) and marks
            // the instance terminated so a later cancel walk doesn't
            // double-emit closures on the same parent_frames.
            //
            // Reason mirrors the live-path logic in `record_loop_out`:
            // when iter_count was capped at max_iters (including the
            // user writing `max_iters: 0`), the binding constraint is
            // MaxItersReached. Otherwise OverExhausted (empty over).
            let reason = if config.max_iters == Some(iter_count) {
                LoopTerminationReason::MaxItersReached
            } else {
                LoopTerminationReason::OverExhausted
            };
            match loop_runtime.emit_outward(&key, reason) {
                LoopAdvance::EmitOutward { reason, gather, carry } => {
                    emit_loop_outward(
                        project,
                        edge_idx,
                        pulses,
                        journal,
                        pod_name,
                        group.color,
                        &group_id,
                        &group.frames,
                        gather,
                        carry,
                        reason,
                        loop_runtime,
                        &key,
                    )
                    .await?;
                }
                // Idle = the instance was already terminated: a
                // crash-resume replay of this LoopIn after the
                // LoopTerminated row landed. The outward pulses are
                // already journaled; emitting again would duplicate
                // them downstream.
                LoopAdvance::Idle => {}
                LoopAdvance::LaunchNext { .. } => {
                    return Err(format!(
                        "LoopIn '{}': emit_outward returned LaunchNext for a \
                         zero-iteration loop; LoopRuntime invariant violated",
                        node_def.id,
                    ));
                }
            }
        } else {
            for index in to_launch {
                launch_iteration(
                    project,
                    edge_idx,
                    pulses,
                    journal,
                    pod_name,
                    group.color,
                    &group_id,
                    &group.frames,
                    &node_def.id,
                    index,
                    &config,
                    &input_obj,
                    &inst_carry,
                )
                .await?;
                loop_runtime.record_launched(&key, index);
            }
        }
        Ok(())
    } else {
        // LoopOut firing for one iteration.
        let index = group
            .frames
            .last()
            .map(|f| f.index)
            .ok_or_else(|| format!("LoopOut '{}' fired with empty frame stack", node_def.id))?;
        let inst_config = loop_runtime
            .get(&key)
            .map(|inst| inst.config.clone())
            .ok_or_else(|| {
                format!(
                    "LoopOut '{}' fired at parent_frames={:?} index={index} but no LoopInstance exists; \
                     LoopIn must fire before LoopOut",
                    node_def.id, key.parent_frames,
                )
            })?;

        let mut gather_writes: HashMap<String, weft_core::primitive::LoopWrite> = HashMap::new();
        let mut carry_writes: HashMap<String, weft_core::primitive::LoopWrite> = HashMap::new();
        let mut done_vote: Option<bool> = None;

        let closed: std::collections::HashSet<&str> =
            group.closed_ports.iter().map(|s| s.as_str()).collect();
        for port in &node_def.inputs {
            let name = &port.name;
            if name == "done" {
                done_vote = if closed.contains(name.as_str()) {
                    None
                } else {
                    // `done` is a Boolean port marked optional. Three
                    // legitimate inbound shapes, all "no vote":
                    //   - absent from input_obj    (port not wired)
                    //   - present as Value::Null   (no-value marker;
                    //     `check_input` in ready.rs treats Null as Ok
                    //     for any port AND maps non-matching values on
                    //     optional ports to Null via NullIt, so Null
                    //     is the normalized form of "no usable vote")
                    //   - present as a real bool   (the actual vote)
                    // A non-null non-bool value here is impossible
                    // post-type-check; if one slips through, fail loud
                    // because silently dropping the vote would let a
                    // wrongly-typed body skip the termination check.
                    match input_obj.get(name) {
                        None => None,
                        Some(v) if v.is_null() => None,
                        Some(v) => match v.as_bool() {
                            Some(b) => Some(b),
                            None => return Err(format!(
                                "LoopOut '{}' 'done' port received non-boolean non-null value {:?}; \
                                 type-check should have rejected this upstream",
                                node_def.id, v,
                            )),
                        },
                    }
                };
                continue;
            }
            // `LoopWrite::Closed` for closure, `LoopWrite::Value(v)`
            // for an actual write (including a real JSON null, which
            // must round-trip through the journal without collapsing
            // into "closure"). The dispatch invariant says every
            // non-closed input port has a pulse in `input_obj`; if
            // it's missing, that's corruption, not "default to null"
            // (which for carry ports would silently overwrite the
            // current value to null instead of keeping the previous).
            let write = if closed.contains(name.as_str()) {
                weft_core::primitive::LoopWrite::Closed
            } else {
                let v = input_obj.get(name).cloned().ok_or_else(|| format!(
                    "LoopOut '{}' input port '{}' is neither closed nor present in input bag; \
                     dispatch invariant violated",
                    node_def.id, name,
                ))?;
                weft_core::primitive::LoopWrite::Value(v)
            };
            if inst_config.carry.contains(name) {
                carry_writes.insert(name.clone(), write);
            } else {
                gather_writes.insert(name.clone(), write);
            }
        }

        // A `done` vote on a PARALLEL loop has no decision tree to
        // enter (all iterations launched upfront; termination is
        // all-fired). Validate rejects wiring `done` in parallel mode,
        // so a vote arriving here means the compiled config drifted;
        // silently discarding it would let a wrongly-compiled loop
        // ignore its own termination signal. Checked BEFORE the
        // journal write so the refused firing leaves no row behind.
        if inst_config.parallel && done_vote.is_some() {
            return Err(format!(
                "LoopOut '{}': `done` vote received on a parallel loop; the compiler's \
                 validation rejects `done` in parallel mode, so this compiled config drifted",
                node_def.id,
            ));
        }

        // Journal the firing ONLY when the runtime will record it as
        // new state (instance live, index not already fired). The fold
        // applies `LoopOutFired` unconditionally, so a row for a
        // firing the live runtime refused (post-termination) or
        // already holds (crash-resume replay) would diverge the
        // rehydrated instance from the live one.
        if loop_runtime.loop_out_is_new(&key, index)? {
            crate::context::record_from_pod(
                journal,
                weft_journal::ExecEvent::LoopOutFired {
                    color: group.color,
                    group_id: group_id.clone(),
                    parent_frames: key.parent_frames.clone(),
                    index,
                    gather_writes: gather_writes.clone(),
                    carry_writes: carry_writes.clone(),
                    done_vote,
                    at_unix: now_unix(),
                },
                pod_name,
            )
            .await;
        }

        let advance = loop_runtime
            .record_loop_out(&key, index, gather_writes, carry_writes, done_vote)?;
        match advance {
            LoopAdvance::Idle => Ok(()),
            LoopAdvance::LaunchNext { index: next } => {
                let (inst_carry, loop_in_input): (
                    Vec<(String, serde_json::Value)>,
                    serde_json::Map<String, serde_json::Value>,
                ) = loop_runtime
                    .get(&key)
                    .map(|inst| {
                        let carry = inst.carry_values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        let input: serde_json::Map<String, serde_json::Value> =
                            inst.outer_input.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        (carry, input)
                    })
                    .unwrap_or_default();
                let loop_in_id = format!("{}__in", group_id);
                launch_iteration(
                    project,
                    edge_idx,
                    pulses,
                    journal,
                    pod_name,
                    group.color,
                    &group_id,
                    &key.parent_frames,
                    &loop_in_id,
                    next,
                    &inst_config,
                    &loop_in_input,
                    &inst_carry,
                )
                .await?;
                loop_runtime.record_launched(&key, next);
                Ok(())
            }
            LoopAdvance::EmitOutward { reason, gather, carry } => {
                let parent_frames = key.parent_frames.clone();
                emit_loop_outward(
                    project,
                    edge_idx,
                    pulses,
                    journal,
                    pod_name,
                    group.color,
                    &group_id,
                    &parent_frames,
                    gather,
                    carry,
                    reason,
                    loop_runtime,
                    &key,
                )
                .await?;
                Ok(())
            }
        }
    }
}

pub(crate) fn compute_loop_iter_count(
    config: &crate::loop_runtime::LoopConfig,
    input: &serde_json::Map<String, serde_json::Value>,
) -> Result<u32, String> {
    let mut lengths: Vec<usize> = Vec::new();
    for port in &config.over {
        match input.get(port).and_then(|v| v.as_array()) {
            Some(arr) => lengths.push(arr.len()),
            None => {
                // An absent `over` port (unwired, or an optional input
                // whose upstream closed) must NOT silently degrade:
                // skipping it would either iterate over the remaining
                // lists with this port missing inside the body, or
                // (all absent) reclassify a list-driven loop as a
                // done-driven one (unbounded, or capped only by
                // max_iters).
                return Err(format!(
                    "loop 'over' port '{port}' must be a List; got {}",
                    input
                        .get(port)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "no value (unwired, or upstream closed)".into())
                ));
            }
        }
    }
    let count = if lengths.is_empty() {
        // Pure done-driven loop: no `over`, so there is no upfront
        // iteration count. Termination comes from `self.done` (or an
        // explicit `max_iters` ceiling). There is NO hidden cap: the
        // settled contract is "absent max_iters = no cap", and the
        // compiler already rejects a sequential loop that wires neither
        // `done` nor `max_iters` nor `over` (`loop-unbounded-no-
        // termination`), so a loop reaching here either votes `done` or
        // sets `max_iters`. `u32::MAX` means "no upfront ceiling"; the
        // `max_iters` floor below still applies when set.
        config.max_iters.unwrap_or(u32::MAX)
    } else if config.trim_on_mismatch {
        lengths.into_iter().min().unwrap_or(0) as u32
    } else {
        let first = lengths[0];
        for l in &lengths[1..] {
            if *l != first {
                return Err(format!(
                    "loop 'over' length mismatch with trim_on_mismatch=false: {lengths:?}",
                    lengths = lengths
                ));
            }
        }
        first as u32
    };
    Ok(match config.max_iters {
        Some(m) => count.min(m),
        None => count,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn launch_iteration(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    color: weft_core::Color,
    group_id: &str,
    parent_frames: &weft_core::frames::LoopFrames,
    loop_in_id: &str,
    index: u32,
    config: &crate::loop_runtime::LoopConfig,
    outer_input: &serde_json::Map<String, serde_json::Value>,
    carry_values: &[(String, serde_json::Value)],
) -> Result<(), String> {
    use crate::loop_runtime::iteration_frames;

    let body_frames = iteration_frames(parent_frames, index);
    let mut output = serde_json::Map::new();
    for (port, value) in outer_input {
        if config.over.contains(port) {
            // The iteration count was derived from this same array's
            // length, so an out-of-range index can only mean iter_count
            // vs input-list drift (a corrupt snapshot, or a definition
            // change across resume). Fail loud like the rest of this
            // module rather than injecting Null and running the iteration
            // on fabricated data; the caller routes this into
            // `handle_loop_boundary_failure`.
            let arr = value.as_array().ok_or_else(|| {
                format!("loop '{group_id}': over port '{port}' is not a List at launch")
            })?;
            let elem = arr.get(index as usize).cloned().ok_or_else(|| {
                format!(
                    "loop '{group_id}': over port '{port}' has no element at index {index} \
                     (len {}); iter_count/input drift",
                    arr.len()
                )
            })?;
            output.insert(port.clone(), elem);
        } else if !config.carry.contains(port) {
            // Broadcast input: emit verbatim per iteration.
            output.insert(port.clone(), value.clone());
        }
    }
    // Carry values: read current carry on the inside-out side per
    // iteration. Closure carry-write fallback already handled by the
    // runtime; what reaches here is always a real value.
    for (port, value) in carry_values {
        output.insert(port.clone(), value.clone());
    }
    // Implicit `self.index`.
    output.insert("index".to_string(), serde_json::json!(index));

    let mut emissions = Vec::new();
    let mentioned = weft_core::exec::postprocess_output(
        loop_in_id,
        &serde_json::Value::Object(output),
        color,
        &body_frames,
        project,
        pulses,
        edge_idx,
        &mut emissions,
    )
    .map_err(|e| e.to_string())?;
    // Close every inside output port this iteration did NOT emit (an
    // optional broadcast/over input that arrived closed or unwired is
    // absent from `output`). Without this, a body node wired to that
    // port waits forever on a pulse that never comes and the whole loop
    // hangs (mislabeled Stuck). The generic termination-time sweep skips
    // loop boundaries, so the per-iteration analogue lives here, scoped
    // to this iteration's own frame stack so it can't touch the outward
    // ports. A plain Group already gets this via its Passthrough sweep;
    // loops must not break the skip cascade.
    weft_core::exec::close_unmentioned_downstream(
        loop_in_id,
        &mentioned,
        color,
        &body_frames,
        project,
        pulses,
        edge_idx,
        &mut emissions,
    )
    .map_err(|e| e.to_string())?;
    // The body pulses ride INSIDE the `LoopIterationLaunched` row
    // (one atomic journal write) instead of separate `PulseEmitted`
    // rows. Two writes would have a crash window with no safe order:
    // pulses-first replays the body twice on resume (`launched` lacks
    // the index, so the LaunchNext path re-ships them), marker-first
    // hangs it (index in `launched`, no body pulses). With the atomic
    // row, on crash-resume `launched.contains(index)` is true iff the
    // body pulses are in the folded table, so `record_loop_out`'s
    // already-launched guard correctly returns Idle instead of
    // double-launching.
    crate::context::record_from_pod(
        journal,
        weft_journal::ExecEvent::LoopIterationLaunched {
            color,
            group_id: group_id.to_string(),
            parent_frames: parent_frames.clone(),
            index,
            body_emissions: emissions.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
    Ok(())
}

/// Ship a terminated loop's outward payload: postprocess the assembled
/// gather lists + final carry values onto LoopOut's outer outputs at
/// the parent frame stack, then journal `LoopTerminated`.
///
/// On postprocess failure nothing partial ships (postprocess
/// pre-validates before touching state): the outward ports are CLOSED
/// instead so downstream skips cascade rather than deadlock, the
/// termination is journaled as `Failed` (and the RAM instance
/// re-marked to match, keeping live and rehydrated state identical),
/// and the error propagates so the boundary firing lands in the
/// standard failure path.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_loop_outward(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    color: weft_core::Color,
    group_id: &str,
    parent_frames: &weft_core::frames::LoopFrames,
    gather: HashMap<String, Vec<Option<serde_json::Value>>>,
    carry: HashMap<String, serde_json::Value>,
    reason: weft_core::primitive::LoopTerminationReason,
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
    key: &weft_core::primitive::LoopInstanceKey,
) -> Result<(), String> {
    let loop_out_id = format!("{group_id}__out");
    let mut output = serde_json::Map::new();
    for (port, slots) in gather {
        let arr: Vec<serde_json::Value> = slots
            .into_iter()
            .map(|v| v.unwrap_or(serde_json::Value::Null))
            .collect();
        output.insert(port, serde_json::Value::Array(arr));
    }
    for (port, v) in carry {
        output.insert(port, v);
    }
    let mut emissions = Vec::new();
    if let Err(e) = weft_core::exec::postprocess_output(
        &loop_out_id,
        &serde_json::Value::Object(output),
        color,
        parent_frames,
        project,
        pulses,
        edge_idx,
        &mut emissions,
    ) {
        let failed = weft_core::primitive::LoopTerminationReason::Failed;
        if let Some(inst) = loop_runtime.get_mut(key) {
            inst.terminated = Some(failed);
        }
        // Closures carried in the terminal row (atomic), same reason as
        // the success path above.
        let closures = build_loop_outward_closures(
            project, edge_idx, pulses, color, group_id, parent_frames,
        );
        crate::context::record_from_pod(
            journal,
            weft_journal::ExecEvent::LoopTerminated {
                color,
                group_id: group_id.to_string(),
                parent_frames: parent_frames.clone(),
                reason: failed,
                outward_emissions: closures.into_iter().map(Into::into).collect(),
                at_unix: now_unix(),
            },
            pod_name,
        )
        .await;
        return Err(format!("loop '{group_id}' outward emit failed: {e}"));
    }
    // Carry the outward pulses INSIDE the LoopTerminated row (one atomic
    // write) instead of shipping them as separate PulseEmitted rows. A
    // crash between two separate writes would leave the pulses pending
    // with `terminated: None` on refold, re-firing LoopOut and emitting
    // the loop's outputs twice downstream.
    crate::context::record_from_pod(
        journal,
        weft_journal::ExecEvent::LoopTerminated {
            color,
            group_id: group_id.to_string(),
            parent_frames: parent_frames.clone(),
            reason,
            outward_emissions: emissions.into_iter().map(Into::into).collect(),
            at_unix: now_unix(),
        },
        pod_name,
    )
    .await;
    Ok(())
}

/// Build (and apply to `pulses` exactly once) the outward closure pulses
/// for `group_id`'s LoopOut at `parent_frames` (a loop's abnormal ending
/// tells its consumers "nothing will arrive" so they cascade-skip instead
/// of deadlocking), RETURNING them. EVERY caller carries the returned
/// emissions inside a terminal journal row (`LoopTerminated` /
/// `NodeFailed` / `NodeSkipped`) so the marker and its closures fold as
/// one atomic unit (no crash-resume double-emit / lost-closure window);
/// there is no standalone-ship path. A missing `__out` node (impossible
/// unless the compiled project shape is corrupt) is logged at error level
/// rather than returned, since teardown paths cannot propagate.
fn build_loop_outward_closures(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    color: weft_core::Color,
    group_id: &str,
    parent_frames: &weft_core::frames::LoopFrames,
) -> Vec<weft_core::exec::PulseEmission> {
    let loop_out_id = format!("{group_id}__out");
    let Some(loop_out_node) = project.nodes.iter().find(|n| n.id == loop_out_id) else {
        tracing::error!(
            target: "weft_engine::loop_runtime",
            group_id = %group_id,
            "loop teardown: project has no '{loop_out_id}' node; outward consumers \
             will not receive closures (corrupt compiled project shape)"
        );
        return Vec::new();
    };
    let mut emissions = Vec::new();
    for port in &loop_out_node.outputs {
        // Unreachable by construction (we iterate the node's own
        // declared outputs), but teardown cannot propagate, so log
        // loud rather than unwrap.
        if let Err(e) = weft_core::exec::postprocess::emit_port_closure(
            &loop_out_id, &port.name, color, parent_frames,
            project, pulses, edge_idx, &mut emissions,
        ) {
            tracing::error!(
                target: "weft_engine::loop_runtime",
                group_id = %group_id,
                port = %port.name,
                error = %e,
                "loop teardown closure failed"
            );
        }
    }
    emissions
}

/// Failure tail for an inline boundary firing (LoopIn / LoopOut). On
/// top of the standard failure bookkeeping (Failed status + NodeFailed
/// + the generic sweep, which deliberately no-ops for loop boundary
/// nodes), a failed boundary firing kills its WHOLE loop: close the
/// loop's outward surface so downstream skips cascade instead of
/// deadlocking, and when an instance was already journaled, terminate
/// it (RAM + a `LoopTerminated{Failed}` row) so cancel walks and
/// resumed workers both see a dead loop rather than a live instance
/// nothing will ever drive again.
///
/// Pre-instantiation failures (config parse, missing carry seed,
/// iter-count errors) have no instance: closures alone carry the
/// teardown, and the fold never sees a `LoopTerminated` without its
/// `LoopInstantiated`. An instance that is ALREADY terminated means
/// the teardown (closures + terminal row) was journaled by
/// `emit_loop_outward`'s failure path; doing it again would duplicate
/// closure pulses downstream.
#[allow(clippy::too_many_arguments)]
async fn handle_loop_boundary_failure(
    node_def: &weft_core::project::NodeDefinition,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
) {
    use weft_core::primitive::{LoopInstanceKey, LoopTerminationReason};

    // The boundary's outward closures (a failed loop tells its consumers
    // "nothing will arrive") must survive a crash and ride EXACTLY ONE
    // terminal journal row, and the instance-dead marker must never land
    // in a LATER row than the closures. So which row carries them depends
    // on the three cases handled below: a live instance carries them in
    // the LoopTerminated row written FIRST (then NodeFailed empty); a
    // no-instance failure carries them in NodeFailed (the only terminal
    // row); an already-terminated instance carries none (the prior
    // LoopTerminated already did). They are never duplicated across rows.
    let Some(gb) = node_def.group_boundary.as_ref() else {
        // Unreachable for LoopIn/LoopOut (the compiler always sets the
        // boundary). Still fail the node loudly with no loop teardown.
        tracing::error!(
            target: "weft_engine::loop_runtime",
            node = %node_def.id,
            "boundary failure: node has no group_boundary; cannot tear down its loop"
        );
        let mentioned = std::collections::HashSet::new();
        handle_node_failure(
            &node_def.id, &mentioned, color, frames, err,
            project, edge_idx, pulses, executions, journal, pod_name, phase_scope,
        )
        .await;
        return;
    };
    let group_id = gb.group_id.clone();
    let parent_frames = crate::loop_runtime::boundary_parent_frames(&node_def.node_type, frames);
    let key = LoopInstanceKey {
        group_id: group_id.clone(),
        parent_frames: parent_frames.clone(),
        color,
    };
    let already_terminated =
        matches!(loop_runtime.get(&key).map(|inst| inst.terminated.is_some()), Some(true));

    // Each closure set must ride EXACTLY ONE journal row, and the
    // instance-dead marker (LoopTerminated) must land in the SAME or an
    // EARLIER row than the closures, never later. So:
    //   - LIVE instance: write LoopTerminated FIRST (carries the closures
    //     AND marks the instance dead), then NodeFailed with NO closures.
    //     A crash between them leaves the instance dead + closures
    //     delivered; the boundary record is still Running so it re-fires,
    //     hits the terminated instance, and goes idle. Self-consistent.
    //     If we wrote NodeFailed-with-closures first and crashed, the
    //     refold would have closures delivered but the instance LIVE, and
    //     surviving body work could re-fire LoopOut and emit REAL outward
    //     values onto ports whose consumers already got closures.
    //   - NO instance (pre-instantiation failure): there is no
    //     LoopTerminated to write, so NodeFailed carries the closures.
    //   - ALREADY terminated: the prior LoopTerminated already carried
    //     the closures; NodeFailed carries none.
    let live_instance =
        matches!(loop_runtime.get(&key).map(|inst| inst.terminated.is_some()), Some(false));
    let nodefailed_closures = if live_instance {
        if let Some(inst) = loop_runtime.get_mut(&key) {
            inst.terminated = Some(LoopTerminationReason::Failed);
        }
        let closures =
            build_loop_outward_closures(project, edge_idx, pulses, color, &group_id, &parent_frames);
        crate::context::record_from_pod(
            journal,
            weft_journal::ExecEvent::LoopTerminated {
                color,
                group_id: group_id.clone(),
                parent_frames: parent_frames.clone(),
                reason: LoopTerminationReason::Failed,
                outward_emissions: closures.into_iter().map(Into::into).collect(),
                at_unix: now_unix(),
            },
            pod_name,
        )
        .await;
        Vec::new()
    } else if already_terminated {
        Vec::new()
    } else {
        // No instance: NodeFailed is the only terminal row, so it carries
        // the closures.
        build_loop_outward_closures(project, edge_idx, pulses, color, &group_id, &parent_frames)
    };

    // Boundary firings never emit through the task channel, so the
    // generic mentioned set is empty.
    let mentioned = std::collections::HashSet::new();
    handle_node_failure_inner(
        &node_def.id, &mentioned, color, frames, err,
        project, edge_idx, pulses, executions, journal, pod_name, phase_scope,
        /* ship_node_terminal = */ true, nodefailed_closures,
    )
    .await;
}

/// Fail a firing: mark Failed, ship `NodeFailed`, close every
/// output port the firing did NOT already emit on. The single failure
/// path (real `execute` error, dispatch-time error, unknown node type,
/// output type-check failure). Ports already emitted KEEP their
/// values: a node that fired A then crashed before firing B still has
/// A's value live downstream, only B gets closed. "Stuff already sent
/// stays sent" (the principle that drives the closure semantics).
#[allow(clippy::too_many_arguments)]
async fn handle_node_failure(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    handle_node_failure_inner(
        node_id, mentioned, color, frames, err, project, edge_idx, pulses, executions,
        journal, pod_name, phase_scope, /* ship_node_terminal = */ true, Vec::new(),
    )
    .await;
}

/// Cancel-path variant: in-memory `mark_failed` + downstream-closure
/// journal writes (those carry PulseEmitted rows, not node-status), but
/// SKIP the `NodeFailed` node-status write. The dispatcher writes
/// `NodeCancelled` for every non-terminal node found in its
/// journal-folded snapshot at cancel time; if the worker journaled
/// `NodeFailed` for the same `(node, frames)` first, the fold's
/// last-write-wins flips the final state to Failed and the user sees a
/// misleading shape error on a cancelled run. Skipping the node-status
/// write here keeps the dispatcher's `NodeCancelled` as the source of
/// truth for status while preserving downstream-close correctness so no
/// pulses leak.
async fn handle_node_failure_cancel_path(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    handle_node_failure_inner(
        node_id, mentioned, color, frames, err, project, edge_idx, pulses, executions,
        journal, pod_name, phase_scope, /* ship_node_terminal = */ false, Vec::new(),
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_node_failure_inner(
    node_id: &str,
    mentioned: &std::collections::HashSet<String>,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    err: &str,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
    ship_node_terminal: bool,
    // Closures the CALLER already built (and applied to `pulses`) for a
    // terminal row whose ONLY carrier is this NodeFailed: the no-instance
    // loop-boundary failure (no LoopTerminated row exists). The generic
    // sweep no-ops for boundary nodes, so these are passed explicitly.
    // The live-instance case does NOT use this (its closures ride the
    // LoopTerminated row written first); empty for ordinary nodes.
    extra_closures: Vec<weft_core::exec::PulseEmission>,
) {
    mark_failed(executions, node_id, color, frames, err);
    // Build the closures FIRST (mutates RAM pulses, prunes), then ship
    // them INSIDE the NodeFailed row so the terminal marker and its
    // closures are one atomic write. A crash between two separate writes
    // would lose the closures, leaving downstream consumers neither
    // firing nor skipping (the execution refolds Stuck).
    let mut closures = build_unmentioned_closures_and_prune(
        node_id, mentioned, color, frames, project, edge_idx, pulses, phase_scope,
    );
    closures.extend(extra_closures);
    if ship_node_terminal {
        ship_node_failed(journal, pod_name, color, node_id, frames, err, closures).await;
    } else {
        // The caller suppresses the terminal row (cancel re-fold path),
        // so the closures can't ride it; ship them standalone.
        crate::context::ship_pulse_emissions(journal, pod_name, closures).await;
    }
}

/// Skip a firing (a scope/condition decided it shouldn't run):
/// mark Skipped, ship `NodeSkipped`, null+prune downstream. Same
/// downstream teardown as a failure (the prune in particular), just a
/// different lifecycle event.
#[allow(clippy::too_many_arguments)]
async fn handle_node_skip(
    node_id: &str,
    color: weft_core::Color,
    frames: &weft_core::frames::LoopFrames,
    closed_ports: &[String],
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    mark_skipped(executions, node_id, color, frames);
    // A skipped LOOP In-boundary needs its own teardown: the generic
    // sweep below deliberately no-ops for loop boundary nodes
    // (per-iteration firings must not auto-close the outward ports),
    // which would swallow this skip entirely: the loop never
    // instantiates, nothing ever fires its outward ports, and the
    // consumers neither fire nor skip (the execution lands Stuck or
    // silently Completed with the whole subtree missing). The loop's
    // skip surface is its outward ports at the group's own frames.
    let skipped_loop_group = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .filter(|n| n.node_type == "LoopIn")
        .and_then(|n| n.group_boundary.as_ref())
        .map(|gb| gb.group_id.clone());
    if let Some(group_id) = skipped_loop_group {
        // A skipped loop's outward closures ride INSIDE the NodeSkipped
        // row, same atomic discipline as everywhere else: a crash between
        // a bare NodeSkipped and a separate closure write would leave the
        // loop dead but its consumers never closed (refold Stuck). The
        // generic sweep no-ops for loop boundaries, so we build the
        // loop's outward closures explicitly here.
        let closures = build_loop_outward_closures(
            project, edge_idx, pulses, color, &group_id, frames,
        );
        ship_node_skipped(journal, pod_name, color, node_id, frames, closed_ports, closures).await;
        if let Some(scope) = phase_scope {
            drop_out_of_scope_pulses(pulses, scope);
        }
        return;
    }
    // A skipped node's body never runs, so it never emitted on ANY
    // output port → close every port. Same shape as a pre-dispatch
    // failure: empty mentioned set means "close everything". Build the
    // closures FIRST, then ship them INSIDE the NodeSkipped row (atomic
    // marker+closures, same reason as the failure path).
    let mentioned = std::collections::HashSet::new();
    let closures = build_unmentioned_closures_and_prune(
        node_id, &mentioned, color, frames, project, edge_idx, pulses, phase_scope,
    );
    ship_node_skipped(journal, pod_name, color, node_id, frames, closed_ports, closures).await;
}

/// Gives a firing's runtime-granted provider accesses back on every exit
/// of the node task. Normal completion calls [`Self::close_now`] (awaited
/// inline, before the terminal ships); an ABORT (cancel) drops the guard
/// mid-body, and the drop spawns the close detached, because nothing after
/// the abort point ever runs. A close that cannot even be spawned (no
/// runtime) is fine to skip silently here: the credential's own window is
/// the documented backstop for exactly this case.
struct AccessCloseGuard(Option<Arc<crate::context::RunnerHandle>>);

impl AccessCloseGuard {
    async fn close_now(mut self) {
        if let Some(runner) = self.0.take() {
            runner.close_opened_accesses().await;
        }
    }
}

impl Drop for AccessCloseGuard {
    fn drop(&mut self) {
        if let Some(runner) = self.0.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move { runner.close_opened_accesses().await });
            }
        }
    }
}

/// Single cancellation cleanup path. Called from BOTH cancellation
/// entry points (loop-top check and idle-wait branch) so they have
/// identical drain semantics. The order is load-bearing:
///
/// 1. `in_flight.shutdown().await` drives every spawned task to its
///    abort point. A task mid-`record_event` finishes its write; a
///    task waiting on `cursor.next()` wakes via the abort and unwinds.
///    Without this, a journal write racing with the outer cancel
///    path could flip the final state to Completed AFTER we wrote
///    NodeCancelled (last-write-wins fold).
///
/// 2. Drain the task channel (one FIFO pass). In-channel `pulse_downstream`
///    emissions land in `emitted_ports` (and their downstream pulses)
///    BEFORE we compute the "unmentioned" closure set, so a port the body
///    just emitted on isn't wrongly closed as unmentioned ("stuff already
///    sent stays sent"). In-channel terminals (Completed/Failed) land in
///    `executions` STATE ONLY (no journal write here) so they are SKIPPED
///    by the closure sweep below; otherwise a firing that completed during
///    the abort window would get closure-downstreamed AND a NodeCancelled,
///    when the right answer is "it completed, leave it alone". The shared
///    channel's FIFO ordering means a node's emissions precede its
///    terminal in this same pass.
///
/// 3. Walk `executions` and close downstream for every non-terminal
///    firing. This is "two sources of truth" with
///    `journal_node_cancellations` (which re-folds the journal),
///    consolidated by ensuring `executions` is fully drained first
///    so any committed terminal transitions are visible here.
///    Remaining drift between in-memory and journal can only come
///    from a journal write that actually failed (broker error logged
///    upstream), which is a separate dispatcher-side problem.
#[allow(clippy::too_many_arguments)]
async fn cancel_cleanup(
    in_flight: &mut tokio::task::JoinSet<()>,
    task_rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    waiting: &mut HashMap<String, (String, weft_core::frames::LoopFrames)>,
    executions: &mut NodeExecutionTable,
    emitted_ports: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    >,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
) {
    // An in-flight node's future is aborted immediately; it needs no window
    // to wrap up first. A paid call's cost is measured by the metering tap
    // BELOW the node's future, which finalizes on drop and resolves the
    // figure detached (tracked by the pod's pending-cost records), so an
    // aborted node body never loses money bookkeeping.
    // 1. Drive every spawned task to its abort point.
    in_flight.shutdown().await;
    // 2. Drain the task channel in one FIFO pass. Emissions: apply with
    //    `is_cancel=true` so a bad-shape emission queued before the cancel
    //    doesn't journal `NodeFailed` and race the dispatcher's post-cancel
    //    `NodeCancelled` write (last-write-wins would flip the node's final
    //    status to Failed); downstream-closure writes still happen so no
    //    pulses leak, and `emitted_ports` reflects the real "what was sent"
    //    set. Terminals: applied to `executions` STATE ONLY (no journal
    //    write) so a firing that completed during the abort window is
    //    SKIPPED by the closure sweep below instead of getting both a
    //    closure-downstream AND a NodeCancelled. The dispatcher's post-loop
    //    `journal_node_cancellations` owns the journal side via the folded
    //    snapshot. The returned `late_terminated` list (firings that landed
    //    Completed/Failed here) is appended in step 3 so their
    //    unmentioned-port closures get the same walk (else a late-Completed
    //    firing's closures would be lost and downstream never learn).
    let late_terminated = drain_task_msgs_for_cancel(
        task_rx, color, project, edge_idx, pulses, executions, journal, pod_name,
        waiting, emitted_ports, phase_scope,
    )
    .await;
    // 3. Snapshot the (node, frames) keys to close so we don't borrow
    //    `executions` and `emitted_ports` simultaneously, then for each
    //    non-terminal firing close every output port that wasn't already
    //    emitted or closed (mirrors the Completed/Failed/Skipped shape
    //    at the same frame stack). Late-terminated firings (from step
    //    3) are appended so their unmentioned-port closures get the
    //    same walk.
    let mut firings: Vec<(String, weft_core::frames::LoopFrames)> = executions
        .iter()
        .flat_map(|(node_id, execs)| {
            execs
                .iter()
                .filter(|e| e.color == color && !e.status.is_terminal())
                .map(move |e| (node_id.clone(), e.frames.clone()))
        })
        .collect();
    firings.extend(late_terminated);
    for (node_id, frames) in firings {
        let mentioned = emitted_ports
            .remove(&(node_id.clone(), frames.clone()))
            .unwrap_or_default();
        close_unmentioned_downstream_and_prune(
            &node_id, &mentioned, color, &frames, project, edge_idx, pulses, journal, pod_name, phase_scope,
        )
        .await;
    }

    // 5. Mark every live `LoopInstance` for this color as cancelled and
    //    emit closures on the LoopOut's outward output ports at the
    //    instance's parent_frames. No partial outward emit (a real
    //    outward emit would be a lie about how many iterations
    //    completed). The closures propagate through the rest of the
    //    graph as the standard "no value will arrive on this port"
    //    marker. Inner-loop instances inside the cancelled scope are
    //    covered too: each instance lives at its own parent_frames, so
    //    the per-instance close emits at the right level.
    cancel_loop_instances(
        loop_runtime, color, project, edge_idx, pulses, journal, pod_name,
    )
    .await;
}

/// On cancellation, walk every non-terminated `LoopInstance` for this
/// color, mark it cancelled, and emit closures on every outward output
/// port of its LoopOut at `parent_frames`. Idempotent: an instance
/// already marked terminated is skipped.
pub(crate) async fn cancel_loop_instances(
    loop_runtime: &mut crate::loop_runtime::LoopRuntime,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
) {
    use weft_core::primitive::LoopTerminationReason;
    let mut to_close: Vec<(String, weft_core::frames::LoopFrames)> = Vec::new();
    loop_runtime.cancel_inside(&Vec::new(), color);
    for (key, inst) in loop_runtime.iter() {
        if key.color != color {
            continue;
        }
        if !matches!(inst.terminated, Some(LoopTerminationReason::Cancelled)) {
            continue;
        }
        to_close.push((key.group_id.clone(), key.parent_frames.clone()));
    }
    for (group_id, parent_frames) in to_close {
        // Closures carried in the terminal row (atomic). Journaling the
        // cancellation is what makes it durable across resume: a refold
        // without it rebuilds the instance as live (terminated=None) and
        // the engine drives it again. Folding the closures into the same
        // row means a crash can't leave the closures pending with the
        // instance still live (which would re-emit them).
        let closures = build_loop_outward_closures(
            project, edge_idx, pulses, color, &group_id, &parent_frames,
        );
        crate::context::record_from_pod(
            journal,
            weft_journal::ExecEvent::LoopTerminated {
                color,
                group_id: group_id.clone(),
                parent_frames: parent_frames.clone(),
                reason: LoopTerminationReason::Cancelled,
                outward_emissions: closures.into_iter().map(Into::into).collect(),
                at_unix: now_unix(),
            },
            pod_name,
        )
        .await;
    }
}

/// Apply one `pulse_downstream` (or `close_port`) emission: postprocess
/// it into downstream pulses at the firing's frame stack, ship the mutations,
/// and union the mentioned port name into the firing's mentioned-set.
/// The emitting node stays Running.
#[allow(clippy::too_many_arguments)]
async fn apply_one_emission(
    msg: crate::context::EmitMsg,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    emitted_ports: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    >,
    phase_scope: Option<&std::collections::HashSet<String>>,
    is_cancel: bool,
) {
    let mut emissions = Vec::new();
    let just_mentioned: std::collections::HashSet<String> = match msg.kind {
        crate::context::EmitKind::Values(output) => {
            let output_value = output_to_value(&output);

            // Run postprocess FIRST. It pre-validates the output_obj
            // (e.g. an Expand port must carry an array) before touching
            // `pulses` or `mutations`, so a bad-shape emission produces
            // zero live pulses and zero journal mutations: the firing
            // fails atomically with no partial state. Persisting the
            // execution record's `output` only AFTER success keeps the
            // record honest too: a failed firing doesn't carry an
            // "output" that the inspector would render alongside an
            // error.
            match postprocess_output(
                &msg.node_id,
                &output_value,
                color,
                &msg.frames,
                project,
                pulses,
                edge_idx,
                &mut emissions,
            ) {
                Ok(set) => {
                    // Record what this firing emitted onto its own
                    // execution record, so the inspector card shows
                    // the produced values. A firing that emits more
                    // than once merges ports across emits (later port
                    // value wins). Closures (the `Close` branch below)
                    // don't go into the execution record because they
                    // carry no user-facing value.
                    if let Some(rec) = executions.get_mut(&msg.node_id).and_then(|v| {
                        v.iter_mut()
                            .rev()
                            .find(|e| e.color == color && e.frames == msg.frames)
                    }) {
                        let merged = match rec.output.take() {
                            Some(Value::Object(mut prev)) => {
                                if let Value::Object(now) = &output_value {
                                    for (k, v) in now {
                                        prev.insert(k.clone(), v.clone());
                                    }
                                }
                                Value::Object(prev)
                            }
                            _ => output_value.clone(),
                        };
                        rec.output = Some(merged);
                    }
                    set
                }
                Err(err) => {
                    // The node handed the engine a bad-shape value on
                    // an Expand port (or similar): fail the firing
                    // loud. The pre-validation in postprocess_output
                    // means no pulses/mutations were committed, so the
                    // firing fails atomically. Already-mentioned ports
                    // from PRIOR emissions keep their pulses;
                    // unmentioned ports get closed by the failure path.
                    //
                    // Cancel-path variant: skip the `NodeFailed`
                    // node-status journal write so it can't race the
                    // dispatcher's post-cancel `NodeCancelled` write
                    // (the dispatcher writes NodeCancelled for every
                    // non-terminal node it sees in the folded journal;
                    // a NodeFailed lands first, flipping the final
                    // status to Failed). Downstream-closure writes
                    // still happen so the in-memory pulse table stays
                    // consistent with no leaks.
                    let prior_mentioned = emitted_ports
                        .remove(&(msg.node_id.clone(), msg.frames.clone()))
                        .unwrap_or_default();
                    if is_cancel {
                        handle_node_failure_cancel_path(
                            &msg.node_id,
                            &prior_mentioned,
                            color,
                            &msg.frames,
                            &err.to_string(),
                            project,
                            edge_idx,
                            pulses,
                            executions,
                            journal,
                            pod_name,
                            phase_scope,
                        )
                        .await;
                    } else {
                        handle_node_failure(
                            &msg.node_id,
                            &prior_mentioned,
                            color,
                            &msg.frames,
                            &err.to_string(),
                            project,
                            edge_idx,
                            pulses,
                            executions,
                            journal,
                            pod_name,
                            phase_scope,
                        )
                        .await;
                    }
                    return;
                }
            }
        }
        crate::context::EmitKind::Close(port_name) => {
            // Same failure routing as the Values arm above: a
            // `close_port` on an undeclared port is a wiring bug and
            // fails the firing loud (nothing was committed).
            if let Err(err) = weft_core::exec::postprocess::emit_port_closure(
                &msg.node_id,
                &port_name,
                color,
                &msg.frames,
                project,
                pulses,
                edge_idx,
                &mut emissions,
            ) {
                let prior_mentioned = emitted_ports
                    .remove(&(msg.node_id.clone(), msg.frames.clone()))
                    .unwrap_or_default();
                if is_cancel {
                    handle_node_failure_cancel_path(
                        &msg.node_id, &prior_mentioned, color, &msg.frames, &err.to_string(),
                        project, edge_idx, pulses, executions, journal, pod_name, phase_scope,
                    )
                    .await;
                } else {
                    handle_node_failure(
                        &msg.node_id, &prior_mentioned, color, &msg.frames, &err.to_string(),
                        project, edge_idx, pulses, executions, journal, pod_name, phase_scope,
                    )
                    .await;
                }
                return;
            }
            std::iter::once(port_name).collect()
        }
    };

    // Union into the firing's running mentioned-set so termination
    // knows which ports were already touched (whether by value or by
    // closure). Multiple emissions from one firing share the firing's
    // (node, frames), so they union into one entry here.
    let entry = emitted_ports
        .entry((msg.node_id.clone(), msg.frames.clone()))
        .or_default();
    for name in just_mentioned {
        entry.insert(name);
    }

    crate::context::ship_pulse_emissions(journal, pod_name, emissions).await;
    if let Some(scope) = phase_scope {
        drop_out_of_scope_pulses(pulses, scope);
    }
}

/// Drain the task channel in FIFO order, applying each `Emission`
/// (downstream pulse, node stays Running) and each `Terminal` (close
/// the firing + close unmentioned ports). Because emissions and the
/// terminal ride this ONE ordered channel, a node's emissions are
/// always applied before its terminal, so the close-unmentioned sweep
/// sees the complete mentioned set. Returns true if any were applied.
/// `is_cancel` propagates to a bad-shape emission so it doesn't journal
/// `NodeFailed` and race the dispatcher's `NodeCancelled` write.
#[allow(clippy::too_many_arguments)]
async fn apply_task_msgs(
    rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    waiting: &mut HashMap<String, (String, weft_core::frames::LoopFrames)>,
    emitted_ports: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    >,
    phase_scope: Option<&std::collections::HashSet<String>>,
    is_cancel: bool,
) -> bool {
    let mut any = false;
    while let Ok(msg) = rx.try_recv() {
        any = true;
        apply_one_task_msg(
            msg, color, project, edge_idx, pulses, executions, journal, pod_name,
            waiting, emitted_ports, phase_scope, is_cancel,
        )
        .await;
    }
    any
}

/// Apply ONE task message. The single message the idle-wait `select!`
/// consumes goes through here too (it can't be re-queued), so emissions
/// and terminals are both handled in one place.
#[allow(clippy::too_many_arguments)]
async fn apply_one_task_msg(
    msg: TaskMsg,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    waiting: &mut HashMap<String, (String, weft_core::frames::LoopFrames)>,
    emitted_ports: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    >,
    phase_scope: Option<&std::collections::HashSet<String>>,
    is_cancel: bool,
) {
    match msg {
        TaskMsg::Emission(emit) => {
            apply_one_emission(
                emit, color, project, edge_idx, pulses, executions, journal, pod_name,
                emitted_ports, phase_scope, is_cancel,
            )
            .await;
        }
        TaskMsg::Terminal { node_id, color: tcolor, frames, outcome } => match outcome {
            NodeTaskOutcome::Completed => {
                // Emissions already happened via `pulse_downstream` and
                // were applied before this terminal (FIFO on the shared
                // channel). Close the record and ship the recorded
                // output, then emit CLOSURE markers on every output port
                // the firing never mentioned, so downstream consumers
                // learn nothing's coming for those ports. Already-
                // mentioned ports keep their emitted values; a node that
                // emits A then B has both A and B as real values
                // downstream.
                mark_completed(executions, &node_id, tcolor, &frames);
                let output = executions
                    .get(&node_id)
                    .and_then(|v| v.iter().rev().find(|e| e.color == tcolor && e.frames == frames))
                    .and_then(|e| e.output.clone())
                    .unwrap_or(serde_json::Value::Null);
                let mentioned = emitted_ports
                    .remove(&(node_id.clone(), frames.clone()))
                    .unwrap_or_default();
                // Build the unmentioned-port closures FIRST, then carry
                // them INSIDE the NodeCompleted row (atomic marker+
                // closures, same reason as the failure/skip paths).
                let closures = build_unmentioned_closures_and_prune(
                    &node_id, &mentioned, tcolor, &frames,
                    project, edge_idx, pulses, phase_scope,
                );
                ship_node_completed(
                    journal, pod_name, tcolor, &node_id, &frames, &output, closures,
                )
                .await;
            }
            NodeTaskOutcome::Failed(err) => {
                let mentioned = emitted_ports
                    .remove(&(node_id.clone(), frames.clone()))
                    .unwrap_or_default();
                handle_node_failure(
                    &node_id, &mentioned, tcolor, &frames, &err,
                    project, edge_idx, pulses, executions, journal, pod_name, phase_scope,
                )
                .await;
            }
            NodeTaskOutcome::Waiting(token) => {
                mark_waiting(executions, &node_id, tcolor, &frames, &token);
                ship_node_suspended(journal, pod_name, tcolor, &node_id, &frames, &token).await;
                waiting.insert(token, (node_id, frames));
            }
        },
    }
}

/// Drain queued task results into in-memory state ONLY (no journal
/// writes, no downstream closure emission). Used by `cancel_cleanup`
/// where the dispatcher may have already written `NodeCancelled` for
/// the same (node, frames); a subsequent `NodeCompleted` / `NodeFailed`
/// / `NodeSkipped` journal write would last-write-wins flip the
/// state. The in-memory updates are consumed locally by the
/// close-walk's `is_terminal()` filter and discarded when `drive_loop`
/// returns; the journal source-of-truth for cancelled firings is
/// owned by the post-loop `journal_node_cancellations` walk.
///
/// Waiting outcomes are preserved in the `waiting` map and the
/// executions table because a still-Suspended firing remains
/// non-terminal and the close-walk will close its downstream.
/// Returns the set of (node, frames) keys that landed Completed or
/// Failed while draining, so the caller can run the same
/// `close_unmentioned_downstream` pass over them that the live path
/// runs on a normal Completed/Failed transition. Without that pass,
/// downstream nodes never learn no value is coming on the unmentioned
/// output ports of a late-completed firing, and the replay-from-
/// snapshot view diverges from what the live worker actually saw.
#[allow(clippy::too_many_arguments)]
async fn drain_task_msgs_for_cancel(
    rx: &mut mpsc::UnboundedReceiver<TaskMsg>,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    waiting: &mut HashMap<String, (String, weft_core::frames::LoopFrames)>,
    emitted_ports: &mut HashMap<
        (String, weft_core::frames::LoopFrames),
        std::collections::HashSet<String>,
    >,
    phase_scope: Option<&std::collections::HashSet<String>>,
) -> Vec<(String, weft_core::frames::LoopFrames)> {
    let mut late_terminated: Vec<(String, weft_core::frames::LoopFrames)> = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            // Emissions: apply with downstream closures (is_cancel=true),
            // recording the mentioned port. FIFO ordering means a node's
            // emissions are applied before its terminal below.
            TaskMsg::Emission(emit) => {
                apply_one_emission(
                    emit, color, project, edge_idx, pulses, executions, journal, pod_name,
                    emitted_ports, phase_scope, /* is_cancel = */ true,
                )
                .await;
            }
            // Terminals: STATE ONLY, no journal write (the post-loop
            // cancellation walk owns the journal side).
            TaskMsg::Terminal { node_id, color: tcolor, frames, outcome } => match outcome {
                NodeTaskOutcome::Completed => {
                    mark_completed(executions, &node_id, tcolor, &frames);
                    late_terminated.push((node_id, frames));
                }
                NodeTaskOutcome::Failed(err) => {
                    mark_failed(executions, &node_id, tcolor, &frames, &err);
                    late_terminated.push((node_id, frames));
                }
                NodeTaskOutcome::Waiting(token) => {
                    mark_waiting(executions, &node_id, tcolor, &frames, &token);
                    waiting.insert(token, (node_id, frames));
                }
            },
        }
    }
    late_terminated
}

fn mark_waiting(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &[weft_core::frames::LoopIteration],
    token: &str,
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.frames == frames) {
            e.status = NodeExecutionStatus::WaitingForInput;
            e.callback_id = Some(token.to_string());
        }
    }
}

async fn terminate(
    pulses: &PulseTable,
    executions: &mut NodeExecutionTable,
    waiting: &HashMap<String, (String, weft_core::frames::LoopFrames)>,
) -> anyhow::Result<ExecutionOutcome> {
    // `waiting` only tracks suspensions that fired in *this* drive
    // call. After a stall→resume, suspensions from the previous
    // drive() are persisted in `executions` (status =
    // WaitingForInput) but the local map starts empty, so we'd
    // mis-classify a partially-resumed workflow as Stuck. Source of
    // truth is the executions table.
    let has_waiting = waiting_count(executions) > 0;
    let local_waiting = waiting.len();

    let completion = check_completion(pulses, executions);
    match completion {
        Some(false) => Ok(ExecutionOutcome::Completed {
            outputs: final_outputs(executions),
        }),
        Some(true) => Ok(ExecutionOutcome::Failed {
            error: first_failure(executions).unwrap_or_else(|| "execution failed".into()),
        }),
        None => {
            if has_waiting {
                tracing::info!(
                    target: "weft_engine",
                    local_waiting,
                    persisted_waiting = waiting_count(executions),
                    "nothing active; all remaining work is waiting on signals: stalling"
                );
                return Ok(ExecutionOutcome::Stalled);
            }
            tracing::warn!(
                target: "weft_engine",
                pulses = pulses.len(),
                "execution stuck: pending pulses with no ready nodes and no suspensions"
            );
            Ok(ExecutionOutcome::Stuck)
        }
    }
}

fn waiting_count(executions: &NodeExecutionTable) -> usize {
    executions
        .values()
        .flat_map(|v| v.iter())
        .filter(|e| e.status == NodeExecutionStatus::WaitingForInput)
        .count()
}

// ---------- Mutation helpers ----------

fn final_outputs(executions: &NodeExecutionTable) -> Value {
    let mut obj = serde_json::Map::new();
    for (node_id, execs) in executions {
        if let Some(last) = execs.iter().rev().find(|e| e.status == NodeExecutionStatus::Completed) {
            if let Some(output) = &last.output {
                obj.insert(node_id.clone(), output.clone());
            }
        }
    }
    Value::Object(obj)
}

/// The earliest failure in the table (by completion time, node id as
/// tie-break). The table is a HashMap, so a plain "first hit wins"
/// scan would report a different failure run-to-run; the earliest one
/// is the root cause the user should see.
fn first_failure(executions: &NodeExecutionTable) -> Option<String> {
    executions
        .values()
        .flat_map(|v| v.iter())
        .filter(|e| e.status == NodeExecutionStatus::Failed)
        .min_by_key(|e| (e.completed_at.unwrap_or(u64::MAX), e.node_id.clone()))
        .map(|e| {
            format!(
                "{}: {}",
                e.node_id,
                e.error.clone().unwrap_or_else(|| "failed".into())
            )
        })
}

fn output_to_value(output: &NodeOutput) -> Value {
    Value::Object(output.outputs.clone().into_iter().collect())
}

fn mark_completed(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &[weft_core::frames::LoopIteration],
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.frames == frames) {
            e.status = NodeExecutionStatus::Completed;
            e.completed_at = Some(now_unix());
        }
    }
}

fn mark_failed(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &[weft_core::frames::LoopIteration],
    err: &str,
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.frames == frames) {
            e.status = NodeExecutionStatus::Failed;
            e.completed_at = Some(now_unix());
            e.error = Some(err.to_string());
        }
    }
}

fn mark_skipped(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    frames: &[weft_core::frames::LoopIteration],
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.frames == frames) {
            e.status = NodeExecutionStatus::Skipped;
            e.completed_at = Some(now_unix());
        }
    }
}


/// Remove every pending pulse whose target is not in `scope`.
/// Called after each successful node completion to prevent
/// out-of-scope downstream nodes from wedging TriggerSetup.
fn drop_out_of_scope_pulses(
    pulses: &mut PulseTable,
    scope: &std::collections::HashSet<String>,
) {
    let out_of_scope: Vec<String> = pulses
        .keys()
        .filter(|k| !scope.contains(*k))
        .cloned()
        .collect();
    for key in out_of_scope {
        pulses.remove(&key);
    }
}

/// Which `Node` trait method a dispatch invokes. The engine picks it
/// from the phase plus the manifest, so a node never inspects the
/// phase itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeBody {
    /// The node's normal body (`Node::run`).
    Run,
    /// A trigger's registration body (`Node::setup_trigger`).
    SetupTrigger,
    /// A trigger with nothing to do this run (infra setup, or a Fire
    /// where another trigger fired): the dispatch terminates as
    /// Completed without invoking the node, so every output port
    /// closes and the skip cascade prunes its exclusive branches.
    SkipTrigger,
}

/// The one phase-routing rule: a plain node runs its normal body in
/// EVERY phase (a value feeding a trigger's config must be produced at
/// setup time too); a trigger registers at TriggerSetup, runs at Fire
/// only as THE firing trigger (`has_wake`: this dispatch carries the
/// wake payload), and otherwise just closes its ports (infra setup,
/// or a Fire of a different trigger).
fn node_body_for(
    phase: weft_core::context::Phase,
    is_trigger: bool,
    has_wake: bool,
) -> NodeBody {
    use weft_core::context::Phase;
    if !is_trigger {
        return NodeBody::Run;
    }
    match phase {
        Phase::TriggerSetup => NodeBody::SetupTrigger,
        Phase::Fire if has_wake => NodeBody::Run,
        Phase::Fire => NodeBody::SkipTrigger,
        Phase::InfraSetup => NodeBody::SkipTrigger,
    }
}

/// Compute the node-id set that a `Phase::TriggerSetup` run should
/// execute: every trigger node plus its upstream closure. Any node
/// outside this set will receive pulses (bridge output fans out to
/// its downstream), but we auto-skip them so the loop terminates
/// instead of blocking on inputs that will never arrive (e.g. the
/// reply node of an echo bot).
fn compute_trigger_setup_scope(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
) -> std::collections::HashSet<String> {
    let triggers: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.features.is_trigger)
        .map(|n| n.id.clone())
        .collect();
    upstream_closure(project, edge_idx, triggers)
}

/// Compute the node-id set that a `Phase::InfraSetup` run should
/// execute: every `requires_infra` node plus its upstream closure.
/// This is what unblocks "programmatic infra" (a text-field node
/// feeding into an infra node): the upstream nodes execute first so
/// their values are available as `provision_infra`-time inputs.
fn compute_infra_setup_scope(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
) -> std::collections::HashSet<String> {
    let infra: Vec<String> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| n.id.clone())
        .collect();
    upstream_closure(project, edge_idx, infra)
}

fn upstream_closure(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    seeds: Vec<String>,
) -> std::collections::HashSet<String> {
    let mut scope: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut frontier: Vec<String> = seeds;
    while let Some(id) = frontier.pop() {
        if !scope.insert(id.clone()) {
            continue;
        }
        for edge in edge_idx.get_incoming(project, &id) {
            if !scope.contains(&edge.source) {
                frontier.push(edge.source.clone());
            }
        }
    }
    scope
}


async fn fetch_events(
    journal: &dyn JournalClient,
    color: Color,
) -> anyhow::Result<Vec<weft_journal::ExecEvent>> {
    journal.events_for_color(color).await
}

/// Walk the journal, find every (node, frames) that's currently
/// non-terminal, and journal a NodeCancelled for each so the UI
/// flips them out of "running". Called when the loop driver exits
/// with `ExecutionOutcome::Cancelled`.
///
/// Important: the source of truth is the freshly-folded journal,
/// NOT the worker's in-memory `executions` table. The dispatcher's
/// cancel path may have already written some NodeCancelled events
/// (those records will be terminal in the fold and skipped). The
/// worker may have spawned more nodes between when the dispatcher
/// folded and when the worker observed the cancellation flag;
/// those records are still non-terminal, and only the worker can
/// catch them. Per-node idempotency falls out of "if it's already
/// terminal in the journal, skip it."
async fn journal_node_cancellations(
    journal: &dyn JournalClient,
    color: Color,
    pod_name: &str,
) {
    let events = match fetch_events(journal, color).await {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(
                target: "weft_engine",
                error = %err,
                "journal_node_cancellations: failed to fetch events for catch-up fold"
            );
            return;
        }
    };
    let snapshot = weft_journal::fold_to_snapshot(color, &events);
    let now = now_unix();
    let reason = "Cancelled by user".to_string();
    for (node_id, execs) in snapshot.executions.iter() {
        for e in execs {
            if e.status.is_terminal() {
                continue;
            }
            let event = weft_journal::ExecEvent::NodeCancelled {
                color,
                node_id: node_id.clone(),
                frames: e.frames.clone(),
                reason: reason.clone(),
                // This catch-up cancel only flips records terminal; the
                // downstream closure cascade is handled by the cancel
                // cleanup (cancel_loop_instances + the normal teardown),
                // so there are no per-node closures to carry here.
                closure_emissions: Vec::new(),
                at_unix: now,
            };
            if let Err(err) = journal.record_event(&event, Some(pod_name)).await {
                tracing::warn!(
                    target: "weft_engine",
                    error = %err,
                    node = %node_id,
                    "failed to journal NodeCancelled"
                );
            }
        }
    }
}

/// Journal the terminal event for this execution's color. Pure
/// translation from `ExecutionOutcome` to the matching `ExecEvent`
/// variant: `Completed`/`Failed`/`Stuck` map; `Stalled` is a
/// caller-side no-op so this function isn't called for it.
async fn journal_terminal(
    journal: &dyn JournalClient,
    clock: &dyn weft_platform_traits::Clock,
    color: Color,
    pod_name: &str,
    outcome: &ExecutionOutcome,
) {
    // Idempotent: if a terminal event already exists for this color
    // (e.g. the dispatcher's cancel path wrote ExecutionCancelled
    // before the worker's loop driver observed cancellation), skip
    // the write. Avoids the bridge double-publishing. There is NO
    // DB uniqueness guard on terminal events (the write uses
    // record_event, not record_event_dedup), so this check is the
    // only dedup. On a transient read error, skip the write rather
    // than risk a double-publish: a missed terminal is re-folded by
    // the bridge from the journal, a duplicate confuses SSE
    // consumers.
    match journal.has_terminal_event(color).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(
                target: "weft_engine::execution_driver",
                color = %color,
                error = %e,
                "has_terminal_event failed; skipping terminal write to avoid double-publish"
            );
            return;
        }
    }
    let at_unix = now_unix();
    let event = match outcome {
        ExecutionOutcome::Completed { outputs } => weft_journal::ExecEvent::ExecutionCompleted {
            color,
            outputs: outputs.clone(),
            at_unix,
        },
        // A cancel maps to the proper ExecutionCancelled terminal so the
        // UI renders the cancel affordance instead of a generic failure.
        ExecutionOutcome::Cancelled => {
            weft_journal::ExecEvent::ExecutionCancelled {
                color,
                reason: "Cancelled by user".to_string(),
                at_unix,
            }
        }
        ExecutionOutcome::Failed { error } => weft_journal::ExecEvent::ExecutionFailed {
            color,
            error: error.clone(),
            at_unix,
        },
        ExecutionOutcome::Stuck => weft_journal::ExecEvent::ExecutionFailed {
            color,
            error: "execution stuck".to_string(),
            at_unix,
        },
        ExecutionOutcome::Stalled => {
            debug_assert!(false, "journal_terminal must not be called for Stalled");
            return;
        }
    };
    // Terminal events MUST land in the journal: the SSE bridge keys
    // off them, and a missing terminal leaves the UI showing a hung
    // execution forever with no operator recourse. Retry with bounded
    // backoff on transient errors; on persistent failure, panic so
    // k8s restarts the pod (the new pod will run the worker again
    // and re-emit the terminal once the journal is back up).
    let mut delay_ms = 100u64;
    let mut attempt = 0u32;
    const MAX_ATTEMPTS: u32 = 5;
    loop {
        match journal.record_event(&event, Some(pod_name)).await {
            Ok(()) => return,
            Err(e) => {
                attempt += 1;
                if attempt >= MAX_ATTEMPTS {
                    panic!(
                        "failed to journal terminal event after {MAX_ATTEMPTS} attempts: {e}; \
                         panicking so the pod restarts and the next run can re-emit"
                    );
                }
                tracing::warn!(
                    target: "weft_engine",
                    error = %e,
                    attempt,
                    "retrying terminal-event journal write"
                );
                clock.sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(5000);
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// The engine's test modules live in `execution_driver_tests/` as child
// modules (`#[path]`), so they keep private-item access (`use super::*`)
// without bloating this file.

#[cfg(test)]
#[path = "execution_driver_tests/resume.rs"]
mod resume_tests;

#[cfg(test)]
#[path = "execution_driver_tests/scope.rs"]
mod scope_tests;

/// Shared layer-3 rig for tests that drive `run_one_execution` with real
/// inline nodes: an in-memory recording journal plus Noop fakes for every
/// I/O client the engine composes. Test modules bring their own nodes,
/// projects, and assertions; the rig is only the dumb plumbing.
#[cfg(test)]
#[path = "execution_driver_tests/rig.rs"]
mod engine_test_rig;

#[cfg(test)]
#[path = "execution_driver_tests/phase_routing.rs"]
mod phase_routing_tests;

#[cfg(test)]
#[path = "execution_driver_tests/bus_comm.rs"]
mod bus_comm_tests;

// Layer 3: LoopRuntime integration rig tests. These exercise the engine's
// loop boundary handlers (`handle_loop_boundary_firing`, `launch_iteration`,
// `emit_loop_outward`, `cancel_loop_instances`) against synthetic
// ProjectDefinitions. They confirm the integration points the unit tests on
// `LoopRuntime` alone can't reach: per-iteration pulse emission shapes,
// gather/carry assembly at outward emit, frame-stack keying, and
// cancellation closure emission.
#[cfg(test)]
#[path = "execution_driver_tests/loop_rig.rs"]
mod loop_rig_tests;
