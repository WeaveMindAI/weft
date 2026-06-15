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

use weft_core::context::{ConfigBag, InputBag};
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
) -> anyhow::Result<ExecutionOutcome> {
    let project = &*project;
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    // Kicked roots (entry points of the active execution: firing
    // trigger, manual-run roots, infra-setup roots). Folded from
    // `ExecEvent::NodeKicked`. The scheduler dispatches each not-yet-
    // dispatched kick once at frames=[]; `wake_payload` for the firing
    // trigger threads through to `ctx.wake_payload()`.
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
        if clients.clock.now().saturating_duration_since(refetch_start) > refetch_deadline {
            // Don't overwrite the outcome: a Stalled drive that ran
            // out of refetch budget is STILL Stalled (the worker
            // exits cleanly, dispatcher respawns on next fire). Only
            // an already-Stuck drive stays Stuck. Relabeling Stalled
            // as Stuck here would journal a terminal event and kill
            // legitimate parked work permanently.
            tracing::warn!(
                target: "weft_engine::resume",
                color = %color,
                deadline_secs = REFETCH_WALL_CLOCK_DEADLINE_SECS,
                outcome = ?outcome,
                "refetch loop hit wall-clock deadline; exiting with last drive outcome"
            );
            break;
        }
        let fresh = fetch_events(journal.as_ref(), color).await?;
        // Append-only journal: fresh.len() can only grow or stay equal.
        // No new events since the last fetch means we're parked behind
        // a signal the dispatcher hasn't resolved yet; exit cleanly so
        // the worker dies and the next fire respawns it.
        debug_assert!(fresh.len() >= event_count_before, "journal shrank under us");
        if fresh.len() == event_count_before {
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
            // Eager storage sweep: delete this run's un-kept exec
            // files right away. PURELY an optimization (errors only
            // log): the dispatcher's durable terminate sweep is the
            // guarantee, since a stall-killed worker never reaches
            // this line.
            clients.storage.eager_sweep(color).await;
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
    // already skipped during fold. The same fold runs server-side
    // when the inspector calls `/replay`, and THAT path forwards
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
        //    non-terminal-exec resume path, and the body's
        //    `ctx.wake_payload()` MUST still return the body the
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
            if let Some(payload) = info.payload.clone() {
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
                    Some(def) => weft_core::exec::ready::build_kicked_input(def),
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
            let config = ConfigBag {
                values: node_def.config.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };
            let input = InputBag {
                values: group.input.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };
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
                group.frames.clone(),
                clients.clone(),
                pod_name.to_string(),
                tenant_id.to_string(),
                cancellation.clone(),
                bus_coordinator.clone(),
                declared_outputs,
            )
            .with_awaited_sequence(sequence)
            .with_emit_channel(task_tx.clone());
            if let Some(payload) = wake_payload {
                runner = runner.with_wake_payload(payload);
            }
            let handle = Arc::new(runner) as Arc<dyn weft_core::context::ContextHandle>;

            let ctx = ExecutionContext::new(
                exec_id.to_string(),
                project.id.to_string(),
                node_id.clone(),
                node_def.node_type.clone(),
                node_def.label.clone(),
                group.color,
                group.frames.clone(),
                config,
                input,
                phase,
                handle,
            );

            // The lifecycle event (NodeStarted or NodeResumed) was
            // already shipped earlier in this loop body, before
            // the spawn. Don't ship a second one here.

            // Spawn the node's body as a task. For infra nodes in
            // `Phase::InfraSetup` the body runs in two stages:
            //   1. `node_impl.provision(infra_ctx, input)` returns an
            //      InfraSpec. Failure here = node fails with stage
            //      "provision"; downstream cascade-skips.
            //   2. Engine compiles spec locally, asks broker for prior
            //      applied state, picks skip / fresh / replace, and
            //      (when not skip) enqueues an Apply lifecycle command
            //      via the broker. The tenant's supervisor pod claims
            //      the command and runs kubectl. Failure here = node
            //      fails with stage "apply".
            //   3. `node_impl.execute(ctx)` runs as usual, with
            //      `ctx.endpoint_url(name)` now resolving against the
            //      freshly-applied infra_node row. Failure here = node
            //      fails with stage "execute"; the infra stays up
            //      (provisioned-but-execute-failed sub-state).
            //
            // For non-InfraSetup phases AND non-infra nodes, this
            // is just `execute`. The task sends its terminal back on the
            // shared `task_tx` (after any emissions it sent on the same
            // channel); the main loop applies the effect on
            // `pulses`/`executions`.
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
            // Input bag for provision (mirrors the execute input).
            let provision_input = weft_core::context::InputBag {
                values: group
                    .input
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            };
            let abort_handle = in_flight.spawn(async move {
                if is_infra_setup_provision {
                    // 1. Call the node's provision body.
                    let infra_ctx = weft_core::infra::InfraProvisionContext::new(
                        provision_project_id.clone(),
                        provision_node_id.clone(),
                        provision_namespace.clone(),
                        provision_tenant_id.clone(),
                    );
                    let spec = match node_impl.provision(infra_ctx, provision_input).await {
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
                    // 3. Fall through to execute.
                }

                // `execute` returns `()`: it fires downstream only via
                // `ctx.pulse_downstream` (emissions ride the SAME task
                // channel, applied by the loop while the task runs, and
                // always BEFORE this terminal by FIFO ordering). The
                // return just signals terminal outcome.
                let result = node_impl.execute(ctx).await;
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
/// their values are available as `provision`-time inputs.
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

#[cfg(test)]
mod resume_tests {
    use super::*;
    use serde_json::json;
    use weft_core::signal::{to_spec, Form, FormSchema};
    use weft_journal::ExecEvent;

    fn color() -> Color {
        uuid::Uuid::nil()
    }

    fn spec() -> weft_core::primitive::SignalSpec {
        to_spec(Form {
            form_type: "human_query".into(),
            schema: FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        })
    }

    fn registered(token: &str, call_index: u32) -> ExecEvent {
        ExecEvent::SuspensionRegistered {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            token: token.into(),
            spec: spec(),
            call_index,
            at_unix: 0,
        }
    }

    fn suspended(token: &str) -> ExecEvent {
        ExecEvent::NodeSuspended {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            token: token.into(),
            at_unix: 0,
        }
    }

    /// Multi-await body where the FIRST await resolved (and the body
    /// resumed past it) and the body is now parked on the SECOND.
    /// `apply_snapshot` must NOT mark the node for re-dispatch: the
    /// suspension it is currently parked on is unresolved. The old
    /// "any resolved entry in the sequence" check re-dispatched here,
    /// which livelocked every worker boot of such a color (replay,
    /// re-suspend, two fresh journal rows, refetch sees new rows,
    /// repeat until the wall-clock deadline).
    fn two_await_events() -> (String, Vec<ExecEvent>) {
        let pid = uuid::Uuid::new_v4().to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "n".into(),
                target_port: "in".into(),
                frames: vec![],
                value: json!("x"),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                input: json!({"in": "x"}),
                pulses_absorbed: vec![pid.clone()],
                closed_ports: vec![],
                at_unix: 0,
            },
            registered("t0", 0),
            suspended("t0"),
            ExecEvent::SuspensionResolved {
                color: color(),
                token: "t0".into(),
                value: json!("v0"),
                at_unix: 0,
            },
            ExecEvent::NodeResumed {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                token: Some("t0".into()),
                value: Some(json!("v0")),
                pulses_absorbed: vec![],
                at_unix: 0,
            },
            registered("t1", 1),
            suspended("t1"),
        ];
        (pid, events)
    }

    fn apply(events: &[ExecEvent]) -> (PulseTable, NodeExecutionTable, HashMap<String, weft_core::primitive::KickedNode>) {
        let snap = weft_journal::fold_to_snapshot(color(), events);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        let mut awaited = HashMap::new();
        apply_snapshot(snap, &mut pulses, &mut executions, &mut kicked, &mut awaited);
        (pulses, executions, kicked)
    }

    fn pulse_status(pulses: &PulseTable, node: &str, pid: &str) -> weft_core::pulse::PulseStatus {
        pulses
            .get(node)
            .and_then(|b| b.iter().find(|p| p.id.to_string() == pid))
            .map(|p| p.status)
            .expect("pulse present")
    }

    #[test]
    fn parked_on_unresolved_second_await_does_not_redispatch() {
        let (pid, events) = two_await_events();
        let (pulses, _, _) = apply(&events);
        assert_eq!(
            pulse_status(&pulses, "n", &pid),
            weft_core::pulse::PulseStatus::Absorbed,
            "current suspension (t1) is unresolved; un-absorbing would livelock the boot"
        );
    }

    #[test]
    fn resolved_current_await_redispatches() {
        let (pid, mut events) = two_await_events();
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: "t1".into(),
            value: json!("v1"),
            at_unix: 0,
        });
        let (pulses, _, _) = apply(&events);
        assert_eq!(
            pulse_status(&pulses, "n", &pid),
            weft_core::pulse::PulseStatus::Pending,
            "current suspension (t1) resolved; the node must re-dispatch"
        );
    }

    fn kick_events() -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "n".into(),
                payload: Some(json!({"body": 1})),
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                input: json!({}),
                pulses_absorbed: vec![],
                closed_ports: vec![],
                at_unix: 0,
            },
        ]
    }

    /// Kicked entry node whose worker crashed mid-Fire (Running exec,
    /// no terminal row): `apply_snapshot` must reset `dispatched` so
    /// the kick synthesis re-fires it. Kicked nodes have no inbound
    /// pulses, so the pulse un-absorb path can never cover them; the
    /// old behavior left the exec Running forever and the execution
    /// landed Stuck with the wake payload silently dropped.
    #[test]
    fn crashed_kicked_node_redispatches() {
        let (_, _, kicked) = apply(&kick_events());
        assert!(!kicked.get("n").expect("kick present").dispatched);
    }

    #[test]
    fn completed_kicked_node_stays_dispatched() {
        let mut events = kick_events();
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            output: json!({}),
            closure_emissions: vec![],
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(kicked.get("n").expect("kick present").dispatched);
    }

    /// A kicked node parked on a still-pending suspension must NOT
    /// re-dispatch on every worker boot (that is exactly the churn the
    /// resume-location scoping prevents); once its suspension
    /// resolves, it must.
    #[test]
    fn suspended_kicked_node_redispatches_only_after_resolve() {
        let mut events = kick_events();
        events.push(registered("tk", 0));
        events.push(suspended("tk"));
        let (_, _, kicked) = apply(&events);
        assert!(
            kicked.get("n").expect("kick present").dispatched,
            "pending suspension: no re-dispatch churn"
        );
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: "tk".into(),
            value: json!("answer"),
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(
            !kicked.get("n").expect("kick present").dispatched,
            "resolved suspension: kick synthesis must re-fire the node"
        );
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use weft_core::project::{Edge, NodeDefinition, Position, ProjectDefinition};

    fn mk_node(id: &str, is_trigger: bool, requires_infra: bool) -> NodeDefinition {
        let mut features = weft_core::node::NodeFeatures::default();
        features.is_trigger = is_trigger;
        NodeDefinition {
            id: id.to_string(),
            node_type: "Test".to_string(),
            label: None,
            config: serde_json::Value::Null,
            position: Position { x: 0.0, y: 0.0 },
            inputs: Vec::new(),
            outputs: Vec::new(),
            features,
            scope: Vec::new(),
            group_boundary: None,
            requires_infra,
            images: Vec::new(),
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        }
    }

    fn mk_edge(src: &str, dst: &str) -> Edge {
        Edge {
            id: format!("e-{src}-{dst}"),
            source: src.to_string(),
            target: dst.to_string(),
            source_handle: None,
            target_handle: None,
            span: None,
        }
    }

    fn mk_project(nodes: Vec<NodeDefinition>, edges: Vec<Edge>) -> ProjectDefinition {
        let v = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": nodes,
            "edges": edges,
            "groups": []
        });
        serde_json::from_value(v).expect("test project definition")
    }

    #[test]
    fn infra_setup_scope_includes_infra_and_upstream() {
        // text -> compute -> infra
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("compute", false, false),
                mk_node("infra", false, true),
            ],
            vec![mk_edge("text", "compute"), mk_edge("compute", "infra")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("infra"));
        assert!(scope.contains("compute"));
        assert!(scope.contains("text"));
    }

    #[test]
    fn infra_setup_scope_excludes_downstream_of_infra() {
        // infra -> trigger -> reply (a fire-time-only path)
        let project = mk_project(
            vec![
                mk_node("infra", false, true),
                mk_node("trigger", true, false),
                mk_node("reply", false, false),
            ],
            vec![mk_edge("infra", "trigger"), mk_edge("trigger", "reply")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("infra"));
        // The trigger node is downstream of infra; not part of the
        // InfraSetup scope.
        assert!(!scope.contains("trigger"));
        assert!(!scope.contains("reply"));
    }

    #[test]
    fn infra_setup_scope_handles_multiple_infra_nodes() {
        // text -> infraA ; cfg -> infraB
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("cfg", false, false),
                mk_node("infraA", false, true),
                mk_node("infraB", false, true),
            ],
            vec![mk_edge("text", "infraA"), mk_edge("cfg", "infraB")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("text"));
        assert!(scope.contains("cfg"));
        assert!(scope.contains("infraA"));
        assert!(scope.contains("infraB"));
    }

    #[test]
    fn trigger_setup_scope_unchanged() {
        // text -> trigger ; trigger -> reply (downstream not in scope)
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("trigger", true, false),
                mk_node("reply", false, false),
            ],
            vec![mk_edge("text", "trigger"), mk_edge("trigger", "reply")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_trigger_setup_scope(&project, &idx);
        assert!(scope.contains("trigger"));
        assert!(scope.contains("text"));
        assert!(!scope.contains("reply"));
    }

    #[test]
    fn empty_infra_set_yields_empty_scope() {
        let project = mk_project(
            vec![mk_node("a", false, false), mk_node("b", false, false)],
            vec![mk_edge("a", "b")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.is_empty());
    }
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

#[cfg(test)]
mod bus_comm_tests {
    //! Layer-3 test: two co-alive nodes exchanging messages over a bus,
    //! driven through the real loop. A `Producer` creates a bus, registers
    //! as "producer", emits the handle (which fires the `Consumer`), then
    //! waits via the handshake for the consumer to register as "llm"
    //! before streaming. The `Consumer` receives the live bus, registers
    //! as "llm", and drains the messages into a shared collector. The test
    //! asserts the consumer saw exactly the producer's messages, each
    //! stamped with the sender's REGISTERED name, proving create_bus ->
    //! register -> pulse_downstream -> input_bus -> wait_for -> send/recv
    //! works end to end through the engine.

    use super::*;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::node::{Node, NodeMetadata, NodeOutput};
    use weft_core::error::WeftResult;
    use weft_core::{ExecutionContext, NodeCatalog, ProjectDefinition};
    use weft_journal::{ExecEvent, JournalClient};
    use weft_infra::InfraReader;
    use crate::context::{EngineClients, InfraStateClient};

    fn trivial_metadata(node_type: &str) -> NodeMetadata {
        serde_json::from_value(json!({
            "type": node_type, "label": node_type, "description": "", "category": "test"
        }))
        .expect("trivial metadata")
    }

    /// A minimal wait-for-input signal for the bus + await_signal tests.
    fn human_form() -> weft_core::signal::Form {
        weft_core::signal::Form {
            form_type: "human_query".into(),
            schema: weft_core::signal::FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        }
    }

    /// Producer: create a bus on output port "channel", emit it, then
    /// send three messages and close. Stays alive (its execute does not
    /// return) until it has sent + closed.
    struct Producer;
    #[async_trait]
    impl Node for Producer {
        fn node_type(&self) -> &'static str { "Producer" }
        fn metadata(&self) -> NodeMetadata { trivial_metadata("Producer") }
        async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("producer").expect("register producer");
            // Put the marker on the bus output port; downstream resolves
            // it through the per-execution `BusRegistry`.
            ctx.pulse_downstream(NodeOutput::with("channel", marker))
                .await?;
            // Wait (via the language-level handshake) for the consumer to
            // register under "llm" before streaming: the bus only delivers
            // messages sent AFTER a participant is live. `wait_for` returns
            // an error (never hangs) if "llm" can never register.
            bus.wait_for("llm").await.expect("consumer 'llm' should register");
            for i in 0..3 {
                bus.send("tick", json!({ "i": i })).expect("send to live consumer");
            }
            // close() means "no more messages": the consumer drains the
            // three buffered ticks, THEN recv returns None.
            bus.close();
            Ok(())
        }
    }

    /// Consumer: register under "llm", then drain every "tick" message
    /// into the shared collector until the bus closes. Holds an Arc to the
    /// collector so the test can read it.
    struct Consumer {
        seen: Arc<StdMutex<Vec<(String, i64)>>>,
    }
    #[async_trait]
    impl Node for Consumer {
        fn node_type(&self) -> &'static str { "Consumer" }
        fn metadata(&self) -> NodeMetadata { trivial_metadata("Consumer") }
        async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let mut bus = ctx.bus_from_input("channel")?;
            // Claim our identity: this is what releases the producer's
            // `wait_for("llm")` and stamps our sends.
            bus.register("llm").expect("register llm");
            let mut cursor = bus.cursor().with_filter(|entry| {
                matches!(
                    &entry.kind,
                    weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "tick"
                )
            });
            while let Some(entry) = cursor.next().await.expect("no FellBehind on journaled bus") {
                if let weft_core::bus::BusEntryKind::Message { from, payload, .. } = entry.kind {
                    let payload = payload.expect("journaled payload");
                    let i = payload["i"].as_i64().unwrap_or(-1);
                    self.seen.lock().unwrap().push((from, i));
                }
            }
            Ok(())
        }
    }

    struct TestCatalog {
        producer: &'static Producer,
        consumer: &'static Consumer,
    }
    impl NodeCatalog for TestCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            match node_type {
                "Producer" => Some(self.producer as &'static dyn Node),
                "Consumer" => Some(self.consumer as &'static dyn Node),
                _ => None,
            }
        }
        fn all(&self) -> Vec<&'static str> { vec!["Producer", "Consumer"] }
    }

    /// In-memory recording journal: stores every event and replays them
    /// for the boot fold. Unlike the Noop journals in `replay_tests`,
    /// this actually drives a live execution.
    #[derive(Default)]
    struct MemJournal {
        events: StdMutex<Vec<ExecEvent>>,
    }
    #[async_trait]
    impl JournalClient for MemJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.color() == color)
                .cloned()
                .collect())
        }
        async fn has_terminal_event(&self, color: Color) -> anyhow::Result<bool> {
            Ok(self.events.lock().unwrap().iter().any(|e| matches!(
                e,
                ExecEvent::ExecutionCompleted { color: c, .. }
                    | ExecEvent::ExecutionFailed { color: c, .. }
                    | ExecEvent::ExecutionCancelled { color: c, .. } if *c == color
            )))
        }
    }

    struct NoopTasks;
    #[async_trait]
    impl weft_task_store::TaskStoreClient for NoopTasks {
        async fn enqueue_dedup(&self, _s: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("bus test enqueues no tasks")
        }
        async fn wait_for_terminal(&self, _t: uuid::Uuid, _to: std::time::Duration, _pi: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!()
        }
        async fn claim_one(&self, _p: &str, _f: weft_task_store::tasks::ClaimFilter) -> anyhow::Result<Option<weft_task_store::tasks::Task>> { Ok(None) }
        async fn heartbeat(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn complete(&self, _t: uuid::Uuid, _p: &str, _r: Value) -> anyhow::Result<()> { Ok(()) }
        async fn fail(&self, _t: uuid::Uuid, _p: &str, _e: String) -> anyhow::Result<()> { Ok(()) }
    }
    struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_url(&self, _p: &str, _n: &str, _e: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    }
    struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(&self, _p: &str, _n: &str, _s: serde_json::Value) -> anyhow::Result<i64> { Ok(0) }
        async fn wait_apply(&self, _p: &str, _c: i64) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
    }
    struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: &str,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<ProjectDefinition>> {
            // These execution_driver tests inject the project into
            // `run_one_execution` directly, so the per-execution
            // fetch path is never invoked here. Bail loud if it is.
            anyhow::bail!("NoopProject::fetch_definition not implemented in execution_driver tests")
        }
    }

    /// Project: producer.channel -> consumer.channel, both Bus ports.
    fn bus_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "bus-test",
            "description": null,
            "nodes": [
                {
                    "id": "producer", "nodeType": "Producer", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "channel", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "Consumer", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "channel", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "producer", "target": "consumer", "sourceHandle": "channel", "targetHandle": "channel" }
            ],
            "groups": []
        }))
        .expect("bus project")
    }

    #[tokio::test]
    async fn two_nodes_exchange_messages_over_a_bus() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let catalog: Arc<dyn NodeCatalog> = Arc::new(TestCatalog {
            producer: Box::leak(Box::new(Producer)),
            consumer: Box::leak(Box::new(Consumer { seen: seen.clone() })),
        });

        let project = bus_project();
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());

        // Seed: ExecutionStarted(Fire) + a NodeKicked on the producer
        // so it becomes ready (it has no real inputs).
        journal
            .record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: project.id.to_string(),
                    entry_node: "producer".into(),
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::NodeKicked {
                    color,
                    node_id: "producer".into(),
                    payload: None,
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
        };

        let outcome = run_one_execution(
            Arc::new(project),
            catalog,
            color,
            clients,
            "pod-test".into(),
            "tenant-test".into(),
            "ns-test".into(),
            CancellationFlag::new_arc(),
        )
        .await
        .expect("run_one_execution ok");

        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "execution should complete, got {outcome:?}"
        );

        let mut got = seen.lock().unwrap().clone();
        got.sort_by_key(|(_, i)| *i);
        assert_eq!(
            got,
            vec![
                ("producer".to_string(), 0),
                ("producer".to_string(), 1),
                ("producer".to_string(), 2),
            ],
            "consumer received all three producer messages, each stamped with the producer's registered name"
        );

        // Bus events MUST be journaled so the inspector replays the
        // conversation. After the node_id reshape the bus events
        // carry only (bus_id, name) / (bus_id, from, payload): node
        // attribution is derived from PulseEmitted at the dispatcher
        // bridge, not stamped on the bus stream. Here we assert the
        // raw protocol shape: both names registered, three messages
        // arrived in order with the producer's name stamped on each.
        let journal_events = journal.events.lock().unwrap().clone();
        let mut join_names: Vec<String> = Vec::new();
        let mut messages: Vec<(String, String)> = Vec::new();
        for ev in &journal_events {
            match ev {
                ExecEvent::BusJoined { name, .. } => {
                    join_names.push(name.clone());
                }
                ExecEvent::BusMessage { from, payload, .. } => {
                    let p = payload
                        .value()
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("i"))
                        .and_then(|v| v.as_i64())
                        .map(|i| i.to_string())
                        .unwrap_or_default();
                    messages.push((from.clone(), p));
                }
                _ => {}
            }
        }
        assert!(
            join_names.contains(&"producer".to_string()),
            "producer should have joined; got {join_names:?}"
        );
        assert!(
            join_names.contains(&"llm".to_string()),
            "consumer should have joined as 'llm'; got {join_names:?}"
        );
        assert_eq!(
            messages,
            vec![
                ("producer".to_string(), "0".to_string()),
                ("producer".to_string(), "1".to_string()),
                ("producer".to_string(), "2".to_string()),
            ],
            "every message must be journaled in order, stamped with the sender's registered name"
        );
    }

    /// A node waiting forever on a bus cursor (a warm co-alive node
    /// whose task never ends on its own) must not prevent cancellation:
    /// when the execution is cancelled, the loop wakes, returns
    /// Failed(cancelled), and the waiting task is aborted (its bus
    /// handle drops). This is what makes a user "Stop" tear down a
    /// live-bus execution.
    struct Waiter;
    #[async_trait]
    impl Node for Waiter {
        fn node_type(&self) -> &'static str { "Waiter" }
        fn metadata(&self) -> NodeMetadata { trivial_metadata("Waiter") }
        async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let (mut bus, _marker) = ctx.create_bus(Default::default())?;
            bus.register("waiter").expect("register");
            // Wait forever: no peer ever sends or closes. Only
            // cancellation (task abort) can end this.
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        }
    }

    struct WaiterCatalog(&'static Waiter);
    impl NodeCatalog for WaiterCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            (node_type == "Waiter").then_some(self.0 as &'static dyn Node)
        }
        fn all(&self) -> Vec<&'static str> { vec!["Waiter"] }
    }

    #[tokio::test]
    async fn cancellation_unblocks_a_node_waiting_on_cursor() {
        let catalog: Arc<dyn NodeCatalog> = Arc::new(WaiterCatalog(Box::leak(Box::new(Waiter))));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "waiter", "description": null,
            "nodes": [{
                "id": "waiter", "nodeType": "Waiter", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": [], "outputs": [{ "name": "channel", "portType": "Bus", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [], "groups": []
        }))
        .expect("waiter project");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.record_event(&ExecEvent::ExecutionStarted {
            color, project_id: project.id.to_string(), entry_node: "waiter".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: "test-hash".into(), at_unix: 0,
        }, None).await.unwrap();
        journal.record_event(&ExecEvent::NodeKicked {
            color, node_id: "waiter".into(), payload: None, at_unix: 0,
        }, None).await.unwrap();

        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
        };
        let cancel = CancellationFlag::new_arc();

        let run = tokio::spawn(run_one_execution(
            Arc::new(project), catalog, color, clients,
            "pod".into(), "tenant".into(), "ns".into(),
            cancel.clone(),
        ));
        // Let the waiter reach its cursor wait, then cancel.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();

        // Must finish (not hang). Bound it so a regression fails the
        // test instead of hanging the suite. The Waiter is a lone node
        // waiting on a cursor with no peer; cancellation and the dead-
        // end detector (every in-flight task waiting, nothing can feed
        // them) RACE to unblock it, and either is a correct teardown.
        // The safety property the test pins is "the execution
        // terminates", not the specific terminal outcome.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("run_one_execution must return, not hang")
            .expect("join ok")
            .expect("run ok");
        match outcome {
            ExecutionOutcome::Cancelled => {} // cancel won the race
            ExecutionOutcome::Completed { .. } => {} // dead-end closed the bus, Waiter exited cleanly
            other => panic!("expected Cancelled or Completed via dead-end, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Dead-end coverage: the four holes the bus model has to handle.
    //
    // The bus itself never decides "this wait can never be satisfied":
    // its `wait_for` only releases on register-or-close. The ENGINE
    // closes the bus when its loop concludes nothing can possibly feed
    // the waiting tasks. Each test below pins one such case.
    // -----------------------------------------------------------------

    /// A single configurable test node whose body is looked up from a
    /// global registry by `(project_id, node_id)`. Keying by project_id
    /// scopes bodies per-test (each test creates a fresh project uuid),
    /// so parallel `cargo test` runs of two tests that both use node_id
    /// "a" don't collide on a shared global key. The body is stored as
    /// `Arc<dyn Fn>` and cloned on lookup so a node can be dispatched
    /// more than once in one execution (fan-out, resume) without
    /// the first dispatch consuming the only copy.
    type NodeBody = std::sync::Arc<
        dyn Fn(ExecutionContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = WeftResult<()>> + Send>>
            + Send
            + Sync,
    >;
    type BodyKey = (String, String); // (project_id, node_id)
    static NODE_BODIES: std::sync::OnceLock<StdMutex<std::collections::HashMap<BodyKey, NodeBody>>> =
        std::sync::OnceLock::new();
    fn bodies() -> &'static StdMutex<std::collections::HashMap<BodyKey, NodeBody>> {
        NODE_BODIES.get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
    }
    fn install_body(project_id: &str, node_id: &str, body: NodeBody) {
        bodies()
            .lock()
            .unwrap()
            .insert((project_id.to_string(), node_id.to_string()), body);
    }

    struct Configurable;
    #[async_trait]
    impl Node for Configurable {
        fn node_type(&self) -> &'static str { "Configurable" }
        fn metadata(&self) -> NodeMetadata { trivial_metadata("Configurable") }
        async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let key = (ctx.project_id.clone(), ctx.node_id.clone());
            let body = bodies().lock().unwrap().get(&key).cloned();
            match body {
                Some(b) => b(ctx).await,
                None => Err(weft_core::error::WeftError::Runtime(anyhow::anyhow!(
                    "no body installed for project '{}' node '{}'",
                    key.0, key.1
                ))),
            }
        }
    }
    struct ConfigurableCatalog(&'static Configurable);
    impl NodeCatalog for ConfigurableCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            (node_type == "Configurable").then_some(self.0 as &'static dyn Node)
        }
        fn all(&self) -> Vec<&'static str> { vec!["Configurable"] }
    }
    fn configurable_catalog() -> Arc<dyn NodeCatalog> {
        Arc::new(ConfigurableCatalog(Box::leak(Box::new(Configurable))))
    }

    /// Build a project where one entry node creates a bus on `port`, the
    /// rest of `extra_node_ids` are wired to it (downstream Bus consumers).
    fn bus_topology(creator: &str, extra_node_ids: &[&str], port: &str) -> ProjectDefinition {
        let mut nodes = vec![json!({
            "id": creator, "nodeType": "Configurable", "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [],
            "outputs": [{ "name": port, "portType": "Bus", "required": false }],
            "features": {}, "scope": [], "groupBoundary": null,
            "requiresInfra": false, "images": []
        })];
        let mut edges = Vec::new();
        for (i, peer) in extra_node_ids.iter().enumerate() {
            nodes.push(json!({
                "id": peer, "nodeType": "Configurable", "label": null, "config": null,
                "position": { "x": (i as f64) + 1.0, "y": 0.0 },
                "inputs": [{ "name": port, "portType": "Bus", "required": true }],
                "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            }));
            edges.push(json!({
                "id": format!("e{i}"),
                "source": creator, "target": peer,
                "sourceHandle": port, "targetHandle": port
            }));
        }
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "dead-end", "description": null,
            "nodes": nodes, "edges": edges, "groups": []
        }))
        .expect("project")
    }

    /// Topology for the bus + await_signal matrix: a bus `creator` wired
    /// to a `peer` (the pair holds the worker alive), PLUS an independent
    /// `waiter` entry node that does the `await_signal`. The waiter does
    /// NOT touch the bus and does NOT emit before awaiting (the engine
    /// forbids emit-then-await: replay would re-emit). All three are
    /// kicked as entry roots. This mirrors reality: a HumanQuery-style
    /// node waits for input while OTHER nodes keep a bus conversation
    /// alive; the bus only prevents the worker dying.
    fn bus_plus_waiter_topology() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "bus-plus-waiter", "description": null,
            "nodes": [
                {
                    "id": "creator", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "ch", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "peer", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "ch", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "waiter", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [], "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e0", "source": "creator", "target": "peer", "sourceHandle": "ch", "targetHandle": "ch" }
            ],
            "groups": []
        }))
        .expect("bus_plus_waiter project")
    }

    /// Like `bus_plus_waiter_topology` but the `waiter` is PULSE-FED: two
    /// upstream feeders (`feeder1`, `feeder2`) each emit a plain data pulse
    /// into a distinct required input port (`in1`, `in2`). Because the
    /// waiter has inbound edges, its resume goes through the
    /// `pulses_absorbed` un-absorb path (not the kicked `dispatched=false`
    /// reset). Two required ports means a re-dispatch only forms when BOTH
    /// ports carry a pending pulse, which is the lever the regression test
    /// uses: if a resume-absorbed pulse on `in2` is not recorded into
    /// `pulses_absorbed`, the next resume cannot re-satisfy `in2` and the
    /// waiter never re-fires. The creator+peer bus keeps the worker alive
    /// across the awaits exactly as in the kicked variant.
    fn bus_plus_pulse_fed_waiter_topology() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "bus-plus-pulse-fed-waiter", "description": null,
            "nodes": [
                {
                    "id": "creator", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "ch", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "peer", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "ch", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder1", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder2", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder3", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 2.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "waiter", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 2.0 },
                    "inputs": [
                        { "name": "in1", "portType": "String", "required": true },
                        { "name": "in2", "portType": "String", "required": true }
                    ],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e0", "source": "creator", "target": "peer", "sourceHandle": "ch", "targetHandle": "ch" },
                { "id": "e1", "source": "feeder1", "target": "waiter", "sourceHandle": "out", "targetHandle": "in1" },
                { "id": "e2", "source": "feeder2", "target": "waiter", "sourceHandle": "out", "targetHandle": "in2" },
                { "id": "e3", "source": "feeder3", "target": "waiter", "sourceHandle": "out", "targetHandle": "in2" }
            ],
            "groups": []
        }))
        .expect("bus_plus_pulse_fed_waiter project")
    }

    /// Standard test harness: seed ExecutionStarted + NodeKicked on the
    /// creator, run the execution with a bounded timeout (so a hang fails
    /// the test instead of the whole suite).
    async fn run_test(project: ProjectDefinition, creator: &str) -> ExecutionOutcome {
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.record_event(&ExecEvent::ExecutionStarted {
            color, project_id: project.id.to_string(), entry_node: creator.into(),
            phase: weft_core::context::Phase::Fire, definition_hash: "test-hash".into(), at_unix: 0,
        }, None).await.unwrap();
        journal.record_event(&ExecEvent::NodeKicked {
            color, node_id: creator.into(), payload: None, at_unix: 0,
        }, None).await.unwrap();
        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_one_execution(
                Arc::new(project), configurable_catalog(), color, clients,
                "pod".into(), "tenant".into(), "ns".into(),
                CancellationFlag::new_arc(),
            ),
        )
        .await
        .expect("execution must not hang (the dead-end detector failed)")
        .expect("run_one_execution ok")
    }

    /// A node body that PANICS must NOT re-run forever. The panicked
    /// task never sends a NodeTaskResult, so before the task-id fix its
    /// exec record stayed Running, the crashed-Running refold path
    /// re-dispatched it on every respawn, and the node panicked in a
    /// tight loop until the refetch wall-clock deadline (the execution
    /// effectively hung). Now the loop maps the JoinError's task id back
    /// to (node, frames) and journals a terminal NodeFailed, so the
    /// execution unwinds promptly. The 10s timeout in `run_test` is the
    /// hang tripwire.
    #[tokio::test]
    async fn panicking_node_body_fails_instead_of_re_running_forever() {
        let project = bus_topology("a", &[], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "a", std::sync::Arc::new(|_ctx| Box::pin(async move {
            panic!("node body boom");
        })));
        let outcome = run_test(project, "a").await;
        // The panicked node is the only firing; the execution must reach
        // a terminal outcome (Failed via the cascade, or Completed once
        // the failed node closed its outputs and nothing else remained),
        // never hang. The tripwire is the timeout inside run_test.
        assert!(
            matches!(
                outcome,
                ExecutionOutcome::Failed { .. } | ExecutionOutcome::Completed { .. }
            ),
            "panicking node must terminate the execution, got {outcome:?}"
        );
    }

    /// Announce a previously-created bus by emitting on its output port.
    /// Emit a bus marker on `port`. The producer owns the marker value
    /// (returned by `create_bus()` alongside the handle); putting it on
    /// the output port is what makes the bus reachable downstream.
    async fn emit_bus_marker(
        ctx: &ExecutionContext,
        port: &str,
        marker: serde_json::Value,
    ) -> WeftResult<()> {
        ctx.pulse_downstream(NodeOutput::with(port, marker)).await
    }

    /// HOLE 1: creator registers + waits for a peer whose dispatch happens
    /// LATER (the peer's pulse only fires once the creator emits). The
    /// engine must not declare dead-end while the peer is still scheduled
    /// to dispatch (its pulse is pending, in_flight is about to grow).
    /// The existing happy-path `two_nodes_exchange_messages_over_a_bus`
    /// test exercises this baseline; here we stress it by making the
    /// creator emit AND wait, while the peer takes its time to dispatch.
    #[tokio::test]
    async fn hole1_waits_while_peer_is_still_scheduled() {
        let project = bus_topology("creator", &["peer"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "creator", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("creator").expect("register");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // Wait for the peer to register. The peer is dispatched on the
            // next loop iteration after the emit lands; the engine must
            // not panic / dead-end while it gets there.
            bus.wait_for("peer").await.expect("peer should register");
            bus.send("ping", json!(null)).expect("send to live peer");
            bus.close();
            Ok(())
        })));
        install_body(&pid, "peer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("peer").expect("register");
            // Drain until close.
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        })));
        let outcome = run_test(project, "creator").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "hole 1: peer registers in time, execution completes; got {outcome:?}"
        );
    }

    /// HOLE 2: A → B → C cascade. A waits for "b", B waits for "c", and
    /// C fails before registering. The engine has to: see C terminate,
    /// notice B is waiting with nothing to feed it, close the bus → B
    /// fails → notice A is waiting with nothing to feed it, close again
    /// → A fails. Execution unwinds to Failed without hanging.
    #[tokio::test]
    async fn hole2_cascading_failure_when_peer_crashes_before_registering() {
        let project = bus_topology("a", &["b", "c"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "a", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("a").expect("register a");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // A waits for B. When the engine closes the bus, this errors.
            bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                anyhow::anyhow!("a: wait_for(b) failed: {e}"),
            ))?;
            Ok(())
        })));
        install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("b").expect("register b");
            // B waits for C. When the engine closes the bus, this errors.
            bus.wait_for("c").await.map_err(|e| weft_core::error::WeftError::Runtime(
                anyhow::anyhow!("b: wait_for(c) failed: {e}"),
            ))?;
            Ok(())
        })));
        install_body(&pid, "c", std::sync::Arc::new(|_ctx| Box::pin(async move {
            // C crashes before registering: its handle drops without ever
            // claiming a name. B's `wait_for("c")` would hang forever
            // without the engine's dead-end detector.
            Err(weft_core::error::WeftError::Runtime(anyhow::anyhow!("c: simulated crash")))
        })));
        let outcome = run_test(project, "a").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Failed { .. }),
            "hole 2: the cascade unwinds to Failed (not hang); got {outcome:?}"
        );
    }

    /// HOLE 3: a node receives the bus and keeps the handle alive without
    /// ever registering on it (a "passes through" / "holds without
    /// participating" pattern). The producer waits for the real consumer;
    /// the holder must not be miscounted as a participant. With the new
    /// model the holder is invisible to membership (it never registers),
    /// so the producer waits cleanly for the real consumer and unblocks
    /// when it registers.
    #[tokio::test]
    async fn hole3_node_holds_bus_without_registering_does_not_block_waits() {
        let project = bus_topology("producer", &["inspector", "consumer"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "producer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("producer").expect("register");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // The producer waits for the real consumer. The holder
            // (`inspector`) also has the bus but never registered; if the
            // engine miscounted holders as participants, this would
            // dead-end with the inspector still alive.
            bus.wait_for("consumer").await.expect("real consumer should register");
            bus.send("ping", json!(null)).expect("send");
            bus.close();
            Ok(())
        })));
        install_body(&pid, "inspector", std::sync::Arc::new(|ctx| Box::pin(async move {
            // Holds the bus, never registers, never recvs. Just stays
            // alive briefly then drops it. Simulates "a node that touches
            // the bus but does not participate" (the case where a holder
            // would have falsely counted as a participant under the old
            // drop-based liveness model).
            let _bus = ctx.bus_from_input("ch")?;
            Ok(())
        })));
        install_body(&pid, "consumer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("consumer").expect("register");
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        })));
        let outcome = run_test(project, "producer").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "hole 3: holder does not affect the wait; got {outcome:?}"
        );
    }

    // HOLE 4: mutual deadlock. A registers as "a" and waits for "b". B
    // registers as "b" and waits for "a"... wait, that releases. The
    // real deadlock is: A registers as "a" and waits for "x", B
    // registers as "b" and waits for "y", neither x nor y ever come.
    // Every in-flight task is waiting, nothing can feed them, the engine
    // closes the buses and both error out.
    //
    // MULTI-THREADED to exercise the engine loop's
    // `bus_coordinator.wait_notified()` arm-then-check pattern under
    // contention: a wait-start from the LLM worker thread can race the
    // loop thread's dead-end check. Before the `notify_one` fix this
    // test deadlocked to the 10s harness in ~1/8 of parallel-load
    // runs. Stress-looped at 64 concurrent iterations per invocation
    // so any future regression of the arm-then-check window
    // reproduces on the first failing CI run rather than waiting
    // for the flake to find us.
    weft_core::stress_test!(
        name: hole4_mutual_deadlock_when_both_wait_for_names_that_never_come,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            install_body(&pid, "a", std::sync::Arc::new(|ctx| Box::pin(async move {
                let (mut bus, marker) = ctx.create_bus(Default::default())?;
                bus.register("a").expect("register");
                emit_bus_marker(&ctx, "ch", marker).await?;
                bus.wait_for("x").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("a: {e}"),
                ))?;
                Ok(())
            })));
            install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
                let mut bus = ctx.bus_from_input("ch")?;
                bus.register("b").expect("register");
                bus.wait_for("y").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("b: {e}"),
                ))?;
                Ok(())
            })));
            let outcome = run_test(project, "a").await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "hole 4: mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // The mirror of hole4: a GENUINE live exchange (A sends, B
        // replies) that ends in a deadlock. The send-then-park window is
        // exactly where a naive stuck-check could close the bus under a
        // woken-but-unpolled receiver. With the per-node observed-
        // generation accounting (`deadlock_provable`), the close must
        // wait until B has consumed A's message and A has consumed B's
        // reply; only the final mutual wait-for-never deadlocks. Stress-
        // looped under a 4-thread runtime so the cross-thread "B woken
        // but parked in another worker's queue" interleaving surfaces.
        //
        // Each iteration gets its OWN `Arc<AtomicU32>` exchange counter
        // captured into the node bodies (the 64 stress iterations run
        // concurrently, so a shared/global counter would race across
        // iterations). The counter reaches 2 only if BOTH directions of
        // the exchange complete before the deadlock close.
        name: live_exchange_then_deadlock_is_not_torn_down_early,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let exchanges = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_ex = exchanges.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_ex = a_ex.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    // Wait for B to be live, then send it a message and
                    // immediately park reading for B's reply. The park
                    // right after the send is the false-positive window.
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // Position the pong-cursor BEFORE sending ping, so B's
                    // reply (which may land before A is re-scheduled)
                    // cannot slip behind the cursor's start offset.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "pong"
                    ));
                    bus.send("ping", serde_json::json!({"v": 1})).expect("a sends ping");
                    // The close may legitimately race the deadlock tail;
                    // a closed bus here surfaces as Ok(None), not a panic.
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        a_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    // Now deadlock: wait for a name that never comes.
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let b_ex = exchanges.clone();
            install_body(&pid, "b", std::sync::Arc::new(move |ctx| {
                let b_ex = b_ex.clone();
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("b").expect("register");
                    // Drain A's ping, reply with pong, then deadlock.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                    ));
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        b_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // The reply can race the close; ignore a closed bus.
                        let _ = bus.send("pong", serde_json::json!({"v": 2}));
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("b: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let outcome = run_test(project, "a").await;
            // Both directions of the exchange must have completed before
            // the deadlock close. A premature stuck-close would cut the
            // reply, leaving the counter below 2.
            assert_eq!(
                exchanges.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "live ping/pong must complete before the deadlock close \
                 (bus torn down under a woken-but-unpolled peer)"
            );
            // And the final mutual wait-for-never still unwinds to Failed.
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "post-exchange mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // Targets the observed-to-return window: a cursor's `next()`
        // records its node's observed generation at the TOP of its loop
        // (clearing its parked flag), BEFORE the search runs. Without the
        // per-node parked flag, the stuck-check could run inside that
        // window and read the searching node as "caught up" while its
        // evaluation was about to SUCCEED (find A's ping): A is parked
        // caught-up on wait_for("never"), B is mid-search, so both nodes
        // could read as parked-and-caught-up, the parked count reaches
        // in_flight, close_all() fires, and B's pong reply hits
        // SendError::Closed: a live exchange torn down.
        //
        // The window is widened deliberately: B's filter (which runs
        // synchronously inside the search, after the `observed` call and
        // before the message is returned) sleeps ~1ms on the matching
        // entry, holding B mid-evaluation long enough for the driver
        // thread's stuck-check to interleave on the multi-thread
        // runtime. With the parked flag, B's node reads as not-parked
        // for the whole search, so the parked count stays below
        // in_flight, the close is suppressed, the pong send succeeds, and
        // the exchange counter reaches 2. The final mutual
        // wait-for-never still closes (both nodes parked, both caught
        // up): no false negative.
        name: stuck_check_during_succeeding_evaluation_does_not_close,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let exchanges = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_ex = exchanges.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_ex = a_ex.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // Cursor positioned BEFORE the ping send so B's pong
                    // cannot slip behind the start offset.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "pong"
                    ));
                    // Send ping, then park immediately: A becomes the
                    // parked caught-up half of the false-positive pair
                    // while B is mid-search on the ping.
                    bus.send("ping", serde_json::json!({"v": 1})).expect("a sends ping");
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        a_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let b_ex = exchanges.clone();
            install_body(&pid, "b", std::sync::Arc::new(move |ctx| {
                let b_ex = b_ex.clone();
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("b").expect("register");
                    // The sleep runs inside the search, between B's
                    // record_observed and the message being returned:
                    // exactly the window the parked flag must cover.
                    let mut cursor = bus.cursor().with_filter(|e| {
                        let hit = matches!(
                            &e.kind,
                            weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                        );
                        if hit {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        hit
                    });
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        b_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // The reply MUST succeed: a Closed here is the
                        // false positive this test exists to catch. The
                        // exchange counter (asserted below) carries the
                        // failure; `expect` would also abort the run
                        // loudly at the exact broken send.
                        bus.send("pong", serde_json::json!({"v": 2}))
                            .expect("pong send failed: bus closed under a succeeding evaluation");
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("b: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let outcome = run_test(project, "a").await;
            assert_eq!(
                exchanges.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "ping/pong must complete: the stuck-check must not close \
                 the bus while B's evaluation is mid-search on the ping"
            );
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "post-exchange mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // END-TO-END concurrent waits in ONE node task, through the real
        // WaitGuard/cursor wiring (not the coordinator hooks directly).
        // Node A holds TWO concurrent bus waits at once: a `tokio::select!`
        // over two cursors on its bus (one filtering "ping", one a
        // membership wait). This is the exact shape the per-node single-
        // wait slot got wrong: two WaitGuards under one (node, frames),
        // where the old code clobbered the first wait's state and panicked
        // the worker when the second guard dropped. Here B sends "ping",
        // resolving A's select; A then deadlocks waiting for a name that
        // never comes while B also deadlocks. The run must reach Failed
        // (clean unwind) and NOT panic: if the select's two guards
        // corrupted the liveness map, the worker would abort instead.
        name: concurrent_waits_in_one_task_unwind_cleanly,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let got_ping = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_got = got_ping.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_got = a_got.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // TWO concurrent waits in one task: a cursor for "ping"
                    // and a membership wait for a name that never comes.
                    // select! polls BOTH futures, so both hold a live
                    // WaitGuard under (a, root frames) at once.
                    let mut ping_cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                    ));
                    let never_handle = bus.new_handle();
                    tokio::select! {
                        r = ping_cursor.next() => {
                            if matches!(r, Ok(Some(_))) {
                                a_got.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                        r = never_handle.wait_for("never-x") => {
                            // The bus close can race this; a Closed here is
                            // fine (the select's other branch or the
                            // deadlock won).
                            let _ = r;
                        }
                    }
                    // Now deadlock on a name that never arrives.
                    bus.wait_for("never-y").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
                let mut bus = ctx.bus_from_input("ch")?;
                bus.register("b").expect("register");
                let _ = bus.send("ping", serde_json::json!({"v": 1}));
                bus.wait_for("never-z").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("b: {e}"),
                ))?;
                Ok(())
            })));
            let outcome = run_test(project, "a").await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "concurrent waits then mutual deadlock unwinds to Failed (no worker abort); \
                 got {outcome:?}"
            );
        }
    );

    // ─────────────────────────────────────────────────────────────────
    // Bus + wait-for-input (await_signal). A live bus keeps the worker
    // alive but must be TRANSPARENT to the signal machinery: a node can
    // await_signal while a bus is open; the worker stays alive (bus
    // holds it) and the resume is delivered IN PROCESS the moment the
    // fire's `SuspensionResolved` lands. When no bus holds the worker, a
    // parked await falls through to the normal stall -> die -> respawn
    // path. These tests pin the whole matrix.
    // ─────────────────────────────────────────────────────────────────

    /// Tasks fake for await_signal tests. `enqueue_dedup` of a
    /// RegisterSignal mints a deterministic token (recording it so the
    /// test can inject the matching `SuspensionResolved`) and
    /// `wait_for_terminal` hands back a `RegisterSignalReply { token }`.
    /// Every other task kind is unreachable in these tests.
    struct AwaitTasks {
        // (task_id -> token) so wait_for_terminal returns the same token
        // enqueue minted, and the test can read the token to resolve it.
        tokens: StdMutex<std::collections::HashMap<uuid::Uuid, String>>,
        // The most-recently-minted token, for the test to resolve.
        last_token: StdMutex<Option<String>>,
    }
    impl AwaitTasks {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                tokens: StdMutex::new(std::collections::HashMap::new()),
                last_token: StdMutex::new(None),
            })
        }
        /// Block (test-side) until a token has been minted, then return
        /// it. Buses race the worker; the await may not have registered
        /// the instant the test wants to resolve it.
        async fn await_token(&self) -> String {
            for _ in 0..2000 {
                if let Some(t) = self.last_token.lock().unwrap().clone() {
                    return t;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            panic!("no register_signal token minted within timeout");
        }
    }
    #[async_trait]
    impl weft_task_store::TaskStoreClient for AwaitTasks {
        async fn enqueue_dedup(
            &self,
            t: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            assert_eq!(
                t.kind,
                weft_task_store::TaskKind::RegisterSignal,
                "await tests only enqueue RegisterSignal"
            );
            let id = uuid::Uuid::new_v4();
            // Deterministic token derived from the task id.
            let token = format!("tok-{id}");
            self.tokens.lock().unwrap().insert(id, token.clone());
            *self.last_token.lock().unwrap() = Some(token);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(id))
        }
        async fn wait_for_terminal(
            &self,
            t: uuid::Uuid,
            _to: std::time::Duration,
            _pi: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            let token = self
                .tokens
                .lock()
                .unwrap()
                .get(&t)
                .cloned()
                .expect("token for task id");
            Ok(weft_task_store::tasks::TaskOutcome {
                status: weft_task_store::tasks::TaskStatus::Complete,
                result: Some(serde_json::json!({ "token": token })),
                error: None,
            })
        }
        async fn claim_one(
            &self,
            _p: &str,
            _f: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(&self, _t: uuid::Uuid, _p: &str, _r: Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(&self, _t: uuid::Uuid, _p: &str, _e: String) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Spawn `run_one_execution` with the given tasks fake + journal so a
    /// test can inject a `SuspensionResolved` into the journal while the
    /// worker runs. Every node in `kicked` is seeded as an entry root
    /// (so a bus-holder and an independent await-node can both start).
    /// Returns the join handle and the color.
    fn spawn_run(
        project: ProjectDefinition,
        kicked: &[&str],
        journal: Arc<MemJournal>,
        tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    ) -> (tokio::task::JoinHandle<anyhow::Result<ExecutionOutcome>>, Color) {
        let color = uuid::Uuid::new_v4();
        let entry = kicked[0].to_string();
        let kicked: Vec<String> = kicked.iter().map(|s| s.to_string()).collect();
        let pid = project.id.to_string();
        let j = journal.clone();
        let handle = tokio::spawn(async move {
            j.record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: pid,
                    entry_node: entry,
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
            for node_id in kicked {
                j.record_event(
                    &ExecEvent::NodeKicked {
                        color,
                        node_id,
                        payload: None,
                        at_unix: 0,
                    },
                    None,
                )
                .await
                .unwrap();
            }
            let clients = EngineClients {
                journal: j,
                tasks,
                infra: Arc::new(NoopInfra),
                infra_state: Arc::new(NoopInfraState),
                project: Arc::new(NoopProject),
                clock: Arc::new(weft_platform_traits::clock::SystemClock),
                storage: crate::storage::FakeWorkerStorage::new(),
            };
            run_one_execution(
                Arc::new(project),
                configurable_catalog(),
                color,
                clients,
                "pod".into(),
                "tenant".into(),
                "ns".into(),
                CancellationFlag::new_arc(),
            )
            .await
        });
        (handle, color)
    }

    /// BASELINE (no bus): a lone node awaits. With no bus holding it, the
    /// worker stalls and EXITS (Stalled) so the dispatcher can respawn it
    /// on the fire. This is the unchanged normal path; the bus work must
    /// not have altered it.
    #[tokio::test]
    async fn await_without_bus_stalls_and_exits() {
        let project = bus_topology("waiter", &[], "ch");
        // (no bus consumer wired; the creator just awaits, never touches a bus)
        let pid = project.id.to_string();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    Ok(())
                })
            }),
        );
        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, _color) =
            spawn_run(project, &["waiter"], journal.clone(), tasks.clone());
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Stalled),
            "a bus-less await must stall and exit so a fresh worker resumes on the fire; got {outcome:?}"
        );
    }

    /// IN-FLIGHT RESUME (bus alive): a `creator`+`peer` keep a bus
    /// conversation open (holding the worker alive), while an independent
    /// `waiter` node parks on `await_signal`. The test injects
    /// `SuspensionResolved` mid-flight; the waiter resumes IN PROCESS on
    /// the live worker (no respawn), and once it resumes it tells the
    /// creator to wrap up so the bus closes and the execution completes.
    /// Proves the bus is transparent to the signal: the resume happens on
    /// the running worker exactly as it would without a bus.
    #[tokio::test]
    async fn await_with_live_bus_resumes_in_process() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        // Shared flag: the waiter flips it on resume; the creator polls it
        // on the bus and closes once set. (A plain Arc<AtomicBool> is the
        // cross-node signal; the bus just keeps the worker warm.)
        let resumed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let r_creator = resumed.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumed = r_creator.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // Hold the bus open until the waiter has resumed, then
                    // close so the execution can complete. This keeps the
                    // worker alive across the await + resume.
                    while !resumed.load(std::sync::atomic::Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    // Stay co-alive until the creator closes the bus.
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        let r_waiter = resumed.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumed = r_waiter.clone();
                Box::pin(async move {
                    // Park on a signal WHILE the bus holds the worker.
                    let _ = ctx.await_signal(human_form()).await?;
                    // Resumed in-process: tell the creator to wrap up.
                    resumed.store(true, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // Wait until the waiter's await registered (token minted), give it
        // a beat to reach the suspended state, then write the journal
        // rows the dispatcher would on a real fire: SuspensionRegistered
        // (the fold builds the awaited sequence from THIS) followed by
        // SuspensionResolved carrying the value. Both land while the
        // worker is alive (bus open); the in-loop resume poll picks them
        // up and resumes the waiter in process.
        let token = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color,
                    node_id: "waiter".into(),
                    frames: vec![],
                    token: token.clone(),
                    spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0,
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color,
                    token,
                    value: serde_json::json!({ "answer": 42 }),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: in-flight resume should complete on the live worker")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "the resume must happen in process on the bus-held worker and complete; got {outcome:?}"
        );
        // The journal must show the WAITER resumed (NodeResumed), proving
        // the resume happened on this live worker, not via a respawn.
        let events = journal.events.lock().unwrap().clone();
        assert!(
            events.iter().any(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter")),
            "waiter must have a NodeResumed event (in-process resume); got {events:?}"
        );
    }

    /// TWO awaits on a live bus where the waiter is a KICKED entry node
    /// (no inbound pulses): it parks, resumes, parks again, resumes again,
    /// all in process on the bus-held worker. A kicked node re-fires via
    /// the `dispatched=false` reset (not the pulse un-absorb path), so
    /// this pins the multi-await-on-bus path for entry nodes. The pulse-
    /// fed variant that exercises the `pulses_absorbed` un-absorb across
    /// two resumes is `two_awaits_pulse_fed_waiter_both_resume`.
    #[tokio::test]
    async fn two_awaits_on_live_bus_both_resume_in_process() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        // Counts how many times the waiter has resumed (0 -> 1 -> 2). The
        // creator holds the bus open until BOTH resumes have landed.
        let resumes = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let c_resumes = resumes.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumes = c_resumes.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // Hold the bus open until BOTH awaits have resumed.
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        let w_resumes = resumes.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumes = w_resumes.clone();
                Box::pin(async move {
                    // First park + resume.
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    // Second park + resume on the same live worker.
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // Resolve the FIRST await (call_index 0).
        let token0 = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token0.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token0.clone(),
                    value: serde_json::json!({ "answer": 1 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        // Wait for the SECOND await to register a NEW token (distinct from
        // the first), then resolve it (call_index 1). `await_token` returns
        // the most-recently-minted token, so poll until it changes.
        let token1 = loop {
            let t = tasks.await_token().await;
            if t != token0 {
                break t;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token1.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 1, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token1,
                    value: serde_json::json!({ "answer": 2 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: the SECOND in-place resume must re-fire the waiter")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "both resumes must land in process and the execution complete; got {outcome:?}"
        );
        // Exactly TWO NodeResumed for the waiter: one per await. A single
        // one would mean the second resume never fired (the hang).
        let events = journal.events.lock().unwrap().clone();
        let resumed_count = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter"))
            .count();
        assert_eq!(
            resumed_count, 2,
            "waiter must resume TWICE in process (one NodeResumed per await); got {resumed_count} in {events:?}"
        );
    }

    /// PULSE-FED waiter, two in-place resumes. The waiter has two required
    /// inbound ports (`in1`, `in2`). `feeder1` emits `in1` once, `feeder2`
    /// emits `in2` once before the first await, and `feeder3` emits a second
    /// `in2` pulse after the first resume. Unlike the kicked variant, this
    /// waiter re-fires through the `pulses_absorbed` UN-ABSORB path (it has
    /// inbound edges, so its resume cannot use the `dispatched=false` kick
    /// reset). It pins that the pulse-fed double-resume path completes and
    /// emits exactly two `NodeResumed` events.
    ///
    /// NOTE: this test does NOT pin the `is_resume` `pulses_absorbed`
    /// extension fix (it passes whether that loop is present or reverted).
    /// That extension keeps the live RAM record equal to a journal refold,
    /// but reverting it has no reachable behavioral effect: a re-fire
    /// un-absorbs the ORIGINAL `pulses_absorbed` recorded at `NodeStarted`,
    /// and the first fire already proved every wired port had a pulse there,
    /// so every wired port is always re-satisfied on every resume. A pulse a
    /// resume absorbs on top of that is never the sole satisfier of any
    /// port, so dropping it never starves a re-fire. The full engine suite
    /// passes with that loop reverted; the fix is a defensive RAM==refold
    /// consistency guard, not a fix for a reachable hang.
    #[tokio::test]
    async fn two_awaits_pulse_fed_waiter_both_resume() {
        let project = bus_plus_pulse_fed_waiter_topology();
        let pid = project.id.to_string();
        // 0 -> first await parked, 1 -> first resume done, 2 -> second
        // resume done. feeder2 uses it to time its second emit.
        let resumes = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let c_resumes = resumes.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumes = c_resumes.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        // feeder1: one pulse on in1, then done.
        install_body(
            &pid,
            "feeder1",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    ctx.pulse_downstream(NodeOutput::with("out", json!("a"))).await?;
                    Ok(())
                })
            }),
        );
        // feeder2: the ORIGINAL in2 pulse (before the first await).
        install_body(
            &pid,
            "feeder2",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    ctx.pulse_downstream(NodeOutput::with("out", json!("b1"))).await?;
                    Ok(())
                })
            }),
        );
        // feeder3: a SECOND in2 pulse, emitted only AFTER the first resume
        // (resumes >= 1). A port may be emitted at most once per firing, so
        // this must be a distinct node, not a re-emit by feeder2. This pulse
        // is the one the first resume dispatch absorbs; if the fix does not
        // record it into the waiter's live `pulses_absorbed`, the second
        // resume cannot re-satisfy `in2`.
        let f3_resumes = resumes.clone();
        install_body(
            &pid,
            "feeder3",
            std::sync::Arc::new(move |ctx| {
                let resumes = f3_resumes.clone();
                Box::pin(async move {
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 1 {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    ctx.pulse_downstream(NodeOutput::with("out", json!("b2"))).await?;
                    Ok(())
                })
            }),
        );
        let w_resumes = resumes.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumes = w_resumes.clone();
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        // The waiter is pulse-fed, so it is NOT kicked: the feeders are the
        // kicked roots that produce its input pulses.
        let (handle, color) = spawn_run(
            project,
            &["creator", "feeder1", "feeder2", "feeder3"],
            journal.clone(),
            tasks.clone(),
        );

        // Resolve the FIRST await (call_index 0).
        let token0 = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token0.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token0.clone(),
                    value: serde_json::json!({ "answer": 1 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        // Resolve the SECOND await (call_index 1) once its token appears.
        let token1 = loop {
            let t = tasks.await_token().await;
            if t != token0 {
                break t;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token1.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 1, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token1,
                    value: serde_json::json!({ "answer": 2 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: the SECOND in-place resume must re-fire the pulse-fed waiter")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "both resumes must land in process and the execution complete; got {outcome:?}"
        );
        let events = journal.events.lock().unwrap().clone();
        let resumed_count = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter"))
            .count();
        assert_eq!(
            resumed_count, 2,
            "pulse-fed waiter must resume TWICE in process; got {resumed_count} in {events:?}"
        );
    }

    /// BUS CLOSES BEFORE THE FIRE: the `creator`+`peer` bus conversation
    /// ends (bus closes) while the independent `waiter` is parked on
    /// `await_signal` and the fire has NOT arrived. Once the bus is gone
    /// nothing holds the worker, so the await must fall through to the
    /// normal stall -> exit path (the dispatcher respawns on the eventual
    /// fire). No `SuspensionResolved` is injected, proving the worker
    /// exits rather than waiting forever on the dead bus.
    #[tokio::test]
    async fn bus_closes_before_fire_then_worker_exits_normally() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // The conversation is over: close the bus. After this,
                    // nothing holds the worker, so the parked waiter must
                    // stall+exit (not hang on the dead bus).
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    // Joined; exit immediately. The creator closes the bus.
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, _color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // No SuspensionResolved is injected. The worker must exit Stalled
        // (bus closed, nothing holds it, await unresolved) rather than
        // hang.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: a closed bus must not hold the worker for an unresolved await")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Stalled),
            "once the bus closed, the unresolved await must stall+exit for a respawn; got {outcome:?}"
        );
    }

    /// Two nodes: `producer` emits a value on `out` then RETURNS
    /// IMMEDIATELY (no work between the emit and the return); `consumer`
    /// has a REQUIRED input wired to it and emits nothing. The producer's
    /// emission and its terminal are sent back-to-back, so this is the
    /// exact shape that, with two separate task channels, raced: the
    /// terminal could be observed before the emission, the `out` port
    /// closed as "unmentioned", and the consumer SKIPPED (then re-
    /// dispatched). With one ordered task channel the emission always
    /// precedes the terminal, so the consumer must NEVER be skipped and
    /// must receive the value. Looped many times to surface any residual
    /// scheduling race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn emit_then_immediately_return_never_skips_the_consumer() {
        fn producer_consumer_project() -> ProjectDefinition {
            serde_json::from_value(json!({
                "id": uuid::Uuid::new_v4(), "name": "emit-return", "description": null,
                "nodes": [
                    {
                        "id": "producer", "nodeType": "Configurable", "label": null, "config": null,
                        "position": { "x": 0.0, "y": 0.0 },
                        "inputs": [],
                        "outputs": [{ "name": "out", "portType": "String", "required": false }],
                        "features": {}, "scope": [], "groupBoundary": null,
                        "requiresInfra": false, "images": []
                    },
                    {
                        "id": "consumer", "nodeType": "Configurable", "label": null, "config": null,
                        "position": { "x": 1.0, "y": 0.0 },
                        "inputs": [{ "name": "in", "portType": "String", "required": true }],
                        "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                        "requiresInfra": false, "images": []
                    }
                ],
                "edges": [{
                    "id": "e0", "source": "producer", "target": "consumer",
                    "sourceHandle": "out", "targetHandle": "in"
                }],
                "groups": []
            }))
            .expect("project")
        }

        for i in 0..100 {
            let project = producer_consumer_project();
            let pid = project.id.to_string();
            install_body(
                &pid,
                "producer",
                std::sync::Arc::new(|ctx| {
                    Box::pin(async move {
                        // Emit then return with nothing in between: the
                        // emission and the terminal are sent back-to-back.
                        ctx.pulse_downstream(NodeOutput::with("out", json!("payload"))).await?;
                        Ok(())
                    })
                }),
            );
            install_body(
                &pid,
                "consumer",
                std::sync::Arc::new(|_ctx| Box::pin(async move { Ok(()) })),
            );

            let journal = Arc::new(MemJournal::default());
            let tasks = AwaitTasks::new();
            let (handle, color) = spawn_run(project, &["producer"], journal.clone(), tasks);
            tokio::time::timeout(std::time::Duration::from_secs(10), handle)
                .await
                .expect("must not hang")
                .expect("join")
                .expect("run ok");

            let events = journal.events_for_color(color).await.unwrap();
            // The consumer must NEVER be skipped: its required `in` arrived
            // as a real value, not a closure.
            let consumer_skipped = events.iter().any(|e| {
                matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == "consumer")
            });
            assert!(!consumer_skipped, "iter {i}: consumer was skipped (its emitted input was wrongly closed)");
            // And the consumer's firing must have seen the value on `in`.
            let consumer_got_value = events.iter().any(|e| match e {
                ExecEvent::NodeStarted { node_id, input, .. } if node_id == "consumer" => {
                    input.get("in").and_then(|v| v.as_str()) == Some("payload")
                }
                _ => false,
            });
            assert!(consumer_got_value, "iter {i}: consumer never received the producer's value on `in`");
        }
    }

}

// ─── Layer 3: LoopRuntime integration rig tests ─────────────────────────────
//
// These tests exercise the engine's loop boundary handlers
// (`handle_loop_boundary_firing`, `launch_iteration`, `emit_loop_outward`,
// `cancel_loop_instances`) against synthetic ProjectDefinitions. They
// confirm the integration points the unit tests on `LoopRuntime` alone
// can't reach: per-iteration pulse emission shapes, gather/carry
// assembly at outward emit, frame-stack keying, and cancellation
// closure emission.
#[cfg(test)]
mod loop_rig_tests {
    use super::*;
    use async_trait::async_trait;
    use crate::loop_runtime::LoopRuntime;
    use std::sync::Mutex as StdMutex;
    use weft_core::frames::LoopIteration;
    use weft_core::exec::ready::ReadyGroup;
    use weft_core::primitive::LoopInstanceKey;
    use weft_core::project::{
        Edge, GroupBoundary, GroupBoundaryRole, NodeDefinition, PortDefinition, Position,
        ProjectDefinition,
    };
    use weft_core::pulse::PulseTable;
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    use weft_journal::ExecEvent;

    fn empty_project_dt() -> serde_json::Value {
        serde_json::json!("1970-01-01T00:00:00Z")
    }
    fn parse_dt() -> serde_json::Value {
        empty_project_dt()
    }

    #[derive(Default)]
    struct CapturingJournal {
        events: StdMutex<Vec<ExecEvent>>,
    }
    #[async_trait]
    impl JournalClient for CapturingJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, _color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(self.events.lock().unwrap().clone())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn primitive(p: WeftPrimitive) -> WeftType {
        WeftType::primitive(p)
    }

    fn list_of(inner: WeftType) -> WeftType {
        WeftType::List(Box::new(inner))
    }

    fn list_of_nullable(inner: WeftType) -> WeftType {
        list_of(WeftType::Union(vec![inner, primitive(WeftPrimitive::Null)]))
    }

    /// Build a minimal Loop-shaped project with one LoopIn, one LoopOut,
    /// one body node, one outward consumer node. Returns the project and
    /// the relevant ids.
    struct LoopProject {
        project: ProjectDefinition,
        loop_in_id: String,
        loop_out_id: String,
        body_id: String,
        consumer_id: String,
        group_id: String,
    }

    fn build_parallel_map_project() -> LoopProject {
        // Layout:
        //   producer.items: List[String]
        //   -> loop__in (outer-in: items)
        //   loop__in.items (inside-out, T) -> body.in
        //   body.out -> loop__out.results (inside-in T?)
        //   loop__out.results (outer-out List[String | Null]) -> consumer.data
        let group_id = "myloop".to_string();
        let loop_in_id = format!("{group_id}__in");
        let loop_out_id = format!("{group_id}__out");
        let body_id = "body".to_string();
        let consumer_id = "consumer".to_string();

        // LoopIn boundary node. Loop config lives in `config`.
        let loop_in_cfg = serde_json::json!({
            "parentId": group_id,
            "parallel": true,
            "over": ["items"],
            "carry": [],
        });
        let loop_in = NodeDefinition {
            id: loop_in_id.clone(),
            node_type: "LoopIn".into(),
            label: None,
            config: loop_in_cfg.clone(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::In }),
            inputs: vec![PortDefinition {
                name: "items".into(),
                port_type: list_of(primitive(WeftPrimitive::String)),
                required: true,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            }],
            outputs: vec![
                PortDefinition {
                    name: "items".into(),
                    port_type: primitive(WeftPrimitive::String),
                    required: false,
                    description: None,
                    configurable: false,
                    synthesized_from_carry: false,
                },
                PortDefinition {
                    name: "index".into(),
                    port_type: primitive(WeftPrimitive::Number),
                    required: false,
                    description: None,
                    configurable: false,
                    synthesized_from_carry: false,
                },
            ],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        // LoopOut carries only the parent pointer: loop config
        // (parallel/over/carry/...) is authoritative on LoopIn and a
        // duplicated copy here would create two sources of truth.
        // Matches what the compiler now emits.
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(),
            node_type: "LoopOut".into(),
            label: None,
            config: loop_out_cfg,
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::Out }),
            inputs: vec![
                PortDefinition {
                    name: "results".into(),
                    port_type: primitive(WeftPrimitive::String),
                    required: false,
                    description: None,
                    configurable: false,
                    synthesized_from_carry: false,
                },
                PortDefinition {
                    name: "done".into(),
                    port_type: primitive(WeftPrimitive::Boolean),
                    required: false,
                    description: None,
                    configurable: false,
                    synthesized_from_carry: false,
                },
            ],
            outputs: vec![PortDefinition {
                name: "results".into(),
                port_type: list_of_nullable(primitive(WeftPrimitive::String)),
                required: false,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            }],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        // Body node: simple Echo with one input port `in: String` and
        // one output `out: String`.
        let body = NodeDefinition {
            id: body_id.clone(),
            node_type: "Echo".into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![group_id.clone()],
            group_boundary: None,
            inputs: vec![PortDefinition {
                name: "in".into(),
                port_type: primitive(WeftPrimitive::String),
                required: true,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            }],
            outputs: vec![PortDefinition {
                name: "out".into(),
                port_type: primitive(WeftPrimitive::String),
                required: false,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            }],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        let consumer = NodeDefinition {
            id: consumer_id.clone(),
            node_type: "Sink".into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: vec![PortDefinition {
                name: "data".into(),
                port_type: list_of_nullable(primitive(WeftPrimitive::String)),
                required: true,
                description: None,
                configurable: false,
                synthesized_from_carry: false,
            }],
            outputs: vec![],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        let edges = vec![
            Edge {
                id: "e1".into(),
                source: loop_in_id.clone(),
                source_handle: Some("items".into()),
                target: body_id.clone(),
                target_handle: Some("in".into()),
                span: None,
            },
            Edge {
                id: "e2".into(),
                source: body_id.clone(),
                source_handle: Some("out".into()),
                target: loop_out_id.clone(),
                target_handle: Some("results".into()),
                span: None,
            },
            Edge {
                id: "e3".into(),
                source: loop_out_id.clone(),
                source_handle: Some("results".into()),
                target: consumer_id.clone(),
                target_handle: Some("data".into()),
                span: None,
            },
        ];

        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [],
            "createdAt": parse_dt(),
            "updatedAt": parse_dt(),
        });
        let project: ProjectDefinition = serde_json::from_value(project_json).expect("project deserialize");

        LoopProject {
            project,
            loop_in_id,
            loop_out_id,
            body_id,
            consumer_id,
            group_id,
        }
    }

    /// Helper: fire LoopIn with the given outer input bag at parent_frames=[].
    async fn fire_loop_in(
        lp: &LoopProject,
        outer_input: serde_json::Value,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) {
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        let loop_in = lp.project.nodes.iter().find(|n| n.id == lp.loop_in_id).unwrap();
        let group = ReadyGroup {
            frames: Vec::new(),
            color: uuid::Uuid::nil(),
            input: outer_input,
            closed_ports: Vec::new(),
            should_skip: false,
            pulse_ids: Vec::new(),
            error: None,
        };
        handle_loop_boundary_firing(loop_in, &group, &lp.project, &edge_idx, pulses, journal, "test-pod", rt)
            .await
            .expect("LoopIn firing");
    }

    /// Helper: fire LoopOut for iteration `i` with the given writes.
    async fn fire_loop_out(
        lp: &LoopProject,
        iter: u32,
        writes: serde_json::Value,
        closed_ports: Vec<String>,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) {
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        let loop_out = lp.project.nodes.iter().find(|n| n.id == lp.loop_out_id).unwrap();
        let group = ReadyGroup {
            frames: vec![LoopIteration { index: iter }],
            color: uuid::Uuid::nil(),
            input: writes,
            closed_ports,
            should_skip: false,
            pulse_ids: Vec::new(),
            error: None,
        };
        handle_loop_boundary_firing(loop_out, &group, &lp.project, &edge_idx, pulses, journal, "test-pod", rt)
            .await
            .expect("LoopOut firing");
    }

    /// Layer-3 rig 1: parallel-map LoopIn fires per-iteration body pulses
    /// at distinct frame stacks. Three elements -> three body pulses, one
    /// per iteration's body frame stack.
    #[tokio::test]
    async fn parallel_loop_in_emits_per_iteration_body_pulses() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let body = pulses.get(&lp.body_id).expect("body bucket");
        let on_in: Vec<_> = body
            .iter()
            .filter(|p| p.target_port == "in" && !p.closed)
            .collect();
        assert_eq!(on_in.len(), 3, "three body pulses, one per iteration");
        let frames: Vec<u32> = on_in.iter().map(|p| p.frames[0].index).collect();
        let mut sorted = frames.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2], "iterations 0..3 fired: {:?}", frames);
        let values: Vec<&str> = on_in
            .iter()
            .map(|p| p.value.as_str().unwrap_or(""))
            .collect();
        assert!(values.contains(&"a") && values.contains(&"b") && values.contains(&"c"),
            "all elements distributed: {:?}", values);
    }

    /// Layer-3 rig 2: LoopRuntime records instantiation + per-iteration
    /// launch events on the journal.
    #[tokio::test]
    async fn parallel_loop_in_journal_records_instantiation_and_launches() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let instantiations: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopInstantiated { .. })).collect();
        let launches: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopIterationLaunched { .. })).collect();
        assert_eq!(instantiations.len(), 1, "one LoopInstantiated event");
        assert_eq!(launches.len(), 2, "one LoopIterationLaunched per iteration");
    }

    /// Layer-3 rig 3: LoopOut firings collect gather writes per iteration
    /// and emit the assembled List[T | Null] outwardly when all iterations
    /// have fired.
    #[tokio::test]
    async fn parallel_loop_out_assembles_and_emits_outward() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Fire LoopOut for each iteration with a real gather write.
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // The outward consumer should now have one pulse on `data` at
        // parent_frames=[] carrying ["A","B","C"].
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data.len(), 1, "one outward pulse on consumer.data");
        assert!(data[0].frames.is_empty(), "outward emit at parent_frames=[]");
        assert_eq!(data[0].value, serde_json::json!(["A", "B", "C"]),
            "assembled in iteration-index order: {:?}", data[0].value);
        let events = journal.events.lock().unwrap();
        let terminated: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).collect();
        assert_eq!(terminated.len(), 1, "one LoopTerminated event");
    }

    /// Crash-resume: only iteration 0's `LoopIterationLaunched` row
    /// survived the crash. The re-fired LoopIn must launch exactly the
    /// MISSING iterations (1, 2): deriving launches from
    /// `first_instantiation` instead of the rehydrated `launched` set
    /// silently launched nothing (whole loop skipped), and launching
    /// all three would duplicate iteration 0's journaled body pulses.
    #[tokio::test]
    async fn crash_resumed_loop_in_launches_only_missing_iterations() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        let bag = serde_json::json!({"items": ["a", "b", "c"]});
        fire_loop_in(&lp, bag.clone(), &mut rt, &mut pulses, &journal).await;
        // Keep LoopInstantiated + the FIRST launch row only.
        let mut kept = 0;
        let events: Vec<ExecEvent> = journal
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| match e {
                ExecEvent::LoopIterationLaunched { .. } => {
                    kept += 1;
                    kept <= 1
                }
                _ => true,
            })
            .cloned()
            .collect();
        let snap = weft_journal::fold_to_snapshot(uuid::Uuid::nil(), &events);
        let mut rt2 = rehydrate_loop_runtime(&lp.project, &snap.loop_instances).expect("rehydrate");
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        let body = pulses2.get(&lp.body_id).expect("body bucket");
        let mut frames: Vec<u32> = body
            .iter()
            .filter(|p| p.target_port == "in" && !p.closed)
            .map(|p| p.frames[0].index)
            .collect();
        frames.sort();
        assert_eq!(frames, vec![1, 2], "only the missing iterations relaunch");
        let events2 = journal2.events.lock().unwrap();
        let launches = events2.iter().filter(|e| matches!(e, ExecEvent::LoopIterationLaunched { .. })).count();
        assert_eq!(launches, 2, "one launch row per relaunched iteration");
        let instantiations = events2.iter().filter(|e| matches!(e, ExecEvent::LoopInstantiated { .. })).count();
        assert_eq!(instantiations, 0, "rehydrated instance must not re-journal LoopInstantiated");
    }

    /// Crash-resume replay of a zero-iteration LoopIn AFTER its
    /// `LoopTerminated` row landed: the outward emit must not run
    /// again (duplicate empty-list pulses would re-fire downstream).
    #[tokio::test]
    async fn replayed_zero_iter_loop_in_does_not_duplicate_outward() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        let bag = serde_json::json!({"items": []});
        fire_loop_in(&lp, bag.clone(), &mut rt, &mut pulses, &journal).await;
        assert_eq!(
            pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            1,
            "zero-iter loop emits one outward pulse"
        );
        let events: Vec<ExecEvent> = journal.events.lock().unwrap().clone();
        let snap = weft_journal::fold_to_snapshot(uuid::Uuid::nil(), &events);
        let mut rt2 = rehydrate_loop_runtime(&lp.project, &snap.loop_instances).expect("rehydrate");
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        assert_eq!(
            pulses2.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            0,
            "replay on a terminated instance emits nothing"
        );
        let events2 = journal2.events.lock().unwrap();
        let terminated = events2.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).count();
        assert_eq!(terminated, 0, "no duplicate LoopTerminated row");
    }

    /// Crash-resume replay of a LoopOut firing the runtime refuses
    /// (post-termination / already fired) must journal NOTHING: the
    /// fold applies `LoopOutFired` unconditionally, so a row for a
    /// refused firing diverges the rehydrated instance from the live
    /// one.
    #[tokio::test]
    async fn replayed_loop_out_journals_no_duplicate_rows() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(&lp, serde_json::json!({"items": ["a", "b"]}), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let count_rows = |j: &CapturingJournal| {
            let evs = j.events.lock().unwrap();
            (
                evs.iter().filter(|e| matches!(e, ExecEvent::LoopOutFired { .. })).count(),
                evs.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).count(),
            )
        };
        assert_eq!(count_rows(&journal), (2, 1));
        let outward_before = pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0);
        // Replay LoopOut@1 (crash between its journal row and its
        // NodeCompleted): runtime refuses, journal must not grow.
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        assert_eq!(count_rows(&journal), (2, 1), "no duplicate LoopOutFired / LoopTerminated rows");
        assert_eq!(
            pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            outward_before,
            "no duplicate outward pulses"
        );
    }

    /// Layer-3 rig 4: gather-port closure at LoopOut produces `null` in
    /// the assembled outward list at that iteration's slot.
    #[tokio::test]
    async fn closure_on_gather_port_becomes_null_at_index() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Iteration 1: body failed to write `results` (port closed).
        fire_loop_out(&lp, 1, serde_json::json!({}), vec!["results".into()], &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data[0].value, serde_json::json!(["A", null, "C"]),
            "closed iteration becomes null at its index: {:?}", data[0].value);
    }

    /// Layer-3 rig 5: zero-iteration loop (empty `over`) terminates
    /// immediately and emits an empty list outwardly.
    #[tokio::test]
    async fn zero_iteration_loop_terminates_with_empty_list() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": []}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // No body pulses, no iterations.
        let body = pulses.get(&lp.body_id);
        assert!(body.map(|b| b.is_empty()).unwrap_or(true), "no body work for zero iterations");
        // But still, the loop emits outwardly with an empty assembly.
        let events = journal.events.lock().unwrap();
        assert!(events.iter().any(|e| matches!(e, ExecEvent::LoopTerminated { .. })),
            "zero-iteration loop terminates outwardly");
    }

    /// Termination reason on the zero-iteration shortcut. When the
    /// user writes `max_iters: 0`, the binding constraint is
    /// MaxItersReached, not OverExhausted. The live (non-zero) path's
    /// reason logic must hold for the zero-iter shortcut too;
    /// hardcoding `OverExhausted` (the old shape) lies to the
    /// inspector about which knob bound the loop.
    #[tokio::test]
    async fn zero_iteration_with_max_iters_zero_reports_max_iters_reason() {
        use weft_core::primitive::LoopTerminationReason;
        let mut lp = build_parallel_map_project();
        for n in lp.project.nodes.iter_mut() {
            if matches!(n.node_type.as_str(), "LoopIn" | "LoopOut") {
                if let Some(obj) = n.config.as_object_mut() {
                    obj.insert("max_iters".into(), serde_json::json!(0));
                }
            }
        }
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let term = events.iter().find_map(|e| match e {
            ExecEvent::LoopTerminated { reason, .. } => Some(*reason),
            _ => None,
        }).expect("loop terminated");
        assert_eq!(
            term, LoopTerminationReason::MaxItersReached,
            "max_iters=0 is the binding constraint, not over-exhausted"
        );
    }

    /// Layer-3 rig 6: cancellation marks every live LoopInstance for the
    /// color as cancelled AND emits closures on the LoopOut's outward
    /// output ports at parent_frames.
    #[tokio::test]
    async fn cancel_loop_instances_emits_outward_closures() {
        use weft_core::primitive::LoopTerminationReason;
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Mid-flight: no LoopOut firings happened. Cancel.
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        cancel_loop_instances(
            &mut rt,
            uuid::Uuid::nil(),
            &lp.project,
            &edge_idx,
            &mut pulses,
            &journal,
            "test-pod",
        )
        .await;
        // Instance is now terminated::Cancelled.
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            color: uuid::Uuid::nil(),
        };
        let inst = rt.get(&key).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::Cancelled));
        // Consumer received a closure on `data` at parent_frames=[].
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let closures: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && p.closed).collect();
        assert_eq!(closures.len(), 1, "one outward closure on consumer.data");
        assert!(closures[0].frames.is_empty(), "closure at parent_frames=[]");
    }

    /// Layer-3 rig 7: `max_iters` caps the launched iteration count even
    /// when `over` is longer.
    #[tokio::test]
    async fn max_iters_caps_launched_count() {
        // Rebuild project with max_iters=2 on the loop config.
        let mut lp = build_parallel_map_project();
        for n in lp.project.nodes.iter_mut() {
            if matches!(n.node_type.as_str(), "LoopIn" | "LoopOut") {
                if let Some(obj) = n.config.as_object_mut() {
                    obj.insert("max_iters".into(), serde_json::json!(2));
                }
            }
        }
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c", "d", "e"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let body = pulses.get(&lp.body_id).expect("body bucket");
        let on_in: Vec<_> = body.iter().filter(|p| p.target_port == "in" && !p.closed).collect();
        assert_eq!(on_in.len(), 2, "max_iters=2 caps body firings to 2: got {}", on_in.len());
    }

    /// Layer-3 rig 8: parallel ordering preservation. Fire LoopOut events
    /// out of order; the assembled outward list still matches input order.
    #[tokio::test]
    async fn parallel_ordering_preserved_regardless_of_loop_out_firing_order() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Fire out of order: 2, 0, 1.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data[0].value, serde_json::json!(["A", "B", "C"]),
            "BTreeMap-driven assembly preserves input order: {:?}", data[0].value);
    }

    /// Layer-3 rig 9: compute_loop_iter_count zip-trim behavior with two
    /// `over` ports of different lengths.
    #[test]
    fn compute_iter_count_trims_to_shortest_with_trim_on() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: true,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30]})
        ).unwrap();
        let count = compute_loop_iter_count(&cfg, &input).expect("ok");
        assert_eq!(count, 3, "trims to shortest: {count}");
    }

    /// Layer-3 rig 10: compute_loop_iter_count panics loud with
    /// trim_on_mismatch=false and unequal lengths.
    #[test]
    fn compute_iter_count_rejects_mismatch_with_trim_off() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: false,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3], "b": [10, 20]})
        ).unwrap();
        let err = compute_loop_iter_count(&cfg, &input).expect_err("must err on mismatch");
        assert!(err.contains("mismatch"), "loud mismatch error: {err}");
    }

    /// Layer-3 rig 11: max_iters cap applies in compute_iter_count.
    #[test]
    fn compute_iter_count_caps_at_max_iters() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into()],
            carry: vec![],
            max_iters: Some(2),
            trim_on_mismatch: true,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3, 4, 5]})
        ).unwrap();
        let count = compute_loop_iter_count(&cfg, &input).expect("ok");
        assert_eq!(count, 2, "max_iters caps: {count}");
    }

    /// Layer-3 rig 12: `self.index` pulse arrives at each iteration's
    /// frame stack with the correct index value.
    #[tokio::test]
    async fn implicit_index_pulse_at_each_iteration_frame() {
        // Wire the index port into a body input on the body node by
        // editing the project. Simpler: just check the pulse at LoopIn's
        // `index` output reaches downstream nodes wired to it. The body
        // node's `in` port is wired to `items`, not `index`, so `index`
        // pulses won't reach `body.in`. Instead, scan all pulses for a
        // PulseEmitted on source_port=index. But this fires through
        // postprocess; pulses with no consumer edge are silently dropped.
        // Easier path: check the journal's LoopIterationLaunched events
        // line up with the iteration count.
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let mut indices: Vec<u32> = events.iter().filter_map(|e| match e {
            ExecEvent::LoopIterationLaunched { index, .. } => Some(*index),
            _ => None,
        }).collect();
        indices.sort();
        assert_eq!(indices, vec![0, 1, 2], "each iteration launched with its index: {:?}", indices);
    }

    /// Layer-3 rig 13: nested loops produce distinct LoopInstance entries
    /// keyed by parent_frames.
    #[tokio::test]
    async fn nested_loops_have_distinct_instance_keys() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Simulate: outer LoopIn instantiates at parent_frames=[].
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["x", "y"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Now imagine an inner loop instance keyed at parent_frames=[{0}]
        // (one inner instance per outer iteration). The runtime keys by
        // (group_id, parent_frames, color); two distinct parent_frames
        // mean two distinct instances even for the same group_id.
        let key_outer = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            color: uuid::Uuid::nil(),
        };
        let key_inner_iter0 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 0 }],
            color: uuid::Uuid::nil(),
        };
        let key_inner_iter1 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 1 }],
            color: uuid::Uuid::nil(),
        };
        rt.ensure(key_inner_iter0.clone(), crate::loop_runtime::LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, 1, vec![]);
        rt.ensure(key_inner_iter1.clone(), crate::loop_runtime::LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, 1, vec![]);
        assert!(rt.get(&key_outer).is_some(), "outer instance lives");
        assert!(rt.get(&key_inner_iter0).is_some(), "inner instance at outer iter 0 lives");
        assert!(rt.get(&key_inner_iter1).is_some(), "inner instance at outer iter 1 lives");
        // Distinct: cancelling one does not affect the other.
        rt.cancel_inside(&vec![LoopIteration { index: 0 }], uuid::Uuid::nil());
        use weft_core::primitive::LoopTerminationReason;
        assert_eq!(rt.get(&key_inner_iter0).unwrap().terminated, Some(LoopTerminationReason::Cancelled),
            "iter 0's inner instance cancelled");
        assert!(rt.get(&key_inner_iter1).unwrap().terminated.is_none(),
            "iter 1's inner instance untouched");
    }

    /// Sequential-fold project: `over: ["items"]`, `carry: ["acc"]`.
    /// LoopOut has a `results` gather output (List[String | Null]) AND
    /// an `acc` carry output (String). Body wires `self.items` to a
    /// concat node and writes both `self.results` and `self.acc`.
    fn build_sequential_fold_project() -> LoopProject {
        let group_id = "fold".to_string();
        let loop_in_id = format!("{group_id}__in");
        let loop_out_id = format!("{group_id}__out");
        let body_id = "body".to_string();
        let consumer_id = "consumer".to_string();
        let loop_cfg = serde_json::json!({
            "parentId": group_id,
            "parallel": false,
            "over": ["items"],
            "carry": ["acc"],
        });
        let loop_in = NodeDefinition {
            id: loop_in_id.clone(), node_type: "LoopIn".into(), label: None,
            config: loop_cfg.clone(), position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::In }),
            inputs: vec![
                PortDefinition { name: "items".into(), port_type: list_of(primitive(WeftPrimitive::String)), required: true, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String),         required: false, description: None, configurable: false, synthesized_from_carry: false },
            ],
            outputs: vec![
                PortDefinition { name: "items".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String), required: false, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "index".into(), port_type: primitive(WeftPrimitive::Number), required: false, description: None, configurable: false, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        // LoopOut carries only `{"parentId": ...}` (matches compiler).
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(), node_type: "LoopOut".into(), label: None,
            config: loop_out_cfg, position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::Out }),
            inputs: vec![
                PortDefinition { name: "results".into(), port_type: primitive(WeftPrimitive::String),  required: false, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),  required: false, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "done".into(),    port_type: primitive(WeftPrimitive::Boolean), required: false, description: None, configurable: false, synthesized_from_carry: false },
            ],
            outputs: vec![
                PortDefinition { name: "results".into(), port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: false, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),                   required: false, description: None, configurable: false, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let body = NodeDefinition {
            id: body_id.clone(), node_type: "Concat".into(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![group_id.clone()], group_boundary: None,
            inputs: vec![
                PortDefinition { name: "left".into(),  port_type: primitive(WeftPrimitive::String), required: true, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "right".into(), port_type: primitive(WeftPrimitive::String), required: true, description: None, configurable: false, synthesized_from_carry: false },
            ],
            outputs: vec![
                PortDefinition { name: "out".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, configurable: false, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let consumer = NodeDefinition {
            id: consumer_id.clone(), node_type: "Sink".into(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 }, scope: vec![], group_boundary: None,
            inputs: vec![
                PortDefinition { name: "data".into(),  port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: true, description: None, configurable: false, synthesized_from_carry: false },
                PortDefinition { name: "final".into(), port_type: primitive(WeftPrimitive::String),                    required: true, description: None, configurable: false, synthesized_from_carry: false },
            ],
            outputs: vec![], features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let edges = vec![
            // body reads element + carry from LoopIn.
            Edge { id: "e1".into(), source: loop_in_id.clone(),  source_handle: Some("items".into()), target: body_id.clone(),     target_handle: Some("right".into()), span: None },
            Edge { id: "e2".into(), source: loop_in_id.clone(),  source_handle: Some("acc".into()),   target: body_id.clone(),     target_handle: Some("left".into()),  span: None },
            // body writes back to LoopOut on both results and acc.
            Edge { id: "e3".into(), source: body_id.clone(),     source_handle: Some("out".into()),   target: loop_out_id.clone(), target_handle: Some("results".into()), span: None },
            Edge { id: "e4".into(), source: body_id.clone(),     source_handle: Some("out".into()),   target: loop_out_id.clone(), target_handle: Some("acc".into()),     span: None },
            // outward to consumer.
            Edge { id: "e5".into(), source: loop_out_id.clone(), source_handle: Some("results".into()), target: consumer_id.clone(), target_handle: Some("data".into()),  span: None },
            Edge { id: "e6".into(), source: loop_out_id.clone(), source_handle: Some("acc".into()),     target: consumer_id.clone(), target_handle: Some("final".into()), span: None },
        ];
        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [], "createdAt": parse_dt(), "updatedAt": parse_dt(),
        });
        let project: ProjectDefinition = serde_json::from_value(project_json).expect("project");
        LoopProject { project, loop_in_id, loop_out_id, body_id, consumer_id, group_id }
    }

    /// Sequential mode launches iteration 0 on LoopIn fire, then each
    /// LoopOut fire either launches the next iteration or emits outward.
    /// This test pins the regression where LoopIn's input bag had been
    /// absorbed before iteration 1's launch, leaving the next iteration
    /// with no `items` / `acc` to read.
    #[tokio::test]
    async fn sequential_fold_threads_carry_across_iterations() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Fold "a" + "b" + "c" with initial acc = "".
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"], "acc": ""}),
            &mut rt, &mut pulses, &journal,
        ).await;

        // After LoopIn: iteration 0's body bucket has `items=a`, `acc=""`.
        fn iter_ports<'a>(
            pulses: &'a PulseTable, body_id: &str, idx: u32,
        ) -> std::collections::HashMap<String, serde_json::Value> {
            pulses.get(body_id)
                .map(|b| b.iter()
                    .filter(|p| p.frames.len() == 1 && p.frames[0].index == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), p.value.clone()))
                    .collect())
                .unwrap_or_default()
        }
        let by_port = iter_ports(&pulses, &lp.body_id, 0);
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("a")));
        assert_eq!(by_port.get("left"),  Some(&serde_json::json!("")));

        // Body for iteration 0 writes "a" on `results` AND `acc`.
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a"}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        // Iteration 1 must have been launched at frame=[{1}] with the
        // outer items still flowing through AND the updated carry. This
        // is the regression check: before the fix, the LoopIn's input
        // bag was gone and iter 1's body bucket would be empty.
        let by_port = iter_ports(&pulses, &lp.body_id, 1);
        assert!(!by_port.is_empty(),
            "sequential iteration 1 must launch body pulses (regression: outer input gone)");
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("b")),
            "iter 1 sees element 'b': {:?}", by_port);
        assert_eq!(by_port.get("left"), Some(&serde_json::json!("a")),
            "iter 1 sees carry='a' from iter 0: {:?}", by_port);

        // Iteration 1 body writes "ab".
        fire_loop_out(&lp, 1, serde_json::json!({"results": "ab", "acc": "ab"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Iteration 2 launched with carry='ab', element='c'.
        let by_port = iter_ports(&pulses, &lp.body_id, 2);
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("c")));
        assert_eq!(by_port.get("left"),  Some(&serde_json::json!("ab")));

        // Iteration 2 body writes "abc". This is the last over element,
        // so the loop should emit outward with the assembled list and
        // the final carry.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "abc", "acc": "abc"}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        let final_carry: Vec<_> = consumer.iter().filter(|p| p.target_port == "final" && !p.closed).collect();
        assert_eq!(data.len(), 1, "one outward pulse on consumer.data: {} pulses found", data.len());
        assert_eq!(data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list assembled in iteration order: {:?}", data[0].value);
        assert_eq!(final_carry.len(), 1, "one outward pulse on consumer.final");
        assert_eq!(final_carry[0].value, serde_json::json!("abc"),
            "final carry value is the last successful write: {:?}", final_carry[0].value);

        // The instance is gone in the runtime perspective: terminated.
        use weft_core::primitive::LoopTerminationReason;
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        let inst = rt.get(&key).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::OverExhausted));
    }

    /// An UNWIRED optional carry seeds iteration 0 from its declared
    /// type's ZERO VALUE, not an error. The `acc` carry is a `String`
    /// (optional, unwired here: no `acc` in the input bag), so iteration
    /// 0's body must see `left = ""` (String zero) rather than failing
    /// the dispatch invariant. This is the path that lets a loop
    /// accumulate from a clean default without an explicit seed.
    #[tokio::test]
    async fn unwired_optional_carry_seeds_type_zero_value() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Fire WITHOUT `acc` in the input bag (unwired optional carry).
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt, &mut pulses, &journal,
        ).await;

        let by_port = |pulses: &PulseTable, idx: u32| -> std::collections::HashMap<String, serde_json::Value> {
            pulses.get(&lp.body_id)
                .map(|b| b.iter()
                    .filter(|p| p.frames.len() == 1 && p.frames[0].index == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), p.value.clone()))
                    .collect())
                .unwrap_or_default()
        };
        let iter0 = by_port(&pulses, 0);
        assert_eq!(iter0.get("right"), Some(&serde_json::json!("a")));
        assert_eq!(
            iter0.get("left"),
            Some(&serde_json::json!("")),
            "unwired String carry seeds iteration 0 with the String zero value \"\": {iter0:?}"
        );

        // And the instance's carry_values reflect the seeded zero.
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        assert_eq!(
            rt.get(&key).expect("instance").carry_values.get("acc"),
            Some(&serde_json::json!("")),
            "seeded carry value is the type zero, not null or missing"
        );
    }

    /// Done-driven loop with carry: body writes self.done = true at iter 2.
    /// Loop must terminate at that point, gather has 3 slots, carry final
    /// value is iter 2's write.
    #[tokio::test]
    async fn done_voted_sequential_loop_terminates_at_done() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Use a very long `over` list so we know termination came from
        // `done`, not exhaustion.
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a","b","c","d","e","f","g","h"], "acc": ""}),
            &mut rt, &mut pulses, &journal,
        ).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a", "done": false}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "ab", "acc": "ab", "done": false}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Vote done at iter 2.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "abc", "acc": "abc", "done": true}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list capped at iter 2's done vote: {:?}", data[0].value);

        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        use weft_core::primitive::LoopTerminationReason;
        assert_eq!(rt.get(&key).unwrap().terminated, Some(LoopTerminationReason::DoneVoted));
    }
}
