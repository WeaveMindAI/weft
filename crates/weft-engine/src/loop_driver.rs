//! The pulse loop. Drives the scheduler for a single execution
//! color until completion, failure, or stall.
//!
//! Shape:
//! - Boot: fold the journal for this color to recover pulses,
//!   executions, and pending deliveries. If the journal is empty
//!   we wait briefly for the producer to write `ExecutionStarted`
//!   + `PulseSeeded`, then re-fold.
//! - Dispatch: ready nodes go into a `JoinSet` as tokio tasks.
//!   Each task runs the node's async `execute` and reports back
//!   through an mpsc channel; the main loop applies results to
//!   `pulses` and `executions` (single-writer invariant).
//! - Suspend: a node calling `ctx.await_signal(...)` returns
//!   `WeftError::Suspended { token }` from the spawned task; the
//!   loop's `apply_results` records the token in `waiting`. The
//!   fold at boot seeds any already-resolved suspensions in
//!   `awaited_sequences`; bodies pop entries in call_index order.
//!   When nothing is making progress and at least one lane is
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
    check_completion, find_ready_nodes, postprocess::{emit_null_downstream, postprocess_output},
    preprocess_input, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use weft_core::node::NodeOutput;
use weft_core::primitive::ExecutionSnapshot;
use weft_core::project::EdgeIndex;
use weft_core::pulse::PulseTable;
use weft_core::cancellation::CancellationFlag;
use weft_core::{Color, ExecutionContext, NodeCatalog, ProjectDefinition};

use weft_journal::JournalClient;

use crate::context::{
    ship_node_completed, ship_node_failed, ship_node_resumed, ship_node_skipped,
    ship_node_started, ship_node_suspended, EngineClients, RunnerHandle,
};
use crate::now_unix;

/// Maximum re-fetch attempts when drive() returns Stalled/Stuck.
/// Each iteration re-reads the journal and reapplies the snapshot,
/// so this bounds the worst-case loop on a misconfigured graph.
/// In practice 0 or 1 is enough: 0 when the snapshot was already
/// complete, 1 when fires landed during drive's run.
const REFETCH_MAX_ITERS: u32 = 8;

/// Outcome the loop reports back to the binary wrapper.
#[derive(Debug, Clone)]
pub enum LoopOutcome {
    Completed { outputs: Value },
    Failed { error: String },
    /// Worker stalled: at least one lane is waiting for a signal.
    /// Worker should exit; the next fire's `register_signal` task
    /// will resume by re-folding the journal.
    Stalled,
    /// Scheduler ran to quiescence but pulses remain pending and
    /// nothing is waiting. Treat as a graph-shape bug.
    Stuck,
}

/// Run one execution to a terminal state or a stall. Each call folds
/// the journal once on entry and re-folds after Stalled/Stuck up to
/// `REFETCH_MAX_ITERS` to absorb deliveries that landed during
/// drive(). `pod_name` stamps every journal write so the fencing
/// trigger can reject writes from a Pod whose row is no longer alive.
pub async fn run_one_execution(
    project: ProjectDefinition,
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
) -> anyhow::Result<LoopOutcome> {
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    // Per-(node, lane) ordered list of past `await_signal` calls.
    // Pre-loaded from the journal fold; consumed by the body's
    // `await_signal` calls in call_index order. Replaces the
    // single-token `expected_tokens` HashMap from the
    // single-await-per-body world.
    let mut awaited_sequences: HashMap<
        (String, weft_core::lane::Lane),
        Vec<weft_core::primitive::AwaitedEntry>,
    > = HashMap::new();

    // Fold the journal: this is the source of truth. If the log is
    // non-empty (resume case), apply it to seed pulses, executions,
    // and pending deliveries. If empty, the producer just journaled
    // ExecutionStarted + PulseSeeded; wait briefly for the rows.
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
                    return Ok(LoopOutcome::Failed { error: "cancelled".into() });
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
    apply_snapshot(snap, &mut pulses, &mut executions, &mut awaited_sequences);

    let exec_id = uuid::Uuid::new_v4().to_string();
    // Drive in a re-fetch loop. drive() folds the journal once at
    // boot and works off that snapshot; SuspensionResolved rows that
    // arrive while drive() is running are invisible to it. When
    // drive() returns Stalled/Stuck, refetch the journal: if new
    // deliveries arrived, fold the new state on top and re-drive.
    // Cap the loop so a misbehaving fold can't spin forever.
    let mut event_count_before = events.len();
    let mut outcome;
    let mut iters_left = REFETCH_MAX_ITERS;
    loop {
        outcome = drive(
            &project,
            &edge_idx,
            catalog.as_ref(),
            &exec_id,
            color,
            &clients,
            &pod_name,
            &tenant_id,
            &namespace,
            &cancellation,
            &mut pulses,
            &mut executions,
            std::mem::take(&mut awaited_sequences),
            phase,
        )
        .await?;
        if !matches!(outcome, LoopOutcome::Stalled | LoopOutcome::Stuck) {
            break;
        }
        if iters_left == 0 {
            break;
        }
        iters_left -= 1;
        let fresh = fetch_events(journal.as_ref(), color).await?;
        if fresh.len() <= event_count_before {
            break;
        }
        event_count_before = fresh.len();
        let snap = weft_journal::fold_to_snapshot(color, &fresh);
        apply_snapshot(snap, &mut pulses, &mut executions, &mut awaited_sequences);
        tracing::info!(
            target: "weft_engine::resume",
            color = %color,
            "re-fetched journal after stall/stuck; re-driving"
        );
    }

    match &outcome {
        // Cancellation: walk the in-memory snapshot and journal a
        // NodeCancelled per non-terminal node so the UI's per-node
        // tally flips to cancelled (not stuck-running). Only the
        // worker knows which nodes are non-terminal AT the moment
        // we exit, so this can't live in the dispatcher's cancel
        // path. The terminal write happens after, via the shared
        // `journal_terminal` path below.
        LoopOutcome::Failed { error } if error == "cancelled" => {
            journal_node_cancellations(journal.as_ref(), color, &pod_name).await;
            journal_terminal(journal.as_ref(), clients.clock.as_ref(), color, &pod_name, &outcome).await;
        }
        LoopOutcome::Completed { .. } | LoopOutcome::Failed { .. } | LoopOutcome::Stuck => {
            journal_terminal(journal.as_ref(), clients.clock.as_ref(), color, &pod_name, &outcome).await;
        }
        LoopOutcome::Stalled => {
            // Worker exits cleanly without writing a terminal event.
            // Resume happens on the next fire: dispatcher writes a
            // SuspensionResolved row + enqueues a fresh `resume`
            // task (the prior task is `complete` so dedup lets a
            // new one through), and a worker spawns to fold the
            // updated journal. Nothing extra to journal here.
        }
    }
    Ok(outcome)
}

fn apply_snapshot(
    snap: ExecutionSnapshot,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    awaited_sequences: &mut HashMap<
        (String, weft_core::lane::Lane),
        Vec<weft_core::primitive::AwaitedEntry>,
    >,
) {
    *pulses = snap.pulses;
    *executions = snap.executions;
    *awaited_sequences = snap.awaited_sequences;

    // A WaitingForInput exec re-dispatches ONLY if at least one of
    // its sequence's entries has been resolved by a fire AND the
    // exec is still non-terminal. Without this scoping, every fresh
    // worker spawn would re-dispatch every still-suspended sibling,
    // re-run the body, hit the first await, find no delivery yet,
    // re-suspend, churning the journal with spurious
    // NodeStarted/Suspended cycles per fresh worker.
    //
    // The mechanics: un-absorb the pulses for resuming lanes only.
    // Their execs stay in WaitingForInput; the dispatch loop will
    // detect (non-terminal exec exists + pulse Pending again) and
    // ship `NodeResumed` instead of `NodeStarted` (same record,
    // state Suspended → Running).
    let resume_locations: std::collections::HashSet<(String, weft_core::lane::Lane)> =
        awaited_sequences
            .iter()
            .filter(|(_, seq)| {
                seq.iter().any(|e| matches!(
                    &e.kind,
                    weft_core::primitive::AwaitedEntryKind::Await { resolved: Some(_), .. }
                ))
            })
            .map(|(key, _)| key.clone())
            .collect();

    // Crashed-worker recovery: any Running exec (no terminal event
    // arrived because the worker died mid-flight) gets its pulses
    // un-absorbed too so we re-dispatch it. We keep the exec
    // record; the dispatch path detects "non-terminal exec exists"
    // and ships NodeResumed.
    // A `Running` lane with no terminal in the journal is assumed to
    // belong to a worker that DIED mid-node; we un-absorb its pulses
    // and re-run the node. This is only safe because at most ONE
    // worker exists per color at a time: the dispatcher's spawn path
    // dedups (enqueue-dedup key + the partial unique index on
    // worker_pod + a NOT-EXISTS-live-pod check in cold_start), so a
    // fresh worker can't re-fold and re-run a node body while the
    // prior worker is still alive and about to ship its own
    // NodeCompleted. If that one-worker-per-color invariant ever
    // broke, this re-run would double-execute the node (double LLM
    // spend / double side-effects).
    let crashed_running: std::collections::HashSet<(String, weft_core::lane::Lane)> =
        executions
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.status == NodeExecutionStatus::Running)
            .map(|e| (e.node_id.clone(), e.lane.clone()))
            .collect();

    let to_un_absorb: std::collections::HashSet<(String, weft_core::lane::Lane)> =
        resume_locations.union(&crashed_running).cloned().collect();

    // Per location, recover the count of pulses to un-absorb (one
    // per absorbed pulse the original dispatch consumed).
    let mut un_absorb_counts: HashMap<(String, weft_core::lane::Lane), usize> = HashMap::new();
    for execs in executions.values() {
        for e in execs {
            let key = (e.node_id.clone(), e.lane.clone());
            if to_un_absorb.contains(&key) && !e.status.is_terminal() {
                *un_absorb_counts.entry(key).or_default() +=
                    e.pulses_absorbed.len().max(1);
            }
        }
    }

    for ((node_id, lane), count) in un_absorb_counts {
        if let Some(bucket) = pulses.get_mut(&node_id) {
            let mut remaining = count;
            for p in bucket.iter_mut() {
                if remaining == 0 {
                    break;
                }
                if p.status == weft_core::pulse::PulseStatus::Absorbed && p.lane == lane {
                    p.status = weft_core::pulse::PulseStatus::Pending;
                    remaining -= 1;
                }
            }
        }
    }
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
    pod_name: &str,
    tenant_id: &str,
    // namespace: project namespace this worker is running in. Used
    // to populate InfraProvisionContext when dispatching infra nodes.
    namespace: &str,
    cancellation: &Arc<CancellationFlag>,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    mut awaited_sequences: HashMap<
        (String, weft_core::lane::Lane),
        Vec<weft_core::primitive::AwaitedEntry>,
    >,
    phase: weft_core::context::Phase,
) -> anyhow::Result<LoopOutcome> {
    let journal = clients.journal.as_ref();
    let (result_tx, mut result_rx) = mpsc::unbounded_channel::<NodeTaskResult>();
    let mut in_flight: JoinSet<()> = JoinSet::new();
    // Nodes that called `await_signal` and returned `Suspended`.
    // Keyed by token; value is (node_id, lane). When the loop finds
    // no active work to run and this map is non-empty, we stall:
    // the worker tells the dispatcher "I'm just waiting, please
    // kill me" and exits.
    let mut waiting: HashMap<String, (String, weft_core::lane::Lane)> = HashMap::new();

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

    loop {
        // Cancellation checkpoint. Checked at the TOP of every
        // iteration regardless of whether the previous iteration
        // made progress. The flag is persistent (AtomicBool), so
        // there's no race between cancel() landing and the next
        // check.
        //
        // Before returning Failed("cancelled"), `shutdown().await`
        // the in-flight JoinSet: simply dropping the JoinSet aborts
        // its tasks at their next yield point, but a task that's
        // mid-journal-write may finish writing (e.g. NodeCompleted)
        // AFTER the cancel path wrote NodeCancelled for the same
        // (node, lane). The fold is last-write-wins, so the final
        // state would flip to Completed and downstream nodes would
        // receive a fake output. Awaiting shutdown drives every
        // task to its abort point deterministically before we
        // declare the execution cancelled.
        if cancellation.is_cancelled() {
            tracing::info!(
                target: "weft_engine::loop_driver",
                color = %color,
                in_flight = in_flight.len(),
                "cancellation observed at loop top; draining in-flight tasks"
            );
            in_flight.shutdown().await;
            return Ok(LoopOutcome::Failed { error: "cancelled".into() });
        }

        let mut mutations = Vec::new();
        preprocess_input(project, pulses, &mut mutations);
        if !mutations.is_empty() {
            crate::context::ship_pulse_mutations(journal, pod_name,std::mem::take(&mut mutations)).await;
        }
        let ready = find_ready_nodes(project, pulses, edge_idx);
        if !ready.is_empty() {
            let ids: Vec<&str> = ready.iter().map(|(id, _)| id.as_str()).collect();
            tracing::info!(
                target: "weft_engine::loop_driver",
                color = %color,
                ready_ids = ?ids,
                "ready batch"
            );
        }

        // Dispatch every ready group that isn't already Running for
        // this (node_id, color, lane). Each dispatch either short-
        // circuits (skip/failure) or spawns a task.
        for (node_id, mut group) in ready {
            tracing::info!(
                target: "weft_engine::loop_driver",
                node = %node_id,
                color = %group.color,
                lane = ?group.lane,
                "dispatching ready group"
            );
            let Some(node_def) = project.nodes.iter().find(|n| n.id == node_id) else {
                continue;
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
            // exists at this (node, lane), this dispatch continues
            // that record (state Suspended → Running). Otherwise
            // it's a first dispatch and we open a new record.
            let existing = executions
                .get(&node_id)
                .and_then(|v| v.iter().rposition(|e| e.lane == group.lane && !e.status.is_terminal()));
            let is_resume = existing.is_some();
            // For NodeResumed event reporting: the token + resolved
            // value of the most-recently-resolved await in the
            // sequence (the fire that triggered this dispatch).
            // None = crashed-Running recovery, no fresh delivery.
            let resume_token_value: Option<(String, serde_json::Value)> = awaited_sequences
                .get(&(node_id.clone(), group.lane.clone()))
                .and_then(|seq| {
                    seq.iter().rev().find_map(|e| match &e.kind {
                        weft_core::primitive::AwaitedEntryKind::Await {
                            token,
                            resolved: Some(value),
                        } => Some((token.clone(), value.clone())),
                        _ => None,
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
                    color: group.color,
                    lane: group.lane.clone(),
                    prior_attempts: Vec::new(),
                };
                executions.entry(node_id.clone()).or_default().push(record);
            }

            if group.should_skip {
                handle_node_skip(
                    &node_id, group.color, &group.lane,
                    project, edge_idx, pulses, executions, journal, pod_name,
                    phase_scope.as_ref(),
                )
                .await;
                continue;
            }

            if let Some(err) = &group.error {
                handle_node_failure(
                    &node_id, group.color, &group.lane, err,
                    project, edge_idx, pulses, executions, journal, pod_name,
                    phase_scope.as_ref(),
                )
                .await;
                continue;
            }

            let node_impl = match catalog.lookup(&node_def.node_type) {
                Some(n) => n,
                None => {
                    let err = format!("unknown node type: {}", node_def.node_type);
                    handle_node_failure(
                        &node_id, group.color, &group.lane, &err,
                        project, edge_idx, pulses, executions, journal, pod_name,
                        phase_scope.as_ref(),
                    )
                    .await;
                    continue;
                }
            };

            // Ship the lifecycle event AFTER the early-return
            // checks so we don't emit Started/Resumed for a path
            // that bails to skipped/failed.
            if is_resume {
                if let Some((token, value)) = &resume_token_value {
                    ship_node_resumed(journal, pod_name,color, &node_id, &group.lane, token, value).await;
                }
                // Resume detected without a token = crashed-Running
                // recovery. The exec record is already Running; no
                // lifecycle event needed (the journal fold already
                // sees this lane as in-flight).
            } else {
                ship_node_started(journal, pod_name,color, &node_id, &group.lane, &group.input, &group.pulse_ids).await;
            }

            let config = ConfigBag {
                values: node_def.config.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };
            let input = InputBag {
                values: group.input.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };

            // Hand the per-(node, lane) await sequence to the
            // handle. The body's `await_signal` calls pop entries
            // in call_index order: resolved entries replay
            // instantly, the pending tail re-suspends, and an
            // exhausted sequence (or fresh node) registers a new
            // await with the next call_index.
            let sequence = awaited_sequences
                .remove(&(node_id.clone(), group.lane.clone()))
                .unwrap_or_default();

            let handle = Arc::new(
                RunnerHandle::new(
                    exec_id.to_string(),
                    project.id.to_string(),
                    group.color,
                    node_id.clone(),
                    group.lane.clone(),
                    clients.clone(),
                    pod_name.to_string(),
                    tenant_id.to_string(),
                    cancellation.clone(),
                )
                .with_awaited_sequence(sequence),
            ) as Arc<dyn weft_core::context::ContextHandle>;

            let ctx = ExecutionContext::new(
                exec_id.to_string(),
                project.id.to_string(),
                node_id.clone(),
                node_def.node_type.clone(),
                node_def.label.clone(),
                group.color,
                group.lane.clone(),
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
            // is just `execute`. The task writes its result back via
            // `result_tx`; the main loop applies the effect on
            // `pulses`/`executions`.
            let tx = result_tx.clone();
            let node_id_task = node_id.clone();
            let color_task = group.color;
            let lane_task = group.lane.clone();
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
            let provision_input = weft_core::node::NodeInput {
                values: group
                    .input
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            };
            in_flight.spawn(async move {
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
                            let _ = tx.send(NodeTaskResult {
                                node_id: node_id_task,
                                color: color_task,
                                lane: lane_task,
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
                        let _ = tx.send(NodeTaskResult {
                            node_id: node_id_task,
                            color: color_task,
                            lane: lane_task,
                            outcome: NodeTaskOutcome::Failed(format!("apply: {e}")),
                        });
                        return;
                    }
                    // 3. Fall through to execute.
                }

                let result = node_impl.execute(ctx).await;
                let outcome = match result {
                    Ok(output) => NodeTaskOutcome::Completed(output),
                    Err(weft_core::error::WeftError::Suspended { token }) => {
                        NodeTaskOutcome::Waiting(token)
                    }
                    Err(e) => NodeTaskOutcome::Failed(format!("{e}")),
                };
                let _ = tx.send(NodeTaskResult {
                    node_id: node_id_task,
                    color: color_task,
                    lane: lane_task,
                    outcome,
                });
            });
        }

        // Drain completed tasks' results WITHOUT blocking so we can
        // keep dispatching newly-ready nodes in the next iteration.
        // If nothing ran this turn AND tasks are in flight, block
        // on the next result.
        let progress = apply_results(
            &mut result_rx,
            project,
            edge_idx,
            pulses,
            executions,
            journal,
            pod_name,
            &mut waiting,
            phase_scope.as_ref(),
        )
        .await;

        if progress {
            continue;
        }

        // No progress from draining. Check: is anything still in flight?
        if in_flight.is_empty() {
            return terminate(pulses, executions, &waiting).await;
        }

        // At least one in-flight task. Block on either the next
        // JoinSet termination or cancellation. DO NOT poll
        // `result_rx` here: `recv().await` would consume the
        // message and drop it, so the next `apply_results` iter
        // would miss it. Let join_next observe the task completing;
        // apply_results on the next iter drains the message.
        tokio::select! {
            _ = in_flight.join_next() => {}
            _ = cancellation.cancelled() => {
                tracing::info!(
                    target: "weft_engine::loop_driver",
                    color = %color,
                    "cancellation observed at idle wait; exiting Failed(cancelled)"
                );
                return Ok(LoopOutcome::Failed { error: "cancelled".into() });
            }
        }
    }
}

/// Tear down a terminated node lane's downstream: null every
/// connected output pulse, ship those mutations, then prune
/// out-of-scope pulses. The SHARED tail of every node-down path
/// (failure, skip): the mark + lifecycle-event differ, this is what's
/// identical. The prune is here, not at the call sites, so a new
/// node-down path can't forget it (during a setup phase
/// `emit_null_downstream` can emit onto out-of-scope nodes, which
/// would otherwise be dispatched, auto-skipped, and churn the loop).
#[allow(clippy::too_many_arguments)]
async fn null_downstream_and_prune(
    node_id: &str,
    color: weft_core::Color,
    lane: &weft_core::lane::Lane,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    let mut muts = Vec::new();
    emit_null_downstream(node_id, color, lane, project, pulses, edge_idx, &mut muts);
    crate::context::ship_pulse_mutations(journal, pod_name, muts).await;
    if let Some(scope) = phase_scope {
        drop_out_of_scope_pulses(pulses, scope);
    }
}

/// Fail a node lane: mark Failed, ship `NodeFailed`, null+prune
/// downstream. The SINGLE failure path: a genuine `execute` error, a
/// dispatch-time error, an unknown node type, and an output
/// type-check failure all route here, so "X-failed node = failed
/// node" is structurally true (not a hand-copied near-dup that can
/// drift). A failed node never propagates its real output values:
/// status (Failed) and data (null) always agree.
#[allow(clippy::too_many_arguments)]
async fn handle_node_failure(
    node_id: &str,
    color: weft_core::Color,
    lane: &weft_core::lane::Lane,
    err: &str,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    mark_failed(executions, node_id, color, lane, err);
    ship_node_failed(journal, pod_name, color, node_id, lane, err).await;
    null_downstream_and_prune(
        node_id, color, lane, project, edge_idx, pulses, journal, pod_name, phase_scope,
    )
    .await;
}

/// Skip a node lane (a scope/condition decided it shouldn't run):
/// mark Skipped, ship `NodeSkipped`, null+prune downstream. Same
/// downstream teardown as a failure (the prune in particular), just a
/// different lifecycle event.
#[allow(clippy::too_many_arguments)]
async fn handle_node_skip(
    node_id: &str,
    color: weft_core::Color,
    lane: &weft_core::lane::Lane,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    phase_scope: Option<&std::collections::HashSet<String>>,
) {
    mark_skipped(executions, node_id, color, lane);
    ship_node_skipped(journal, pod_name, color, node_id, lane).await;
    null_downstream_and_prune(
        node_id, color, lane, project, edge_idx, pulses, journal, pod_name, phase_scope,
    )
    .await;
}

/// Drain pending task results and apply them to the state tables.
/// Returns true if any result was drained. `waiting` accumulates
/// tokens of nodes that returned `Suspended`: when the loop finds
/// nothing active to run and this map is non-empty, it stalls the
/// worker so the dispatcher can kill the process and respawn on
/// fire.
#[allow(clippy::too_many_arguments)]
async fn apply_results(
    rx: &mut mpsc::UnboundedReceiver<NodeTaskResult>,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    journal: &dyn JournalClient,
    pod_name: &str,
    waiting: &mut HashMap<String, (String, weft_core::lane::Lane)>,
    phase_scope: Option<&std::collections::HashSet<String>>,
) -> bool {
    let mut any = false;
    while let Ok(result) = rx.try_recv() {
        any = true;
        match result.outcome {
            NodeTaskOutcome::Completed(output) => {
                // No output-side type check: type enforcement lives at
                // exactly one boundary, the CONSUMER's input
                // (`ready::check_input`). A node ships its output as
                // produced; a bad-typed value surfaces at whatever node
                // consumes it (with the source named in the error).
                let output_value = output_to_value(&output);
                mark_completed(executions, &result.node_id, result.color, &result.lane, &output);
                ship_node_completed(journal, pod_name, result.color, &result.node_id, &result.lane, &output_value).await;
                let mut muts = Vec::new();
                postprocess_output(
                    &result.node_id,
                    &output_value,
                    result.color,
                    &result.lane,
                    project,
                    pulses,
                    edge_idx,
                    &mut muts,
                );
                crate::context::ship_pulse_mutations(journal, pod_name, muts).await;
                if let Some(scope) = phase_scope.as_ref() {
                    drop_out_of_scope_pulses(pulses, scope);
                }
            }
            NodeTaskOutcome::Failed(err) => {
                handle_node_failure(
                    &result.node_id, result.color, &result.lane, &err,
                    project, edge_idx, pulses, executions, journal, pod_name,
                    phase_scope,
                )
                .await;
            }
            NodeTaskOutcome::Waiting(token) => {
                mark_waiting(
                    executions,
                    &result.node_id,
                    result.color,
                    &result.lane,
                    &token,
                );
                ship_node_suspended(journal, pod_name,result.color, &result.node_id, &result.lane, &token).await;
                waiting.insert(token, (result.node_id, result.lane));
            }
        }
    }
    any
}

fn mark_waiting(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    lane: &[weft_core::lane::LaneFrame],
    token: &str,
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.lane == lane) {
            e.status = NodeExecutionStatus::WaitingForInput;
            e.callback_id = Some(token.to_string());
        }
    }
}

async fn terminate(
    pulses: &PulseTable,
    executions: &mut NodeExecutionTable,
    waiting: &HashMap<String, (String, weft_core::lane::Lane)>,
) -> anyhow::Result<LoopOutcome> {
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
        Some(false) => Ok(LoopOutcome::Completed {
            outputs: final_outputs(executions),
        }),
        Some(true) => Ok(LoopOutcome::Failed {
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
                return Ok(LoopOutcome::Stalled);
            }
            tracing::warn!(
                target: "weft_engine",
                pulses = pulses.len(),
                "execution stuck: pending pulses with no ready nodes and no suspensions"
            );
            Ok(LoopOutcome::Stuck)
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

// ---------- Task plumbing ----------

struct NodeTaskResult {
    node_id: String,
    color: Color,
    lane: weft_core::lane::Lane,
    outcome: NodeTaskOutcome,
}

enum NodeTaskOutcome {
    Completed(NodeOutput),
    Failed(String),
    /// The node called `await_signal` and is now waiting on a fired
    /// wake signal. The loop driver marks the node's execution
    /// WaitingForInput and adds the token to the waiting list; when
    /// all tasks are done and some are waiting, the loop stalls
    /// the worker (sends `Stalled`, exits) so the dispatcher can
    /// kill the process and respawn on fire.
    Waiting(String),
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

fn first_failure(executions: &NodeExecutionTable) -> Option<String> {
    for execs in executions.values() {
        for e in execs {
            if e.status == NodeExecutionStatus::Failed {
                return Some(format!(
                    "{}: {}",
                    e.node_id,
                    e.error.clone().unwrap_or_else(|| "failed".into())
                ));
            }
        }
    }
    None
}

fn output_to_value(output: &NodeOutput) -> Value {
    Value::Object(output.outputs.clone().into_iter().collect())
}

fn mark_completed(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    lane: &[weft_core::lane::LaneFrame],
    output: &NodeOutput,
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.lane == lane) {
            e.status = NodeExecutionStatus::Completed;
            e.completed_at = Some(now_unix());
            e.output = Some(output_to_value(output));
        }
    }
}

fn mark_failed(
    executions: &mut NodeExecutionTable,
    node_id: &str,
    color: Color,
    lane: &[weft_core::lane::LaneFrame],
    err: &str,
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.lane == lane) {
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
    lane: &[weft_core::lane::LaneFrame],
) {
    if let Some(execs) = executions.get_mut(node_id) {
        if let Some(e) = execs.iter_mut().rev().find(|e| e.color == color && e.lane == lane) {
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
            "name": "t",
            "description": null,
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

/// Walk the journal, find every (node, lane) that's currently
/// non-terminal, and journal a NodeCancelled for each so the UI
/// flips them out of "running". Called when the loop driver exits
/// with `Failed { error: "cancelled" }`.
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
                lane: e.lane.clone(),
                reason: reason.clone(),
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
/// translation from `LoopOutcome` to the matching `ExecEvent`
/// variant: `Completed`/`Failed`/`Stuck` map; `Stalled` is a
/// caller-side no-op so this function isn't called for it.
async fn journal_terminal(
    journal: &dyn JournalClient,
    clock: &dyn weft_platform_traits::Clock,
    color: Color,
    pod_name: &str,
    outcome: &LoopOutcome,
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
                target: "weft_engine::loop_driver",
                color = %color,
                error = %e,
                "has_terminal_event failed; skipping terminal write to avoid double-publish"
            );
            return;
        }
    }
    let at_unix = now_unix();
    let event = match outcome {
        LoopOutcome::Completed { outputs } => weft_journal::ExecEvent::ExecutionCompleted {
            color,
            outputs: outputs.clone(),
            at_unix,
        },
        // The loop driver returns Failed { error: "cancelled" } when
        // the cancellation flag fires. Translate that to the proper
        // ExecutionCancelled terminal so the UI renders the cancel
        // affordance instead of a generic failure.
        LoopOutcome::Failed { error } if error == "cancelled" => {
            weft_journal::ExecEvent::ExecutionCancelled {
                color,
                reason: "Cancelled by user".to_string(),
                at_unix,
            }
        }
        LoopOutcome::Failed { error } => weft_journal::ExecEvent::ExecutionFailed {
            color,
            error: error.clone(),
            at_unix,
        },
        LoopOutcome::Stuck => weft_journal::ExecEvent::ExecutionFailed {
            color,
            error: "execution stuck".to_string(),
            at_unix,
        },
        LoopOutcome::Stalled => {
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
