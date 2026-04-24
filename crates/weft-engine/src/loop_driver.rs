//! The pulse loop. Given a project, a wake spec, and a dispatcher
//! link, drive the scheduler until completion, failure, or stall.
//!
//! Key shape (Phase A Slice 3):
//! - Ready nodes are dispatched as tokio tasks into a `JoinSet`.
//! - Each task runs the node's async `execute` and reports back
//!   through an mpsc channel whose items the main loop applies to
//!   `pulses` and `executions` (single-writer invariant).
//! - When a node calls `ctx.await_signal(...)`, its task parks on a
//!   oneshot inside the `DispatcherLink` until the dispatcher sends
//!   the matching `Deliver`. Other lanes keep running.
//! - Stall = no ready nodes, no in-flight tasks, at least one
//!   NodeExecution in WaitingForInput. The loop serializes an
//!   `ExecutionSnapshot`, sends `Stalled` over the link, awaits the
//!   `StalledAck`, and exits. A later worker invocation picks up the
//!   snapshot and resumes.
//! - Completion = no ready nodes, no in-flight tasks, no waiting.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;

use weft_core::context::{ConfigBag, InputBag};
use weft_core::exec::{
    check_completion, find_ready_nodes, postprocess::{emit_null_downstream, postprocess_output},
    preprocess_input, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use weft_core::node::NodeOutput;
use weft_core::primitive::{ExecutionSnapshot, WakeMessage};
use weft_core::project::EdgeIndex;
use weft_core::pulse::{Pulse, PulseTable};
use weft_core::{Color, ExecutionContext, NodeCatalog, ProjectDefinition};

use crate::context::{ship_node_event, RunnerHandle};
use crate::dispatcher_link::{DispatcherLink, StartPacket};

// Re-exported for backwards compat; the wake spec is now carried by
// the Start packet but callers may still want to construct wakes
// manually for detached tests.
pub use weft_core::primitive::RootSeed;

#[derive(Debug, Clone)]
pub enum WakeSpec {
    Fresh {
        entry_node: Option<String>,
        entry_value: Value,
    },
    Resume {
        entry_node: String,
        entry_value: Value,
    },
    FreshMulti {
        seeds: Vec<RootSeed>,
    },
}

/// Outcome the loop reports back to the binary wrapper.
#[derive(Debug, Clone)]
pub enum LoopOutcome {
    Completed { outputs: Value },
    Failed { error: String },
    /// Worker stalled: at least one lane is waiting for a signal.
    /// Snapshot has been shipped over the link; worker should exit.
    Stalled,
    /// Scheduler ran to quiescence but pulses remain pending and
    /// nothing is waiting. Treat as a graph-shape bug.
    Stuck,
}

/// Entry point used by in-process tests (no dispatcher link).
/// Seeds pulses per the given `wake` and drives the loop to
/// completion. Nodes calling `await_signal` will error because the
/// link is absent; keep tests to graphs that don't suspend.
pub async fn run_loop(
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    color: Color,
    wake: WakeSpec,
    dispatcher_url: Option<&str>,
    cancellation: Arc<Notify>,
) -> anyhow::Result<LoopOutcome> {
    let _ = dispatcher_url;
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();

    seed_from_wake_spec(&wake, color, &project, &edge_idx, &mut pulses)?;

    let exec_id = uuid::Uuid::new_v4().to_string();
    drive(
        &project,
        &edge_idx,
        catalog.as_ref(),
        &exec_id,
        color,
        None,
        &cancellation,
        &mut pulses,
        &mut executions,
        HashMap::new(),
        weft_core::context::Phase::Fire,
    )
    .await
}

/// Entry point used by the compiled project binary. Connects to the
/// dispatcher, handshakes, restores state from the Start packet,
/// drives the loop, ships the terminal status over the link.
pub async fn run_with_link(
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    color: Color,
    dispatcher_url: &str,
    cancellation: Arc<Notify>,
) -> anyhow::Result<LoopOutcome> {
    let (link, start) = DispatcherLink::connect(dispatcher_url, color).await?;
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();
    let mut expected_tokens: HashMap<(String, weft_core::lane::Lane), String> = HashMap::new();

    let StartPacket { wake, snapshot, worker_instance_id: _ } = start;

    // The dispatcher's fold is the single source of truth for this
    // color's state. When `snapshot` is present we seed the link
    // with every pending delivery (fires not yet consumed) and
    // restore pulses + executions. When absent (unusual: event log
    // empty), we seed from the wake message.
    let had_snapshot = snapshot.is_some();
    // Phase for this worker lifetime. Fresh runs carry it on the
    // wake; Resume continues the original phase (Fire, since no
    // snapshotted run was ever launched in InfraSetup or
    // TriggerSetup).
    let phase = match &wake {
        WakeMessage::Fresh { phase, .. } => *phase,
        WakeMessage::Resume => weft_core::context::Phase::Fire,
    };
    if let Some(snap) = snapshot {
        for (token, value) in snap.pending_deliveries.clone() {
            link.seed_delivery(token, value).await;
        }
        apply_snapshot(snap, &mut pulses, &mut executions, &mut expected_tokens);
    }
    if !had_snapshot {
        seed_from_wake_message(&wake, color, &project, &edge_idx, &mut pulses, &expected_tokens)?;
    }

    let exec_id = uuid::Uuid::new_v4().to_string();
    let outcome = drive(
        &project,
        &edge_idx,
        catalog.as_ref(),
        &exec_id,
        color,
        Some(&link),
        &cancellation,
        &mut pulses,
        &mut executions,
        expected_tokens,
        phase,
    )
    .await?;

    match &outcome {
        LoopOutcome::Completed { outputs } => {
            link.completed(outputs.clone()).await;
        }
        LoopOutcome::Failed { error } => {
            link.failed(error.clone()).await;
        }
        LoopOutcome::Stalled => {
            // The link already received the stalled snapshot inside
            // `drive`; nothing more to ship.
        }
        LoopOutcome::Stuck => {
            link.failed("execution stuck".into()).await;
        }
    }
    Ok(outcome)
}

fn seed_from_wake_spec(
    wake: &WakeSpec,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
) -> anyhow::Result<()> {
    match wake {
        WakeSpec::Fresh { entry_node: Some(entry), entry_value } => {
            let entry_node_def = project
                .nodes
                .iter()
                .find(|n| &n.id == entry)
                .ok_or_else(|| anyhow::anyhow!("entry node '{entry}' not found"))?;
            let entry_port = entry_node_def
                .inputs
                .first()
                .map(|p| p.name.clone())
                .unwrap_or_else(|| "__seed__".into());
            pulses
                .entry(entry.clone())
                .or_default()
                .push(Pulse::new(color, Vec::new(), entry.clone(), entry_port, entry_value.clone()));
        }
        WakeSpec::Fresh { entry_node: None, .. } => {}
        WakeSpec::Resume { entry_node, entry_value } => {
            seed_resume(entry_node, entry_value, color, project, edge_idx, pulses)?;
        }
        WakeSpec::FreshMulti { seeds } => {
            for seed in seeds {
                pulses.entry(seed.node_id.clone()).or_default().push(Pulse::new(
                    color,
                    Vec::new(),
                    seed.node_id.clone(),
                    "__seed__".to_string(),
                    seed.value.clone(),
                ));
            }
        }
    }
    Ok(())
}

fn seed_from_wake_message(
    wake: &WakeMessage,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    _expected_tokens: &HashMap<(String, weft_core::lane::Lane), String>,
) -> anyhow::Result<()> {
    let _ = (project, edge_idx);
    match wake {
        WakeMessage::Fresh { seeds, phase: _ } => {
            for seed in seeds {
                pulses.entry(seed.node_id.clone()).or_default().push(Pulse::new(
                    color,
                    Vec::new(),
                    seed.node_id.clone(),
                    "__seed__".to_string(),
                    seed.value.clone(),
                ));
            }
        }
        WakeMessage::Resume => {
            // Nothing to seed: the snapshot already populated pulses.
        }
    }
    Ok(())
}

fn seed_resume(
    entry_node: &str,
    entry_value: &Value,
    color: Color,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
) -> anyhow::Result<()> {
    let entry_node_def = project
        .nodes
        .iter()
        .find(|n| n.id == entry_node)
        .ok_or_else(|| anyhow::anyhow!("entry node '{entry_node}' not found"))?;
    for port in &entry_node_def.outputs {
        let outgoing = edge_idx.get_outgoing(project, entry_node);
        for edge in outgoing.iter().filter(|e| e.source_handle.as_deref() == Some(&port.name)) {
            let target_port = edge.target_handle.as_deref().unwrap_or("default");
            pulses.entry(edge.target.clone()).or_default().push(Pulse::new(
                color,
                Vec::new(),
                edge.target.clone(),
                target_port.to_string(),
                entry_value.clone(),
            ));
        }
    }
    Ok(())
}

fn apply_snapshot(
    snap: ExecutionSnapshot,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    expected_tokens: &mut HashMap<(String, weft_core::lane::Lane), String>,
) {
    *pulses = snap.pulses;
    *executions = snap.executions;
    for (token, info) in snap.suspensions {
        expected_tokens.insert((info.node_id.clone(), info.lane.clone()), token);
    }
    // Flip every WaitingForInput execution back to Running + clear
    // the completed_at timestamp so the re-dispatch finds the node
    // in the "pending work" state. The pulses bucket should still
    // contain the absorbed pulses that triggered the original
    // dispatch; find_ready_nodes will skip over absorbed ones but
    // re-dispatch requires the prior execution record to be
    // non-terminal. We simulate that by clearing it.
    //
    // Remove any non-terminal execution; the re-dispatch here will
    // create a fresh Running record for it. For each removed exec,
    // un-absorb the pulses it consumed (so find_ready_nodes
    // re-dispatches the node). The fold marks pulses Absorbed at
    // the granularity of "first N pending pulses per (node, lane)
    // when NodeStarted fires"; we restore up to N per removed
    // exec.
    let mut restored_per_node: HashMap<(String, weft_core::lane::Lane), usize> = HashMap::new();
    for execs in executions.values_mut() {
        let (kept, removed): (Vec<_>, Vec<_>) = execs.drain(..).partition(|e| {
            matches!(
                e.status,
                NodeExecutionStatus::Completed
                    | NodeExecutionStatus::Failed
                    | NodeExecutionStatus::Skipped
            )
        });
        *execs = kept;
        for e in removed {
            *restored_per_node
                .entry((e.node_id.clone(), e.lane.clone()))
                .or_default() += e.pulses_absorbed.len().max(1);
        }
    }
    // For each location with a removed exec, flip up to N Absorbed
    // pulses back to Pending.
    for ((node_id, lane), count) in restored_per_node {
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

/// Internal loop body shared by `run_loop` and `run_with_link`.
async fn drive(
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    catalog: &dyn NodeCatalog,
    exec_id: &str,
    _color: Color,
    link: Option<&DispatcherLink>,
    cancellation: &Arc<Notify>,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    mut expected_tokens: HashMap<(String, weft_core::lane::Lane), String>,
    phase: weft_core::context::Phase,
) -> anyhow::Result<LoopOutcome> {
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
    // execute; any node outside that set must be auto-skipped when
    // a pulse lands on it, otherwise downstream nodes of the
    // triggers (e.g. the WhatsAppSend reply in an echo bot) block
    // forever waiting for inputs that TriggerSetup never produces.
    //
    // Empty set means "no scoping" (Fire / InfraSetup phases).
    let phase_scope: Option<std::collections::HashSet<String>> =
        match phase {
            weft_core::context::Phase::TriggerSetup => {
                Some(compute_trigger_setup_scope(project, edge_idx))
            }
            _ => None,
        };
    if let Some(scope) = phase_scope.as_ref() {
        drop_out_of_scope_pulses(pulses, scope);
    }

    loop {
        // Pause dispatching when the link is not Live. In-flight
        // node futures keep running; their results and any cost /
        // log events queue up in the outbound mpsc and flush when
        // the supervisor reconnects. A `Dead` link means the
        // supervisor gave up on reconnect; bail out with a failure.
        if let Some(link) = link {
            let mut status = link.status();
            loop {
                let current = *status.borrow_and_update();
                match current {
                    crate::dispatcher_link::LinkStatus::Live => break,
                    crate::dispatcher_link::LinkStatus::Dead => {
                        tracing::error!(
                            target: "weft_engine",
                            "dispatcher link is dead (reconnect failed); aborting execution"
                        );
                        return Ok(LoopOutcome::Failed {
                            error: "dispatcher link dead".into(),
                        });
                    }
                    crate::dispatcher_link::LinkStatus::Connecting
                    | crate::dispatcher_link::LinkStatus::Disconnected => {
                        if status.changed().await.is_err() {
                            return Ok(LoopOutcome::Failed {
                                error: "dispatcher link closed".into(),
                            });
                        }
                    }
                }
            }
        }

        preprocess_input(project, pulses);
        let ready = find_ready_nodes(project, pulses, edge_idx);

        // Dispatch every ready group that isn't already Running for
        // this (node_id, color, lane). Each dispatch either short-
        // circuits (skip/failure) or spawns a task.
        for (node_id, mut group) in ready {
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
            };
            executions.entry(node_id.clone()).or_default().push(record);

            if group.should_skip {
                mark_skipped(executions, &node_id, group.color, &group.lane);
                ship_node_event(
                    link,
                    group.color,
                    &node_id,
                    &group.lane,
                    "skipped",
                    None,
                    None,
                    None,
                    &group.pulse_ids,
                )
                .await;
                emit_null_downstream(&node_id, group.color, &group.lane, project, pulses, edge_idx, executions);
                continue;
            }

            if let Some(err) = &group.error {
                mark_failed(executions, &node_id, group.color, &group.lane, err);
                ship_node_event(
                    link,
                    group.color,
                    &node_id,
                    &group.lane,
                    "failed",
                    None,
                    None,
                    Some(err),
                    &group.pulse_ids,
                )
                .await;
                emit_null_downstream(&node_id, group.color, &group.lane, project, pulses, edge_idx, executions);
                continue;
            }

            let node_impl = match catalog.lookup(&node_def.node_type) {
                Some(n) => n,
                None => {
                    let err = format!("unknown node type: {}", node_def.node_type);
                    mark_failed(executions, &node_id, group.color, &group.lane, &err);
                    ship_node_event(
                        link,
                        group.color,
                        &node_id,
                        &group.lane,
                        "failed",
                        None,
                        None,
                        Some(&err),
                        &group.pulse_ids,
                    )
                    .await;
                    emit_null_downstream(&node_id, group.color, &group.lane, project, pulses, edge_idx, executions);
                    continue;
                }
            };

            let config = ConfigBag {
                values: node_def.config.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };
            let input = InputBag {
                values: group.input.as_object().cloned().unwrap_or_default().into_iter().collect(),
            };

            // Give this invocation an expected_token if we have one
            // carrying over from a snapshot.
            let token = expected_tokens.remove(&(node_id.clone(), group.lane.clone()));

            let handle = Arc::new(
                RunnerHandle::new(
                    exec_id.to_string(),
                    project.id.to_string(),
                    group.color,
                    node_id.clone(),
                    group.lane.clone(),
                    link.cloned(),
                    cancellation.clone(),
                )
                .with_expected_token(token),
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

            ship_node_event(
                link,
                group.color,
                &node_id,
                &group.lane,
                "started",
                Some(&group.input),
                None,
                None,
                &group.pulse_ids,
            )
            .await;

            // Spawn the node's execute as a task. It writes its
            // result back over `result_tx`; the main loop applies
            // the effect on `pulses`/`executions`.
            let tx = result_tx.clone();
            let node_id_task = node_id.clone();
            let color_task = group.color;
            let lane_task = group.lane.clone();
            // node_impl is &'static dyn Node (see NodeCatalog::lookup
            // contract). No allocation or unsafe needed.
            in_flight.spawn(async move {
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
            link,
            &mut waiting,
            phase_scope.as_ref(),
        )
        .await;

        if progress {
            continue;
        }

        // No progress from draining. Check: is anything still in flight?
        if in_flight.is_empty() {
            return terminate(pulses, executions, link, &waiting).await;
        }

        // At least one in-flight task. Block on either the next
        // JoinSet termination or cancellation. DO NOT poll
        // `result_rx` here: `recv().await` would consume the
        // message and drop it, so the next `apply_results` iter
        // would miss it. Let join_next observe the task completing;
        // apply_results on the next iter drains the message.
        tokio::select! {
            _ = in_flight.join_next() => {}
            _ = cancellation.notified() => {
                return Ok(LoopOutcome::Failed { error: "cancelled".into() });
            }
        }
    }
}

/// Drain pending task results and apply them to the state tables.
/// Returns true if any result was drained. `waiting` accumulates
/// tokens of nodes that returned `Suspended`: when the loop finds
/// nothing active to run and this map is non-empty, it stalls the
/// worker so the dispatcher can kill the process and respawn on
/// fire.
async fn apply_results(
    rx: &mut mpsc::UnboundedReceiver<NodeTaskResult>,
    project: &ProjectDefinition,
    edge_idx: &EdgeIndex,
    pulses: &mut PulseTable,
    executions: &mut NodeExecutionTable,
    link: Option<&DispatcherLink>,
    waiting: &mut HashMap<String, (String, weft_core::lane::Lane)>,
    phase_scope: Option<&std::collections::HashSet<String>>,
) -> bool {
    let mut any = false;
    while let Ok(result) = rx.try_recv() {
        any = true;
        match result.outcome {
            NodeTaskOutcome::Completed(output) => {
                mark_completed(executions, &result.node_id, result.color, &result.lane, &output);
                let output_value = output_to_value(&output);
                ship_node_event(
                    link,
                    result.color,
                    &result.node_id,
                    &result.lane,
                    "completed",
                    None,
                    Some(&output_value),
                    None,
                    &[],
                )
                .await;
                postprocess_output(
                    &result.node_id,
                    &output_value,
                    result.color,
                    &result.lane,
                    project,
                    pulses,
                    edge_idx,
                    executions,
                );
                // TriggerSetup: drop any pulses that just landed on
                // nodes outside the trigger-upstream closure. Those
                // nodes' inputs are incomplete by design (their
                // non-scope upstream won't fire) and the engine
                // would otherwise wedge on them.
                if let Some(scope) = phase_scope.as_ref() {
                    drop_out_of_scope_pulses(pulses, scope);
                }
            }
            NodeTaskOutcome::Failed(err) => {
                mark_failed(executions, &result.node_id, result.color, &result.lane, &err);
                ship_node_event(
                    link,
                    result.color,
                    &result.node_id,
                    &result.lane,
                    "failed",
                    None,
                    None,
                    Some(&err),
                    &[],
                )
                .await;
                emit_null_downstream(
                    &result.node_id,
                    result.color,
                    &result.lane,
                    project,
                    pulses,
                    edge_idx,
                    executions,
                );
            }
            NodeTaskOutcome::Waiting(token) => {
                mark_waiting(
                    executions,
                    &result.node_id,
                    result.color,
                    &result.lane,
                    &token,
                );
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
    link: Option<&DispatcherLink>,
    waiting: &HashMap<String, (String, weft_core::lane::Lane)>,
) -> anyhow::Result<LoopOutcome> {
    let has_waiting = !waiting.is_empty();

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
                if let Some(link) = link {
                    tracing::info!(
                        target: "weft_engine",
                        count = waiting.len(),
                        "nothing active; all remaining work is waiting on signals: stalling"
                    );
                    link.stall().await;
                    return Ok(LoopOutcome::Stalled);
                }
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

// ---------- Mutation helpers (unchanged from Slice 2) ----------

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

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        .filter(|n| n.features.is_trigger || !n.entry_signals.is_empty())
        .map(|n| n.id.clone())
        .collect();
    let mut scope: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut frontier: Vec<String> = triggers;
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
