//! The pulse loop. Given a project, an entry (or resume), drive the
//! scheduler until completion or suspension.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Notify;

use weft_core::context::{ConfigBag, InputBag};
use weft_core::exec::{
    check_completion, find_ready_nodes, postprocess::{emit_null_downstream, postprocess_output},
    preprocess_input, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use weft_core::node::NodeOutput;
use weft_core::project::EdgeIndex;
use weft_core::pulse::{Pulse, PulseTable};
use weft_core::{Color, ExecutionContext, NodeCatalog, ProjectDefinition};

use crate::context::{ship_node_event, RunnerHandle};

pub enum EntryMode {
    /// Fresh run: pulse targets the entry node's first input port
    /// so the node fires and its output propagates.
    Fresh,
    /// Resume after a suspension: the node already ran up to the
    /// suspension point. Inject `entry_value` as the node's output,
    /// skipping the node body. Downstream reacts as if the suspension
    /// completed with `entry_value`.
    Resume,
}

/// Outcome the loop reports back to the binary wrapper. The
/// wrapper uses this to POST the final status to the dispatcher
/// (so `weft follow` sees completed/failed/suspended).
#[derive(Debug, Clone)]
pub enum LoopOutcome {
    Completed { outputs: Value },
    Failed { error: String },
    Suspended { token: String },
    /// Scheduler ran to quiescence but some pulses remain pending.
    /// Means a graph shape bug or a readiness check missed something;
    /// report as failure so the user notices.
    Stuck,
}

pub async fn run_loop(
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    color: Color,
    entry_node: Option<&str>,
    entry_value: Value,
    entry_mode: EntryMode,
    dispatcher_url: Option<&str>,
    cancellation: Arc<Notify>,
) -> anyhow::Result<LoopOutcome> {
    // cancellation lives on the runner handle; passed to each node's
    // ExecutionContext.
    let _ = &cancellation;
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = Default::default();
    let mut executions: NodeExecutionTable = Default::default();

    // Seed entry pulse.
    if let Some(entry) = entry_node {
        let entry_node_def = project
            .nodes
            .iter()
            .find(|n| n.id == entry)
            .ok_or_else(|| anyhow::anyhow!("entry node '{entry}' not found"))?;
        match entry_mode {
            EntryMode::Fresh => {
                let entry_port = entry_node_def
                    .inputs
                    .first()
                    .map(|p| p.name.clone())
                    .unwrap_or_else(|| "body".into());
                pulses
                    .entry(entry.to_string())
                    .or_default()
                    .push(Pulse::new(color, Vec::new(), entry, entry_port, entry_value));
            }
            EntryMode::Resume => {
                // Seed the entry node's output ports with
                // entry_value (the form submission, timer wake, etc)
                // so downstream fires as if the suspension returned.
                for port in &entry_node_def.outputs {
                    let outgoing = edge_idx.get_outgoing(&project, entry);
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
            }
        }
    }

    let exec_id = uuid::Uuid::new_v4().to_string();

    loop {
        // Preprocess (Expand/Gather). May add pulses; pulls out
        // expanded/gathered versions.
        preprocess_input(&project, &mut pulses);

        // Find ready nodes.
        let ready = find_ready_nodes(&project, &pulses, &edge_idx);
        if ready.is_empty() {
            break;
        }

        for (node_id, group) in ready {
            let node_def = match project.nodes.iter().find(|n| n.id == node_id) {
                Some(n) => n,
                None => continue,
            };

            // Absorb input pulses.
            if let Some(bucket) = pulses.get_mut(&node_id) {
                for p in bucket.iter_mut() {
                    if group.pulse_ids.contains(&p.id) {
                        p.absorb();
                    }
                }
            }

            let dispatch_pulse_id = uuid::Uuid::new_v4();
            let execution_record = NodeExecution {
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
            executions.entry(node_id.clone()).or_default().push(execution_record);

            if group.should_skip {
                mark_skipped(&mut executions, &node_id, group.color, &group.lane);
                ship_node_event(
                    dispatcher_url,
                    group.color,
                    &node_id,
                    &group.lane,
                    "skipped",
                    None,
                    None,
                    None,
                );
                emit_null_downstream(
                    &node_id,
                    group.color,
                    &group.lane,
                    &project,
                    &mut pulses,
                    &edge_idx,
                    &mut executions,
                );
                continue;
            }

            if let Some(err) = &group.error {
                mark_failed(&mut executions, &node_id, group.color, &group.lane, err);
                ship_node_event(
                    dispatcher_url,
                    group.color,
                    &node_id,
                    &group.lane,
                    "failed",
                    None,
                    None,
                    Some(err),
                );
                emit_null_downstream(
                    &node_id,
                    group.color,
                    &group.lane,
                    &project,
                    &mut pulses,
                    &edge_idx,
                    &mut executions,
                );
                continue;
            }

            // Build ExecutionContext and call the node.
            let node_impl = match catalog.lookup(&node_def.node_type) {
                Some(n) => n,
                None => {
                    let err = format!("unknown node type: {}", node_def.node_type);
                    mark_failed(&mut executions, &node_id, group.color, &group.lane, &err);
                    ship_node_event(
                        dispatcher_url,
                        group.color,
                        &node_id,
                        &group.lane,
                        "failed",
                        None,
                        None,
                        Some(&err),
                    );
                    emit_null_downstream(&node_id, group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut executions);
                    continue;
                }
            };

            let config = ConfigBag { values: node_def.config.as_object().cloned().unwrap_or_default().into_iter().collect() };
            let input = InputBag { values: group.input.as_object().cloned().unwrap_or_default().into_iter().collect() };

            let handle = Arc::new(RunnerHandle::new(
                exec_id.clone(),
                project.id.to_string(),
                group.color,
                node_id.clone(),
                dispatcher_url.map(str::to_string),
                cancellation.clone(),
            )) as Arc<dyn weft_core::context::ContextHandle>;

            let ctx = ExecutionContext::new(
                exec_id.clone(),
                project.id.to_string(),
                node_id.clone(),
                node_def.node_type.clone(),
                group.color,
                group.lane.clone(),
                config,
                input,
                handle,
            );

            ship_node_event(
                dispatcher_url,
                group.color,
                &node_id,
                &group.lane,
                "started",
                Some(&group.input),
                None,
                None,
            );

            let node_result = node_impl.execute(ctx).await;

            match node_result {
                Ok(output) => {
                    mark_completed(&mut executions, &node_id, group.color, &group.lane, &output);
                    let output_value = output_to_value(&output);
                    ship_node_event(
                        dispatcher_url,
                        group.color,
                        &node_id,
                        &group.lane,
                        "completed",
                        None,
                        Some(&output_value),
                        None,
                    );
                    postprocess_output(
                        &node_id,
                        &output_value,
                        group.color,
                        &group.lane,
                        &project,
                        &mut pulses,
                        &edge_idx,
                        &mut executions,
                    );
                }
                Err(weft_core::error::WeftError::Suspended { token }) => {
                    tracing::info!(
                        target: "weft_runner",
                        node = %node_id, token = %token,
                        "execution suspended; worker exiting cleanly"
                    );
                    mark_waiting(&mut executions, &node_id, group.color, &group.lane, &token);
                    return Ok(LoopOutcome::Suspended { token });
                }
                Err(e) => {
                    let err = format!("{e}");
                    tracing::error!(target: "weft_runner", node = %node_id, "{err}");
                    mark_failed(&mut executions, &node_id, group.color, &group.lane, &err);
                    ship_node_event(
                        dispatcher_url,
                        group.color,
                        &node_id,
                        &group.lane,
                        "failed",
                        None,
                        None,
                        Some(&err),
                    );
                    emit_null_downstream(&node_id, group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut executions);
                }
            }
        }
    }

    match check_completion(&pulses, &executions) {
        Some(false) => {
            tracing::info!(target: "weft_runner", exec = %exec_id, "execution completed");
            Ok(LoopOutcome::Completed { outputs: final_outputs(&executions) })
        }
        Some(true) => {
            tracing::warn!(target: "weft_runner", exec = %exec_id, "execution completed with failures");
            Ok(LoopOutcome::Failed { error: first_failure(&executions).unwrap_or_else(|| "node(s) failed".into()) })
        }
        None => {
            tracing::warn!(target: "weft_runner", exec = %exec_id, pulses = pulses.len(), "execution stuck: pending pulses with no ready nodes");
            Ok(LoopOutcome::Stuck)
        }
    }
}

/// Collect the last output value per node, keyed by node id. Gives
/// the dispatcher something meaningful to publish on
/// ExecutionCompleted.
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
                if let Some(err) = &e.error {
                    return Some(format!("{}: {}", e.node_id, err));
                }
                return Some(format!("{}: failed", e.node_id));
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

