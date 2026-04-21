//! The pulse loop. Given a project, an entry (or resume), drive the
//! scheduler until completion or suspension.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Notify;

use weft_core::context::{ConfigBag, InputBag, LogLevel};
use weft_core::exec::{
    check_completion, find_ready_nodes, postprocess::{emit_null_downstream, postprocess_output},
    preprocess_input, NodeExecution, NodeExecutionStatus, NodeExecutionTable,
};
use weft_core::node::NodeOutput;
use weft_core::project::EdgeIndex;
use weft_core::pulse::{Pulse, PulseTable};
use weft_core::{Color, ExecutionContext, NodeCatalog, ProjectDefinition};

use crate::context::RunnerHandle;

pub async fn run_loop(
    project: ProjectDefinition,
    catalog: Arc<dyn NodeCatalog>,
    color: Color,
    entry_node: Option<&str>,
    entry_value: Value,
    dispatcher_url: Option<&str>,
    cancellation: Arc<Notify>,
) -> anyhow::Result<()> {
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
        let entry_port = entry_node_def
            .outputs
            .first()
            .map(|p| p.name.clone())
            .unwrap_or_else(|| "value".into());
        pulses
            .entry(entry.to_string())
            .or_default()
            .push(Pulse::new(color, Vec::new(), entry, entry_port, entry_value));
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

            let node_result = node_impl.execute(ctx).await;

            match node_result {
                Ok(output) => {
                    mark_completed(&mut executions, &node_id, group.color, &group.lane, &output);
                    let output_value = output_to_value(&output);
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
                Err(e) => {
                    let err = format!("{e}");
                    tracing::error!(target: "weft_runner", node = %node_id, "{err}");
                    mark_failed(&mut executions, &node_id, group.color, &group.lane, &err);
                    emit_null_downstream(&node_id, group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut executions);
                }
            }
        }
    }

    match check_completion(&pulses, &executions) {
        Some(false) => {
            tracing::info!(target: "weft_runner", exec = %exec_id, "execution completed");
        }
        Some(true) => {
            tracing::warn!(target: "weft_runner", exec = %exec_id, "execution completed with failures");
        }
        None => {
            // Pending pulses remain but no ready nodes: stuck. Log
            // the state so debugging is possible.
            tracing::warn!(target: "weft_runner", exec = %exec_id, pulses = pulses.len(), "execution stuck: pending pulses with no ready nodes");
        }
    }

    Ok(())
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

