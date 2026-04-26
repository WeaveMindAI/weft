//! Pure executor logic,no Restate dependency.
//!
//! All functions here operate on plain data structures (ProjectDefinition,
//! PulseTable, EdgeIndex, etc.) and return results. Used by the axum-based
//! in-memory executor.

use std::collections::{BTreeMap, HashMap};
use serde::{Deserialize, Serialize};
use crate::{ProjectDefinition, NodeDefinition, WeftType};
use crate::project::{LaneMode, EdgeIndex};

// =============================================================================
// REQUEST / RESPONSE TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct ProjectExecutionRequest {
    pub project: ProjectDefinition,
    pub input: serde_json::Value,
    #[serde(default)]
    pub userId: Option<String>,
    #[serde(default)]
    pub statusCallbackUrl: Option<String>,
    #[serde(default)]
    pub isInfraSetup: bool,
    #[serde(default)]
    pub isTriggerSetup: bool,
    #[serde(default)]
    pub weftCode: Option<String>,
    #[serde(default)]
    pub testMode: bool,
    /// Optional metadata for the executions row: which trigger fired this run.
    /// Internal callers (webhook handler, trigger poll, publish) set these.
    /// Dashboard manual runs leave them None.
    #[serde(default)]
    pub triggerId: Option<String>,
    #[serde(default)]
    pub nodeType: Option<String>,
    /// Mock overrides from test configs. Keys are node/group IDs,
    /// values are objects mapping output port names to mock values.
    /// When a node ID appears here, the executor skips real execution
    /// and emits the mock data. For group IDs, all internal nodes are skipped.
    #[serde(default)]
    pub mocks: Option<std::collections::HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectExecutionResult {
    pub executionId: String,
    pub status: String,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvideInputRequest {
    pub nodeId: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub pulseId: String,
    #[serde(default)]
    pub skip: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct NodeStatusMap {
    pub statuses: HashMap<String, String>,
    /// Epoch millis when each node first became active (Running, Skipped, Failed).
    /// Used by the frontend to sort statuses in execution order.
    #[serde(default)]
    pub ordering: HashMap<String, u64>,
    /// Edge IDs that currently have Pending pulses flowing through them.
    #[serde(default)]
    pub activeEdges: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOutputMap {
    pub outputs: HashMap<String, serde_json::Value>,
}

// =============================================================================
// TASK TYPES (shared by both executor variants)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum TaskType {
    #[default]
    Task,
    Action,
    Trigger,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct FormField {
    pub fieldType: String,
    pub key: String,
    #[serde(default)]
    pub render: serde_json::Value,
    #[serde(default)]
    pub value: Option<serde_json::Value>,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormSchema {
    pub fields: Vec<FormField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct PendingTask {
    pub executionId: String,
    pub nodeId: String,
    pub title: String,
    pub description: Option<String>,
    pub data: serde_json::Value,
    pub createdAt: String,
    pub userId: Option<String>,
    #[serde(default)]
    pub taskType: TaskType,
    #[serde(default)]
    pub actionUrl: Option<String>,
    #[serde(default)]
    pub formSchema: Option<FormSchema>,
    /// Free-form metadata for consumer filtering (e.g., { "source": "human" }).
    /// Consumers query by metadata to decide which tasks they handle.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTasksList {
    pub tasks: Vec<PendingTask>,
}


// =============================================================================
// NODE EXECUTION MODEL
// =============================================================================

/// Status of a single node execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionStatus {
    Running,
    Completed,
    Failed,
    WaitingForInput,
    Skipped,
    Cancelled,
}

impl NodeExecutionStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, NodeExecutionStatus::Completed | NodeExecutionStatus::Failed | NodeExecutionStatus::Cancelled | NodeExecutionStatus::Skipped)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            NodeExecutionStatus::Running => "running",
            NodeExecutionStatus::Completed => "completed",
            NodeExecutionStatus::Failed => "failed",
            NodeExecutionStatus::WaitingForInput => "waiting_for_input",
            NodeExecutionStatus::Skipped => "skipped",
            NodeExecutionStatus::Cancelled => "cancelled",
        }
    }
}

/// Record of a single execution of a node.
///
/// Created when a node is dispatched (or skipped/failed at dispatch time).
/// Updated when the node completes, fails, or enters WaitingForInput.
/// Pulses remain pure data carriers; all execution metadata lives here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct NodeExecution {
    pub id: String,
    pub nodeId: String,
    pub status: NodeExecutionStatus,
    /// Input pulses consumed by this execution.
    pub pulseIdsAbsorbed: Vec<String>,
    /// The "running" pulse created for this execution (used for callback routing).
    pub pulseId: String,
    pub error: Option<String>,
    /// Callback ID for human-in-the-loop routing.
    pub callbackId: Option<String>,
    /// Id of the node-runner instance holding the in-memory form-input channel.
    #[serde(default)]
    pub runnerInstanceId: Option<String>,
    pub startedAt: u64,
    pub completedAt: Option<u64>,
    /// Node input value (the data the node received).
    pub input: Option<serde_json::Value>,
    /// Node output value (for dashboard display).
    pub output: Option<serde_json::Value>,
    /// Accumulated cost in USD during this execution.
    pub costUsd: f64,
    /// Structured log entries. Schema defined by the node type.
    pub logs: Vec<serde_json::Value>,
    /// Flow color (for correlation with pulses).
    pub color: String,
    /// Lane context (for ForEach tracking).
    pub lane: Vec<SplitFrame>,
}

/// Node executions keyed by node_id, ordered by creation time.
pub type NodeExecutionTable = BTreeMap<String, Vec<NodeExecution>>;

/// Summary status for a node derived from its executions.
/// Returns a string like "completed" or "completed (15 completed, 15 failed)" for multi-execution nodes.
pub fn node_execution_summary(executions: &[NodeExecution]) -> String {
    if executions.is_empty() {
        return "pending".to_string();
    }

    let total = executions.len();
    let running = executions.iter().filter(|e| matches!(e.status, NodeExecutionStatus::Running | NodeExecutionStatus::WaitingForInput)).count();
    let failed = executions.iter().filter(|e| e.status == NodeExecutionStatus::Failed).count();
    let completed = executions.iter().filter(|e| e.status == NodeExecutionStatus::Completed).count();
    let skipped = executions.iter().filter(|e| e.status == NodeExecutionStatus::Skipped).count();


    let cancelled = executions.iter().filter(|e| e.status == NodeExecutionStatus::Cancelled).count();

    let base = if running > 0 {
        "running"
    } else if cancelled == total {
        "cancelled"
    } else if skipped == total {
        "skipped"
    } else if failed > 0 && completed == 0 {
        "failed"
    } else if failed > 0 {
        "completed"
    } else {
        "completed"
    };

    // Single execution: just the base status
    if total <= 1 {
        return base.to_string();
    }

    // Multiple executions: include breakdown
    let mut parts = Vec::new();
    if completed > 0 { parts.push(format!("{completed} completed")); }
    if failed > 0 { parts.push(format!("{failed} failed")); }
    if running > 0 { parts.push(format!("{running} running")); }
    if skipped > 0 { parts.push(format!("{skipped} skipped")); }
    if cancelled > 0 { parts.push(format!("{cancelled} cancelled")); }

    format!("{base} ({total} executions: {parts})", parts = parts.join(", "))
}

// =============================================================================
// PULSE MODEL
// =============================================================================

/// Pulse lifecycle: data is either waiting to be consumed (Pending) or has been consumed (Absorbed).
/// All execution metadata (running, completed, failed, etc.) lives on NodeExecution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PulseStatus {
    /// Data is here, waiting to be consumed by a node dispatch or preprocessing.
    Pending,
    /// Data was consumed (by dispatch, Expand split, Gather collect, or cancellation).
    Absorbed,
}

impl PulseStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PulseStatus::Pending => "pending",
            PulseStatus::Absorbed => "absorbed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SplitFrame {
    pub count: u32,
    pub index: u32,
}

/// A pulse is a unit of data flowing through the execution graph.
/// Pulses carry data between nodes. They do not carry execution metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pulse {
    pub id: String,
    pub color: String,
    pub lane: Vec<SplitFrame>,
    pub status: PulseStatus,
    pub data: serde_json::Value,
    #[serde(default)]
    pub port: Option<String>,
    /// True when this pulse was synthesized by Gather preprocessing.
    /// Prevents the same pulse from being re-gathered on subsequent passes.
    #[serde(default)]
    pub gathered: bool,
}

impl Pulse {
    pub fn new(color: String, lane: Vec<SplitFrame>, data: serde_json::Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            color,
            lane,
            status: PulseStatus::Pending,
            data,
            port: None,
            gathered: false,
        }
    }

    pub fn new_on_port(color: String, lane: Vec<SplitFrame>, data: serde_json::Value, port: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            color,
            lane,
            status: PulseStatus::Pending,
            data,
            port: Some(port),
            gathered: false,
        }
    }
}

pub type PulseTable = BTreeMap<String, Vec<Pulse>>;

// =============================================================================
// READINESS LOGIC, pure functions
// =============================================================================

/// A ready group: inputs aggregated for a node, ready to dispatch.
pub struct ReadyGroup {
    pub lane: Vec<SplitFrame>,
    pub color: String,
    pub input: serde_json::Value,
    pub should_skip: bool,
    pub pulse_ids: Vec<String>,
    pub error: Option<String>,
}

/// Input preprocessing: mutate the pulse table in-place so that all Pending
/// pulses on a node end up at compatible lane depths before readiness checking.
///
/// Two transformations, applied per node:
///
/// 1. **Expand input**: a Pending pulse on an Expand port carrying a list is
///    replaced by N child-lane Pending pulses (one per item). The original is
///    Absorbed. Child pulses carry scalar values, so they won't be re-expanded.
///
/// 2. **Gather input**: when all sibling Pending pulses for a Gather port at the
///    deepest lane level have arrived (count == expected), they are replaced by a
///    single parent-lane Pending pulse carrying a list. The originals are Absorbed.
///
/// After both transformations, `find_normal_ready_groups` can match all ports by
/// lane without any special Gather-aware logic.
///
/// Returns true if any transformation was applied (caller should re-run readiness).
/// Recursively expand a nested list into leaf items with their lane paths.
/// For depth=1: [a, b, c] → [(frame(3,0), a), (frame(3,1), b), (frame(3,2), c)]
/// For depth=2: [[a,b],[c]] → [(f(2,0).f(2,0), a), (f(2,0).f(2,1), b), (f(1,0).f(1,0), c)]
fn expand_recursive(
    data: &serde_json::Value,
    depth: u32,
) -> Vec<(Vec<SplitFrame>, serde_json::Value)> {
    if depth == 0 {
        return vec![(vec![], data.clone())];
    }
    let items = match data.as_array() {
        Some(arr) if !arr.is_empty() => arr,
        _ => {
            // Null or non-array or empty: treat as single null item
            return vec![(vec![SplitFrame { count: 1, index: 0 }], data.clone())];
        }
    };
    let n = items.len() as u32;
    let mut results = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let frame = SplitFrame { count: n, index: i as u32 };
        if depth == 1 {
            results.push((vec![frame], item.clone()));
        } else {
            // Recurse: expand the next level
            let sub_items = expand_recursive(item, depth - 1);
            for (mut sub_lane, value) in sub_items {
                let mut full_lane = vec![frame.clone()];
                full_lane.append(&mut sub_lane);
                results.push((full_lane, value));
            }
        }
    }
    results
}

pub fn preprocess_input(
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
) -> bool {
    let mut changed = false;

    // Collect all mutations first (read phase), then apply them (write phase)
    // to satisfy the borrow checker.

    struct ExpandWork {
        node_id: String,
        absorb_id: String,
        port: String,
        color: String,
        base_lane: Vec<SplitFrame>,
        /// Each leaf item is (lane_suffix, value) where lane_suffix contains
        /// the SplitFrames to append to base_lane.
        leaf_items: Vec<(Vec<SplitFrame>, serde_json::Value)>,
    }

    struct GatherWork {
        node_id: String,
        absorb_ids: Vec<String>,
        port: String,
        color: String,
        parent_lane: Vec<SplitFrame>,
        gathered_list: Vec<serde_json::Value>,
    }

    let mut expand_work: Vec<ExpandWork> = Vec::new();
    let mut gather_work: Vec<GatherWork> = Vec::new();

    for node in &project.nodes {
        let expand_ports: Vec<&str> = node.inputs.iter()
            .filter(|p| p.laneMode == LaneMode::Expand)
            .map(|p| p.name.as_str())
            .collect();
        let gather_ports: Vec<&str> = node.inputs.iter()
            .filter(|p| p.laneMode == LaneMode::Gather)
            .map(|p| p.name.as_str())
            .collect();

        if expand_ports.is_empty() && gather_ports.is_empty() {
            continue;
        }

        let node_pulses = match pulses.get(&node.id) {
            Some(ps) => ps,
            None => continue,
        };

        // --- Expand input: find list pulses to split ---
        for port_name in &expand_ports {
            let lane_depth = node.inputs.iter()
                .find(|p| p.name == *port_name)
                .map(|p| p.laneDepth.max(1))
                .unwrap_or(1);

            let to_expand: Vec<(String, String, Vec<SplitFrame>, serde_json::Value)> = node_pulses.iter()
                .filter(|p| {
                    p.status == PulseStatus::Pending
                        && p.port.as_deref() == Some(port_name)
                        && p.data.is_array()
                })
                .filter_map(|p| {
                    if p.data.as_array().map(|a| a.is_empty()).unwrap_or(true) { return None; }
                    Some((p.id.clone(), p.color.clone(), p.lane.clone(), p.data.clone()))
                })
                .collect();

            for (pulse_id, color, lane, data) in to_expand {
                // Recursively expand multi-level: each level peels one List[] wrapper
                let leaf_items = expand_recursive(&data, lane_depth);

                tracing::debug!("[preprocess_input] node={} Expand port={} depth={} leaves={} base_lane={:?}",
                    node.id, port_name, lane_depth, leaf_items.len(), lane);

                expand_work.push(ExpandWork {
                    node_id: node.id.clone(),
                    absorb_id: pulse_id,
                    port: port_name.to_string(),
                    color,
                    base_lane: lane,
                    leaf_items,
                });
            }
        }

        // --- Gather input: find complete sibling groups ---
        for port_name in &gather_ports {
            let mut groups: HashMap<(String, Vec<SplitFrame>, u32), Vec<(u32, String, serde_json::Value)>> =
                HashMap::new();
            for p in node_pulses.iter().filter(|p| {
                p.status == PulseStatus::Pending
                    && p.port.as_deref() == Some(port_name)
                    && !p.lane.is_empty()
                    && !p.gathered // skip pulses already synthesized by gather preprocessing
            }) {
                let parent_lane = p.lane[..p.lane.len() - 1].to_vec();
                let frame = p.lane.last().unwrap();
                groups.entry((p.color.clone(), parent_lane, frame.count))
                    .or_default()
                    .push((frame.index, p.id.clone(), p.data.clone()));
            }

            for ((color, parent_lane, expected_count), mut siblings) in groups {
                siblings.sort_by_key(|(idx, _, _)| *idx);
                siblings.dedup_by_key(|(idx, _, _)| *idx);

                if (siblings.len() as u32) < expected_count {
                    continue;
                }

                // Check no gathered pulse already exists at parent_lane
                let already_gathered = node_pulses.iter().any(|p| {
                    p.color == color && p.lane == parent_lane
                        && p.port.as_deref() == Some(port_name)
                        && matches!(p.status, PulseStatus::Pending)
                });
                if already_gathered { continue; }

                let gathered_list: Vec<serde_json::Value> = siblings.iter()
                    .map(|(_, _, data)| data.clone())
                    .collect();
                tracing::debug!("[preprocess_input] node={} Gather port={} count={} parent_lane={:?}",
                    node.id, port_name, expected_count, parent_lane);
                let absorb_ids: Vec<String> = siblings.iter().map(|(_, id, _)| id.clone()).collect();

                gather_work.push(GatherWork {
                    node_id: node.id.clone(),
                    absorb_ids,
                    port: port_name.to_string(),
                    color,
                    parent_lane,
                    gathered_list,
                });
            }
        }
    }

    // --- Apply expand mutations ---
    for w in expand_work {
        if let Some(ps) = pulses.get_mut(&w.node_id) {
            if let Some(p) = ps.iter_mut().find(|p| p.id == w.absorb_id) {
                p.status = PulseStatus::Absorbed;
            }
        }

        // Find the port's declared type for post-expand type checking
        let port_type = project.nodes.iter()
            .find(|n| n.id == w.node_id)
            .and_then(|nd| nd.inputs.iter().find(|p| p.name == w.port))
            .map(|p| &p.portType);

        let ps = pulses.entry(w.node_id.clone()).or_default();
        for (lane_suffix, item) in &w.leaf_items {
            let mut child_lane = w.base_lane.clone();
            child_lane.extend_from_slice(lane_suffix);

            // Type check each expanded leaf against declared type (post-expand = element type)
            let checked_item = if let Some(pt) = port_type {
                if !item.is_null() && !pt.is_unresolved() && !runtime_type_check(pt, item) {
                    tracing::error!(
                        "[runtime_type_check] node={} expand input port '{}' lane {:?}: expected {}, got {}",
                        w.node_id, w.port, child_lane, pt, WeftType::infer(item),
                    );
                    serde_json::Value::Null
                } else {
                    item.clone()
                }
            } else {
                item.clone()
            };

            ps.push(Pulse::new_on_port(
                w.color.clone(),
                child_lane,
                checked_item,
                w.port.clone(),
            ));
        }
        changed = true;
    }

    // --- Apply gather mutations ---
    for w in gather_work {
        if let Some(ps) = pulses.get_mut(&w.node_id) {
            for p in ps.iter_mut() {
                if w.absorb_ids.contains(&p.id) {
                    p.status = PulseStatus::Absorbed;
                }
            }
        }
        let gathered_value = serde_json::Value::Array(w.gathered_list);

        // Type check the gathered list against declared type (post-gather = collected list)
        let checked_value = if let Some(port_def) = project.nodes.iter()
            .find(|n| n.id == w.node_id)
            .and_then(|n| n.inputs.iter().find(|p| p.name == w.port))
        {
            if !port_def.portType.is_unresolved() && !runtime_type_check(&port_def.portType, &gathered_value) {
                tracing::error!(
                    "[runtime_type_check] node={} gather input port '{}': expected {}, got {}",
                    w.node_id, w.port, port_def.portType, WeftType::infer(&gathered_value),
                );
                // Don't null it here : the node should still see the data and the error
                // will be caught by the downstream input check
                gathered_value
            } else {
                gathered_value
            }
        } else {
            gathered_value
        };

        let ps = pulses.entry(w.node_id).or_default();
        let mut gathered_pulse = Pulse::new_on_port(
            w.color,
            w.parent_lane,
            checked_value,
            w.port,
        );
        gathered_pulse.gathered = true;
        ps.push(gathered_pulse);
        changed = true;
    }

    changed
}

/// Find all ready groups across all nodes. Returns (node_id, ReadyGroup) pairs.
pub fn find_ready_nodes(
    project: &ProjectDefinition,
    pulses: &PulseTable,
    initial_input: &serde_json::Value,
    edge_idx: &EdgeIndex,
) -> Vec<(String, ReadyGroup)> {
    let mut result = Vec::new();

    for node in &project.nodes {
        let node_pulses = match pulses.get(&node.id) {
            Some(ps) => ps,
            None => continue,
        };

        let pending: Vec<&Pulse> = node_pulses.iter()
            .filter(|p| p.status == PulseStatus::Pending)
            .collect();
        if pending.is_empty() {
            continue;
        }

        let incoming_edges = edge_idx.get_incoming(project, &node.id);
        let has_incoming = !incoming_edges.is_empty();

        let required_ports: std::collections::HashSet<&str> = node.inputs.iter()
            .filter(|p| p.required)
            .map(|p| p.name.as_str())
            .collect();

        // wired_ports: ports with an incoming edge (need a pulse to be satisfied).
        // config_filled_ports: ports satisfied by a same-named config value on a
        // configurable port, no edge required. The union is what the skip check
        // uses; the readiness check only waits for wired_ports.
        let wired_ports: std::collections::HashSet<&str> = incoming_edges.iter()
            .map(|e| e.targetHandle.as_deref().unwrap_or("default"))
            .collect();
        let mut config_filled_ports: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for port in &node.inputs {
            if !port.configurable { continue; }
            if wired_ports.contains(port.name.as_str()) { continue; }
            let has_config = node.config.get(&port.name)
                .map(|v| !v.is_null())
                .unwrap_or(false);
            if has_config {
                config_filled_ports.insert(port.name.as_str());
            }
        }

        // Gather/Expand input preprocessing has already normalized all pulses
        // to compatible depths, so find_normal_ready_groups handles everything.
        let groups = find_normal_ready_groups(node, node_pulses, &required_ports, &wired_ports, &config_filled_ports, initial_input, has_incoming);
        for g in groups {
            result.push((node.id.clone(), g));
        }
    }

    result
}

/// Find ready groups for a node.
/// Pulses on Gather input ports that haven't been gathered yet (still at
/// child-lane depth) are excluded. Only the parent-lane pulse produced by
/// `preprocess_input` Gather collapsing is visible to readiness.
pub fn find_normal_ready_groups(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    required_ports: &std::collections::HashSet<&str>,
    wired_ports: &std::collections::HashSet<&str>,
    config_filled_ports: &std::collections::HashSet<&str>,
    initial_input: &serde_json::Value,
    has_incoming: bool,
) -> Vec<ReadyGroup> {
    let gather_port_names: std::collections::HashSet<&str> = node.inputs.iter()
        .filter(|p| p.laneMode == LaneMode::Gather)
        .map(|p| p.name.as_str())
        .collect();

    let pending: Vec<&Pulse> = node_pulses.iter()
        .filter(|p| p.status == PulseStatus::Pending)
        .filter(|p| {
            // Exclude ungathered pulses on Gather ports.
            // Only gather-synthesized pulses (gathered=true, produced by
            // preprocess_input) are visible to readiness. Ungathered pulses
            // are still waiting for siblings and must not trigger dispatch.
            if let Some(port) = p.port.as_deref() {
                if gather_port_names.contains(port) {
                    return p.gathered;
                }
            }
            true
        })
        .collect();

    let mut groups: std::collections::HashMap<(String, Vec<SplitFrame>), Vec<&Pulse>> = std::collections::HashMap::new();
    for p in &pending {
        groups.entry((p.color.clone(), p.lane.clone())).or_default().push(p);
    }

    // Suppress broadcast lanes if deeper lanes exist for the same color.
    let all_keys: Vec<(String, Vec<SplitFrame>)> = groups.keys().cloned().collect();
    let suppressed: std::collections::HashSet<(String, Vec<SplitFrame>)> = all_keys.iter()
        .filter(|(color_a, lane_a)| {
            all_keys.iter().any(|(color_b, lane_b)| {
                color_a == color_b && lane_a.len() < lane_b.len()
                    && lane_b[..lane_a.len()] == lane_a[..]
            })
        })
        .cloned()
        .collect();

    let mut ready = Vec::new();

    for ((color, lane), group_pulses) in &groups {
        if suppressed.contains(&(color.clone(), lane.clone())) {
            continue;
        }

        let all_satisfied = wired_ports.iter().all(|port_name| {
            let has_exact = group_pulses.iter().any(|p| p.port.as_deref() == Some(port_name));
            if has_exact { return true; }
            if !lane.is_empty() {
                let has_broadcast = node_pulses.iter().any(|p| {
                    p.status == PulseStatus::Pending && p.color == *color
                        && p.lane.len() < lane.len()
                        && lane[..p.lane.len()] == p.lane[..]
                        && p.port.as_deref() == Some(port_name)
                });
                if has_broadcast { return true; }
            }
            false
        });

        if has_incoming && !all_satisfied {
            continue;
        }

        let mut type_errors: Vec<String> = Vec::new();
        let input = build_input_from_pulses(node, node_pulses, lane, color, initial_input, has_incoming, &mut type_errors);

        // Group boundary skip rules:
        //   - The In boundary runs the normal skip check using the group's
        //     required/oneOfRequired metadata (copied from the group signature
        //     onto the passthrough's ports + features at compile time). If
        //     any required group input is null, or all ports in a oneOfRequired
        //     group are null, the entire group body is skipped as a unit. The
        //     orchestrator handles the fan-out to inner nodes and the Out
        //     passthrough.
        //   - The Out boundary never decides to skip on its own: it forwards
        //     whatever the inner nodes produced. If the whole group was skipped
        //     at the In boundary, the orchestrator will have already emitted
        //     null on the Out boundary's outputs.
        let is_out_boundary = node.groupBoundary.as_ref()
            .map(|gb| gb.role == crate::project::GroupBoundaryRole::Out)
            .unwrap_or(false);
        let should_skip = if is_out_boundary {
            false
        } else if has_incoming {
            check_should_skip(node, node_pulses, lane, color, required_ports, wired_ports, config_filled_ports)
        } else {
            false
        };

        let pulse_ids: Vec<String> = group_pulses.iter()
            .filter(|p| p.lane == *lane)
            .map(|p| p.id.clone())
            .collect();

        let error = if type_errors.is_empty() {
            None
        } else {
            Some(type_errors.join("; "))
        };

        ready.push(ReadyGroup {
            lane: lane.clone(),
            color: color.clone(),
            input,
            should_skip,
            pulse_ids,
            error,
        });
    }

    // Shape mismatch detection
    // Two lanes are "compatible" if one is a prefix of the other with matching
    // SplitFrame.count at every shared level (broadcast relationship), or both are empty.
    // A mismatch means pulses on different ports come from incompatible ForEach branches
    // and can never be grouped together.
    if ready.is_empty() && wired_ports.len() > 1 {
        let mut port_lanes: std::collections::HashMap<&str, Vec<&Vec<SplitFrame>>> =
            std::collections::HashMap::new();
        for p in &pending {
            if let Some(port) = p.port.as_deref() {
                if wired_ports.contains(port) {
                    port_lanes.entry(port).or_default().push(&p.lane);
                }
            }
        }

        let ports_with_lanes: Vec<(&str, &Vec<&Vec<SplitFrame>>)> = port_lanes.iter()
            .map(|(p, ls)| (*p, ls))
            .collect();

        // Check all port pairs for compatibility
        let mut mismatch_found = false;
        if ports_with_lanes.len() > 1 {
            'outer: for i in 0..ports_with_lanes.len() {
                for j in (i + 1)..ports_with_lanes.len() {
                    let lanes_a = ports_with_lanes[i].1;
                    let lanes_b = ports_with_lanes[j].1;
                    // Two ports are compatible if ANY lane from A is compatible with ANY lane from B
                    let any_compatible = lanes_a.iter().any(|la| {
                        lanes_b.iter().any(|lb| lanes_are_compatible(la, lb))
                    });
                    if !any_compatible {
                        mismatch_found = true;
                        break 'outer;
                    }
                }
            }
        }

        if mismatch_found {
            let shape_detail: Vec<String> = port_lanes.iter()
                .map(|(port, lanes)| {
                    let lane_strs: Vec<String> = lanes.iter().map(|l| {
                        if l.is_empty() {
                            "scalar (depth 0)".to_string()
                        } else {
                            let parts: Vec<String> = l.iter().map(|f| format!("{}:{}", f.count, f.index)).collect();
                            format!("[{}]", parts.join(", "))
                        }
                    }).collect();
                    let unique: std::collections::HashSet<String> = lane_strs.into_iter().collect();
                    format!("port '{}': {}", port, unique.into_iter().collect::<Vec<_>>().join(" | "))
                })
                .collect();
            let error_msg = format!(
                "Shape mismatch: inputs from incompatible ForEach branches ({}). \
                 Pulses on different ports come from ForEach nodes with different sizes and cannot be matched.",
                shape_detail.join("; ")
            );
            tracing::error!("[SHAPE MISMATCH] node={}: {}", node.id, error_msg);
            let all_pending_ids: Vec<String> = pending.iter().map(|p| p.id.clone()).collect();
            ready.push(ReadyGroup {
                lane: vec![],
                color: pending.first().map(|p| p.color.clone()).unwrap_or_default(),
                input: serde_json::Value::Null,
                should_skip: false,
                pulse_ids: all_pending_ids,
                error: Some(error_msg),
            });
        }
    }

    ready
}


/// Runtime type check for a port value, accounting for Expand/Gather lane modes.
///
/// For **Single** ports, the value must match the declared type directly.
///
/// For **Expand** and **Gather** ports, the declared type is the **element type**
/// (what each individual item is). The Expand/Gather mechanism wraps/unwraps
/// lists automatically. The runtime accepts either shape:
///   - The declared element type (e.g. String : a single element)
///   - Array (the collection before expansion / after gathering)
///
/// If neither matches, the check fails.
/// Runtime type check. Always checks against the post-transform declared type.
/// For Expand/Gather ports, the caller must ensure the value is already in post-transform form
/// (i.e., check AFTER expand splits elements, AFTER gather collects the list).
pub fn runtime_type_check(
    port_type: &crate::project::WeftType,
    value: &serde_json::Value,
) -> bool {
    use crate::weft_type::WeftType;
    if port_type.is_unresolved() { return true; }
    let inferred = WeftType::infer(value);
    WeftType::is_compatible(&inferred, port_type)
}

/// Build aggregated input for a normal (non-Gather) node from its local pulses.
pub fn build_input_from_pulses(
    node: &NodeDefinition,
    node_pulses: &[Pulse],
    lane: &[SplitFrame],
    color: &str,
    initial_input: &serde_json::Value,
    has_incoming: bool,
    type_errors: &mut Vec<String>,
) -> serde_json::Value {
    let mut input_obj = serde_json::Map::new();

    for p in node_pulses.iter().filter(|p| p.status == PulseStatus::Pending && p.color == color) {
        let port = match p.port.as_deref() {
            Some(port) => port,
            None => continue,
        };
        if p.lane == lane {
            input_obj.insert(port.to_string(), p.data.clone());
        } else if p.lane.len() < lane.len() && lane[..p.lane.len()] == p.lane[..] {
            if !input_obj.contains_key(port) {
                input_obj.insert(port.to_string(), p.data.clone());
            }
        }
    }

    merge_trigger_payload(node, initial_input, &mut input_obj);

    // Config-fills-port: for each configurable input port with a same-named
    // config value, inject the value into input_obj if no edge pulse provided
    // it. Edge wins over config, we only insert when the key is missing.
    for port in &node.inputs {
        if !port.configurable { continue; }
        if input_obj.contains_key(&port.name) { continue; }
        if let Some(cfg_val) = node.config.get(&port.name) {
            if !cfg_val.is_null() {
                input_obj.insert(port.name.clone(), cfg_val.clone());
            }
        }
    }

    // Runtime type enforcement: check each input value against its port's expected type.
    // For Expand/Gather ports, accept either pre-transform or post-transform shape.
    // On mismatch, log an error, replace with null, and record the error for the caller.
    for port_def in &node.inputs {
        if let Some(value) = input_obj.get(&port_def.name) {
            if !value.is_null() && !port_def.portType.is_unresolved() && port_def.laneMode == LaneMode::Single {
                if !runtime_type_check(&port_def.portType, value) {
                    let err = format!(
                        "Type mismatch on input port '{}': expected {}, got {}",
                        port_def.name, port_def.portType, WeftType::infer(value),
                    );
                    let value_preview = {
                        let s = serde_json::to_string(value).unwrap_or_default();
                        if s.len() > 500 { format!("{}...", &s[..500]) } else { s }
                    };
                    tracing::error!("[runtime_type_check] node={} {} Value: {}", node.id, err, value_preview);
                    type_errors.push(err);
                    input_obj.insert(port_def.name.clone(), serde_json::Value::Null);
                }
            }
        }
    }

    if has_incoming {
        serde_json::Value::Object(input_obj)
    } else {
        // Trigger gating: if this node IS a trigger but NOT the firing one,
        // it should receive an empty input so it stays dormant. A different
        // trigger is firing; this one must not run.
        let is_trigger_node = initial_input.get("triggerNodeId")
            .and_then(|v| v.as_str())
            .map(|id| id == node.id)
            .unwrap_or(false);
        let is_any_trigger = node.features.isTrigger;
        if is_any_trigger && !is_trigger_node {
            return serde_json::Value::Object(serde_json::Map::new());
        }
        // Non-incoming node that IS the firing trigger (or a regular
        // no-incoming root like a Template with only literal config):
        // start from initial_input, then overlay any config-fills we
        // collected above. Without this overlay, literal-only root nodes
        // would run with an empty input and ignore their configured
        // values.
        let mut merged = match initial_input {
            serde_json::Value::Object(m) => m.clone(),
            _ => serde_json::Map::new(),
        };
        for (k, v) in input_obj {
            merged.entry(k).or_insert(v);
        }
        serde_json::Value::Object(merged)
    }
}

/// Two lanes are "compatible" (broadcast relationship) if one is a prefix of the other
/// with matching SplitFrame.count at every shared level, or both are empty.
/// Examples:
///   [] and [3:1] → compatible (scalar broadcasts into lane)
///   [3:1] and [3:1, 2:0] → compatible (shallower broadcasts into deeper)
///   [3:1] and [4:0] → INCOMPATIBLE (different count at level 0)
///   [3:1, 2:0] and [3:1, 5:3] → INCOMPATIBLE (same prefix at level 0, different count at level 1)
fn lanes_are_compatible(a: &[SplitFrame], b: &[SplitFrame]) -> bool {
    let min_len = a.len().min(b.len());
    // If both are empty, compatible
    if min_len == 0 {
        return true;
    }
    // Check that shared prefix has matching counts
    for i in 0..min_len {
        if a[i].count != b[i].count {
            return false;
        }
    }
    true
}

/// Check if a normal node should be skipped (required port has null data,
/// or all ports in a oneOfRequired group are null/missing).
pub fn check_should_skip(
    _node: &NodeDefinition,
    node_pulses: &[Pulse],
    lane: &[SplitFrame],
    color: &str,
    required_ports: &std::collections::HashSet<&str>,
    wired_ports: &std::collections::HashSet<&str>,
    config_filled_ports: &std::collections::HashSet<&str>,
) -> bool {
    tracing::debug!("[check_should_skip] node={} required_ports={:?} wired_ports={:?} config_filled_ports={:?}", _node.id, required_ports, wired_ports, config_filled_ports);
    for port_name in required_ports {
        if config_filled_ports.contains(port_name) {
            // Port has a non-null config value; treated as a valid input.
            continue;
        }
        if !wired_ports.contains(port_name) {
            continue;
        }
        let pulse = node_pulses.iter().find(|p| {
            p.status == PulseStatus::Pending && p.color == color
                && p.port.as_deref() == Some(port_name)
                && (p.lane == lane || (p.lane.len() < lane.len() && lane[..p.lane.len()] == p.lane[..]))
        });
        match pulse {
            Some(p) => {
                tracing::debug!("[check_should_skip] node={} port={} data={} is_null={}", _node.id, port_name, p.data, p.data.is_null());
                if p.data.is_null() {
                    // If the port's type includes Null in its union, null is a valid value, not a skip signal.
                    let port_accepts_null = _node.inputs.iter()
                        .find(|inp| inp.name == *port_name)
                        .map(|inp| inp.portType.contains_null())
                        .unwrap_or(false);
                    if !port_accepts_null {
                        return true;
                    }
                }
            }
            None => {
                tracing::debug!("[check_should_skip] node={} port={} NO pulse found -> skip", _node.id, port_name);
                return true;
            }
        }
    }

    // Check oneOfRequired groups: for each group, if ALL ports are null/missing, skip.
    for group in &_node.features.oneOfRequired {
        if group.is_empty() {
            continue;
        }
        let all_null = group.iter().all(|port_name| {
            if config_filled_ports.contains(port_name.as_str()) {
                return false; // config-filled counts as non-null for oneOfRequired
            }
            if !wired_ports.contains(port_name.as_str()) {
                return true; // not wired = effectively null
            }
            let pulse = node_pulses.iter().find(|p| {
                p.status == PulseStatus::Pending && p.color == color
                    && p.port.as_deref() == Some(port_name.as_str())
                    && (p.lane == lane || (p.lane.len() < lane.len() && lane[..p.lane.len()] == p.lane[..]))
            });
            match pulse {
                Some(p) => {
                    if p.data.is_null() {
                        // If port accepts Null in its type, null is a valid value
                        let port_accepts_null = _node.inputs.iter()
                            .find(|inp| inp.name == port_name.as_str())
                            .map(|inp| inp.portType.contains_null())
                            .unwrap_or(false);
                        !port_accepts_null // true = effectively null (for skip), false = valid value
                    } else {
                        false // not null
                    }
                }
                None => true,
            }
        });
        if all_null {
            tracing::debug!("[check_should_skip] node={} oneOfRequired group {:?} all null -> skip", _node.id, group);
            return true;
        }
    }

    false
}

/// Merge trigger payload into input if applicable.
pub fn merge_trigger_payload(
    node: &NodeDefinition,
    initial_input: &serde_json::Value,
    input_obj: &mut serde_json::Map<String, serde_json::Value>,
) {
    let is_trigger_node = initial_input.get("triggerNodeId")
        .and_then(|v| v.as_str())
        .map(|id| id == node.id)
        .unwrap_or(false);

    if is_trigger_node {
        if let Some(payload) = initial_input.get("triggerPayload") {
            input_obj.insert("triggerPayload".to_string(), payload.clone());
        }
    }
}

// =============================================================================
// OUTPUT POSTPROCESSING : per-port, independent, parallel
// =============================================================================

/// Output postprocessing: after a node completes, process each output port
/// independently based on its laneMode and emit downstream pulses.
///
/// Each output port is handled independently:
/// - **Single**: emit 1 pulse per downstream edge at the current lane
/// - **Expand**: split the array value into N child-lane pulses
/// - **Gather**: buffer value; when all siblings at this lane level have
///   completed, gather each gather port's values into a list and emit at
///   the parent lane
///
/// Returns true if any gather port fired (all siblings collected).
pub fn postprocess_output(
    node_id: &str,
    output: &serde_json::Value,
    color: &str,
    lane: &[SplitFrame],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    node_executions: &mut NodeExecutionTable,
) -> bool {
    let node_def = match project.nodes.iter().find(|n| n.id == node_id) {
        Some(n) => n,
        None => {
            tracing::error!(
                "[postprocess_output] BUG: node '{}' not found in project definition. This should never happen : the node completed but doesn't exist in the project.",
                node_id,
            );
            return false;
        }
    };

    let output_obj = output.as_object();
    let mut gather_fired = false;

    tracing::info!(
        "[postprocess_output] node={} output_ports={:?} output_keys={:?}",
        node_id,
        node_def.outputs.iter().map(|p| &p.name).collect::<Vec<_>>(),
        output_obj.map(|o| o.keys().collect::<Vec<_>>()),
    );

    // Runtime type check connected Single output ports.
    // Only connected ports are checked, unconnected values are dropped anyway.
    // Failed ports emit null downstream; passing ports emit real data.
    let outgoing_edges = edge_idx.get_outgoing(project, node_id);
    let mut failed_ports: std::collections::HashSet<String> = std::collections::HashSet::new();
    for port in &node_def.outputs {
        if port.laneMode != LaneMode::Single { continue; }
        let has_edge = outgoing_edges.iter().any(|e| e.sourceHandle.as_deref() == Some(&port.name));
        if !has_edge { continue; }
        let port_value = output_obj
            .and_then(|obj| obj.get(&port.name))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if port_value.is_null() || port.portType.is_unresolved() { continue; }
        if !runtime_type_check(&port.portType, &port_value) {
            let value_preview = {
                let s = serde_json::to_string(&port_value).unwrap_or_default();
                if s.len() > 500 { format!("{}...", &s[..500]) } else { s }
            };
            tracing::error!(
                "[runtime_type_check] node={} output port '{}': expected {}, got {}. Value: {}.",
                node_id, port.name, port.portType, WeftType::infer(&port_value), value_preview,
            );
            let type_err_msg = format!(
                "Output port '{}': expected {}, got {}",
                port.name, port.portType, WeftType::infer(&port_value),
            );
            if let Some(execs) = node_executions.get_mut(node_id) {
                if let Some(exec) = execs.iter_mut().rev().find(|e| e.color == color && e.lane == lane) {
                    exec.status = NodeExecutionStatus::Failed;
                    exec.error = Some(type_err_msg);
                }
            }
            failed_ports.insert(port.name.clone());
        }
    }

    // Classify and emit output ports. Failed ports emit null, others emit real data.
    let mut gather_ports: Vec<&str> = Vec::new();
    let mut gather_checked_values: HashMap<String, serde_json::Value> = HashMap::new();
    for port in &node_def.outputs {
        let port_value = if failed_ports.contains(&port.name) {
            serde_json::Value::Null
        } else {
            output_obj
                .and_then(|obj| obj.get(&port.name))
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        };

        match port.laneMode {
            LaneMode::Single => {
                emit_single_port(node_id, &port.name, &port_value, color, lane, project, pulses, edge_idx);
            }
            LaneMode::Expand => {
                emit_expand_port(node_id, &port.name, &port_value, color, lane, project, pulses, edge_idx);
            }
            LaneMode::Gather => {
                gather_checked_values.insert(port.name.clone(), port_value);
                gather_ports.push(&port.name);
            }
        }
    }

    // Handle all Gather ports together (they share the sibling-counting logic)
    if !gather_ports.is_empty() {
        if lane.is_empty() {
            // Scalar lane : no siblings to wait for, emit directly using type-checked values
            for port_name in &gather_ports {
                let port_value = gather_checked_values.get(*port_name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                emit_single_port(node_id, port_name, &port_value, color, lane, project, pulses, edge_idx);
            }
        } else {
            gather_fired = try_gather_and_emit(node_id, color, lane, &gather_ports, project, pulses, edge_idx, node_executions);
        }
    }

    gather_fired
}

/// Emit a Single output port value on all downstream edges from this port.
/// One pulse per edge at the current lane.
pub fn emit_single_port(
    node_id: &str,
    port_name: &str,
    value: &serde_json::Value,
    color: &str,
    lane: &[SplitFrame],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
) {
    let port_filter: std::collections::HashSet<&str> = [port_name].into_iter().collect();
    let wrapped = serde_json::json!({ port_name: value });
    emit_pulses_on_edges(node_id, &wrapped, color, lane, project, pulses, edge_idx, Some(&port_filter));
}

/// Emit an Expand output port: split the array value into N child-lane pulses.
/// Null is treated as [null] (one child lane with null data) so downstream
/// Gather ports can collect it normally.
pub fn emit_expand_port(
    node_id: &str,
    port_name: &str,
    value: &serde_json::Value,
    color: &str,
    lane: &[SplitFrame],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
) {
    let items = if value.is_null() {
        vec![serde_json::Value::Null]
    } else {
        match value.as_array() {
            Some(arr) => arr.clone(),
            None => {
                tracing::error!("[emit_expand] node={} port={} value is not an array: {:?}", node_id, port_name, value);
                vec![]
            }
        }
    };

    let outgoing = edge_idx.get_outgoing(project, node_id);
    let n = items.len() as u32;

    for edge in &outgoing {
        let source_handle = edge.sourceHandle.as_deref().unwrap_or("default");
        if source_handle != port_name { continue; }
        let target_handle = edge.targetHandle.as_deref().unwrap_or("default");

        for (i, item) in items.iter().enumerate() {
            let mut child_lane = lane.to_vec();
            child_lane.push(SplitFrame { count: n, index: i as u32 });

            // Type check each expanded element against the declared type (post-transform = element type)
            let checked_item = if !item.is_null() {
                if let Some(port_def) = project.nodes.iter()
                    .find(|n| n.id == node_id)
                    .and_then(|n| n.outputs.iter().find(|p| p.name == port_name))
                {
                    if !port_def.portType.is_unresolved() && !runtime_type_check(&port_def.portType, item) {
                        tracing::error!(
                            "[runtime_type_check] node={} expand output port '{}' lane {:?}: expected {}, got {}",
                            node_id, port_name, child_lane, port_def.portType, WeftType::infer(item),
                        );
                        serde_json::Value::Null
                    } else {
                        item.clone()
                    }
                } else {
                    item.clone()
                }
            } else {
                item.clone()
            };

            let pulse = Pulse::new_on_port(
                color.to_string(),
                child_lane,
                checked_item,
                target_handle.to_string(),
            );
            pulses.entry(edge.target.clone()).or_default().push(pulse);
        }
    }
}

/// Check if all sibling lanes have completed for this node. If yes, build
/// gathered lists for each gather port, absorb siblings, and emit at parent lane.
/// Returns true if gather fired.
pub fn try_gather_and_emit(
    node_id: &str,
    color: &str,
    lane: &[SplitFrame],
    gather_ports: &[&str],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    node_executions: &NodeExecutionTable,
) -> bool {
    let top = lane.last().unwrap();
    let expected_count = top.count;
    let parent_lane: Vec<SplitFrame> = lane[..lane.len() - 1].to_vec();

    // Count sibling executions: all terminal NodeExecutions for this node at same lane depth+prefix
    let sibling_execs: Vec<&NodeExecution> = node_executions.get(node_id)
        .map(|execs| execs.iter()
            .filter(|e| {
                e.color == color
                    && e.lane.len() == lane.len()
                    && e.lane[..e.lane.len() - 1] == parent_lane[..]
                    && e.lane.last().map(|f| f.count) == Some(expected_count)
                    && e.status.is_terminal()
            })
            .collect())
        .unwrap_or_default();

    tracing::debug!(
        "[gather output] node={} gather_ports={:?} siblings={}/{} lane={:?}",
        node_id, gather_ports, sibling_execs.len(), expected_count, lane
    );

    if (sibling_execs.len() as u32) < expected_count {
        return false; // Not all siblings yet
    }

    // Build gathered output: each gather port gets its own list
    let mut gathered_obj = serde_json::Map::new();
    for port_name in gather_ports {
        let mut ordered: Vec<(u32, serde_json::Value)> = sibling_execs.iter()
            .map(|e| {
                let idx = e.lane.last().unwrap().index;
                if matches!(e.status, NodeExecutionStatus::Failed | NodeExecutionStatus::Skipped) {
                    (idx, serde_json::Value::Null)
                } else {
                    let val = e.output.as_ref()
                        .and_then(|o| o.get(*port_name))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    (idx, val)
                }
            })
            .collect();
        ordered.sort_by_key(|(idx, _)| *idx);
        let gathered_list: Vec<serde_json::Value> = ordered.into_iter().map(|(_, v)| v).collect();
        let gathered_value = serde_json::Value::Array(gathered_list);

        // Type check the gathered list against the declared type (post-transform = collected list)
        if let Some(port_def) = project.nodes.iter()
            .find(|n| n.id == node_id)
            .and_then(|n| n.outputs.iter().find(|p| p.name == *port_name))
        {
            if !port_def.portType.is_unresolved() && !gathered_value.is_null() && !runtime_type_check(&port_def.portType, &gathered_value) {
                tracing::error!(
                    "[runtime_type_check] node={} gather output port '{}': expected {}, got {}",
                    node_id, port_name, port_def.portType, WeftType::infer(&gathered_value),
                );
            }
        }

        gathered_obj.insert(port_name.to_string(), gathered_value);
    }

    // Emit gathered data at parent lane : each gather port emits as Single
    let gathered_output = serde_json::Value::Object(gathered_obj);
    let port_filter: std::collections::HashSet<&str> = gather_ports.iter().copied().collect();
    emit_pulses_on_edges(node_id, &gathered_output, color, &parent_lane, project, pulses, edge_idx, Some(&port_filter));
    true
}

/// Low-level edge emitter: emit pulses on outgoing edges.
/// No laneMode awareness: routes values from the output object to
/// downstream targets as single pulses at the given lane.
///
/// If `only_ports` is Some, only edges from those source ports are emitted.
fn emit_pulses_on_edges(
    node_id: &str,
    output: &serde_json::Value,
    color: &str,
    lane: &[SplitFrame],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    only_ports: Option<&std::collections::HashSet<&str>>,
) {
    let outgoing = edge_idx.get_outgoing(project, node_id);

    for edge in &outgoing {
        let source_handle = edge.sourceHandle.as_deref().unwrap_or("default");

        if let Some(only) = only_ports {
            if !only.contains(source_handle) { continue; }
        }
        let target_handle = edge.targetHandle.as_deref().unwrap_or("default");

        let routed_value = if let Some(obj) = output.as_object() {
            obj.get(source_handle).cloned().unwrap_or(serde_json::Value::Null)
        } else {
            output.clone()
        };

        let already_pending = pulses.get(&edge.target)
            .map(|ps| ps.iter().any(|p| {
                p.status == PulseStatus::Pending && p.color == color
                    && p.lane == lane && p.port.as_deref() == Some(target_handle)
            }))
            .unwrap_or(false);
        if !already_pending {
            let pulse = Pulse::new_on_port(
                color.to_string(),
                lane.to_vec(),
                routed_value,
                target_handle.to_string(),
            );
            pulses.entry(edge.target.clone()).or_default().push(pulse);
        }
    }
}

/// Emit null on all output ports of a node. Used when a node is skipped
/// or fails at dispatch time. Routes through postprocess_output so null
/// correctly flows through Expand/Gather.
pub fn emit_null_downstream(
    node_id: &str,
    color: &str,
    lane: &[SplitFrame],
    project: &ProjectDefinition,
    pulses: &mut PulseTable,
    edge_idx: &EdgeIndex,
    node_executions: &mut NodeExecutionTable,
) {
    let node_def = project.nodes.iter().find(|n| n.id == node_id);
    let null_output = match node_def {
        Some(node) if !node.outputs.is_empty() => {
            let mut obj = serde_json::Map::new();
            for port in &node.outputs {
                obj.insert(port.name.clone(), serde_json::Value::Null);
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    };
    postprocess_output(node_id, &null_output, color, lane, project, pulses, edge_idx, node_executions);
}

/// Get the output value for an execution, synthesizing `{"_error": "..."}` for failures.
fn exec_output_or_error(exec: &NodeExecution) -> serde_json::Value {
    if exec.status == NodeExecutionStatus::Failed {
        let error_msg = exec.error.as_deref().unwrap_or("Unknown error");
        serde_json::json!({"_error": error_msg})
    } else {
        exec.output.clone().unwrap_or(serde_json::Value::Null)
    }
}

/// Reconstruct nested output structure from NodeExecution records using their lane stacks.
pub fn build_nested_exec_output(execs: &[&NodeExecution], depth: usize, max_depth: usize) -> serde_json::Value {
    if depth >= max_depth {
        return execs.last()
            .map(|e| exec_output_or_error(e))
            .unwrap_or(serde_json::Value::Null);
    }

    let mut groups: BTreeMap<u32, Vec<&NodeExecution>> = BTreeMap::new();
    let mut count = 0u32;
    for e in execs {
        if e.lane.len() <= depth {
            continue;
        }
        let frame = &e.lane[depth];
        count = count.max(frame.count);
        groups.entry(frame.index).or_default().push(e);
    }

    if groups.is_empty() {
        return execs.last()
            .map(|e| exec_output_or_error(e))
            .unwrap_or(serde_json::Value::Null);
    }

    let mut arr = Vec::with_capacity(count as usize);
    for i in 0..count {
        if let Some(group) = groups.get(&i) {
            arr.push(build_nested_exec_output(group, depth + 1, max_depth));
        } else {
            arr.push(serde_json::Value::Null);
        }
    }
    serde_json::Value::Array(arr)
}

/// Check if the project has settled.
/// Complete when: no Pending pulses AND no Running/WaitingForInput NodeExecutions.
/// Returns Some(any_failed) if complete, None if still running.
pub fn check_completion(pulses: &PulseTable, node_executions: &NodeExecutionTable) -> Option<bool> {
    let any_pending = pulses.values()
        .flat_map(|ps| ps.iter())
        .any(|p| p.status == PulseStatus::Pending);

    let any_active_exec = node_executions.values()
        .flat_map(|es| es.iter())
        .any(|e| !e.status.is_terminal());

    if any_pending || any_active_exec {
        return None; // not complete
    }

    let any_failed = node_executions.values()
        .flat_map(|es| es.iter())
        .any(|e| e.status == NodeExecutionStatus::Failed);
    Some(any_failed)
}

/// Build the status callback payload from NodeExecution records.
pub fn build_completion_callback_payload(
    execution_id: &str,
    node_executions: &NodeExecutionTable,
    pulses: &PulseTable,
    any_failed: bool,
) -> serde_json::Value {
    let summary_statuses = build_node_statuses_from_executions(node_executions, pulses);
    let summary_outputs = build_node_outputs_from_executions(node_executions);
    let status_str = if any_failed { "failed" } else { "completed" };
    serde_json::json!({
        "status": status_str,
        "executionId": execution_id,
        "nodeOutputs": summary_outputs,
        "nodeStatuses": summary_statuses,
    })
}

/// Build cancel callback payload from NodeExecution records.
pub fn build_cancel_callback_payload(
    execution_id: &str,
    node_executions: &NodeExecutionTable,
    pulses: &PulseTable,
) -> serde_json::Value {
    let summary_statuses = build_node_statuses_from_executions(node_executions, pulses);
    serde_json::json!({
        "status": "cancelled",
        "executionId": execution_id,
        "nodeStatuses": summary_statuses,
    })
}

/// Initialize pulses for a new project execution: one Pending pulse per source node.
pub fn init_pulses(project: &ProjectDefinition, edge_idx: &EdgeIndex) -> PulseTable {
    let default_color = "wave-0".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    for node in &project.nodes {
        let has_incoming = !edge_idx.get_incoming(project, &node.id).is_empty();
        if !has_incoming {
            let pulse = Pulse::new(default_color.clone(), vec![], serde_json::Value::Null);
            pulses.insert(node.id.clone(), vec![pulse]);
        }
    }
    pulses
}

/// Build node statuses from NodeExecution records.
/// Falls back to "pending" for nodes that haven't been dispatched yet.
pub fn build_node_statuses_from_executions(
    node_executions: &NodeExecutionTable,
    pulses: &PulseTable,
) -> HashMap<String, String> {
    let mut statuses = HashMap::new();

    for node_id in pulses.keys() {
        if let Some(execs) = node_executions.get(node_id) {
            if !execs.is_empty() {
                statuses.insert(node_id.clone(), node_execution_summary(execs));
                continue;
            }
        }
        // No executions yet: node has only Pending pulses
        statuses.insert(node_id.clone(), "pending".to_string());
    }

    statuses
}

/// Compute active edge IDs from pulse data.
/// An edge is active when the target node has a Pending pulse on the target port.
pub fn compute_active_edges(
    pulses: &PulseTable,
    project: &ProjectDefinition,
) -> Vec<String> {
    let mut active = Vec::new();
    for edge in &project.edges {
        let target_handle = edge.targetHandle.as_deref().unwrap_or("default");
        if let Some(target_pulses) = pulses.get(&edge.target) {
            let has_pending = target_pulses.iter().any(|p| {
                p.status == PulseStatus::Pending
                    && p.port.as_deref() == Some(target_handle)
            });
            if has_pending {
                active.push(edge.id.clone());
            }
        }
    }
    active
}

/// Build ordering map from NodeExecution records.
pub fn build_node_ordering_from_executions(node_executions: &NodeExecutionTable) -> HashMap<String, u64> {
    let mut ordering = HashMap::new();
    for (node_id, execs) in node_executions {
        if let Some(first) = execs.first() {
            ordering.insert(node_id.clone(), first.startedAt);
        }
    }
    ordering
}

/// Build node outputs from NodeExecution records.
/// For failed executions, synthesizes an `{"_error": "..."}` output so errors are visible.
pub fn build_node_outputs_from_executions(
    node_executions: &NodeExecutionTable,
) -> HashMap<String, serde_json::Value> {
    let mut outputs = HashMap::new();

    for (node_id, execs) in node_executions {
        let terminal: Vec<&NodeExecution> = execs.iter()
            .filter(|e| matches!(e.status, NodeExecutionStatus::Completed | NodeExecutionStatus::Failed))
            .filter(|e| {
                // Include completed with output, or failed (with or without output)
                e.status == NodeExecutionStatus::Failed
                    || e.output.as_ref().map(|v| !v.is_null()).unwrap_or(false)
            })
            .collect();

        if terminal.is_empty() {
            continue;
        }

        let max_depth = terminal.iter().map(|e| e.lane.len()).max().unwrap_or(0);
        if max_depth == 0 {
            if let Some(last) = terminal.last() {
                let output = exec_output_or_error(last);
                outputs.insert(node_id.clone(), output);
            }
        } else {
            let result = build_nested_exec_output(&terminal, 0, max_depth);
            outputs.insert(node_id.clone(), result);
        }
    }

    outputs
}

// =============================================================================
// MOCK HELPERS (used by the executor for test mode)
// =============================================================================

/// Check if a node is inside a mocked group by checking its scope chain.
/// Returns true if any entry in the node's scope matches a key in the mocks map.
pub fn is_inside_mocked_group(
    node: &crate::project::NodeDefinition,
    mocks: &std::collections::HashMap<String, serde_json::Value>,
) -> bool {
    node.scope.iter().any(|group_id| mocks.contains_key(group_id))
}

/// Sanitize mock output: keep only ports that exist on the node, fill missing ones with null.
/// Prevents mock data with extra/wrong port names from breaking the execution graph.
pub fn sanitize_mock_output(
    mock: &serde_json::Value,
    output_ports: &[crate::project::PortDefinition],
) -> serde_json::Value {
    let mock_obj = match mock.as_object() {
        Some(obj) => obj,
        None => return mock.clone(),
    };
    let mut sanitized = serde_json::Map::new();
    for port in output_ports {
        let value = mock_obj.get(&port.name).cloned().unwrap_or(serde_json::Value::Null);
        sanitized.insert(port.name.clone(), value);
    }
    serde_json::Value::Object(sanitized)
}

#[cfg(test)]
#[path = "tests/executor_tests.rs"]
mod tests;
