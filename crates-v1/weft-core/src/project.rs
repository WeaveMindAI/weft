use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, TS)]
#[ts(export)]
pub enum ProjectStatus {
    #[default]
    Draft,
    Active,
    Inactive,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectDefinition {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub status: ProjectStatus,
    pub createdAt: DateTime<Utc>,
    pub updatedAt: DateTime<Utc>,
}

/// Role of a Passthrough node at a group boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum GroupBoundaryRole {
    In,
    Out,
}

/// Marks a Passthrough node as a group boundary.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct GroupBoundary {
    pub groupId: String,
    pub role: GroupBoundaryRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct NodeDefinition {
    pub id: String,
    pub nodeType: NodeType,
    pub label: Option<String>,
    pub config: serde_json::Value,
    pub position: Position,
    #[serde(default)]
    pub inputs: Vec<PortDefinition>,
    #[serde(default)]
    pub outputs: Vec<PortDefinition>,
    #[serde(default)]
    pub features: crate::node::NodeFeatures,
    /// Group IDs this node is nested inside, from outermost to innermost.
    /// Empty for top-level nodes. E.g. ["outer", "outer.inner"] for a node in outer.inner.
    #[serde(default)]
    pub scope: Vec<String>,
    /// If this node is a Passthrough at a group boundary, which group and which side.
    #[serde(default)]
    pub groupBoundary: Option<GroupBoundary>,
}

/// How a port interacts with the lane/stack system.
/// - Single: normal, one value per lane (default)
/// - Expand: this port carries a list that expands into N lanes downstream
/// - Gather: this port collects values from all N lanes into a single list
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum LaneMode {
    #[default]
    Single,
    Expand,
    Gather,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PortDefinition {
    pub name: String,
    pub portType: WeftType,
    pub required: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub laneMode: LaneMode,
    /// Number of List[] levels to expand/gather. Default 1.
    /// For `List[List[String]]` → `String`, this is 2 (peel two List layers).
    #[serde(default = "default_lane_depth")]
    pub laneDepth: u32,
    /// Whether this port can be filled by a same-named config field on the
    /// node (in addition to being wired by an edge). Defaults to true for
    /// every port type except Media (Image/Audio/Video/Document and unions
    /// thereof), TypeVar, and MustOverride, those are wired-only.
    /// Catalog authors can opt out per port via PortDef::wired_only(...).
    /// Edge wins over config when both are present.
    #[serde(default = "default_configurable")]
    pub configurable: bool,
}

fn default_lane_depth() -> u32 { 1 }
fn default_configurable() -> bool { true }

// Port type system lives in weft_type.rs : re-exported here for convenience.
pub use crate::weft_type::{WeftPrimitive, WeftType};

/// NodeType is now a simple String wrapper to allow dynamic node types.
/// New nodes can be added by dropping them in the nodes folder without
/// modifying this enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[ts(export)]
pub struct NodeType(pub String);


impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for NodeType {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for NodeType {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Edge {
    pub id: String,
    pub source: String,
    pub target: String,
    pub sourceHandle: Option<String>,
    pub targetHandle: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectExecution {
    pub id: Uuid,
    pub projectId: Uuid,
    pub status: ExecutionStatus,
    pub startedAt: DateTime<Utc>,
    pub completedAt: Option<DateTime<Utc>>,
    pub currentNode: Option<String>,
    pub state: serde_json::Value,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[ts(export)]
pub enum ExecutionStatus {
    Pending,
    Running,
    WaitingForInput,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

/// Pre-indexed edge lookups. Build once, use many times.
/// Avoids O(edges) linear scan on every get_incoming/outgoing call.
pub struct EdgeIndex {
    /// node_id -> indices into ProjectDefinition.edges for outgoing edges
    outgoing: std::collections::HashMap<String, Vec<usize>>,
    /// node_id -> indices into ProjectDefinition.edges for incoming edges
    incoming: std::collections::HashMap<String, Vec<usize>>,
}

impl EdgeIndex {
    pub fn build(project: &ProjectDefinition) -> Self {
        let mut outgoing: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let mut incoming: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        for (i, edge) in project.edges.iter().enumerate() {
            outgoing.entry(edge.source.clone()).or_default().push(i);
            incoming.entry(edge.target.clone()).or_default().push(i);
        }
        Self { outgoing, incoming }
    }

    pub fn get_outgoing<'a>(&self, project: &'a ProjectDefinition, node_id: &str) -> Vec<&'a Edge> {
        self.outgoing.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i]).collect())
            .unwrap_or_default()
    }

    pub fn get_incoming<'a>(&self, project: &'a ProjectDefinition, node_id: &str) -> Vec<&'a Edge> {
        self.incoming.get(node_id)
            .map(|indices| indices.iter().map(|&i| &project.edges[i]).collect())
            .unwrap_or_default()
    }
}

impl ProjectDefinition {
    /// Backward-BFS subgraph extraction.
    ///
    /// Starting from `seed_ids`, walks backwards along incoming edges to collect
    /// all upstream dependencies. Optionally validates each collected node via
    /// `validate_node` (return Err to reject). Builds a new ProjectDefinition
    /// containing only the collected nodes and edges between them.
    fn extract_subgraph(
        &self,
        seed_ids: Vec<String>,
        label: &str,
        description: String,
        validate_node: Option<&dyn Fn(&NodeDefinition) -> Result<(), String>>,
    ) -> Result<ProjectDefinition, String> {
        let mut required_node_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<String> = seed_ids.into_iter().collect();

        while let Some(node_id) = queue.pop_front() {
            if !required_node_ids.insert(node_id.clone()) {
                continue;
            }
            for edge in self.edges.iter().filter(|e| e.target == node_id) {
                queue.push_back(edge.source.clone());
            }
        }

        if let Some(validate) = validate_node {
            for node in &self.nodes {
                if required_node_ids.contains(&node.id) {
                    validate(node)?;
                }
            }
        }

        let sub_nodes: Vec<NodeDefinition> = self.nodes.iter()
            .filter(|n| required_node_ids.contains(&n.id))
            .cloned()
            .collect();

        let sub_edges: Vec<Edge> = self.edges.iter()
            .filter(|e| required_node_ids.contains(&e.source) && required_node_ids.contains(&e.target))
            .cloned()
            .collect();

        Ok(ProjectDefinition {
            id: self.id,
            name: format!("{} [{}]", self.name, label),
            description: Some(description),
            nodes: sub_nodes,
            edges: sub_edges,
            status: self.status.clone(),
            createdAt: self.createdAt,
            updatedAt: self.updatedAt,
        })
    }

    /// Extract the infrastructure sub-graph from this project.
    ///
    /// Walks backwards from every infrastructure node, collecting all upstream
    /// dependencies (Text nodes, Config nodes, etc.) that feed into them.
    /// Returns a new ProjectDefinition containing only those nodes and edges.
    ///
    /// Errors if a trigger node is found in the sub-graph (triggers can't be
    /// part of infrastructure setup).
    pub fn extract_infra_subgraph(&self) -> Result<ProjectDefinition, String> {
        let seed_ids: Vec<String> = self.nodes.iter()
            .filter(|n| n.features.isInfrastructure)
            .map(|n| n.id.clone())
            .collect();

        if seed_ids.is_empty() {
            return Err("No infrastructure nodes found in project".to_string());
        }

        self.extract_subgraph(
            seed_ids,
            "infra",
            "Auto-extracted infrastructure sub-graph".to_string(),
            Some(&|node: &NodeDefinition| {
                if node.features.isTrigger {
                    Err(format!(
                        "Trigger node '{}' (type: {}) cannot be part of the infrastructure sub-graph. \
                         Infrastructure nodes and their dependencies must not include triggers.",
                        node.id, node.nodeType
                    ))
                } else {
                    Ok(())
                }
            }),
        )
    }

    /// Extract the trigger setup sub-graph for a specific trigger node.
    ///
    /// Walks backwards from the trigger node, collecting all upstream
    /// dependencies (Config nodes, Infrastructure nodes, etc.).
    /// The trigger node itself is included in the sub-graph.
    ///
    /// During trigger setup execution (`isTriggerSetup = true`):
    /// - Infrastructure nodes return their outputs (endpointUrl etc.)
    ///   via the executor's endpoint injection (same as normal execution)
    /// - The trigger node runs its setup logic (register webhook, etc.)
    ///   instead of its normal execution behavior
    pub fn extract_trigger_setup_subgraph(&self, trigger_node_id: &str) -> Result<ProjectDefinition, String> {
        let trigger_node = self.nodes.iter()
            .find(|n| n.id == trigger_node_id)
            .ok_or_else(|| format!("Trigger node '{}' not found in project", trigger_node_id))?;

        if !trigger_node.features.isTrigger {
            return Err(format!(
                "Node '{}' (type: {}) is not a trigger node",
                trigger_node_id, trigger_node.nodeType
            ));
        }

        self.extract_subgraph(
            vec![trigger_node_id.to_string()],
            "trigger-setup",
            format!("Auto-extracted trigger setup sub-graph for node {}", trigger_node_id),
            None,
        )
    }
}
