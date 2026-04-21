// Minimal project/graph types. Full implementation will be ported in
// phase A2 from crates-v1/weft-core/src/project.rs (ProjectDefinition,
// NodeDefinition, Edge, PortDefinition, LaneMode, EdgeIndex).
//
// Kept minimal for now so the workspace can compile end-to-end.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDefinition {
    pub id: String,
    pub name: String,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub config: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: String,
    pub from_node: String,
    pub from_port: String,
    pub to_node: String,
    pub to_port: String,
}
