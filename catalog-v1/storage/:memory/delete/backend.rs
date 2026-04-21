//! Memory Delete Node - Deletes data from a Database infrastructure instance by pattern.
//!
//! This is a regular node that references a Database infrastructure node.
//! It deletes keys matching a regex pattern using the DurableKV capability.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

pub struct MemoryDeleteNode;

#[async_trait]
impl Node for MemoryDeleteNode {
    fn node_type(&self) -> &'static str {
        "MemoryDelete"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Memory Delete",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("pattern", "String", true),
            ],
            outputs: vec![
                PortDef::new("deleted", "List[String]", false),
                PortDef::new("count", "Number", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) => url,
            None => return NodeResult::failed("No endpointUrl provided. Connect the endpointUrl output of a Database node."),
        };

        let pattern = match ctx.input.get("pattern").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => return NodeResult::failed("Missing required input: pattern"),
        };

        let client = ctx.infra_client(endpoint_url);

        match client.execute_action("kv_delete_pattern", serde_json::json!({"pattern": pattern})).await {
            Ok(result) => {
                let deleted = result.get("deleted").cloned().unwrap_or(serde_json::json!([]));
                let count = result.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                NodeResult::completed(serde_json::json!({
                    "deleted": deleted,
                    "count": count,
                }))
            }
            Err(e) => NodeResult::failed(&format!("Failed to delete by pattern: {}", e)),
        }
    }
}

register_node!(MemoryDeleteNode);
