//! Memory Query Node - Queries data from a Database infrastructure instance.
//!
//! This is a regular node that references a Database infrastructure node.
//! It retrieves data using the DurableKV capability.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

pub struct MemoryQueryNode;

#[async_trait]
impl Node for MemoryQueryNode {
    fn node_type(&self) -> &'static str {
        "MemoryQuery"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Memory Query",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("pattern", "String", true),
            ],
            outputs: vec![
                PortDef::new("value", "Dict[String, T]", false),
                PortDef::new("found", "Boolean", false),
                PortDef::new("count", "Number", false),
                PortDef::new("keys", "List[String]", false),
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

        match client.execute_action("kv_query", serde_json::json!({"pattern": pattern})).await {
            Ok(result) => {
                let matches = result.get("matches").cloned().unwrap_or(serde_json::json!({}));
                let count = result.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let keys: Vec<String> = matches.as_object()
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default();
                NodeResult::completed(serde_json::json!({
                    "value": matches,
                    "found": count > 0,
                    "count": count,
                    "keys": keys,
                }))
            }
            Err(e) => NodeResult::failed(&format!("Failed to query by pattern: {}", e)),
        }
    }
}

register_node!(MemoryQueryNode);
