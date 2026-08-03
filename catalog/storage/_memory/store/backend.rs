//! Memory Store Node - Stores structured data in a Database infrastructure instance.
//!
//! This is a regular node that references a Database infrastructure node.
//! It structures data and stores it using the DurableKV capability.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

pub struct MemoryStoreNode;

#[async_trait]
impl Node for MemoryStoreNode {
    fn node_type(&self) -> &'static str {
        "MemoryStore"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Memory Store",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("key", "String", true),
                PortDef::new("value", "T", true),
            ],
            outputs: vec![
                PortDef::new("stored", "Boolean", false),
                PortDef::new("key", "String", false),
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

        let client = ctx.infra_client(endpoint_url);

        let key = match ctx.input.get("key").and_then(|v| v.as_str()) {
            Some(k) => k.to_string(),
            None => return NodeResult::failed("Missing required input: key"),
        };

        let value = match ctx.input.get("value") {
            Some(v) => v.clone(),
            None => return NodeResult::failed("Missing required input: value"),
        };

        // Exclusive: kv_set mutates state, so we use execute_action (serialized writes).
        match client.execute_action("kv_set", serde_json::json!({"key": key, "value": value})).await {
            Ok(_) => {
                NodeResult::completed(serde_json::json!({
                    "stored": true,
                    "key": key,
                }))
            }
            Err(e) => NodeResult::failed(&format!("Failed to store value: {}", e)),
        }
    }
}

register_node!(MemoryStoreNode);
