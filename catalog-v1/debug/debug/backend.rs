//! Debug Node - Display incoming data for debugging

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

/// Debug node for displaying incoming data for debugging.
#[derive(Default)]
pub struct DebugNode;

#[async_trait]
impl Node for DebugNode {
    fn node_type(&self) -> &'static str {
        "Debug"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Debug",
            inputs: vec![
                PortDef::new("data", "T", true),
            ],
            outputs: vec![],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let data = ctx.input.get("data").cloned().unwrap_or(ctx.input.clone());
        tracing::info!("Debug node: {:?}", data);
        NodeResult::completed(serde_json::json!({ "data": data }))
    }
}

register_node!(DebugNode);
