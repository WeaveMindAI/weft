//! StackDepth Node - Outputs the current lane/stack depth as a number.
//!
//! The input port has `laneMode: Gather`, so the executor handles this inline:
//! it reads the lane count and outputs it as a single number at depth 1.
//! The node's execute() is a fallback for when there's no stack context (depth 1).

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct StackDepthNode;

#[async_trait]
impl Node for StackDepthNode {
    fn node_type(&self) -> &'static str {
        "StackDepth"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Stack Depth",
            inputs: vec![
                PortDef::gather("value", "List[T]", true),
            ],
            outputs: vec![
                PortDef::new("depth", "Number", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let depth = ctx.lane_count();
        NodeResult::completed(serde_json::json!({
            "depth": depth,
        }))
    }
}

register_node!(StackDepthNode);
