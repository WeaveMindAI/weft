//! StackTrim Node - Trims a stack to a specified number of lanes.
//!
//! The `value` input port has `laneMode: Gather` and the `value` output port
//! has `laneMode: Expand`, so the executor handles this inline: it gathers
//! all lanes, reads the `count` input (depth 1, broadcast), keeps only the
//! first `count` lanes, and re-expands downstream to that count.
//! The node's execute() is a fallback for depth-1 context.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct StackTrimNode;

#[async_trait]
impl Node for StackTrimNode {
    fn node_type(&self) -> &'static str {
        "StackTrim"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Stack Trim",
            inputs: vec![
                PortDef::gather("value", "List[T]", true),
                PortDef::new("count", "Number", true),
            ],
            outputs: vec![
                PortDef::expand("value", "T", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        // The executor sends Gather port values as an array on "value".
        // Read "count" to know how many to keep, then return the trimmed list
        // on the Expand output port "value".
        let value = ctx.input.get("value").cloned().unwrap_or(serde_json::Value::Null);
        let count = ctx.input.get("count")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as usize;

        let trimmed = if let Some(arr) = value.as_array() {
            let n = count.min(arr.len());
            serde_json::Value::Array(arr[..n].to_vec())
        } else {
            // Not in parallel context: if count >= 1, pass through as single-item list
            if count >= 1 {
                serde_json::json!([value])
            } else {
                serde_json::json!([])
            }
        };

        NodeResult::completed(serde_json::json!({
            "value": trimmed,
        }))
    }
}

register_node!(StackTrimNode);
