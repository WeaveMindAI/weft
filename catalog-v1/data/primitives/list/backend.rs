//! List Node - Array/list input

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// List node for array/list input.
#[derive(Default)]
pub struct ListNode;

#[async_trait]
impl Node for ListNode {
    fn node_type(&self) -> &'static str {
        "List"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "List",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "MustOverride", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::textarea("value"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let raw = ctx.config.get("value").cloned().unwrap_or(serde_json::json!([]));
        let value = if let Some(s) = raw.as_str() {
            serde_json::from_str(s).unwrap_or(serde_json::json!([]))
        } else {
            raw
        };
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(ListNode);
