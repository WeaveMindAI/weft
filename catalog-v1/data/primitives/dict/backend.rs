//! Dict Node - JSON dictionary/object input

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Dict node for JSON dictionary/object input.
#[derive(Default)]
pub struct DictNode;

#[async_trait]
impl Node for DictNode {
    fn node_type(&self) -> &'static str {
        "Dict"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Dict",
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
        let raw = ctx.config.get("value").cloned().unwrap_or(serde_json::json!({}));
        let value = if let Some(s) = raw.as_str() {
            serde_json::from_str(s).unwrap_or(serde_json::json!({}))
        } else {
            raw
        };
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(DictNode);
