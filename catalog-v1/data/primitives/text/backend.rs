//! Text Node - Text input value

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Text node for text input values.
#[derive(Default)]
pub struct TextNode;

#[async_trait]
impl Node for TextNode {
    fn node_type(&self) -> &'static str {
        "Text"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Text",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "String", false),
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
        let value = ctx.config.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(TextNode);
