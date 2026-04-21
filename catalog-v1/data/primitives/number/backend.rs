//! Number Node - Numeric input value

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Number node for numeric input values.
#[derive(Default)]
pub struct NumberNode;

#[async_trait]
impl Node for NumberNode {
    fn node_type(&self) -> &'static str {
        "Number"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Number",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Number", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("value"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let raw = ctx.config.get("value").cloned().unwrap_or(serde_json::json!(0));
        let value = raw.as_f64()
            .or_else(|| raw.as_str().and_then(|s| s.parse::<f64>().ok()))
            .unwrap_or(0.0);
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(NumberNode);
