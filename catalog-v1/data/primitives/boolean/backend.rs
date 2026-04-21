//! Boolean Node - True/false value

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Boolean node for true/false input values.
#[derive(Default)]
pub struct BooleanNode;

#[async_trait]
impl Node for BooleanNode {
    fn node_type(&self) -> &'static str {
        "Boolean"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Boolean",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "Boolean", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::checkbox("value"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        // Accept true/false as: literal JSON bool, the string "true"/"false"
        // (case-insensitive), or 1/0 numeric. Everything else is false.
        let raw = ctx.config.get("value").cloned().unwrap_or(serde_json::json!(false));
        let value = match raw {
            serde_json::Value::Bool(b) => b,
            serde_json::Value::String(s) => {
                let s = s.trim().to_lowercase();
                s == "true" || s == "1" || s == "yes" || s == "y"
            }
            serde_json::Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            _ => false,
        };
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(BooleanNode);
