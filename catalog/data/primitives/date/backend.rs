//! Date Node - Date/time input value

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Date node for date/time input values.
#[derive(Default)]
pub struct DateNode;

#[async_trait]
impl Node for DateNode {
    fn node_type(&self) -> &'static str {
        "Date"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Date",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "String", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("value"),
                FieldDef::select("format", vec!["ISO", "Unix", "Custom"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let default_date = chrono::Utc::now().to_rfc3339();
        let raw = ctx.config.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or(&default_date);

        let format = ctx.config.get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("ISO");

        let value = match format {
            "Unix" => {
                let ts = chrono::DateTime::parse_from_rfc3339(raw)
                    .map(|d| d.timestamp())
                    .unwrap_or_else(|_| chrono::Utc::now().timestamp());
                ts.to_string()
            }
            _ => raw.to_string(),
        };

        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(DateNode);
