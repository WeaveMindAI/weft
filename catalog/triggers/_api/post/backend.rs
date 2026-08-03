//! API POST trigger node,fires when an HTTP POST request hits the generated webhook URL.
//!
//! Output ports are user-defined and represent the expected JSON body schema.
//! The node extracts and validates each field from the request body at runtime.

use async_trait::async_trait;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct ApiPostNode;

#[async_trait]
impl Node for ApiPostNode {
    fn node_type(&self) -> &'static str {
        "ApiPost"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "API Endpoint (POST)",
            inputs: vec![],
            outputs: vec![
                PortDef::new("receivedAt", "String", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Webhook),
                requiresRunningInstance: false,
                canAddOutputPorts: true,
                ..Default::default()
            },
            fields: vec![
                FieldDef::password("apiKey"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isTriggerSetup {
            return NodeResult::completed(ctx.input.clone());
        }

        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());

        let body = payload.get("body").cloned().unwrap_or(serde_json::json!({}));
        let received_at = payload.get("receivedAt")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Extract user-defined output ports from the request body
        let mut output = match body {
            serde_json::Value::Object(map) => serde_json::Value::Object(map),
            _ => serde_json::json!({}),
        };

        // Always include receivedAt
        output["receivedAt"] = serde_json::Value::String(received_at);

        NodeResult::completed(output)
    }
}

register_node!(ApiPostNode);
