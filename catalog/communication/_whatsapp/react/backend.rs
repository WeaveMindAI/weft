//! WhatsAppReact Node - React to a WhatsApp message with an emoji.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

pub struct WhatsAppReactNode;

#[async_trait]
impl Node for WhatsAppReactNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppReact"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp React",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("messageId", "String", true),
                PortDef::new("emoji", "String", true),
            ],
            outputs: vec![
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures { ..Default::default() },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) if !url.is_empty() => url,
            _ => return NodeResult::failed("No endpointUrl provided. Connect a WhatsAppBridge node."),
        };
        let chat_id = ctx.input.get("chatId").and_then(|v| v.as_str()).unwrap_or("");
        let message_id = ctx.input.get("messageId").and_then(|v| v.as_str()).unwrap_or("");
        let emoji = ctx.input.get("emoji").and_then(|v| v.as_str()).unwrap_or("");

        if chat_id.is_empty() { return NodeResult::failed("Chat ID is required"); }
        if message_id.is_empty() { return NodeResult::failed("Message ID is required"); }
        if emoji.is_empty() { return NodeResult::failed("Emoji is required"); }

        let action_req = ActionRequest {
            action: "sendReaction".to_string(),
            payload: serde_json::json!({ "chatId": chat_id, "messageId": message_id, "emoji": emoji }),
        };

        let client = reqwest::Client::new();
        let resp = client.post(endpoint_url).json(&action_req)
            .timeout(std::time::Duration::from_secs(30)).send().await;

        match resp {
            Ok(r) if r.status().is_success() => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let text = r.text().await.unwrap_or_default();
                NodeResult::completed(serde_json::json!({ "success": false, "error": text }))
            }
            Err(e) => NodeResult::failed(&format!("Failed to reach bridge: {}", e)),
        }
    }
}

register_node!(WhatsAppReactNode);
