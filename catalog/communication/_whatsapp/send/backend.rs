//! WhatsAppSend Node - sends text messages via a WhatsApp bridge sidecar.
//!
//! For media, use WhatsAppSendMedia instead.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

pub struct WhatsAppSendNode;

#[async_trait]
impl Node for WhatsAppSendNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppSend"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Send",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("to", "String", true),
                PortDef::new("message", "String", true),
            ],
            outputs: vec![
                PortDef::new("messageId", "String", false),
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) if !url.is_empty() => url,
            _ => return NodeResult::failed("No endpointUrl provided. Connect the endpointUrl output of a WhatsAppBridge node."),
        };

        let to = match ctx.input.get("to").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t,
            _ => return NodeResult::failed("'to' is required (e.g. 1234567890@s.whatsapp.net)"),
        };

        let message = match ctx.input.get("message").and_then(|v| v.as_str()) {
            Some(m) if !m.is_empty() => m,
            _ => return NodeResult::failed("'message' is required"),
        };

        let client = reqwest::Client::new();
        let action_req = ActionRequest {
            action: "sendMessage".to_string(),
            payload: serde_json::json!({ "to": to, "text": message }),
        };

        let resp = client.post(endpoint_url)
            .json(&action_req)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let body: serde_json::Value = r.json().await.unwrap_or_default();
                let result = body.get("result").cloned().unwrap_or_default();
                if let Some(err) = result.get("error").and_then(|v| v.as_str()) {
                    return NodeResult::completed(serde_json::json!({
                        "messageId": "",
                        "success": false,
                        "error": err,
                    }));
                }
                let message_id = result.get("messageId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                NodeResult::completed(serde_json::json!({
                    "messageId": message_id,
                    "success": true,
                }))
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                NodeResult::failed(&format!("Bridge returned {}: {}", status, text))
            }
            Err(e) => NodeResult::failed(&format!("Failed to reach bridge: {}", e)),
        }
    }
}

register_node!(WhatsAppSendNode);
