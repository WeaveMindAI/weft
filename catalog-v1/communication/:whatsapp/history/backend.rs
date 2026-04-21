//! WhatsAppHistory Node,fetch recent messages from a WhatsApp chat via the bridge sidecar.
//!
//! The sidecar stores raw WAMessage protobufs from initial history sync and live messages.
//! At query time, audio messages are lazy-downloaded from WhatsApp (via the stored protobuf).
//! If the store has fewer messages than requested, on-demand history sync is attempted.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

pub struct WhatsAppHistoryNode;

#[async_trait]
impl Node for WhatsAppHistoryNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppHistory"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp History",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("chatId", "String", true),
            ],
            outputs: vec![
                PortDef::new("contents", "List[String]", false),
                PortDef::new("senderNames", "List[String]", false),
                PortDef::new("timestamps", "List[String]", false),
                PortDef::new("fromMe", "List[Boolean]", false),
                PortDef::new("messageTypes", "List[String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::number("count").with_default(serde_json::json!(20)).with_range(1.0, 50.0),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) if !url.is_empty() => url,
            _ => return NodeResult::failed("No endpointUrl provided. Connect the endpointUrl output of a WhatsAppBridge node."),
        };

        let chat_id = match ctx.input.get("chatId").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id,
            _ => return NodeResult::failed("No chatId provided."),
        };

        let count = ctx.config.get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(20) as usize;

        let client = reqwest::Client::new();
        let action_req = ActionRequest {
            action: "fetchMessages".to_string(),
            payload: serde_json::json!({
                "chatId": chat_id,
                "count": count,
            }),
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
                    return NodeResult::failed(err);
                }

                let raw_messages = result.get("messages")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                let contents: Vec<serde_json::Value> = raw_messages.iter()
                    .map(|m| serde_json::json!(m.get("content").and_then(|v| v.as_str()).unwrap_or("")))
                    .collect();
                let sender_names: Vec<serde_json::Value> = raw_messages.iter()
                    .map(|m| serde_json::json!(m.get("pushName").and_then(|v| v.as_str()).unwrap_or("")))
                    .collect();
                let timestamps: Vec<serde_json::Value> = raw_messages.iter()
                    .map(|m| serde_json::json!(m.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0)))
                    .collect();
                let from_me: Vec<serde_json::Value> = raw_messages.iter()
                    .map(|m| serde_json::json!(m.get("fromMe").and_then(|v| v.as_bool()).unwrap_or(false)))
                    .collect();
                let message_types: Vec<serde_json::Value> = raw_messages.iter()
                    .map(|m| serde_json::json!(m.get("messageType").and_then(|v| v.as_str()).unwrap_or("")))
                    .collect();

                NodeResult::completed(serde_json::json!({
                    "contents": contents,
                    "senderNames": sender_names,
                    "timestamps": timestamps,
                    "fromMe": from_me,
                    "messageTypes": message_types,
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

register_node!(WhatsAppHistoryNode);
