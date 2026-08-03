//! TelegramForwardMessage Node - Forward a message between Telegram chats.
//!
//! POST /bot<token>/forwardMessage

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramForwardMessageNode;

#[async_trait]
impl Node for TelegramForwardMessageNode {
    fn node_type(&self) -> &'static str {
        "TelegramForwardMessage"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Forward Message",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("fromChatId", "String", true),
                PortDef::new("messageId", "String", true),
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
        let bot_token = ctx.input.get("config")
            .and_then(|v| v.get("botToken")).and_then(|v| v.as_str())
            .unwrap_or("");
        let chat_id = ctx.input.get("chatId").and_then(|v| v.as_str()).unwrap_or("");
        let from_chat_id = ctx.input.get("fromChatId").and_then(|v| v.as_str()).unwrap_or("");
        let message_id = ctx.input.get("messageId").and_then(|v| v.as_str()).unwrap_or("");

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if chat_id.is_empty() { return NodeResult::failed("Target Chat ID is required"); }
        if from_chat_id.is_empty() { return NodeResult::failed("Source Chat ID is required"); }
        if message_id.is_empty() { return NodeResult::failed("Message ID is required"); }

        let url = format!("https://api.telegram.org/bot{}/forwardMessage", bot_token);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id.parse::<i64>().unwrap_or(0),
        });

        let client = reqwest::Client::new();
        let resp = client.post(&url).json(&body).send().await;

        match resp {
            Ok(r) => {
                let body: serde_json::Value = r.json().await.unwrap_or_default();
                let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                let msg_id = body.get("result")
                    .and_then(|r| r.get("message_id"))
                    .and_then(|v| v.as_i64())
                    .map(|id| id.to_string())
                    .unwrap_or_default();
                NodeResult::completed(serde_json::json!({
                    "messageId": msg_id,
                    "success": ok,
                }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(TelegramForwardMessageNode);
