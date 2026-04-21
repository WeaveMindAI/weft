//! TelegramDeleteMessage Node - Delete a message in a Telegram chat.
//!
//! POST /bot<token>/deleteMessage

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramDeleteMessageNode;

#[async_trait]
impl Node for TelegramDeleteMessageNode {
    fn node_type(&self) -> &'static str {
        "TelegramDeleteMessage"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Delete Message",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("messageId", "String", true),
            ],
            outputs: vec![
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
        let message_id = ctx.input.get("messageId").and_then(|v| v.as_str()).unwrap_or("");

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if chat_id.is_empty() { return NodeResult::failed("Chat ID is required"); }
        if message_id.is_empty() { return NodeResult::failed("Message ID is required"); }

        let url = format!("https://api.telegram.org/bot{}/deleteMessage", bot_token);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id.parse::<i64>().unwrap_or(0),
        });

        let client = reqwest::Client::new();
        let resp = client.post(&url).json(&body).send().await;

        match resp {
            Ok(r) => {
                let body: serde_json::Value = r.json().await.unwrap_or_default();
                let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                NodeResult::completed(serde_json::json!({ "success": ok }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(TelegramDeleteMessageNode);
