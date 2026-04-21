//! TelegramUnban Node - Unban a user from a Telegram chat.
//!
//! POST /bot<token>/unbanChatMember

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramUnbanNode;

#[async_trait]
impl Node for TelegramUnbanNode {
    fn node_type(&self) -> &'static str {
        "TelegramUnban"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Unban",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("userId", "String", true),
                PortDef::new("onlyIfBanned", "Boolean", false),
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
        let user_id = ctx.input.get("userId").and_then(|v| v.as_str()).unwrap_or("");
        let only_if_banned = ctx.input.get("onlyIfBanned").and_then(|v| v.as_bool()).unwrap_or(true);

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if chat_id.is_empty() { return NodeResult::failed("Chat ID is required"); }
        if user_id.is_empty() { return NodeResult::failed("User ID is required"); }

        let url = format!("https://api.telegram.org/bot{}/unbanChatMember", bot_token);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "user_id": user_id.parse::<i64>().unwrap_or(0),
            "only_if_banned": only_if_banned,
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

register_node!(TelegramUnbanNode);
