//! TelegramRestrict Node - Restrict a user's permissions in a Telegram supergroup.
//!
//! POST /bot<token>/restrictChatMember

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramRestrictNode;

#[async_trait]
impl Node for TelegramRestrictNode {
    fn node_type(&self) -> &'static str {
        "TelegramRestrict"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Restrict",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("userId", "String", true),
                PortDef::new("canSendMessages", "Boolean", false),
                PortDef::new("canSendMedia", "Boolean", false),
                PortDef::new("canSendPolls", "Boolean", false),
                PortDef::new("canAddWebPagePreviews", "Boolean", false),
                PortDef::new("canInviteUsers", "Boolean", false),
                PortDef::new("canPinMessages", "Boolean", false),
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

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if chat_id.is_empty() { return NodeResult::failed("Chat ID is required"); }
        if user_id.is_empty() { return NodeResult::failed("User ID is required"); }

        let get_bool = |key: &str| -> bool {
            ctx.input.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
        };

        let permissions = serde_json::json!({
            "can_send_messages": get_bool("canSendMessages"),
            "can_send_audios": get_bool("canSendMedia"),
            "can_send_documents": get_bool("canSendMedia"),
            "can_send_photos": get_bool("canSendMedia"),
            "can_send_videos": get_bool("canSendMedia"),
            "can_send_video_notes": get_bool("canSendMedia"),
            "can_send_voice_notes": get_bool("canSendMedia"),
            "can_send_polls": get_bool("canSendPolls"),
            "can_add_web_page_previews": get_bool("canAddWebPagePreviews"),
            "can_invite_users": get_bool("canInviteUsers"),
            "can_pin_messages": get_bool("canPinMessages"),
        });

        let url = format!("https://api.telegram.org/bot{}/restrictChatMember", bot_token);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "user_id": user_id.parse::<i64>().unwrap_or(0),
            "permissions": permissions,
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

register_node!(TelegramRestrictNode);
