//! TelegramPromote Node - Promote or demote a user in a Telegram chat.
//!
//! POST /bot<token>/promoteChatMember

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramPromoteNode;

#[async_trait]
impl Node for TelegramPromoteNode {
    fn node_type(&self) -> &'static str {
        "TelegramPromote"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Promote",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("userId", "String", true),
                PortDef::new("canManageChat", "Boolean", false),
                PortDef::new("canDeleteMessages", "Boolean", false),
                PortDef::new("canManageVideoChats", "Boolean", false),
                PortDef::new("canRestrictMembers", "Boolean", false),
                PortDef::new("canPromoteMembers", "Boolean", false),
                PortDef::new("canChangeInfo", "Boolean", false),
                PortDef::new("canInviteUsers", "Boolean", false),
                PortDef::new("canPinMessages", "Boolean", false),
                PortDef::new("canPostMessages", "Boolean", false),
                PortDef::new("canEditMessages", "Boolean", false),
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

        let url = format!("https://api.telegram.org/bot{}/promoteChatMember", bot_token);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "user_id": user_id.parse::<i64>().unwrap_or(0),
            "can_manage_chat": get_bool("canManageChat"),
            "can_delete_messages": get_bool("canDeleteMessages"),
            "can_manage_video_chats": get_bool("canManageVideoChats"),
            "can_restrict_members": get_bool("canRestrictMembers"),
            "can_promote_members": get_bool("canPromoteMembers"),
            "can_change_info": get_bool("canChangeInfo"),
            "can_invite_users": get_bool("canInviteUsers"),
            "can_pin_messages": get_bool("canPinMessages"),
            "can_post_messages": get_bool("canPostMessages"),
            "can_edit_messages": get_bool("canEditMessages"),
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

register_node!(TelegramPromoteNode);
