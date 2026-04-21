//! DiscordBan Node - Ban a user from a Discord guild.
//!
//! PUT /guilds/{guild_id}/bans/{user_id}

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(Default)]
pub struct DiscordBanNode;

#[async_trait]
impl Node for DiscordBanNode {
    fn node_type(&self) -> &'static str {
        "DiscordBan"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord Ban",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("guildId", "String", true),
                PortDef::new("userId", "String", true),
                PortDef::new("deleteMessageSeconds", "Number", false),
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
        let guild_id = ctx.input.get("guildId").and_then(|v| v.as_str()).unwrap_or("");
        let user_id = ctx.input.get("userId").and_then(|v| v.as_str()).unwrap_or("");
        let delete_seconds = ctx.input.get("deleteMessageSeconds")
            .and_then(|v| v.as_f64())
            .map(|v| v as u64);

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if guild_id.is_empty() { return NodeResult::failed("Guild ID is required"); }
        if user_id.is_empty() { return NodeResult::failed("User ID is required"); }

        let url = format!("{}/guilds/{}/bans/{}", DISCORD_API_BASE, guild_id, user_id);
        let mut body = serde_json::json!({});
        if let Some(secs) = delete_seconds {
            body["delete_message_seconds"] = serde_json::json!(secs);
        }

        let client = reqwest::Client::new();
        let resp = client.put(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send().await;

        match resp {
            Ok(r) if r.status().is_success() || r.status().as_u16() == 204 => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::error!("Discord ban error: {} - {}", status, text);
                NodeResult::completed(serde_json::json!({ "success": false }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(DiscordBanNode);
