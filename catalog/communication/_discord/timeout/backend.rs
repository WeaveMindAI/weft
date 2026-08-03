//! DiscordTimeout Node - Timeout a member in a Discord guild.
//!
//! PATCH /guilds/{guild_id}/members/{user_id}
//! Sets communication_disabled_until to mute a user.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(Default)]
pub struct DiscordTimeoutNode;

#[async_trait]
impl Node for DiscordTimeoutNode {
    fn node_type(&self) -> &'static str {
        "DiscordTimeout"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord Timeout",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("guildId", "String", true),
                PortDef::new("userId", "String", true),
                PortDef::new("durationSeconds", "Number", true),
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
        let duration_secs = ctx.input.get("durationSeconds")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0) as i64;

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if guild_id.is_empty() { return NodeResult::failed("Guild ID is required"); }
        if user_id.is_empty() { return NodeResult::failed("User ID is required"); }

        // Max timeout is 28 days (2419200 seconds)
        let duration_secs = duration_secs.clamp(0, 2419200);

        let timeout_until = if duration_secs > 0 {
            let until = chrono::Utc::now() + chrono::Duration::seconds(duration_secs);
            serde_json::json!(until.to_rfc3339())
        } else {
            // duration 0 removes the timeout
            serde_json::json!(null)
        };

        let url = format!("{}/guilds/{}/members/{}", DISCORD_API_BASE, guild_id, user_id);
        let body = serde_json::json!({
            "communication_disabled_until": timeout_until,
        });

        let client = reqwest::Client::new();
        let resp = client.patch(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send().await;

        match resp {
            Ok(r) if r.status().is_success() => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::error!("Discord timeout error: {} - {}", status, text);
                NodeResult::completed(serde_json::json!({ "success": false }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(DiscordTimeoutNode);
