//! DiscordReact Node - Add a reaction to a Discord message.
//!
//! PUT /channels/{channel_id}/messages/{message_id}/reactions/{emoji}/@me

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(Default)]
pub struct DiscordReactNode;

#[async_trait]
impl Node for DiscordReactNode {
    fn node_type(&self) -> &'static str {
        "DiscordReact"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord React",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("channelId", "String", true),
                PortDef::new("messageId", "String", true),
                PortDef::new("emoji", "String", true),
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
        let channel_id = ctx.input.get("channelId").and_then(|v| v.as_str()).unwrap_or("");
        let message_id = ctx.input.get("messageId").and_then(|v| v.as_str()).unwrap_or("");
        let emoji = ctx.input.get("emoji").and_then(|v| v.as_str()).unwrap_or("");

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if channel_id.is_empty() { return NodeResult::failed("Channel ID is required"); }
        if message_id.is_empty() { return NodeResult::failed("Message ID is required"); }
        if emoji.is_empty() { return NodeResult::failed("Emoji is required"); }

        // URL-encode the emoji for the path
        let encoded_emoji = urlencoding::encode(emoji);
        let url = format!(
            "{}/channels/{}/messages/{}/reactions/{}/@me",
            DISCORD_API_BASE, channel_id, message_id, encoded_emoji
        );

        let client = reqwest::Client::new();
        let resp = client.put(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .header("Content-Length", "0")
            .send().await;

        match resp {
            Ok(r) if r.status().is_success() || r.status().as_u16() == 204 => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::error!("Discord react error: {} - {}", status, text);
                NodeResult::completed(serde_json::json!({ "success": false }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(DiscordReactNode);
