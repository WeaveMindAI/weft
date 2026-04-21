//! Discord Send Node - Send messages and media to Discord channels
//!
//! When a media object is provided (from Image/Video/Audio/Document nodes),
//! the media URL is included as an embed. Text is optional when media is provided.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

#[derive(Default)]
pub struct DiscordSendNode;

#[async_trait]
impl Node for DiscordSendNode {
    fn node_type(&self) -> &'static str {
        "DiscordSend"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord Send",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("message", "String", false),
                PortDef::new("channelId", "String", true),
                PortDef::new("media", "Media", false),
            ],
            outputs: vec![
                PortDef::new("messageId", "String", false),
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec!["message".into(), "media".into()]],
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let message = ctx.input.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let channel_id = ctx.input.get("channelId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let media = ctx.input.get("media")
            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());

        let bot_token = ctx.input.get("config")
            .and_then(|v| v.get("botToken"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if bot_token.is_empty() {
            return NodeResult::failed("Discord bot token is required in node config");
        }

        if channel_id.is_empty() {
            return NodeResult::failed("Channel ID is required");
        }

        if message.is_empty() && media.is_none() {
            return NodeResult::failed("Either message text or media is required");
        }

        let client = reqwest::Client::new();
        let url = format!("{}/channels/{}/messages", DISCORD_API_BASE, channel_id);

        // Build JSON body: text + optional embed for media URL
        let mut body = serde_json::json!({});
        if !message.is_empty() {
            body["content"] = serde_json::json!(message);
        }
        if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let mime = media_obj.get("mimeType").and_then(|v| v.as_str()).unwrap_or("");
            let media_type = weft_core::media_category_from_mime(mime);
            if !media_url.is_empty() {
                match media_type {
                    "image" => {
                        body["embeds"] = serde_json::json!([{ "image": { "url": media_url } }]);
                    }
                    "video" => {
                        body["embeds"] = serde_json::json!([{ "video": { "url": media_url } }]);
                    }
                    _ => {
                        // For audio/document, append the URL to the message content
                        let existing = body.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let combined = if existing.is_empty() {
                            media_url.to_string()
                        } else {
                            format!("{}\n{}", existing, media_url)
                        };
                        body["content"] = serde_json::json!(combined);
                    }
                }
            }
        }

        let response = client
            .post(&url)
            .header("Authorization", format!("Bot {}", bot_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    let body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let message_id = body.get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    NodeResult::completed(serde_json::json!({
                        "messageId": message_id,
                        "success": true,
                    }))
                } else {
                    let status = resp.status();
                    let error_text = resp.text().await.unwrap_or_default();
                    tracing::error!("Discord API error: {} - {}", status, error_text);
                    NodeResult::completed(serde_json::json!({
                        "messageId": "",
                        "success": false,
                    }))
                }
            }
            Err(e) => {
                tracing::error!("Discord request failed: {}", e);
                NodeResult::failed(&format!("Failed to send Discord message: {}", e))
            }
        }
    }
}

register_node!(DiscordSendNode);
