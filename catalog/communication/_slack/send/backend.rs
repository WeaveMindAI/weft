//! SlackSend Node - Send messages and media via Slack Bot API.
//!
//! Uses chat.postMessage to send text messages to a channel.
//! When media is provided, images are sent as blocks, other types as URL in text.
//! Supports thread replies via threadTs.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct SlackSendNode;

#[async_trait]
impl Node for SlackSendNode {
    fn node_type(&self) -> &'static str {
        "SlackSend"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Slack Send",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("channelId", "String", true),
                PortDef::new("text", "String", false),
                PortDef::new("threadTs", "String", false),
                PortDef::new("media", "Media", false),
            ],
            outputs: vec![
                PortDef::new("messageTs", "String", false),
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec!["text".into(), "media".into()]],
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let text = ctx.input.get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let channel_id = ctx.input.get("channelId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let thread_ts = ctx.input.get("threadTs")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let media = ctx.input.get("media")
            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());

        let bot_token = ctx.input.get("config")
            .and_then(|v| v.get("botToken"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if bot_token.is_empty() {
            return NodeResult::failed("Slack Bot Token is required. Connect a SlackConfig node.");
        }

        if channel_id.is_empty() {
            return NodeResult::failed("Channel ID is required");
        }

        if text.is_empty() && media.is_none() {
            return NodeResult::failed("Either text or media is required");
        }

        // Build message text, appending media URL for non-image types
        let effective_text = if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let mime = media_obj.get("mimeType").and_then(|v| v.as_str()).unwrap_or("");
            let media_type = weft_core::media_category_from_mime(mime);
            if media_type != "image" && !media_url.is_empty() {
                if text.is_empty() { media_url.to_string() } else { format!("{}\n{}", text, media_url) }
            } else {
                text.to_string()
            }
        } else {
            text.to_string()
        };

        let mut body = serde_json::json!({
            "channel": channel_id,
            "text": if effective_text.is_empty() { " " } else { &effective_text },
        });

        // For images, add an image block
        if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let mime = media_obj.get("mimeType").and_then(|v| v.as_str()).unwrap_or("");
            let media_type = weft_core::media_category_from_mime(mime);
            if media_type == "image" && !media_url.is_empty() {
                body["blocks"] = serde_json::json!([
                    {
                        "type": "section",
                        "text": { "type": "mrkdwn", "text": if text.is_empty() { " " } else { text } }
                    },
                    {
                        "type": "image",
                        "image_url": media_url,
                        "alt_text": media_obj.get("filename").and_then(|v| v.as_str()).unwrap_or("image")
                    }
                ]);
            }
        }

        if let Some(ts) = thread_ts {
            body["thread_ts"] = serde_json::json!(ts);
        }

        let client = reqwest::Client::new();
        let response = client
            .post("https://slack.com/api/chat.postMessage")
            .header("Authorization", format!("Bearer {}", bot_token))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let resp_body: serde_json::Value = resp.json().await.unwrap_or_default();
                let ok = resp_body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);

                if ok {
                    let message_ts = resp_body.get("ts")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    NodeResult::completed(serde_json::json!({
                        "messageTs": message_ts,
                        "success": true,
                    }))
                } else {
                    let error = resp_body.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
                    tracing::error!("Slack API error: {}", error);
                    NodeResult::completed(serde_json::json!({
                        "messageTs": "",
                        "success": false,
                    }))
                }
            }
            Err(e) => {
                tracing::error!("Slack request failed: {}", e);
                NodeResult::failed(&format!("Failed to send Slack message: {}", e))
            }
        }
    }
}

register_node!(SlackSendNode);
