//! TelegramSend Node - Send messages and media via Telegram Bot API.
//!
//! Uses POST /bot<token>/sendMessage for text, and sendPhoto/sendVideo/
//! sendAudio/sendDocument when a media object is provided.
//! Supports replies via replyToMessageId.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramSendNode;

#[async_trait]
impl Node for TelegramSendNode {
    fn node_type(&self) -> &'static str {
        "TelegramSend"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Send",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("text", "String", false),
                PortDef::new("replyToMessageId", "String", false),
                PortDef::new("media", "Media", false),
            ],
            outputs: vec![
                PortDef::new("messageId", "String", false),
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

        let chat_id = ctx.input.get("chatId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let reply_to = ctx.input.get("replyToMessageId")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let media = ctx.input.get("media")
            .filter(|v| v.is_object() && !v.as_object().unwrap().is_empty());

        let bot_token = ctx.input.get("config")
            .and_then(|v| v.get("botToken"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if bot_token.is_empty() {
            return NodeResult::failed("Telegram bot token is required. Connect a TelegramConfig node.");
        }

        if chat_id.is_empty() {
            return NodeResult::failed("Chat ID is required");
        }

        if text.is_empty() && media.is_none() {
            return NodeResult::failed("Either text or media is required");
        }

        let client = reqwest::Client::new();

        // Determine the API method and body based on whether media is present
        let (url, body) = if let Some(media_obj) = media {
            let media_url = media_obj.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let mime = media_obj.get("mimeType").and_then(|v| v.as_str()).unwrap_or("");
            let media_type = weft_core::media_category_from_mime(mime);

            if media_url.is_empty() {
                // Fall back to text-only
                let mut b = serde_json::json!({ "chat_id": chat_id, "text": text });
                if let Some(reply_id) = reply_to {
                    b["reply_parameters"] = serde_json::json!({ "message_id": reply_id.parse::<i64>().unwrap_or(0) });
                }
                (format!("https://api.telegram.org/bot{}/sendMessage", bot_token), b)
            } else {
                let (method, field_name) = match media_type {
                    "image" => ("sendPhoto", "photo"),
                    "video" => ("sendVideo", "video"),
                    "audio" => ("sendAudio", "audio"),
                    _ => ("sendDocument", "document"),
                };
                let mut b = serde_json::json!({
                    "chat_id": chat_id,
                    field_name: media_url,
                });
                if !text.is_empty() {
                    b["caption"] = serde_json::json!(text);
                }
                if let Some(reply_id) = reply_to {
                    b["reply_parameters"] = serde_json::json!({ "message_id": reply_id.parse::<i64>().unwrap_or(0) });
                }
                (format!("https://api.telegram.org/bot{}/{}", bot_token, method), b)
            }
        } else {
            let mut b = serde_json::json!({ "chat_id": chat_id, "text": text });
            if let Some(reply_id) = reply_to {
                b["reply_parameters"] = serde_json::json!({ "message_id": reply_id.parse::<i64>().unwrap_or(0) });
            }
            (format!("https://api.telegram.org/bot{}/sendMessage", bot_token), b)
        };

        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    let resp_body: serde_json::Value = resp.json().await.unwrap_or_default();
                    let message_id = resp_body.get("result")
                        .and_then(|r| r.get("message_id"))
                        .and_then(|v| v.as_i64())
                        .map(|id| id.to_string())
                        .unwrap_or_default();

                    NodeResult::completed(serde_json::json!({
                        "messageId": message_id,
                        "success": true,
                    }))
                } else {
                    let status = resp.status();
                    let error_text = resp.text().await.unwrap_or_default();
                    tracing::error!("Telegram API error: {} - {}", status, error_text);
                    NodeResult::completed(serde_json::json!({
                        "messageId": "",
                        "success": false,
                    }))
                }
            }
            Err(e) => {
                tracing::error!("Telegram request failed: {}", e);
                NodeResult::failed(&format!("Failed to send Telegram message: {}", e))
            }
        }
    }
}

register_node!(TelegramSendNode);
