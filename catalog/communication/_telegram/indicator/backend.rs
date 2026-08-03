//! TelegramIndicator Node - Show a chat action indicator in a Telegram chat.
//!
//! Uses POST /bot<token>/sendChatAction. Telegram's action indicator decays
//! after ~5s, so this node loops to sustain it for the configured duration
//! with configurable interval and random jitter.
//!
//! Supported actions: typing, upload_photo, record_video, upload_video,
//! record_voice, upload_voice, upload_document, choose_sticker,
//! find_location, record_video_note, upload_video_note.
//!
//! Platform limit: action decays after ~5s, so intervalMs is capped at 4000ms.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Telegram chat action decays after ~5s.
const DECAY_MS: u64 = 5_000;

#[derive(Default)]
pub struct TelegramIndicatorNode;

#[async_trait]
impl Node for TelegramIndicatorNode {
    fn node_type(&self) -> &'static str {
        "TelegramIndicator"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Indicator",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("durationMs", "Number", false),
                PortDef::new("intervalMs", "Number", false),
                PortDef::new("jitterMs", "Number", false),
            ],
            outputs: vec![
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::select("action", vec!["typing", "upload_photo", "record_video", "upload_video", "record_voice", "upload_voice", "upload_document", "choose_sticker", "find_location", "record_video_note", "upload_video_note"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let chat_id = ctx.input.get("chatId")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let bot_token = ctx.input.get("config")
            .and_then(|v| v.get("botToken"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // action: typing, upload_photo, record_video, upload_video,
        //         record_voice, upload_voice, upload_document, etc.
        let action = ctx.config.get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("typing");

        // Timing from input ports (programmatic) with defaults
        let duration_ms: u64 = ctx.input.get("durationMs")
            .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()).or_else(|| v.as_f64().map(|f| f as u64)))
            .unwrap_or(3000);
        let interval_ms: u64 = ctx.input.get("intervalMs")
            .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()).or_else(|| v.as_f64().map(|f| f as u64)))
            .unwrap_or(800)
            .min(DECAY_MS);
        // Cap jitter so worst case (interval + jitter) never exceeds decay window
        let jitter_ms: u64 = ctx.input.get("jitterMs")
            .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()).or_else(|| v.as_f64().map(|f| f as u64)))
            .unwrap_or(300)
            .min(DECAY_MS.saturating_sub(interval_ms));

        if bot_token.is_empty() {
            return NodeResult::failed("Telegram bot token is required. Connect a TelegramConfig node.");
        }
        if chat_id.is_empty() {
            return NodeResult::failed("Chat ID is required");
        }

        let client = reqwest::Client::new();
        let url = format!("https://api.telegram.org/bot{}/sendChatAction", bot_token);

        let start = std::time::Instant::now();
        let max_duration = std::time::Duration::from_millis(duration_ms.min(300_000));
        let mut any_success = false;

        loop {
            let body = serde_json::json!({
                "chat_id": chat_id,
                "action": action,
            });

            let resp = client.post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => { any_success = true; }
                Ok(r) => {
                    let status = r.status();
                    let text = r.text().await.unwrap_or_default();
                    tracing::error!("Telegram indicator API error: {} - {}", status, text);
                }
                Err(e) => {
                    tracing::error!("Telegram indicator request failed: {}", e);
                    if !any_success {
                        return NodeResult::failed(&format!("Failed to trigger indicator: {}", e));
                    }
                }
            }

            if start.elapsed() >= max_duration {
                break;
            }

            let jitter = if jitter_ms > 0 { rand::random::<u64>() % (jitter_ms * 2) } else { 0 };
            let sleep_ms = interval_ms.saturating_sub(jitter_ms) + jitter;
            let remaining = max_duration.saturating_sub(start.elapsed());
            let actual_sleep = std::time::Duration::from_millis(sleep_ms).min(remaining);

            if actual_sleep.is_zero() {
                break;
            }
            tokio::time::sleep(actual_sleep).await;
        }

        NodeResult::completed(serde_json::json!({ "success": any_success }))
    }
}

register_node!(TelegramIndicatorNode);
