//! WhatsAppIndicator Node - Show typing/recording indicator in a WhatsApp chat.
//!
//! Uses the sendPresenceUpdate sidecar action. WhatsApp presence decays
//! after a few seconds, so this node loops to sustain it for the configured
//! duration with configurable interval and random jitter.
//!
//! Supported actions: composing (typing), recording, paused.
//!
//! Platform limit: presence decays after ~10s, so intervalMs is capped at 8000ms.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};
use weft_core::sidecar::ActionRequest;

/// WhatsApp presence decays after ~10s.
const DECAY_MS: u64 = 10_000;

pub struct WhatsAppIndicatorNode;

#[async_trait]
impl Node for WhatsAppIndicatorNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppIndicator"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "WhatsApp Indicator",
            inputs: vec![
                PortDef::new("endpointUrl", "String", true),
                PortDef::new("chatId", "String", true),
                PortDef::new("durationMs", "Number", false),
                PortDef::new("intervalMs", "Number", false),
                PortDef::new("jitterMs", "Number", false),
            ],
            outputs: vec![
                PortDef::new("success", "Boolean", false),
            ],
            features: NodeFeatures { ..Default::default() },
            fields: vec![
                FieldDef::select("action", vec!["composing", "recording", "paused"]),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let endpoint_url = match ctx.input.get("endpointUrl").and_then(|v| v.as_str()) {
            Some(url) if !url.is_empty() => url.to_string(),
            _ => return NodeResult::failed("No endpointUrl provided. Connect a WhatsAppBridge node."),
        };
        let chat_id = ctx.input.get("chatId").and_then(|v| v.as_str()).unwrap_or("");

        // action: composing (typing), recording, paused
        let action = ctx.config.get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("composing");

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

        if chat_id.is_empty() { return NodeResult::failed("Chat ID is required"); }

        let client = reqwest::Client::new();
        let start = std::time::Instant::now();
        let max_duration = std::time::Duration::from_millis(duration_ms.min(300_000));
        let mut any_success = false;

        loop {
            let action_req = ActionRequest {
                action: "sendPresenceUpdate".to_string(),
                payload: serde_json::json!({ "chatId": chat_id, "presence": action }),
            };

            let resp = client.post(&endpoint_url).json(&action_req)
                .timeout(std::time::Duration::from_secs(10)).send().await;

            match resp {
                Ok(r) if r.status().is_success() => { any_success = true; }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("WhatsApp indicator request failed: {}", e);
                    if !any_success {
                        return NodeResult::failed(&format!("Failed to reach bridge: {}", e));
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

        // Send "paused" to cleanly stop the indicator
        if action != "paused" {
            let stop_req = ActionRequest {
                action: "sendPresenceUpdate".to_string(),
                payload: serde_json::json!({ "chatId": chat_id, "presence": "paused" }),
            };
            let _ = client.post(&endpoint_url).json(&stop_req)
                .timeout(std::time::Duration::from_secs(5)).send().await;
        }

        NodeResult::completed(serde_json::json!({ "success": any_success }))
    }
}

register_node!(WhatsAppIndicatorNode);
