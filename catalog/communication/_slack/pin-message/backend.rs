//! SlackPinMessage Node - Pin a message in a Slack channel.
//!
//! POST pins.add

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct SlackPinMessageNode;

#[async_trait]
impl Node for SlackPinMessageNode {
    fn node_type(&self) -> &'static str {
        "SlackPinMessage"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Slack Pin Message",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("channelId", "String", true),
                PortDef::new("messageTs", "String", true),
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
        let message_ts = ctx.input.get("messageTs").and_then(|v| v.as_str()).unwrap_or("");

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if channel_id.is_empty() { return NodeResult::failed("Channel ID is required"); }
        if message_ts.is_empty() { return NodeResult::failed("Message timestamp is required"); }

        let body = serde_json::json!({
            "channel": channel_id,
            "timestamp": message_ts,
        });

        let client = reqwest::Client::new();
        let resp = client.post("https://slack.com/api/pins.add")
            .header("Authorization", format!("Bearer {}", bot_token))
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body).send().await;

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

register_node!(SlackPinMessageNode);
