//! SlackSetTopic Node - Set the topic of a Slack channel.
//!
//! POST conversations.setTopic

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct SlackSetTopicNode;

#[async_trait]
impl Node for SlackSetTopicNode {
    fn node_type(&self) -> &'static str {
        "SlackSetTopic"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Slack Set Topic",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("channelId", "String", true),
                PortDef::new("topic", "String", true),
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
        let topic = ctx.input.get("topic").and_then(|v| v.as_str()).unwrap_or("");

        if bot_token.is_empty() { return NodeResult::failed("Bot token is required"); }
        if channel_id.is_empty() { return NodeResult::failed("Channel ID is required"); }

        let body = serde_json::json!({
            "channel": channel_id,
            "topic": topic,
        });

        let client = reqwest::Client::new();
        let resp = client.post("https://slack.com/api/conversations.setTopic")
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

register_node!(SlackSetTopicNode);
