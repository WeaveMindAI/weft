//! SlackConfig Node - Slack API credentials
//!
//! Stores the Bot Token and App-Level Token. Connect its "config" output
//! to SlackReceive (trigger) or SlackSend nodes.
//!
//! - Bot Token (xoxb-...): for API calls (sending messages, etc.)
//! - App-Level Token (xapp-...): for Socket Mode WebSocket connection

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct SlackConfigNode;

#[async_trait]
impl Node for SlackConfigNode {
    fn node_type(&self) -> &'static str {
        "SlackConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Slack Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::password("botToken"),
                FieldDef::password("appToken"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(SlackConfigNode);
