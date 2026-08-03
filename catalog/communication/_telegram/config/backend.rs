//! TelegramConfig Node - Telegram Bot API credentials
//!
//! Stores the bot token. Connect its "config" output to
//! TelegramReceive (trigger) or TelegramSend nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TelegramConfigNode;

#[async_trait]
impl Node for TelegramConfigNode {
    fn node_type(&self) -> &'static str {
        "TelegramConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Telegram Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::password("botToken"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(TelegramConfigNode);
