//! DiscordConfig Node - Discord bot credentials

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct DiscordConfigNode;

#[async_trait]
impl Node for DiscordConfigNode {
    fn node_type(&self) -> &'static str {
        "DiscordConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Discord Config",
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

register_node!(DiscordConfigNode);
