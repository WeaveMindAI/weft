//! ElevenLabsConfig Node - ElevenLabs API credentials.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct ElevenLabsConfigNode;

#[async_trait]
impl Node for ElevenLabsConfigNode {
    fn node_type(&self) -> &'static str {
        "ElevenLabsConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "ElevenLabs Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::api_key("apiKey", "elevenlabs"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(ElevenLabsConfigNode);
