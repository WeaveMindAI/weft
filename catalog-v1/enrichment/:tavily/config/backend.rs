//! TavilyConfig Node - Tavily web search API credentials.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct TavilyConfigNode;

#[async_trait]
impl Node for TavilyConfigNode {
    fn node_type(&self) -> &'static str {
        "TavilyConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Tavily Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::api_key("apiKey", "tavily"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(TavilyConfigNode);
