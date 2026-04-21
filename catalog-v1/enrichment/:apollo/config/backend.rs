//! ApolloConfig Node - Apollo.io API credentials shared across all Apollo nodes.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct ApolloConfigNode;

#[async_trait]
impl Node for ApolloConfigNode {
    fn node_type(&self) -> &'static str {
        "ApolloConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Apollo Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::api_key("apiKey", "apollo"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(ApolloConfigNode);
