//! Credential Node - Sensitive value (API key, token, secret, password)

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct CredentialNode;

#[async_trait]
impl Node for CredentialNode {
    fn node_type(&self) -> &'static str {
        "Credential"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Credential",
            inputs: vec![],
            outputs: vec![
                PortDef::new("value", "String", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::password("value"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let value = ctx.config.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        NodeResult::completed(serde_json::json!({ "value": value }))
    }
}

register_node!(CredentialNode);
