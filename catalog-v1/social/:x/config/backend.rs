//! XConfig Node - X (Twitter) API credentials
//!
//! Stores the user's X API credentials. Connect its "config" output
//! to XReceive (trigger) or XPost (send) nodes.
//!
//! Two auth methods are supported:
//! - Bearer Token (app-only): for read endpoints like search
//! - OAuth 1.0a (api key + access token): for write endpoints like posting

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct XConfigNode;

#[async_trait]
impl Node for XConfigNode {
    fn node_type(&self) -> &'static str {
        "XConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "X Config",
            inputs: vec![],
            outputs: vec![
                PortDef::new("config", "Dict[String, String]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::password("bearerToken"),
                FieldDef::password("apiKey"),
                FieldDef::password("apiKeySecret"),
                FieldDef::password("accessToken"),
                FieldDef::password("accessTokenSecret")
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        NodeResult::completed(serde_json::json!({ "config": ctx.config }))
    }
}

register_node!(XConfigNode);
