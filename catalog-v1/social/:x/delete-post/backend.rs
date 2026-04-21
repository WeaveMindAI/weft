//! XDeletePost Node - Delete a post on X/Twitter.
//!
//! DELETE /2/tweets/{id}
//! Requires OAuth 1.0a authentication.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::nodes::x_post::build_oauth_header;
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct XDeletePostNode;

#[async_trait]
impl Node for XDeletePostNode {
    fn node_type(&self) -> &'static str {
        "XDeletePost"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "X Delete Post",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", true),
                PortDef::new("tweetId", "String", true),
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
        let config = ctx.input.get("config").cloned().unwrap_or_default();
        let api_key = config.get("apiKey").and_then(|v| v.as_str()).unwrap_or("");
        let api_key_secret = config.get("apiKeySecret").and_then(|v| v.as_str()).unwrap_or("");
        let access_token = config.get("accessToken").and_then(|v| v.as_str()).unwrap_or("");
        let access_token_secret = config.get("accessTokenSecret").and_then(|v| v.as_str()).unwrap_or("");
        let tweet_id = ctx.input.get("tweetId").and_then(|v| v.as_str()).unwrap_or("");

        if api_key.is_empty() || api_key_secret.is_empty() || access_token.is_empty() || access_token_secret.is_empty() {
            return NodeResult::failed("OAuth 1.0a credentials are required (apiKey, apiKeySecret, accessToken, accessTokenSecret)");
        }
        if tweet_id.is_empty() { return NodeResult::failed("Tweet ID is required"); }

        let url = format!("https://api.x.com/2/tweets/{}", tweet_id);
        let auth_header = build_oauth_header(
            "DELETE", &url, api_key, api_key_secret, access_token, access_token_secret,
        );

        let client = reqwest::Client::new();
        let resp = client.delete(&url)
            .header("Authorization", &auth_header)
            .send().await;

        match resp {
            Ok(r) if r.status().is_success() => {
                NodeResult::completed(serde_json::json!({ "success": true }))
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::error!("X delete post error: {} - {}", status, text);
                NodeResult::completed(serde_json::json!({ "success": false }))
            }
            Err(e) => NodeResult::failed(&format!("Request failed: {}", e)),
        }
    }
}

register_node!(XDeletePostNode);
