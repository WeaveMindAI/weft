//! ApiPost: a webhook trigger node. Declares a `Webhook` entry
//! primitive in its metadata; the dispatcher mints a URL per project
//! activation. On POST, the dispatcher routes the body into a new
//! execution and invokes this node's `execute` with the body as
//! input.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

pub struct ApiPostNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for ApiPostNode {
    fn node_type(&self) -> &'static str {
        "ApiPost"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("ApiPost metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // The dispatcher populates `body` and `receivedAt` on the input
        // bag when it wakes this entry. Pass them through.
        let body = ctx.input.raw("body").cloned().unwrap_or(Value::Null);
        let received_at = ctx
            .input
            .raw("receivedAt")
            .cloned()
            .unwrap_or_else(|| Value::String(chrono::Utc::now().to_rfc3339()));

        Ok(NodeOutput::empty().set("body", body).set("receivedAt", received_at))
    }
}
