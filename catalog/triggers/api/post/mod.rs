//! ApiPost: a webhook trigger node. Declares a `Webhook` entry
//! primitive in its metadata; the dispatcher mints a URL per project
//! activation. On POST, the dispatcher routes the body into a new
//! execution and invokes this node's `execute` with the body as
//! input.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::{Diagnostic, NodeOutput, Severity};
use weft_core::project::{NodeDefinition, ProjectDefinition};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

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

    fn validate(&self, node: &NodeDefinition, _project: &ProjectDefinition) -> Vec<Diagnostic> {
        let mut d = Vec::new();
        // Webhook entry primitives declare a `path`. If the user
        // overrides it in config, warn on a leading slash. The
        // dispatcher joins it to the base URL so a leading slash
        // produces a double-slash route.
        if let Some(path) = node.config.get("path").and_then(|v| v.as_str()) {
            if path.starts_with('/') {
                d.push(Diagnostic {
                    line: node.header_span.map(|s| s.start_line).unwrap_or(0),
                    column: 0,
                    severity: Severity::Warning,
                    message: format!(
                        "ApiPost '{}' path '{}' starts with '/'. The dispatcher prefixes the route automatically; drop the leading slash.",
                        node.id, path
                    ),
                    code: Some("apipost-leading-slash".into()),
                });
            }
        }
        d
    }
}
