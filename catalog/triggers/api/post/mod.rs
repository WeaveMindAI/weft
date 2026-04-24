//! ApiPost: webhook trigger. Two phases:
//!
//!   - `Phase::TriggerSetup`: build a `WakeSignalKind::Webhook` spec
//!     from the node's config (path + optional apiKey) and hand it
//!     to the dispatcher via `ctx.register_signal`. The listener
//!     mints the URL.
//!
//!   - `Phase::Fire`: the dispatcher seeded the posted JSON on the
//!     `__seed__` port wrapped as `{body: <json>}`. Forward every
//!     body field to the output port with the same name; add
//!     `receivedAt` if no input provided one.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::primitive::{WakeSignalKind, WakeSignalSpec, WebhookAuth};
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
        match ctx.phase {
            Phase::TriggerSetup => {
                let path = ctx
                    .config
                    .values
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let auth = if ctx.config.values.contains_key("apiKey") {
                    WebhookAuth::OptionalApiKey {
                        field: "apiKey".into(),
                    }
                } else {
                    WebhookAuth::None
                };
                let spec = WakeSignalSpec {
                    kind: WakeSignalKind::Webhook { path, auth },
                    is_resume: false,
                };
                ctx.register_signal(spec).await?;
                Ok(NodeOutput::empty())
            }
            Phase::Fire => {
                let payload = ctx
                    .input
                    .values
                    .get("__seed__")
                    .cloned()
                    .unwrap_or(Value::Null);
                let body = payload.get("body").cloned().unwrap_or(payload);
                let mut output = NodeOutput::empty();
                if let Value::Object(obj) = body {
                    for (k, v) in obj {
                        output = output.set(k, v);
                    }
                }
                if output.get("receivedAt").is_none() {
                    output = output.set(
                        "receivedAt",
                        Value::String(chrono::Utc::now().to_rfc3339()),
                    );
                }
                Ok(output)
            }
            Phase::InfraSetup => Ok(NodeOutput::empty()),
        }
    }
}
