//! ApiPost: webhook trigger. Two phases:
//!
//!   - `Phase::TriggerSetup`: build a `Webhook` signal from the
//!     node's config (path + optional apiKey toggle) and register
//!     it. The dispatcher mounts the public URL.
//!
//!   - `Phase::Fire`: the dispatcher seeds the raw posted JSON on
//!     the `__seed__` port. Forward every top-level field to the
//!     output port with the same name; add `receivedAt` if the
//!     payload didn't carry one.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::signal::{Webhook, WebhookAuth};
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
                let auth = if ctx
                    .config
                    .values
                    .get("generateApiKey")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    WebhookAuth::OptionalApiKey
                } else {
                    WebhookAuth::None
                };
                ctx.register_signal(Webhook { path, auth }).await?;
                Ok(NodeOutput::empty())
            }
            Phase::Fire => {
                // `fire_public_entry` seeds the raw POST body into
                // `__seed__`; fan its top-level keys onto output ports.
                let body = ctx
                    .input
                    .values
                    .get("__seed__")
                    .cloned()
                    .unwrap_or(Value::Null);
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
