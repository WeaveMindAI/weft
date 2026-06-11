//! ApiPost: webhook trigger. Two phases:
//!
//!   - `Phase::TriggerSetup`: build a `Webhook` signal from the
//!     node's config (path + optional apiKey toggle) and register
//!     it. The dispatcher mounts the public URL.
//!
//!   - `Phase::Fire`: the wake payload is the parsed POST body.
//!     Forward every top-level field to the output port with the
//!     same name; stamp `observedAt` with the time we received the
//!     request (distinct from any `receivedAt` the sender may have
//!     included in the payload).

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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
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
                Ok(())
            }
            Phase::Fire => {
                // Wake payload is the parsed POST body; missing means
                // the fire path broke (always delivers), non-object
                // means the sender violated the "body fields as ports"
                // contract. Both fail loud via wake_payload_object.
                let body = ctx.wake_payload_object()?;
                // Fan only DECLARED body fields onto ports: a sender
                // can include arbitrary extra fields the user never
                // wired, and emitting an undeclared port would trip the
                // runtime's loud rejection on an otherwise-fine webhook.
                // `observedAt` is our stamp (when WE received it),
                // excluded from the fan so a sender's own `observedAt`
                // can't override it.
                let output = NodeOutput::empty()
                    .extend_from_declared(body, ctx.declared_output_ports(), &["observedAt"])
                    .set("observedAt", Value::String(chrono::Utc::now().to_rfc3339()));
                ctx.pulse_downstream(output).await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}
