//! TestSseTrigger: a project-local custom trigger that subscribes to an SSE
//! feed at a configured URL and fires per matching event, emitting the event's
//! `value` field downstream.
//!
//! Exists so the e2e rig can exercise the reach-out signal path (the system
//! DIALS OUT to a feed) against a fake SSE server, without depending on a
//! domain node (WhatsApp). The reach-out signal KINDS live in the language; a
//! node just has to register one. This is the minimal node that does.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::signal::SseSubscribe;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct TestSseTriggerNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for TestSseTriggerNode {
    fn node_type(&self) -> &'static str {
        "TestSseTrigger"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("TestSseTrigger metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        match ctx.phase {
            Phase::TriggerSetup => {
                let url: String = ctx.config.get("url")?;
                let event_name: String = ctx.config.get("event_name")?;
                ctx.register_signal(SseSubscribe { url, event_name }).await
            }
            Phase::Fire => {
                // The SSE listener delivers the parsed `data:` object as the
                // wake payload. Emit its `value` field on our `value` port.
                let data = ctx.wake_payload_object()?;
                let value = data.get("value").cloned().unwrap_or(Value::Null);
                ctx.pulse_downstream(NodeOutput::empty().set("value", value))
                    .await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}
