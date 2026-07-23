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

use weft_core::node::NodeOutput;
use weft_core::signal::SseSubscribe;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct TestSseTriggerNode;

#[async_trait]
impl Node for TestSseTriggerNode {
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        let event_name: String = ctx.inputs.get("event_name")?;
        ctx.register_signal(SseSubscribe { url, event_name }).await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The SSE listener delivers the parsed `data:` object as the
        // wake payload. Emit its `value` field on our `value` port.
        let value: Value = ctx.wake.get_or("value", Value::Null)?;
        ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
    }
}
