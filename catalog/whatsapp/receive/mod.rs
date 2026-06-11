//! WhatsAppReceive: fires when a WhatsApp message lands at the
//! project's bridge. Two phases:
//!
//!   - `Phase::TriggerSetup`: read the upstream bridge's
//!     `endpointUrl`, compute the `/events` SSE URL, register an
//!     SSE signal. The listener subscribes; the dispatcher receives
//!     `message.received` events and fires fresh executions.
//!
//!   - `Phase::Fire`: the SSE event delivers a parsed JSON object as
//!     the wake payload. Map the WhatsApp-specific fields to output
//!     ports.

use async_trait::async_trait;

use weft_core::context::Phase;
use weft_core::node::NodeOutput;
use weft_core::signal::Sse;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct WhatsAppReceiveNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for WhatsAppReceiveNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("WhatsAppReceive metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        match ctx.phase {
            // Setup registers the SSE signal; emits nothing downstream.
            Phase::TriggerSetup => {
                register(&ctx).await?;
                Ok(())
            }
            Phase::Fire => {
                let out = fire(&ctx)?;
                ctx.pulse_downstream(out).await
            }
            Phase::InfraSetup => Ok(()),
        }
    }
}

async fn register(ctx: &ExecutionContext) -> WeftResult<()> {
    let bridge = ctx
        .input
        .required_str("endpointUrl", "endpointUrl (from WhatsAppBridge)")?;
    // `endpointUrl` is the bridge's bare endpoint URL (the bridge node
    // exports `ctx.endpoint("api").url()`, no path). Append our route.
    let events_url = format!("{}/events", bridge.trim_end_matches('/'));
    ctx.register_signal(Sse {
        url: events_url,
        event_name: "message.received".into(),
    })
    .await
}

fn fire(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    // The SSE listener delivers the parsed `data:` object as the wake
    // payload. Forward each present field on its matching output port;
    // missing fields stay un-mentioned and the engine closes those
    // ports at termination. Substituting empty strings or `false` for
    // absent fields would publish data nulls indistinguishable from
    // real user values to the consumer.
    //
    // The bridge's `message.received` SSE event contract is CLOSED at
    // exactly these 7 fields: `content`, `from`, `pushName`, `messageId`,
    // `timestamp`, `isGroup`, `chatId`. The metadata declares the same
    // 7 output ports. An extra field on the wire would land on an
    // undeclared output port and trip the engine's
    // `check_declared_outputs` (fail-loud); a missing field stays un-
    // mentioned and the engine closes that port at termination.
    let data = ctx.wake_payload_object()?;
    Ok(NodeOutput::empty().extend_from_object(data, &[]))
}
