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
                let out = fire(&ctx).await?;
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

/// Message types whose bytes the bridge can serve via
/// `/media/<messageId>`.
const MEDIA_TYPES: [&str; 5] = ["image", "video", "audio", "document", "sticker"];

async fn fire(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    // The SSE listener delivers the parsed `data:` object as the wake
    // payload. Fan the DECLARED fields onto their matching output
    // ports; payload extras that are routing handles, not data
    // (`messageKey`, used by the bridge's own media resolve), are
    // skipped by intersecting with the declared set. Missing fields
    // stay un-mentioned and the engine closes those ports at
    // termination (substituting empty strings / `false` would
    // publish data nulls indistinguishable from real user values).
    let data = ctx.wake_payload_object()?.clone();
    let mut out =
        NodeOutput::empty().extend_from_declared(&data, ctx.declared_output_ports(), &["file"]);

    // Media messages: stream the bytes from the bridge's media
    // endpoint STRAIGHT into execution-scoped storage and emit the
    // self-describing stored-file reference on `file`. Bytes never ride
    // the pulse path; downstream nodes get/stream/presign via the
    // reference.
    let message_type = data.get("messageType").and_then(|v| v.as_str()).unwrap_or("text");
    if MEDIA_TYPES.contains(&message_type) {
        let bridge = ctx
            .input
            .required_str("endpointUrl", "endpointUrl (from WhatsAppBridge)")?;
        let message_id = data
            .get("messageId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                weft_core::WeftError::NodeExecution(
                    "media message without a messageId; cannot fetch its bytes".into(),
                )
            })?;
        // Stream the bytes from the bridge's media endpoint straight
        // into execution storage via the language capability (it GETs,
        // derives the mime, streams in bounded-memory). The node only
        // builds the URL + a stable filename; no HTTP plumbing here.
        let url = format!("{}/media/{}", bridge.trim_end_matches('/'), message_id);
        let filename = format!("whatsapp-{message_id}");
        let file = ctx
            .storage(weft_core::storage::StorageScope::Execution)
            .put_from_url(&url, Some(&filename), None)
            .await?;
        out = out.set("file", file);
    }
    Ok(out)
}
