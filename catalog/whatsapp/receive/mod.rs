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
use weft_core::signal::SseSubscribe;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct WhatsAppReceiveNode;

#[async_trait]
impl Node for WhatsAppReceiveNode {
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
    ctx.register_signal(SseSubscribe {
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
    // skipped by the declared-set intersection. Missing fields
    // stay un-mentioned and the engine closes those ports at
    // termination (substituting empty strings / `false` would
    // publish data nulls indistinguishable from real user values).
    //
    // `file` is a port THIS node computes (a stored-file reference, only
    // on the media path below); it is never a payload field. Strip it
    // before fanning so an event carrying a `file` key can't put a value
    // on the `file` port. The set-after-fan on the media path only guards
    // that one path; stripping here guards both.
    let mut data = ctx.wake_payload_object()?.clone();
    data.as_object_mut()
        .expect("wake_payload_object returns an object or errors")
        .remove("file");
    let mut out = ctx.fan_declared(&data);

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
