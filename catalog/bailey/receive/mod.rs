//! BaileyReceive: fires when a WhatsApp message lands at the
//! project's bridge.
//!
//!   - `setup_trigger`: read the upstream bridge's `endpointUrl`,
//!     compute the `/events` SSE URL, register an SSE signal. The
//!     listener subscribes; the dispatcher receives `message.received`
//!     events and fires fresh executions.
//!
//!   - `run`: the SSE event delivers a parsed JSON object as the wake
//!     payload. Map the WhatsApp-specific fields to output ports.

use async_trait::async_trait;
use serde_json::Value;

use weft::signal::SseSubscribe;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BaileyReceiveNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for BaileyReceiveNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    // Registers the SSE signal; setup emits nothing downstream.
    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bridge: String = ctx.inputs.get("endpointUrl")?;
        ctx.register_signal(SseSubscribe {
            url: super::bridge_api::route(&bridge, "/events"),
            event_name: "message.received".into(),
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
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
        let mut data = ctx.wake.object()?.clone();
        data.remove("file");
        let data = Value::Object(data);
        let mut out = ctx.fan_declared(&data);

        // Media messages: stream the bytes from the bridge's media
        // endpoint STRAIGHT into execution-scoped storage and emit the
        // self-describing stored-file reference on `file`. Bytes never ride
        // the pulse path; downstream nodes get/stream/presign via the
        // reference.
        let message_type = data.get("messageType").and_then(|v| v.as_str()).unwrap_or("text");
        if MEDIA_TYPES.contains(&message_type) {
            let bridge: String = ctx.inputs.get("endpointUrl")?;
            let message_id = data
                .get("messageId")
                .and_then(|v| v.as_str())
                .node_err("media message without a messageId; cannot fetch its bytes")?;
            // Stream the bytes from the bridge's media endpoint straight
            // into execution storage via the language capability (it GETs,
            // derives the mime, streams in bounded-memory). The node only
            // builds the URL + a stable filename; no HTTP plumbing here.
            let url = super::bridge_api::route(&bridge, &format!("/media/{message_id}"));
            let filename = format!("whatsapp-{message_id}");
            let file = ctx
                .storage(weft::storage::StorageScope::Execution)
                // Incoming media can't be re-fetched once the bridge's
                // copy ages out: keep it past the run (default 30-day
                // access-bumped TTL).
                .put_from_url(&url, Some(&filename), Some(weft::storage::KeepTtl::Default))
                .await?;
            out = out.set("file", file);
        }
        ctx.pulse_downstream(out).await
    }
}

/// Message types whose bytes the bridge can serve via
/// `/media/<messageId>`.
const MEDIA_TYPES: [&str; 5] = ["image", "video", "audio", "document", "sticker"];
