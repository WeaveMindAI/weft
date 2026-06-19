//! LiveHttpResponder: demo node for an HTTP live connection. Reads the
//! caller's request, streams a couple of progress chunks, then sends a
//! final body and closes. Pairs with an `ApiEndpoint` trigger.
//!
//! It is ctx-driven end to end: `ctx.is_api_call()` gates the behavior,
//! `ctx.caller()` gives the protocol-typed handle, and the handle's
//! `request_parts` / `write` / `respond` are the only I/O. No reinvented
//! plumbing; the connection layer owns framing, backpressure, heartbeat.
//!
//! Flow:
//!   1. Gate on `is_api_call()` (this node only makes sense on an http
//!      live run); fail loud otherwise.
//!   2. `http.ensure_connected()` (no-op if already attached; the
//!      barrier the language gives us, bounded by the connect timeout).
//!   3. Read the request body, stream two progress chunks, respond with a
//!      final JSON body that echoes what the caller sent.
//!   4. Emit `done` on success.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft_core::caller::{CallerHandle, InboundMessage, OutboundChunk};
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftError, WeftResult};

pub struct LiveHttpResponderNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for LiveHttpResponderNode {
    fn node_type(&self) -> &'static str {
        "LiveHttpResponder"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("LiveHttpResponder metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        if !ctx.is_api_call() {
            return Err(WeftError::NodeExecution(
                "LiveHttpResponder ran without an HTTP live caller; wire it under an \
                 ApiEndpoint trigger"
                    .into(),
            ));
        }
        // The handle is protocol-typed; an http run yields the Http variant.
        let Some(CallerHandle::Http(http)) = ctx.caller() else {
            return Err(WeftError::NodeExecution(
                "LiveHttpResponder: no HTTP caller handle on this run".into(),
            ));
        };

        // Barrier: make sure the caller is actually attached before we
        // start talking (no-op if already connected, fails loud on the
        // trigger-declared connect timeout).
        http.ensure_connected().await?;

        // Read what the caller sent (decoded per the signal's data type).
        let req = http.request_parts()?;
        let echoed = match &req.body {
            InboundMessage::Json(v) => v.clone(),
            InboundMessage::Text(s) => Value::String(s.clone()),
            InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
        };

        // Stream two progress chunks, then the final body. Streaming is a
        // free-for-all (other nodes could interleave); `respond` is the
        // once-only terminal.
        http.write(OutboundChunk::Json(json!({ "stage": "received", "method": req.method })))
            .await?;
        http.write(OutboundChunk::Json(json!({ "stage": "working" }))).await?;
        http.respond(OutboundChunk::Json(json!({
            "stage": "done",
            "you_sent": echoed,
        })))
        .await?;

        ctx.pulse_downstream(NodeOutput::empty().set("done", Value::Bool(true)))
            .await
    }
}
