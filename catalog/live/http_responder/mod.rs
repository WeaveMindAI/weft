//! LiveHttpResponder: demo node for an HTTP live connection. Reads the
//! caller's request, streams a couple of progress chunks, then sends a
//! final body and closes. Pairs with an `ApiEndpoint` trigger.
//!
//! It is ctx-driven end to end: `ctx.http_caller()` gives the
//! connected HTTP handle (failing loud on a non-HTTP run), and the
//! handle's `request_parts` / `write` / `respond` are the only I/O. No
//! reinvented plumbing; the connection layer owns framing,
//! backpressure, heartbeat.
//!
//! Flow:
//!   1. `ctx.http_caller()` (caller present, HTTP, connected; the
//!      barrier is bounded by the trigger-declared connect timeout).
//!   2. Read the request body, stream two progress chunks, respond with a
//!      final JSON body that echoes what the caller sent.
//!   3. Emit `done` on success.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft_core::caller::{InboundMessage, OutboundChunk};
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LiveHttpResponderNode;

#[async_trait]
impl Node for LiveHttpResponderNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let http = ctx.http_caller().await?;

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

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
