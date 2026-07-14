//! LiveWsEcho: demo node for a WebSocket live connection. Holds a
//! two-way conversation with the caller: receives each inbound message
//! and sends back an echo with a turn counter, until the caller
//! disconnects (or the trigger's session cap fires). Pairs with a
//! `LiveSocket` trigger.
//!
//! ctx-driven: `ctx.is_websocket()` gates it, `ctx.caller()` gives the
//! WebSocket handle, and `recv_next` / `send` / `close` are the only I/O.
//! Inbound is broadcast (every listening node sees every message), so a
//! second node could observe the same stream; this one is the responder.
//!
//! Flow:
//!   1. Gate on `is_websocket()`; fail loud otherwise.
//!   2. `ws.ensure_connected()`.
//!   3. Loop: `recv_next()` the next message (waits as long as needed;
//!      a node may legitimately idle for hours); `send()` an echo. The
//!      loop ends when `recv_next()` yields `None` (caller disconnected
//!      or the session cap fired).
//!   4. `close()` and emit `done`.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft_core::caller::{CallerHandle, InboundMessage, OutboundChunk};
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct LiveWsEchoNode;

#[async_trait]
impl Node for LiveWsEchoNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        if !ctx.is_websocket() {
            return Err(WeftError::NodeExecution(
                "LiveWsEcho ran without a WebSocket live caller; wire it under a \
                 LiveSocket trigger"
                    .into(),
            ));
        }
        let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
            return Err(WeftError::NodeExecution(
                "LiveWsEcho: no WebSocket caller handle on this run".into(),
            ));
        };

        ws.ensure_connected().await?;

        let mut turn: u64 = 0;
        // `recv_next()` yields the next message, or `None` when the stream
        // ends (caller gone, session capped, etc); a real failure propagates
        // via `?`. The language classifies the terminal outcomes, so the node
        // is just domain logic.
        while let Some(msg) = ws.recv_next().await? {
            let text = match msg {
                InboundMessage::Json(v) => v,
                InboundMessage::Text(s) => Value::String(s),
                InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
            };
            turn += 1;
            ws.send(OutboundChunk::Json(json!({ "echo": text, "turn": turn })))
                .await?;
        }

        // Best-effort close (it may already be closed if the caller left).
        let _ = ws.close().await;
        ctx.pulse_downstream(NodeOutput::empty().set("done", Value::Bool(true)))
            .await
    }
}
