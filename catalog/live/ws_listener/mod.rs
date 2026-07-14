//! LiveWsListener: demo node proving inbound is BROADCAST. Wire two of
//! these (or one of these alongside a LiveWsEcho) under the same
//! LiveSocket trigger: every node that is reading sees
//! every message the caller sends, rather than one node stealing them.
//!
//! This node only observes: it `recv_next()`s each inbound message and
//! `ctx.log()`s it, never replying. It exists to make the broadcast
//! semantics visible (compare the logs of two listeners: identical), and
//! to show a passive observer composing with an active responder on one
//! connection.
//!
//! Flow:
//!   1. Gate on `is_websocket()`.
//!   2. `ws.ensure_connected()`.
//!   3. Loop: `recv_next()` each message, log it, count it. Ends when the
//!      caller disconnects or the session cap fires.
//!   4. Emit `count`.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::caller::{CallerHandle, InboundMessage};
use weft_core::context::LogLevel;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct LiveWsListenerNode;

#[async_trait]
impl Node for LiveWsListenerNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        if !ctx.is_websocket() {
            return Err(WeftError::NodeExecution(
                "LiveWsListener ran without a WebSocket live caller; wire it under a \
                 LiveSocket trigger"
                    .into(),
            ));
        }
        let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
            return Err(WeftError::NodeExecution(
                "LiveWsListener: no WebSocket caller handle on this run".into(),
            ));
        };

        ws.ensure_connected().await?;

        let mut count: u64 = 0;
        // `recv_next()` yields each message, or `None` when the stream ends
        // (caller disconnected or the session cap fired); a real failure
        // (including a fell-behind, which is resumable, not end-of-stream)
        // propagates via `?`. This observer uses the built-in forward cursor,
        // which never falls behind, so in practice only a disconnect ends it.
        while let Some(msg) = ws.recv_next().await? {
            let rendered = match msg {
                InboundMessage::Json(v) => v.to_string(),
                InboundMessage::Text(s) => s,
                InboundMessage::Bytes(b) => format!("<{} bytes>", b.len()),
            };
            count += 1;
            ctx.log(LogLevel::Info, format!("listener saw inbound #{count}: {rendered}"))
                .await?;
        }

        ctx.pulse_downstream(NodeOutput::empty().set("count", Value::from(count)))
            .await
    }
}
