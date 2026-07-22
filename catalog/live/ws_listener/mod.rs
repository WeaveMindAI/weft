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
//!   1. `ctx.ws_caller()` (caller present, WebSocket, connected).
//!   2. Loop: `recv_next()` each message, log it, count it. Ends when the
//!      caller disconnects or the session cap fires.
//!   3. Emit `count`.

use async_trait::async_trait;

use weft_core::caller::InboundMessage;
use weft_core::context::LogLevel;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LiveWsListenerNode;

#[async_trait]
impl Node for LiveWsListenerNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let ws = ctx.ws_caller().await?;

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

        ctx.pulse_downstream(NodeOutput::new().set("count", count)).await
    }
}
