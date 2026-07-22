//! BusChatHost: demo node that opens a bus, talks back-and-forth with
//! a BusChatGuest peer, then closes the bus. Pairs structurally: the
//! host creates the bus and emits the marker, the guest receives the
//! marker and joins. Both register identities ("host" / "guest") so
//! each `send` is stamped and the inspector renders an IRC-style log.
//!
//! ## No envelope, close is the goodbye signal
//!
//! The bus is a durable log: closing it is itself a journaled `Closed`
//! entry every cursor reads. The host closes after its last `send`;
//! the guest's cursor returns `None` when it reaches the close entry.
//! No `bye` envelope kind, no race against a guest send. The host
//! also closes on every error path AFTER the marker has been emitted,
//! so a half-broken host doesn't leave the guest's cursor parked
//! forever. The guest's filter ignores the host's `Left` Drop entry,
//! so only an explicit `Closed` wakes it.
//!
//! Flow:
//!   1. host: create_bus, emit marker on `channel`
//!   2. host: from here on, EVERY exit closes the bus
//!   3. host: register("host"), wait_for("guest")
//!   4. host: open a cursor filtered to guest messages
//!   5. host: for each line: send "msg", read the next guest reply
//!   6. host: close (on every exit path, success or error)
//!   7. host: pulse_downstream(done=true) on success

use async_trait::async_trait;
use serde_json::json;

use weft_core::bus::{BusEntryKind, BusOptions};
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct BusChatHostNode;

/// Lines the host sends. Each line is sent and waits for the guest's
/// reply; the conversation ends when the host has run out of lines and
/// calls `close()`.
// SYNC: HOST_LINES <-> catalog::basic::bus_chat_guest::GUEST_LINES.
// Catalog nodes are independently compiled (no shared module), so the
// 1:1 pairing is a doc contract, not a compile-time check. If you grow
// or shrink one side, update the other in the same commit. The guest
// fails loudly at runtime if it runs out of replies.
const HOST_LINES: &[&str] = &["hello", "what's up", "ok bye"];

#[async_trait]
impl Node for BusChatHostNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let (mut bus, marker) = ctx.create_bus(BusOptions::default())?;

        // Marker emission BEFORE the guarded block: if it fails, no
        // peer is parked anywhere yet, nothing to close.
        ctx.pulse_downstream(NodeOutput::new().set("channel", marker))
            .await?;

        // Try-block guards bus.close() on every error path (see module doc).
        let result: WeftResult<()> = async {
            bus.register("host").node_err("host register")?;

            bus.wait_for("guest").await.node_err("waiting for guest")?;

            // One cursor filtered to guest messages; reused across
            // turns so we walk the log forward in one pass instead of
            // opening a new cursor at tail every iteration.
            let mut reply_cursor = bus.cursor().with_filter(|entry| {
                matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "guest")
            });

            for (turn, line) in HOST_LINES.iter().enumerate() {
                bus.send("msg", json!(line)).node_err(format!("host send '{line}'"))?;
                let _reply = reply_cursor
                    .next()
                    .await
                    .node_err(format!("host cursor on turn {}", turn + 1))?
                    .ok_or_else(|| {
                        // A `None` cursor means the bus closed before a
                        // reply arrived. Don't assert WHO closed it: it
                        // could be the guest exiting, but also the
                        // stuck-detector tearing the bus down. State the
                        // observable fact, not a guessed culprit.
                        WeftError::NodeExecution(format!(
                            "host: bus closed before a reply to turn {} arrived",
                            turn + 1
                        ))
                    })?;
            }
            Ok(())
        }
        .await;

        // Close on EVERY exit path past the marker emission. The
        // guest's cursor reaches the appended `Closed` entry and
        // returns None, ending its loop whether or not the host got
        // through every turn.
        bus.close();

        result?;

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
