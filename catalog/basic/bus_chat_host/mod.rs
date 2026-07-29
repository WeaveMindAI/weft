//! BusChatHost: demo node that opens a bus, talks back-and-forth with
//! a BusChatGuest peer, then closes the bus. Pairs structurally: the
//! host creates the bus and emits the marker, the guest receives the
//! marker and joins. Both register identities ("host" / "guest") so
//! each `send` is stamped and the inspector renders an IRC-style log.
//!
//! ## No envelope, close is the goodbye signal
//!
//! The bus is a durable log: closing it is itself a journaled `Closed`
//! entry every cursor reads. `ctx.open_bus` hands back a guard that
//! closes on EVERY exit path (success or error), so a half-broken host
//! can never leave the guest's cursor parked forever; the guest's
//! cursor returns `None` when it reaches the close entry.
//!
//! Flow:
//!   1. host: open_bus (create + emit marker + register, close-on-drop)
//!   2. host: wait_for("guest")
//!   3. host: for each line: send "msg", read the guest's next reply
//!   4. host: drop the bus (the close), pulse_downstream(done=true)

use async_trait::async_trait;
use serde_json::json;

use weft::bus::{BusEntryKind, BusOptions};
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BusChatHostNode;

/// Lines the host sends. Each line is sent and waits for the guest's
/// reply; the conversation ends when the host has run out of lines and
/// the bus closes.
// SYNC: HOST_LINES <-> catalog::basic::bus_chat_guest::GUEST_LINES.
// Catalog nodes are independently compiled (no shared module), so the
// 1:1 pairing is a doc contract, not a compile-time check. If you grow
// or shrink one side, update the other in the same commit. The guest
// fails loudly at runtime if it runs out of replies.
const HOST_LINES: &[&str] = &["hello", "what's up", "ok bye"];

#[async_trait]
impl Node for BusChatHostNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
        bus.wait_for("guest").await.node_err("waiting for guest")?;

        // One cursor filtered to guest messages; reused across turns so
        // we walk the log forward in one pass.
        let mut replies = bus.cursor().with_filter(|entry| {
            matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "guest")
        });
        for (turn, line) in HOST_LINES.iter().enumerate() {
            bus.send("msg", json!(line)).node_err(format!("host send '{line}'"))?;
            let _reply = replies
                .next_json("msg")
                .await
                .node_err(format!("host cursor on turn {}", turn + 1))?
                // A `None` read means the bus closed before a reply
                // arrived (the guest exited, or the stuck-detector tore
                // the bus down). State the observable fact.
                .node_err(format!(
                    "host: bus closed before a reply to turn {} arrived",
                    turn + 1
                ))?;
        }
        drop(bus); // the goodbye: every cursor reads the Closed entry

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
