//! BusChatGuest: demo node that joins a bus created by a BusChatHost,
//! replies to each of the host's messages, and exits when the host
//! closes the bus.
//!
//! There is no envelope. The bus's `Closed` entry IS the end-of-
//! conversation signal: the guest's cursor returns `None` when it
//! reaches it. `ctx.join_bus` hands back a guard that closes on EVERY
//! exit path (happy, error, or early-return), so a half-broken guest
//! can never leave the host parked forever on its reply cursor.
//!
//! Flow:
//!   1. join_bus (resolve marker + register "guest", close-on-drop)
//!   2. wait_for("host")
//!   3. loop: pull the next host message; reply with the next scripted
//!      line. Exits when the cursor returns None (host closed).
//!   4. drop the bus, pulse_downstream(done=true)

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusEntryKind;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BusChatGuestNode;

/// The guest's replies, one per host turn.
// SYNC: GUEST_LINES <-> catalog::basic::bus_chat_host::HOST_LINES.
// Catalog nodes are independently compiled (no shared module), so the
// 1:1 pairing is a doc contract, not a compile-time check. If you grow
// or shrink one side, update the other in the same commit.
const GUEST_LINES: &[&str] = &["hey there", "not much, you?", "later"];

#[async_trait]
impl Node for BusChatGuestNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let bus = ctx.join_bus("channel", "guest")?;
        bus.wait_for("host").await.node_err("guest waiting for host")?;

        let mut host_msgs = bus.cursor().with_filter(|entry| {
            matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "host")
        });
        let mut reply_idx = 0;
        while host_msgs.next_json("msg").await.node_err("guest cursor")?.is_some() {
            // No silent "..." fallback: if HOST_LINES has more turns
            // than GUEST_LINES, the SYNC contract was broken and we
            // want the demo to fail loud rather than ship a generic
            // placeholder reply.
            let Some(reply) = GUEST_LINES.get(reply_idx).copied() else {
                weft::node_bail!(
                    "guest out of replies at turn {reply_idx}: HOST_LINES outgrew GUEST_LINES \
                     without updating the SYNC contract in bus_chat_guest/mod.rs"
                );
            };
            bus.send("msg", json!(reply)).node_err(format!("guest send '{reply}'"))?;
            reply_idx += 1;
        }
        drop(bus);

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
