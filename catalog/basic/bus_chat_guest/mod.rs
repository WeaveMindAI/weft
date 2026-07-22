//! BusChatGuest: demo node that joins a bus created by a BusChatHost,
//! replies to each of the host's messages, and exits when the host
//! closes the bus.
//!
//! There is no envelope. The bus's `Closed` entry IS the end-of-
//! conversation signal: the guest's cursor returns `None` when it
//! reaches it. The guest also calls `close()` itself on EVERY exit
//! path AFTER it acquired the BusHandle (happy, error, or early-
//! return) so the host doesn't park forever on a half-broken guest
//! that died mid-handshake or mid-loop. The host's `recv` filter is
//! `from == "guest"` (it only wants the guest's replies), which skips
//! the guest's `Left` Drop entry, so without the explicit close the
//! host would never wake.
//!
//! `bus_from_input` is the only op outside the guarded block: it
//! either yields a BusHandle (then we're committed to closing) or it
//! fails with no handle to close. In the latter case the engine's
//! stuck-detector closes the bus from the outside when it concludes
//! the host's `wait_for("guest")` can never be satisfied.
//!
//! Flow:
//!   1. resolve the marker on `channel` to a live bus handle
//!   2. from here on, EVERY exit closes the bus
//!   3. register("guest"), wait_for("host")
//!   4. loop: pull the next host message; reply with the next
//!      scripted line. Exits when the cursor returns None (host closed).
//!   5. close the bus on every exit path
//!   6. pulse_downstream(done=true) on success

use async_trait::async_trait;
use serde_json::json;

use weft_core::bus::BusEntryKind;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

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
        let mut bus = ctx.bus_from_input("channel")?;

        // Try-block guards bus.close() on every error path (see module doc).
        let result: WeftResult<()> = async {
            bus.register("guest").node_err("guest register")?;

            bus.wait_for("host").await.node_err("guest waiting for host")?;

            let mut host_cursor = bus.cursor().with_filter(|entry| {
                matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "host")
            });
            let mut reply_idx = 0;
            loop {
                let entry = host_cursor.next().await.node_err("guest cursor")?;
                let Some(_msg) = entry else { break };
                // No silent "..." fallback: if HOST_LINES has more turns
                // than GUEST_LINES, the SYNC contract was broken and we
                // want the demo to fail loud rather than ship a generic
                // placeholder reply.
                let Some(reply) = GUEST_LINES.get(reply_idx).copied() else {
                    return Err(WeftError::NodeExecution(format!(
                        "guest out of replies at turn {reply_idx}: HOST_LINES outgrew GUEST_LINES \
                         without updating the SYNC contract in bus_chat_guest/mod.rs"
                    )));
                };
                bus.send("msg", json!(reply)).node_err(format!("guest send '{reply}'"))?;
                reply_idx += 1;
            }
            Ok(())
        }
        .await;

        bus.close();

        result?;

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
