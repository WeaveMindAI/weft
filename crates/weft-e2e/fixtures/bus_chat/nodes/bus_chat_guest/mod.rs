//! BusChatGuest: demo node that joins a bus created by a BusChatHost,
//! replies to each of the host's messages, and exits when the host
//! closes the bus.
//!
//! There is no envelope. The bus's `Closed` entry IS the end-of-
//! conversation signal: the guest's cursor returns `None` when it
//! reaches it. `ctx.join_bus` makes the guest a member like the host:
//! when its body returns, on any exit path, its name leaves and the host
//! stays on the bus (a pause is no exit, the name stays for the resumed
//! body). A guest that fails halfway leaves the host waiting for a reply
//! until the run's stuck check ends the bus failed.
//!
//! Flow:
//!   1. join_bus (resolve marker + register "guest": a member)
//!   2. wait_for("host")
//!   3. loop: pull the next host message; reply with the next scripted
//!      line. Exits when the cursor returns None (host closed).
//!   4. pulse_downstream(done=true)
//!
//! The run can pause while the guest waits for the host's next message.
//! The state keeps how many replies went out, so the resumed guest (its
//! cursor carries on past what it read, and the host is still in the
//! bus's members) answers the next message with the next line.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusEntryKind;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BusChatGuestNode;

/// How many replies went out: the index of the next one.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Replied {
    replies: usize,
}

/// The guest's replies, one per host turn.
// SYNC: GUEST_LINES <-> bus_chat_host::HOST_LINES (this fixture's nodes/bus_chat_host).
// Nodes are independently compiled (no shared module), so the
// 1:1 pairing is a doc contract, not a compile-time check. If you grow
// or shrink one side, update the other in the same commit.
const GUEST_LINES: &[&str] = &["hey there", "not much, you?", "later"];

#[async_trait]
impl Node for BusChatGuestNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let mut state = ctx.keep(Replied::default()).await?;
        let bus = ctx.join_bus("channel", "guest")?;
        bus.wait_for("host", &mut state).await?;

        let mut host_msgs = bus.cursor()?.with_filter(|entry| {
            matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "host")
        });
        while host_msgs.next_json("msg", &mut state).await?.is_some() {
            let reply_idx = state.replies;
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
            state.replies += 1;
        }

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
