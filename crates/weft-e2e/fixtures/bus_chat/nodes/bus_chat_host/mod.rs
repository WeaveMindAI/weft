//! BusChatHost: demo node that opens a bus, talks back-and-forth with
//! a BusChatGuest peer, then closes the bus. Pairs structurally: the
//! host creates the bus and emits the marker, the guest receives the
//! marker and joins. Both register identities ("host" / "guest") so
//! each `send` is stamped and the inspector renders an IRC-style log.
//!
//! ## No envelope, close is the goodbye signal
//!
//! The bus is a durable log: closing it is itself a journaled `Closed`
//! entry every cursor reads. Host and guest are both members (each
//! registered a name), and the bus lives until the last of them leaves
//! and no node can still join it, so the host returning would leave the
//! guest on an open bus, reading
//! on: the host closes it on purpose once it has said its last line, and
//! the guest's cursor returns `None` when it reaches the close entry. A
//! host that fails halfway just leaves; the guest, alone on the bus,
//! waits until the run's stuck check ends it failed.
//!
//! Flow:
//!   1. host: open_bus (create + emit marker + register: a member)
//!   2. host: wait_for("guest")
//!   3. host: for each line: send "msg", read the guest's next reply
//!   4. host: close the bus (the goodbye), pulse_downstream(done=true)
//!
//! The run can pause while the host waits for a reply, and, with
//! `pauseSeconds` set, while it waits on a timer after the first exchange.
//! The state keeps the turn, whether its line went out, the reply read
//! and whether the timer ended, so the resumed host (its bus, its name and
//! its cursor all come back as they were) carries on where it stopped
//! instead of sending a line a second time. The reply is read through the
//! state: the timer comes after the first reply, and a body may not read
//! directly before a place it can pause that it reaches for the first time.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::{BusEntry, BusEntryKind, BusOptions};
use weft::node::NodeOutput;
use weft::signal::{Timer, TimerSpec};
use weft::{RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct BusChatHostNode;

/// Where the conversation stands: the turn under way, whether its line
/// already went out, the guest's reply to it once read, and the end of
/// the timer after the first exchange.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Conversation {
    turn: usize,
    sent: bool,
    reply: Option<BusEntry>,
    paused: Option<serde_json::Value>,
}

/// Lines the host sends. Each line is sent and waits for the guest's
/// reply; the conversation ends when the host has run out of lines and
/// the bus closes.
// SYNC: HOST_LINES <-> bus_chat_guest::GUEST_LINES (this fixture's nodes/bus_chat_guest).
// Nodes are independently compiled (no shared module), so the
// 1:1 pairing is a doc contract, not a compile-time check. If you grow
// or shrink one side, update the other in the same commit. The guest
// fails loudly at runtime if it runs out of replies.
const HOST_LINES: &[&str] = &["hello", "what's up", "ok bye"];

#[async_trait]
impl Node for BusChatHostNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let pause_seconds: f64 = ctx.inputs.get("pauseSeconds")?;
        let mut state = ctx.keep(Conversation::default()).await?;
        let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
        bus.wait_for("guest", &mut state).await?;

        // One cursor filtered to the guest's "msg" messages; reused across
        // turns so we walk the log forward in one pass.
        let mut replies = bus.cursor()?.with_filter(|entry| {
            matches!(&entry.kind, BusEntryKind::Message { from, msg_kind, .. } if from == "guest" && msg_kind == "msg")
        });
        while let Some(line) = HOST_LINES.get(state.turn) {
            if state.turn == 1 && pause_seconds > 0.0 {
                let timer = Timer { spec: TimerSpec::After { duration_ms: (pause_seconds * 1000.0).ceil() as u64 } };
                state.wait(|s| &mut s.paused, timer).await?;
            }
            if !state.sent {
                bus.send("msg", json!(line)).node_err(format!("host send '{line}'"))?;
                state.sent = true;
            }
            let turn = state.turn + 1;
            state
                .read(|s| &mut s.reply, &mut replies)
                .await?
                // A `None` read means the bus closed before a reply
                // arrived (the guest exited, or the stuck-detector tore
                // the bus down). State the observable fact.
                .node_err(format!("host: bus closed before a reply to turn {turn} arrived"))?;
            state.turn = turn;
            state.sent = false;
            state.reply = None;
        }
        bus.close(); // the goodbye: every cursor reads the Closed entry

        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
        Ok(())
    }
}
