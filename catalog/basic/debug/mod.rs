//! Debug: log whatever flows in. A terminal sink (no output ports): the
//! graph view reads the input value off the SSE event stream and renders
//! it inline via `features.showDebugPreview`. The node's user-facing
//! label is the log prefix; if unset, we fall back to the node id so the
//! log still points at something identifiable.
//!
//! A BUS input is followed live: each message is logged as it lands (so
//! the inspector shows the stream as a stream), the payloads are
//! accumulated, and the accumulated log is printed once the bus closes.
//! A bus that ended failed fails the node with why (the read past it is
//! the failure), every message having been logged as it landed. Anything else is
//! logged once, as before. The accumulated payloads are the node's state,
//! so a run that pauses while it follows the bus closes with every
//! message in the closing line, the ones from before the pause included.

use async_trait::async_trait;
use serde_json::Value;

use weft::bus::BusEntryKind;
use weft::context::LogLevel;
use weft::{RunContext, Node, NodeManifest, WeftResult, WeftType};

#[derive(NodeManifest)]
pub struct DebugNode;

/// What a bus followed so far: every payload logged, for the closing
/// line.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Followed {
    seen: Vec<Value>,
}

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DebugNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let data = ctx.inputs.raw("data").cloned().unwrap_or(Value::Null);
        let label = ctx.node_label.as_deref().unwrap_or(&ctx.node_id).to_string();

        // A bus marker is a CHANNEL, not a value: follow it live instead
        // of printing the marker.
        if WeftType::bus_marker_id(&data).is_some() {
            let bus = ctx.bus_from_input("data")?;
            // A resumed body's cursor starts past what it already read,
            // and what it read is back in `seen`.
            let mut state = ctx.keep(Followed::default()).await?;
            let mut cursor = bus.cursor_from_start()?;
            // A debug sink that lagged an ephemeral stream silently
            // resumes at the oldest retained entry (the cursor bridges
            // evicted gaps on its own).
            while let Some(entry) = cursor.next(&mut state).await? {
                if let BusEntryKind::Message {
                    from, msg_kind, payload_byte_size, ..
                } = &entry.kind
                {
                    // A JSON payload shows its value; a byte
                    // frame shows its size (the bytes are noise).
                    let shown = match entry.as_json() {
                        Some((_, v)) => v.clone(),
                        None => serde_json::json!({ "bytes": payload_byte_size }),
                    };
                    ctx.log(
                        LogLevel::Info,
                        format!("[{label}] {from}/{msg_kind}: {shown}"),
                    )
                    .await?;
                    state.seen.push(shown);
                }
            }
            let count = state.seen.len();
            let seen = Value::Array(std::mem::take(&mut state.seen));
            ctx.log(LogLevel::Info, format!("[{label}] bus closed after {count} message(s): {seen}")).await?;
            return Ok(());
        }

        ctx.log(LogLevel::Info, format!("[{label}] {data}")).await?;
        Ok(())
    }
}
