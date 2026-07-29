//! Debug: log whatever flows in. A terminal sink (no output ports): the
//! graph view reads the input value off the SSE event stream and renders
//! it inline via `features.showDebugPreview`. The node's user-facing
//! label is the log prefix; if unset, we fall back to the node id so the
//! log still points at something identifiable.
//!
//! A BUS input is followed live: each message is logged as it lands (so
//! the inspector shows the stream as a stream), the payloads are
//! accumulated, and the accumulated log is printed once the bus closes.
//! Anything else is logged once, as before.

use async_trait::async_trait;
use serde_json::Value;

use weft::bus::BusEntryKind;
use weft::context::LogLevel;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct DebugNode;

#[async_trait]
impl Node for DebugNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let data = ctx.inputs.raw("data").cloned().unwrap_or(Value::Null);
        let label = ctx.node_label.as_deref().unwrap_or(&ctx.node_id).to_string();

        // A bus marker is a CHANNEL, not a value: follow it live instead
        // of printing the marker.
        if data.get("__weft_bus__").is_some() {
            let bus = ctx.bus_from_input("data")?;
            let mut cursor = bus.cursor_from_start();
            let mut seen: Vec<Value> = Vec::new();
            // A debug sink that lagged an ephemeral stream silently
            // resumes at the oldest retained entry (the cursor bridges
            // evicted gaps on its own).
            while let Some(entry) = cursor.next().await {
                if let BusEntryKind::Message {
                    from, msg_kind, payload, payload_byte_size, ..
                } = &entry.kind
                {
                    // A JSON payload shows its value; a byte
                    // frame shows its size (the bytes are noise).
                    let shown = match payload {
                        Some(weft::bus::WirePayload::Json(v)) => v.clone(),
                        Some(weft::bus::WirePayload::Bytes(_)) | None => {
                            serde_json::json!({ "bytes": payload_byte_size })
                        }
                    };
                    ctx.log(
                        LogLevel::Info,
                        format!("[{label}] {from}/{msg_kind}: {shown}"),
                    )
                    .await?;
                    seen.push(shown);
                }
            }
            ctx.log(
                LogLevel::Info,
                format!(
                    "[{label}] bus closed after {} message(s): {}",
                    seen.len(),
                    Value::Array(seen)
                ),
            )
            .await?;
            return Ok(());
        }

        ctx.log(LogLevel::Info, format!("[{label}] {data}")).await?;
        Ok(())
    }
}
