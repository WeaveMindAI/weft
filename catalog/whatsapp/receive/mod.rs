//! WhatsAppReceive: fires when a WhatsApp message lands at the
//! project's bridge. Two phases:
//!
//!   - `Phase::TriggerSetup`: read the upstream bridge's
//!     `endpointUrl`, compute the `/events` SSE URL, register an
//!     SSE wake signal via `ctx.register_signal`. The listener
//!     subscribes; the dispatcher receives `message.received`
//!     events and fires fresh executions.
//!
//!   - `Phase::Fire`: the event data seeded `__seed__`. Map the
//!     WhatsApp-specific fields to output ports.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::Phase;
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::primitive::{WakeSignalKind, WakeSignalSpec};
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct WhatsAppReceiveNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for WhatsAppReceiveNode {
    fn node_type(&self) -> &'static str {
        "WhatsAppReceive"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("WhatsAppReceive metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        match ctx.phase {
            Phase::TriggerSetup => register(&ctx).await,
            Phase::Fire => fire(&ctx),
            Phase::InfraSetup => Ok(NodeOutput::empty()),
        }
    }
}

async fn register(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    let bridge = ctx
        .input
        .values
        .get("endpointUrl")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            WeftError::Config(
                "WhatsAppReceive requires an `endpointUrl` input (from WhatsAppBridge)".into(),
            )
        })?
        .to_string();
    let base = bridge.trim_end_matches('/');
    let events_url = if base.ends_with("/action") {
        format!("{}/events", &base[..base.len() - "/action".len()])
    } else {
        format!("{}/events", base)
    };
    ctx.register_signal(WakeSignalSpec {
        kind: WakeSignalKind::Sse {
            url: events_url,
            event_name: "message.received".into(),
        },
        is_resume: false,
    })
    .await?;
    Ok(NodeOutput::empty())
}

fn fire(ctx: &ExecutionContext) -> WeftResult<NodeOutput> {
    let payload = ctx
        .input
        .values
        .get("__seed__")
        .cloned()
        .unwrap_or(Value::Null);
    // Support both the SSE payload shape (listener passes `evt.data`
    // as the payload) and the legacy webhook nesting `{body: {data: ...}}`.
    let data = if let Some(nested) = payload
        .get("body")
        .and_then(|b| b.get("data"))
    {
        nested.clone()
    } else {
        payload
    };

    let mut output = NodeOutput::empty();
    output = output.set(
        "content",
        data.get("content").cloned().unwrap_or(Value::Null),
    );
    output = output.set(
        "from",
        data.get("from").cloned().unwrap_or(Value::String(String::new())),
    );
    output = output.set(
        "pushName",
        data.get("pushName")
            .cloned()
            .unwrap_or(Value::String(String::new())),
    );
    output = output.set(
        "messageId",
        data.get("messageId")
            .cloned()
            .unwrap_or(Value::String(String::new())),
    );
    output = output.set(
        "timestamp",
        data.get("timestamp").cloned().unwrap_or(Value::Null),
    );
    output = output.set(
        "isGroup",
        data.get("isGroup").cloned().unwrap_or(Value::Bool(false)),
    );
    output = output.set(
        "chatId",
        data.get("chatId")
            .cloned()
            .unwrap_or(Value::String(String::new())),
    );
    output = output.set(
        "messageType",
        data.get("messageType")
            .cloned()
            .unwrap_or(Value::String("text".into())),
    );
    Ok(output)
}
