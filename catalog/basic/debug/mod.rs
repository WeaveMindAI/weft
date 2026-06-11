//! Debug: log whatever flows in. A terminal sink (no output ports): the
//! graph view reads the input value off the SSE event stream and renders
//! it inline via `features.showDebugPreview`. The node's user-facing
//! label is the log prefix; if unset, we fall back to the node id so the
//! log still points at something identifiable.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::LogLevel;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct DebugNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for DebugNode {
    fn node_type(&self) -> &'static str {
        "Debug"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Debug metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let data = ctx.input.raw("data").cloned().unwrap_or(Value::Null);
        let label = ctx.node_label.as_deref().unwrap_or(&ctx.node_id);
        ctx.log(LogLevel::Info, format!("[{}] {}", label, data)).await?;
        Ok(())
    }
}
