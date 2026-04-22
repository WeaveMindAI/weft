//! Debug: log whatever flows in. Terminal node, no outputs.
//!
//! The node's user-facing label (the title shown at the top of the
//! node in the editor) is used as the log prefix. If the user hasn't
//! set a label, we fall back to the node id so the log still points
//! at something identifiable.

use async_trait::async_trait;

use weft_core::context::LogLevel;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let data = ctx.input.raw("data").cloned().unwrap_or(serde_json::Value::Null);
        let label = ctx.node_label.as_deref().unwrap_or(&ctx.node_id);
        ctx.log(LogLevel::Info, format!("[{}] {}", label, data));
        Ok(NodeOutput::empty())
    }
}
