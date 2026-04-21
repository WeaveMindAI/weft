//! Debug: log whatever flows in. Terminal node, no outputs.

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
        let label: Option<String> = ctx.config.get_optional("label")?;
        let value = ctx.input.raw("value").cloned().unwrap_or(serde_json::Value::Null);

        let msg = match label {
            Some(l) => format!("[{}] {}", l, value),
            None => format!("{}", value),
        };
        ctx.log(LogLevel::Info, msg);

        Ok(NodeOutput::empty())
    }
}
