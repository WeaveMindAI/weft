//! Text: emit a literal string configured at design time.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

pub struct TextNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for TextNode {
    fn node_type(&self) -> &'static str {
        "Text"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Text metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let value: String = ctx.config.get("value")?;
        Ok(NodeOutput::with("value", Value::String(value)))
    }
}
