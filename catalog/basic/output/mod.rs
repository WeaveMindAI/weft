//! Output: materialize a value as a project-level output. The runtime
//! collects every Output node when computing the manual-run subgraph
//! (see docs/v2-design.md section 3.0), so an Output is semantically
//! "the thing this project produces." Multiple Output nodes in one
//! project express multiple independent endpoints.
//!
//! Behaviorally, Output records the incoming `data` into the node's
//! execution record and logs it. Debug exists separately for inline
//! inspection; Output is for "this is the answer."

use async_trait::async_trait;
use serde_json::Value;

use weft_core::context::LogLevel;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct OutputNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for OutputNode {
    fn node_type(&self) -> &'static str {
        "Output"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Output metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let data = ctx.input.raw("data").cloned().unwrap_or(Value::Null);
        let label = ctx.node_label.as_deref().unwrap_or(&ctx.node_id);
        ctx.log(LogLevel::Info, format!("[output {}] {}", label, data));
        Ok(NodeOutput::empty())
    }
}
