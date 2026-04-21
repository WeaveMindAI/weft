//! Gate: route the `value` input to either `then` or `else` based
//! on the `condition` boolean. The non-chosen branch emits null so
//! downstream should-skip logic can elide whole subgraphs.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::node::NodeOutput;

pub struct GateNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for GateNode {
    fn node_type(&self) -> &'static str {
        "Gate"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Gate metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let value = ctx.input.raw("value").cloned().unwrap_or(Value::Null);
        let cond: bool = ctx.input.get("condition")?;

        let (then_out, else_out) = if cond {
            (value, Value::Null)
        } else {
            (Value::Null, value)
        };

        Ok(NodeOutput::empty().set("then", then_out).set("else", else_out))
    }
}
