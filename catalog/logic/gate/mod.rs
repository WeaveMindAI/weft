//! Gate: route a value based on a pass signal.
//!
//! `pass` null or false closes the `value` output (downstream learns
//! "no value at this frame stack"). `pass` non-null non-false emits
//! `value`. Pairs with the Human node's approve/reject branches.

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

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // `pass` and `value` are BOTH required inputs: the engine only
        // fires this node once they're present, so a missing one is an
        // engine bug. Read them loudly (`get` errors on absent/wrong
        // type) instead of defaulting to Null, which would mask the
        // bug as a legitimate "cut flow" and silently drop the value.
        let pass: bool = ctx.input.get("pass")?;
        if pass {
            let value = ctx.input.raw("value").cloned().ok_or_else(|| {
                weft_core::error::WeftError::NodeExecution(
                    "Gate: required input `value` missing while `pass` is true".into(),
                )
            })?;
            ctx.pulse_downstream(NodeOutput::empty().set("value", value)).await
        } else {
            // pass == false: cut the flow (downstream learns "no value
            // at this frame stack" via the closure).
            ctx.close_port("value").await
        }
    }
}
