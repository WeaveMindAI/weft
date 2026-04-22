//! Gate: route a value based on a pass signal. v1 semantics.
//!
//! `pass` null or false → output value is null (cuts downstream flow
//! via null propagation).
//! `pass` non-null non-false → output = value.
//!
//! Pairs with the Human node's approve_reject field:
//! approve_reject emits true/null → Gate.pass. The Gate forwards
//! `value` only on the active path.

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
        let pass = ctx.input.raw("pass").cloned().unwrap_or(Value::Null);
        let value = ctx.input.raw("value").cloned().unwrap_or(Value::Null);

        let output = match pass {
            Value::Null | Value::Bool(false) => Value::Null,
            _ => value,
        };

        Ok(NodeOutput::empty().set("value", output))
    }
}
