//! Gate Node - Route a value based on a pass signal.
//!
//! Takes two inputs:
//!   - `pass`: Boolean or null. If null or false, output is null (cuts downstream flow).
//!             If true (or any non-null/non-false), the value is forwarded.
//!   - `value`: Any. The value to forward when pass is non-null.
//!
//! This is the companion to the Human node's approve_reject field.
//! Pattern: approve_reject outputs true/null -> Gate pass input.
//! The Gate forwards `value` only on the active path.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct GateNode;

#[async_trait]
impl Node for GateNode {
    fn node_type(&self) -> &'static str {
        "Gate"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Gate",
            inputs: vec![
                PortDef::new("pass", "Boolean", true),
                PortDef::new("value", "T", true),
            ],
            outputs: vec![
                PortDef::new("value", "T", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let pass = ctx.input.get("pass");
        let value = ctx.input.get("value").cloned().unwrap_or(serde_json::Value::Null);

        // null pass = cut flow (output null)
        // any non-null pass = forward value
        let output = match pass {
            None | Some(serde_json::Value::Null) | Some(serde_json::Value::Bool(false)) => serde_json::Value::Null,
            _ => value,
        };

        NodeResult::completed(serde_json::json!({ "value": output }))
    }
}

register_node!(GateNode);
