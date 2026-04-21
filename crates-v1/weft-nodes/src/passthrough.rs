//! Passthrough Node : compiler-internal node for group boundary forwarding.
//!
//! NOT a catalog node. Users never see or create this directly.
//! The Weft compiler injects passthrough nodes when flattening groups:
//!   - Input passthrough: one per group, inherits the group's input ports
//!   - Output passthrough: one per group, inherits the group's output ports
//!
//! Ports are dynamic : the compiler sets them based on the group's interface.
//! At runtime, this node simply copies every input field to the output unchanged.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, ExecutionContext};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct PassthroughNode;

#[async_trait]
impl Node for PassthroughNode {
    fn node_type(&self) -> &'static str {
        "Passthrough"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Passthrough",
            inputs: vec![],
            outputs: vec![],
            features: NodeFeatures {
                canAddInputPorts: true,
                canAddOutputPorts: true,
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        // Forward all input fields directly as output.
        // The compiler guarantees input and output port names match.
        let output = if let Some(obj) = ctx.input.as_object() {
            serde_json::Value::Object(obj.clone())
        } else {
            ctx.input.clone()
        };
        NodeResult::completed(output)
    }
}

register_node!(PassthroughNode);
