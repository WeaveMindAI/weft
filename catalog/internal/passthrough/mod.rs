//! Passthrough: compiler-internal node that hosts a group boundary.
//!
//! The compiler's group-flattening pass emits one Passthrough at each
//! group's input side and one at its output side. The node's input
//! and output port lists are written by the compiler from the group's
//! signature, so `features.can_add_input_ports = true` and the output
//! equivalent are always true.
//!
//! Execute behavior: for every input port whose value was wired, emit
//! the same value on the same-named output port. Lane transforms
//! (Expand on an input-boundary port, Gather on an output-boundary
//! port) are applied by the runtime's pre/post-process steps exactly
//! like on any other node; Passthrough is a first-class participant
//! in the lane model, not a special case.
//!
//! Users cannot declare Passthrough directly: the parser rejects it.

use async_trait::async_trait;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct PassthroughNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for PassthroughNode {
    fn node_type(&self) -> &'static str {
        "Passthrough"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Passthrough metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // Forward every input port verbatim to the same-named output
        // port. The compiler guarantees the two lists are identical
        // for a given boundary (an in-boundary copies the group's
        // inputs, an out-boundary copies the group's outputs).
        let mut out = NodeOutput::empty();
        for (port, value) in ctx.input.iter() {
            out = out.set(port.clone(), value.clone());
        }
        Ok(out)
    }
}
