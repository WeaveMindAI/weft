//! All: the two-part permission a gate takes one wire for.
//!
//! `_should_flow` reads exactly one wire, which is right (one node, one
//! decision) and leaves "delete it only if the model said rude AND said
//! sure" with nowhere to put the second answer. This node is where the
//! second answer goes: wire both onto it and gate on what it emits.
//!
//! It reads a value the way the gate reads one, and that is deliberate:
//! only `false` is a no, anything else is a yes. A node whose gate is
//! fed from here therefore behaves exactly as it would if that one
//! input had been wired straight into it.
//!
//! The no is a CLOSURE rather than `false`, so the answer composes
//! both ways round: `_should_flow` treats a closure as a no, and
//! `_should_not_flow` treats it as the yes, which is how "none of these
//! happened" gets written.
//!
//! Absence counts as a no for free. Every input here is required, so a
//! branch that closed skips this node, and a skipped node closes its
//! output. That makes this the AND of decisions and arrivals in one
//! node: `sure: judge.sure` beside `row: lookup.found` says "the model
//! was sure and the row exists".

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct AllNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AllNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // "All of nothing" is true in logic and a mis-wiring in a graph
        // (a gate fed from here would be permanently open), so the node's
        // rule refuses an All with no input before it runs. Every input is
        // required, so each one arrived.
        let every = ctx.inputs.custom().all(|(_, value)| *value != serde_json::Value::Bool(false));
        let output = if every {
            NodeOutput::new().set("yes", serde_json::Value::Bool(true))
        } else {
            // Emitting nothing closes `yes`, which is the no.
            NodeOutput::new()
        };
        ctx.pulse_downstream(output).await
    }
}
