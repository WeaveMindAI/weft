//! Gate: route a value based on a pass signal.
//!
//! `pass` false closes the `value` output (downstream learns "no value
//! at this frame stack"); `pass` true emits `value`. Pairs with the
//! Human node's approve/reject branches.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft::node::NodeOutput;

#[derive(NodeManifest)]
pub struct GateNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // `pass` and `value` are BOTH required inputs: the engine only
        // fires this node once they're present, so a missing one is an
        // engine bug. Read them loudly (required accessors) instead of
        // defaulting to Null, which would mask the bug as a legitimate
        // "cut flow" and silently drop the value.
        let pass = ctx.inputs.get("pass")?;
        if pass {
            let value: serde_json::Value = ctx.inputs.get("value")?;
            ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
        } else {
            // pass == false: cut the flow (downstream learns "no value
            // at this frame stack" via the closure).
            ctx.close_port("value").await
        }
    }
}
