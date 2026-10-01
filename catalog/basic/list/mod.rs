//! List: the values you wired, as one list, in the order the ports are
//! written.
//!
//! The order is the port list's, read through `ctx.inputs.in_order()`
//! (the bag's own values are sorted by name). An optional port (`name?:`)
//! that received nothing is absent from the bag, so it is left out and
//! the items after it move up: a list with a `null` in it would claim a
//! value arrived that never did. A required port that received nothing
//! skips the node before this body runs, so nothing short escapes.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ListNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // A List with no ports at all is a node somebody added and never
        // declared items on; an empty list from it would say nothing
        // about why. Every optional item staying silent is different: the
        // ports exist, and `[]` is the true answer.
        if ctx.declared_inputs().is_empty() {
            return Err(weft::node_error(
                "this List declares no input ports, so there are no items to build. \
                 Declare one port per item, `List(first: String, second: String)`, and wire each",
            ));
        }
        let items: Vec<Value> = ctx.inputs.in_order()?.map(|(_, value)| value.clone()).collect();
        ctx.pulse_downstream(NodeOutput::new().set("list", Value::Array(items))).await
    }
}
