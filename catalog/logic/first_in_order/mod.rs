//! FirstInOrder: take whichever branch spoke.
//!
//! Two branches that carry the same kind of answer cannot feed one node
//! (an input takes exactly one wire), so they come here and leave as
//! one. The inputs are the ones written on the node, and the first of
//! them that carried a value is the one emitted: written order is
//! PRIORITY, so a human's answer sitting above an automatic one can
//! never be overtaken by it.
//!
//! Order, never arrival. A race would replay differently from the run
//! it recorded, and the journal is supposed to be the truth about what
//! happened.
//!
//! Nothing arrives on a branch that was cut. The node is a join
//! (`features.join`), so when every branch closed it does not run at
//! all: the skip closes its output, carrying the failure of a branch
//! that broke. When it runs, at least one branch delivered.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct FirstInOrderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FirstInOrderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // `in_order` walks the node's ports in the order they were
        // written and yields only the ones that DELIVERED: a branch that
        // closed is absent, so the first pair is the winner. The node's
        // rule refuses a FirstInOrder joining no branch before it runs.
        let mut output = NodeOutput::new();
        if let Some((_, first)) = ctx.inputs.in_order()?.next() {
            output = output.set("value", first.clone());
        }
        ctx.pulse_downstream(output).await
    }
}
