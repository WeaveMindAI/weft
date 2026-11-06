//! CloseBus: end a bus now, no more messages, whoever is still on it.
//! A bus ends on its own once nobody is on it and no node can still join
//! it, but a bus can outlive the nodes sending on it (a session's stream
//! that each turn of a loop sends on, a node reading it until it ends):
//! this is the node that ends it, from wherever the program knows nothing
//! more is coming.
//!
//! While anybody is on the bus it ends finished; when nobody is, it ends
//! the way its last member left, so a stream whose only producer failed
//! still reads as failed (`BusHandle::close`). A failed end is only ever
//! weft's to write. A bus that already ended keeps that end: closing is
//! idempotent, the first end stands.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CloseBusNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CloseBusNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        ctx.bus_from_input("bus")?.close();
        Ok(())
    }
}
