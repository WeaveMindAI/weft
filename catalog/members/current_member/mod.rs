//! CurrentMember: who this run is for. Fires `member` with the id, or
//! `nobody` when the run is for no member, never both.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CurrentMemberNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CurrentMemberNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let output = match ctx.member() {
            Some(member) => NodeOutput::new().set("member", member.as_str()),
            None => NodeOutput::new().set("nobody", true),
        };
        ctx.pulse_downstream(output).await
    }
}
