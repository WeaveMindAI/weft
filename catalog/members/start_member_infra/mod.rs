//! StartMemberInfra: bring up one member's copy of a `@per_member`
//! infra node, and fire `done` once the copy answers.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::member;

#[derive(NodeManifest)]
pub struct StartMemberInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StartMemberInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        ctx.infra(node).member(member(&ctx)?).start().await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
