//! TerminateInfra: delete a copy of an infra node and its disk, the
//! program's own or, given `instance`, that instance's copy, and take the
//! triggers reading it down the way the node's choices say.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{instance_if_given, stop_self, take_down_spec, taken_down};

#[derive(NodeManifest)]
pub struct TerminateInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TerminateInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let spec = take_down_spec(&ctx)?;
        let mut copy = ctx.infra(node);
        if let Some(id) = instance_if_given(&ctx)? {
            copy = copy.instance(id);
        }
        let answer = copy.terminate(spec, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(taken_down(answer)).await
    }
}
