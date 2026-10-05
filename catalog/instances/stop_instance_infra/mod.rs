//! StopInstanceInfra: scale one instance's copy of an infra node down,
//! keeping its disk, taking the instance's triggers reading it down the
//! way the node's choices say.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{instance, stop_self, take_down_spec, taken_down};

#[derive(NodeManifest)]
pub struct StopInstanceInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StopInstanceInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let spec = take_down_spec(&ctx)?;
        let answer = ctx.infra(node).instance(instance(&ctx)?).stop(spec, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(taken_down(answer)).await
    }
}
