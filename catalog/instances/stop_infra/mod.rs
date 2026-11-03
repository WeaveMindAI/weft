//! StopInfra: scale a copy of an infra node down, the program's own or,
//! given `instance`, that instance's copy, keeping its disk, and take the
//! triggers whose cut contains it down the way the node's `door`, `held`
//! and `running` say.

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{stop_self, take_down_choice, taken_down};

#[derive(NodeManifest)]
pub struct StopInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StopInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let take_down = take_down_choice(&ctx)?;
        let mut copy = ctx.infra(node);
        if let Some(id) = ctx.inputs.opt::<InstanceId>("instance")? {
            copy = copy.instance(id);
        }
        let answer = copy.stop(take_down, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(taken_down(answer)).await
    }
}
