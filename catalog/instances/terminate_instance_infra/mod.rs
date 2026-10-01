//! TerminateInstanceInfra: delete one instance's copy of an infra node
//! and its disk, taking the instance's triggers reading it down the way
//! the node's choices say.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{instance, stop_self, take_down_spec};

#[derive(NodeManifest)]
pub struct TerminateInstanceInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for TerminateInstanceInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let spec = take_down_spec(&ctx)?;
        ctx.infra(node).instance(instance(&ctx)?).terminate(spec, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
