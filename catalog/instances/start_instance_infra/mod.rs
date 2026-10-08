//! StartInstanceInfra: bring up one instance's copy of a `@per_instance`
//! infra node, and fire `done` once the copy answers, or, with
//! `waitUntilRunning` off, once weft accepted the start.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct StartInstanceInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for StartInstanceInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let wait_until_running: bool = ctx.inputs.get("waitUntilRunning")?;
        let copy = ctx.infra(node).instance(instance(&ctx)?);
        if wait_until_running {
            copy.start().await?;
        } else {
            copy.request_start().await?;
        }
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
