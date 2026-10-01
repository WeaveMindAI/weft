//! InstanceInfraStatus: the state of one instance's copy of an infra
//! node, `none` when there is no copy.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct InstanceInfraStatusNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for InstanceInfraStatusNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let copy = ctx.infra(node).instance(instance(&ctx)?).status().await?;
        let running = copy.as_ref().is_some_and(|c| c.is_running());
        // SYNC: never-started copy word <-> crates/weft-dispatcher/src/api/display.rs list_displays
        let status = copy.map(|c| c.status.as_str()).unwrap_or("none");
        ctx.pulse_downstream(NodeOutput::new().set("status", status).set("running", running)).await
    }
}
