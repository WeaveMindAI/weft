//! ListInstanceInfra: every copy of one infra node, the shared one and
//! each instance's, with its state.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ListInstanceInfraNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListInstanceInfraNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let copies = ctx.infra(node).copies().await?;
        let instances: Vec<String> =
            copies.iter().filter_map(|c| c.instance.as_ref().map(|i| i.as_str().to_string())).collect();
        ctx.pulse_downstream(NodeOutput::new().set_serialized("copies", &copies)?.set("instances", instances)).await
    }
}
