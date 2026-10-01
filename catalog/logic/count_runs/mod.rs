//! CountRuns: how many of the project's runs match the filter inputs.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::runs::runs_from_inputs;

#[derive(NodeManifest)]
pub struct CountRunsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CountRunsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let count = runs_from_inputs(&ctx)?.count().await?;
        ctx.pulse_downstream(NodeOutput::new().set("count", count)).await
    }
}
