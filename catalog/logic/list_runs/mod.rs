//! ListRuns: the newest of the project's runs matching the filter inputs,
//! with how many match in all.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::runs::runs_from_inputs;

#[derive(NodeManifest)]
pub struct ListRunsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListRunsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let page = runs_from_inputs(&ctx)?.list(ctx.inputs.get::<u32>("limit")?).await?;
        ctx.pulse_downstream(NodeOutput::new().set_serialized("runs", &page.runs)?.set("total", page.total)).await
    }
}
