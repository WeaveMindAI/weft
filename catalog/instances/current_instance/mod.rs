//! CurrentInstance: which instance this run is for. Fires `instance`
//! with the id, or `nobody` when the run is for no instance, never both.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct CurrentInstanceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CurrentInstanceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let output = match ctx.instance() {
            Some(instance) => NodeOutput::new().set("instance", instance.as_str()),
            None => NodeOutput::new().set("nobody", true),
        };
        ctx.pulse_downstream(output).await
    }
}
