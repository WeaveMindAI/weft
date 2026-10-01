//! ListInstances: every instance weft holds anything for in this project
//! (`ctx.instances().list()`), with what it holds.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ListInstancesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListInstancesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let instances = ctx.instances().list().await?;
        let ids: Vec<String> = instances.iter().map(|i| i.instance.as_str().to_string()).collect();
        let waiting: Vec<String> = instances
            .iter()
            .filter(|i| i.triggers.iter().any(|t| t.waiting.is_some()))
            .map(|i| i.instance.as_str().to_string())
            .collect();
        let instances =
            serde_json::to_value(&instances).map_err(|e| weft::WeftError::NodeExecution(format!("instances: {e}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("instances", instances).set("ids", ids).set("waiting", waiting)).await
    }
}
