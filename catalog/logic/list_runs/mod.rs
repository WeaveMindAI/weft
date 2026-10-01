//! ListRuns: the newest of the project's runs matching the filter inputs,
//! with how many match in all.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let limit: f64 = ctx.inputs.get("limit")?;
        // The range is the runtime's to refuse (`RunQuery::list`); a
        // count that is not a whole number never reaches it.
        if limit.fract() != 0.0 || limit < 0.0 {
            return Err(WeftError::Input(format!("limit is how many runs to list, a whole number, so it cannot be {limit}")));
        }
        let page = runs_from_inputs(&ctx)?.list(limit as u32).await?;
        let runs = serde_json::to_value(&page.executions).map_err(|e| WeftError::NodeExecution(format!("runs: {e}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("runs", runs).set("total", page.total)).await
    }
}
