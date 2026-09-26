//! Not: the opposite of one Boolean.
//!
//! Strictly Boolean. The port type refuses anything else before the
//! body runs, so there is no truthiness of strings or numbers to
//! define. A closed input skips the node (the input is required) and
//! a skipped node closes its output: absence stays absence, it is
//! never turned into `true`. For "run when that branch closed", the
//! gate already has `_should_not_flow`.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct NotNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for NotNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: bool = ctx.inputs.get("value")?;
        ctx.pulse_downstream(NodeOutput::new().set("value", serde_json::Value::Bool(!value)))
            .await
    }
}
