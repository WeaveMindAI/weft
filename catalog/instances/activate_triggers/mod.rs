//! ActivateTriggers: turn on the program's own triggers or, given
//! `instance`, that instance's, every one of them or the ones `only`
//! names.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::instance_if_given;

#[derive(NodeManifest)]
pub struct ActivateTriggersNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ActivateTriggersNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let instance = instance_if_given(&ctx)?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        // None named is every trigger of the owner: every shared trigger,
        // or every per-instance trigger of the instance.
        let mut calls = ctx.triggers().only(only);
        if let Some(id) = instance {
            calls = calls.instance(id);
        }
        calls.activate().await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
