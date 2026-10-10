//! DeactivateTriggers: turn off the program's own triggers or, given
//! `instance`, that instance's, every one of them or the ones `only`
//! names, the way the node's choices say.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{instance_if_given, stop_self, take_down_spec};

#[derive(NodeManifest)]
pub struct DeactivateTriggersNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DeactivateTriggersNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let instance = instance_if_given(&ctx)?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        let spec = take_down_spec(&ctx)?;
        // None named is every trigger of the owner: every shared trigger,
        // or every per-instance trigger of the instance.
        let mut calls = ctx.triggers().only(only);
        if let Some(id) = instance {
            calls = calls.instance(id);
        }
        calls.deactivate(spec, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
