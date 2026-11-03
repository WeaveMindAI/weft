//! DeactivateTriggers: turn off the program's own triggers or, given
//! `instance`, that instance's, every one of them or the ones `only`
//! names, the way the node's `door`, `held` and `running` say.

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{stop_self, take_down_choice};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instance = ctx.inputs.opt::<InstanceId>("instance")?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        let take_down = take_down_choice(&ctx)?;
        // None named is every trigger of the owner: every shared trigger,
        // or every per-instance trigger of the instance.
        let mut calls = ctx.triggers().only(only);
        if let Some(id) = instance {
            calls = calls.instance(id);
        }
        calls.deactivate(take_down, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
