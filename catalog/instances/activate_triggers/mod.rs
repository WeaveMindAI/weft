//! ActivateTriggers: turn on the program's own triggers or, given
//! `instance`, that instance's, every one of them or the ones `only`
//! names, releasing what they held while they were down or dropping it
//! (`held`).

use async_trait::async_trait;

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::{ReleaseHeld, RunContext, Node, NodeManifest, WeftResult};


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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let instance = ctx.inputs.opt::<InstanceId>("instance")?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        let held: ReleaseHeld = ctx.inputs.get("held")?;
        // None named is every trigger of the owner: every shared trigger,
        // or every per-instance trigger of the instance.
        let mut calls = ctx.triggers().only(only);
        if let Some(id) = instance {
            calls = calls.instance(id);
        }
        calls.activate(held).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
