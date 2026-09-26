//! DeactivateMemberTriggers: turn off a member's own triggers, every
//! one of them or the ones `only` names, the way the node's choices say.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::{member, stop_self, take_down_spec};

#[derive(NodeManifest)]
pub struct DeactivateMemberTriggersNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DeactivateMemberTriggersNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let member = member(&ctx)?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        let spec = take_down_spec(&ctx)?;
        // None named is every per-member trigger of the member.
        let calls = ctx.triggers().only(only).member(member);
        calls.deactivate(spec, stop_self(&ctx)?).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
