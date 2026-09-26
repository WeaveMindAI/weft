//! ActivateMemberTriggers: turn on a member's own triggers, every one
//! of them or the ones `only` names.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::lifecycle::member;

#[derive(NodeManifest)]
pub struct ActivateMemberTriggersNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ActivateMemberTriggersNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let member = member(&ctx)?;
        let only: Vec<String> = ctx.inputs.opt("only")?.unwrap_or_default();
        // None named is every per-member trigger of the member.
        let calls = ctx.triggers().only(only).member(member);
        calls.activate().await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
