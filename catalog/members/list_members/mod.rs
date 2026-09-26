//! ListMembers: every member weft holds anything for in this project
//! (`ctx.members().list()`), with what it holds.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ListMembersNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListMembersNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let members = ctx.members().list().await?;
        let ids: Vec<String> = members.iter().map(|m| m.member.as_str().to_string()).collect();
        let waiting: Vec<String> = members
            .iter()
            .filter(|m| m.triggers.iter().any(|t| t.waiting.is_some()))
            .map(|m| m.member.as_str().to_string())
            .collect();
        let members = serde_json::to_value(&members).map_err(|e| weft::WeftError::NodeExecution(format!("members: {e}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("members", members).set("ids", ids).set("waiting", waiting)).await
    }
}
