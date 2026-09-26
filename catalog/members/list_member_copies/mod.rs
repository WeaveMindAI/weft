//! ListMemberCopies: every copy of one infra node, the shared one and
//! each member's, with its state.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ListMemberCopiesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ListMemberCopiesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let node: String = ctx.inputs.get("node")?;
        let copies = ctx.infra(node).copies().await?;
        let members: Vec<String> =
            copies.iter().filter_map(|c| c.member.as_ref().map(|m| m.as_str().to_string())).collect();
        let copies = serde_json::to_value(&copies).map_err(|e| weft::WeftError::NodeExecution(format!("copies: {e}")))?;
        ctx.pulse_downstream(NodeOutput::new().set("copies", copies).set("members", members)).await
    }
}
