//! SlackDeleteMessage: remove a message this app posted earlier
//! (Slack only lets an app delete its own messages).

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackDeleteMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackDeleteMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let ts: String = ctx.inputs.get("ts")?;

        api::call(&ctx, &access, "chat.delete", json!({ "channel": channel, "ts": ts })).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
