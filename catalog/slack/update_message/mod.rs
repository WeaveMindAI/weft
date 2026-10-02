//! SlackUpdateMessage: rewrite a message the bot itself posted
//! (Slack only lets an app edit its own messages). The status-board
//! pattern: post once, keep updating the same ts.

use async_trait::async_trait;
use serde_json::Value;

use weft::access::client::required_str;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackUpdateMessageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackUpdateMessageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let ts: String = ctx.inputs.get("ts")?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let blocks: Option<Value> = ctx.inputs.opt("blocks")?;

        let mut payload = serde_json::json!({ "channel": channel, "ts": ts });
        api::set_content(&mut payload, text, blocks);
        let client = ctx.client(&access).await?;

        let answer = api::call(&client, "chat.update", payload).await?;
        let ts = required_str(&answer, "chat.update", "ts")?.to_string();
        ctx.pulse_downstream(NodeOutput::new().set("ts", ts)).await
    }
}
