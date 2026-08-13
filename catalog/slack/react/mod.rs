//! SlackReact: add or remove an emoji reaction on a message. The
//! "mark as processed" acknowledgement pattern: a workflow reacts to
//! the message it just handled.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackReactNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackReactNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let ts: String = ctx.inputs.get("ts")?;
        let emoji: String = ctx.inputs.get("emoji")?;
        let remove: bool = ctx.inputs.get("remove")?;

        // Slack wants the bare emoji name; strip the :colons: people
        // naturally paste so both spellings work.
        let name = emoji.trim_matches(':').to_string();
        let method = if remove { "reactions.remove" } else { "reactions.add" };
        api::call(
            &ctx,
            &access,
            method,
            json!({ "channel": channel, "timestamp": ts, "name": name }),
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
