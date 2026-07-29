//! SlackSendMessage: one authenticated POST on the connected
//! workspace's bot. The compile-time scope check already proved the
//! wired access ticks `chat:write`; the store re-checks at resolution
//! (the drift backstop); Slack's own error is the last resort.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SlackSendMessageNode;

#[async_trait]
impl Node for SlackSendMessageNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let text: String = ctx.inputs.get("text")?;

        let slack = ctx.client(&access).await?;
        let resp = slack
            .post("https://slack.com/api/chat.postMessage")
            .json(&serde_json::json!({ "channel": channel, "text": text }))
            .send()
            .await
            .node_err("slack: post message")?;
        let answer: Value = resp.json().await.node_err("slack: read post response")?;
        if answer.get("ok").and_then(Value::as_bool) != Some(true) {
            weft::node_bail!(
                "slack refused the post: {}",
                answer.get("error").and_then(Value::as_str).unwrap_or("no detail")
            );
        }
        let ts = answer
            .get("ts")
            .and_then(Value::as_str)
            .node_err("slack: posted message carries no ts")?
            .to_string();
        ctx.pulse_downstream(NodeOutput::new().set("ts", ts)).await
    }
}
