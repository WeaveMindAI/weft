//! SlackGetThread: read a thread's replies. Pages through
//! `conversations.replies` until the thread is exhausted and emits the
//! whole conversation in order (parent first, as Slack returns it).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::api;

#[derive(NodeManifest)]
pub struct SlackGetThreadNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for SlackGetThreadNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let channel: String = ctx.inputs.get("channel")?;
        let ts: String = ctx.inputs.get("ts")?;

        // Whole-thread accumulation, bounded by the shared paging cap:
        // past it the node fails loudly instead of consuming the whole
        // rate budget on a degenerate thread.
        let mut messages: Vec<Value> = Vec::new();
        api::paged::<()>(
            &ctx,
            &access,
            "conversations.replies",
            &[("channel", channel.clone()), ("ts", ts.clone())],
            "messages",
            |page| {
                messages.extend(page.iter().cloned());
                Ok(None)
            },
            |cap| {
                format!(
                    "the thread exceeds {cap} messages; this node reads whole threads \
                     and one this size is out of its scope"
                )
            },
        )
        .await?;

        let count = messages.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("messages", json!(messages))
                .set("count", count),
        )
        .await
    }
}
