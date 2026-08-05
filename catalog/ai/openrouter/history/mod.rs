//! ChatHistoryAppend: append one message to a `ChatHistory` value.
//!
//! The `ChatHistory` shape (declared once in the package root's
//! `metadata.json` `types` key) mirrors minillmlib's message serde
//! exactly, with one stored-form difference: media slots hold weft
//! stored-file values, not URLs or base64, so journals stay small and
//! the same image re-presigns fresh on every provider call. A consumer
//! node externalizes the history at its call boundary and deserializes
//! it straight into minillmlib messages. The stored-form builders live
//! in the package's shared `chat.rs`.

use async_trait::async_trait;

use serde_json::Value;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

use super::chat;

#[derive(NodeManifest)]
pub struct ChatHistoryAppendNode;

#[async_trait]
impl Node for ChatHistoryAppendNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let mut history: Vec<Value> = ctx.inputs.opt("history")?.unwrap_or_default();
        let role: String = ctx.inputs.get("role")?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let media = chat::media_items(ctx.inputs.opt("media")?);

        if text.is_none() && media.is_empty() {
            node_bail!("a message needs text or media");
        }
        history.push(chat::stored_message(&role, text.as_deref().unwrap_or(""), &media)?);
        ctx.pulse_downstream(NodeOutput::new().set("history", history)).await
    }
}
