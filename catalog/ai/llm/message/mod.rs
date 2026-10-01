//! ChatHistoryAppend: append one message to a conversation kept in a
//! file.
//!
//! The `ChatHistory` shape (declared once in the package root's
//! `metadata.json` `types` key) mirrors minillmlib's message serde
//! exactly, with one stored-form difference: media slots hold weft
//! stored-file values, not URLs or base64, so the file stays small and
//! the same image re-presigns fresh on every provider call. A consumer
//! node externalizes the conversation at its call boundary and
//! deserializes it straight into minillmlib messages. The stored-form
//! builders live in the package's shared `chat.rs`.
//!
//! Role `tool` is how a tool loop feeds a result back: it answers one
//! call from an LLM node's `toolCalls` output, so it requires that
//! call's id on `toolCallId`.
//!
//! The message is added to the `historyFile` in place, so a long
//! conversation never rides a wire; with no file in, it starts a new
//! conversation file.

use async_trait::async_trait;

use serde_json::Value;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

use super::chat;

#[derive(NodeManifest)]
pub struct ChatHistoryAppendNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ChatHistoryAppendNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let history_ty = chat::history_type()?;
        let history_file: Option<FileHandle> = ctx.inputs.opt("historyFile")?;
        let role: String = ctx.inputs.get("role")?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let media: Vec<Value> = ctx.inputs.list("media")?;
        let tool_call_id: Option<String> = ctx.inputs.opt("toolCallId")?;
        let cache_breakpoint: bool = ctx.inputs.get_or("cacheBreakpoint", false)?;

        if text.is_none() && media.is_empty() {
            node_bail!("a message needs text or media");
        }
        match (role.as_str(), &tool_call_id) {
            ("tool", None) => {
                node_bail!("a tool message answers one tool call; wire the call's id to toolCallId")
            }
            ("tool", Some(_)) | (_, None) => {}
            (other, Some(_)) => {
                node_bail!("toolCallId only belongs on a 'tool' message, not '{other}'")
            }
        }
        let mut message = chat::stored_message(
            &role,
            text.as_deref().unwrap_or(""),
            &media,
            tool_call_id.as_deref(),
        )?;
        if cache_breakpoint {
            chat::mark_cache(&mut message);
        }
        let file = chat::append_turn(&ctx, history_file.as_ref(), &history_ty, &[message], |_| Ok(())).await?;
        ctx.pulse_downstream(NodeOutput::new().set("historyFile", file)).await
    }
}
