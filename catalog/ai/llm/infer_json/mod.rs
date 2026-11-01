//! LlmInferenceJson: one language-model call, buffered, whose answer
//! is JSON. The same call as LlmInference (provider, params, tools, a
//! conversation kept in a file, all through the package's `call.rs`);
//! what differs is `response`: the reply is repaired with the lib's
//! JSON repairer (the streaming transport skips the lib's own
//! post-processing, so the node applies it), parsed, and held to the
//! type the program declared on `response` (`JsonDict` unless the
//! program declares any JSON-made type there).
//!
//! The answer is checked BEFORE the turn is written to the
//! conversation file: an answer that is not JSON, or does not fit the
//! declared type, fails the node (catchable on `_error`) with the reason
//! and leaves `historyFile` exactly as it came in, so a retry sends the
//! model the same conversation rather than one ending in the answer it
//! just got wrong.
//!
//! A turn that calls tools is not the answer yet: it is not parsed,
//! and it finishes like LlmInference's (`toolCalls`, the file), with
//! no `response`.

use async_trait::async_trait;
use serde_json::Value;

use weft::{RunContext, Node, NodeManifest, WeftResult, WeftType};

use super::call;

#[derive(NodeManifest)]
pub struct LlmInferenceJsonNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmInferenceJsonNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let declared = ctx.output_type("response")?;
        let (llm, response) = call::complete(&ctx).await?;
        let output = call::finish(&ctx, llm, &response, |reply| {
            if reply.tool_calls.as_ref().is_some_and(|calls| !calls.is_empty()) {
                return Ok(None);
            }
            parse_answer(&reply.content, &declared).map(Some).map_err(weft::node_error)
        })
        .await?;
        ctx.pulse_downstream(output).await
    }
}

/// The model's text as the value `response` carries: repaired, parsed,
/// and held to `declared` (an open record's undeclared keys dropped).
/// The error says what was wrong, in words the model can be shown on a
/// retry. The value is held to the type's structure, so the reason
/// names the field (`value: missing required field 'score'`) once,
/// after the declared name.
pub fn parse_answer(text: &str, declared: &WeftType) -> Result<Value, String> {
    let repaired = minillmlib::repair_json(text, &minillmlib::RepairOptions::default())
        .map_err(|e| format!("the model's answer is not JSON: {e}"))?;
    let value: Value = serde_json::from_str(&repaired)
        .map_err(|e| format!("the model's answer is not JSON: {e}"))?;
    // A stored-file object names a key or an address: taken from a
    // model's text (which a caller's words can steer), it would point the
    // program at a file of this project or make the worker fetch any URL.
    if weft::storage::media::holds_file_marker(&value) {
        return Err("the model's answer carries a stored-file object, which an answer never may".into());
    }
    declared
        .structural()
        .conform_value(value)
        .map_err(|why| format!("the model's answer does not fit {declared}: {why}"))
}
