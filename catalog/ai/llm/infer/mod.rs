//! LlmInference: one language-model call, buffered. The reply comes
//! out whole on `response` when the model finishes; for live deltas
//! use LlmStream instead.
//!
//! WHO serves the call arrives on the `provider` input: one
//! `LlmProvider` object a provider node emitted (the connection was
//! picked over there, this node opens it inside its own firing). HOW
//! the model samples comes from the optional `params` input (an
//! LlmParams node's object); absent = the defaults. The shared
//! plumbing (generator, params, history assembly, the storage
//! round-trip) lives in the package's `call.rs`.
//!
//! CONVERSATIONS ride the `history` input/output (`ChatHistory`, the
//! typed minillmlib-shaped value whose media slots hold stored files).
//! TOOLS: wired LlmTool declarations ride to the provider; the model's
//! calls come out on `toolCalls` (and inside the emitted history), the
//! graph runs each tool, and a Chat Message node (role: tool) appends
//! the results for the next call, which is why `prompt` is optional
//! once a history is wired.
//!
//! The call streams internally so a Stop lands mid-generation (and the
//! metered client resolves the interrupted call's real cost on its
//! own); this node holds no cost bookkeeping at all.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::call;

#[derive(NodeManifest)]
pub struct LlmInferenceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmInferenceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // `parseJson` declares a metadata default, so the bag always
        // holds a value.
        let parse_json: bool = ctx.inputs.get("parseJson")?;

        let llm = call::assemble(&ctx).await?;
        let leaf = call::to_wire(&ctx, &llm).await?;

        // Stream so a Stop lands mid-generation instead of after it; on
        // cancel, dropping the stream is all the wrap-up there is.
        let stream = leaf
            .complete_streaming(&llm.generator, Some(&llm.params))
            .await
            .map_err(|e| llm.call_error(e))?;
        let cancelled = ctx.cancellation();
        let response = tokio::select! {
            collected = stream.collect() => collected.map_err(|e| llm.call_error(e))?,
            err = cancelled.cancelled_err() => return Err(err),
        };
        let text = response.content.clone();

        // parseJson: repair the reply with the lib's JSON repairer (the
        // streaming transport skips the lib's post-processing, so the node
        // applies it here); an unrepairable reply fails loudly. A reply
        // with NO text is not repaired: a tool-call-only turn carries an
        // empty content by design (the substance lives on `toolCalls`),
        // and a genuinely empty reply gets `finish`'s own precise error
        // rather than a misleading repair failure.
        let response_value = if parse_json && !text.trim().is_empty() {
            let repaired = minillmlib::repair_json(&text, &minillmlib::RepairOptions::default())
                .node_err("llm: response is not repairable JSON")?;
            serde_json::from_str(&repaired)
                .node_err("llm: repaired JSON failed to parse")?
        } else {
            Value::String(text)
        };
        let base =
            if parse_json { ctx.fan_declared(&response_value) } else { NodeOutput::new() };
        let output =
            call::finish(&ctx, llm, &response, base).await?.set("response", response_value);
        ctx.pulse_downstream(output).await
    }
}
