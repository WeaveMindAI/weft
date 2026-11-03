//! LlmStream: one language-model call whose reply STREAMS. The node
//! opens a Bus on its `stream` output with the model's first chunk and
//! sends each text delta the moment the model produces it; the bus close
//! is the end-of-stream signal (a failed end when the call broke off),
//! and the whole reply then pulses on the normal ports
//! (`response`, `historyFile`, `toolCalls`) exactly like LlmInference,
//! the turn added in place to the conversation file on `historyFile`. A
//! consumer that wants the live text joins the bus; one that only
//! wants the final reply ignores it.
//!
//! Everything up to the transport (provider, params, tools, history
//! assembly, the storage round-trip) is the package's shared `call.rs`
//! plumbing; see LlmInference for the buffered twin.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusOptions;
use weft::{RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::call;

#[derive(NodeManifest)]
pub struct LlmStreamNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmStreamNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let llm = call::assemble(&ctx).await?;
        let leaf = call::to_wire(&ctx, &llm).await?;

        // The delta channel: the model name rides the bus metadata so a
        // consumer knows the source before the first delta. It opens on
        // the model's first chunk, so a call refused before it answers
        // is a failure `_error` can catch while nothing has been read;
        // past that, the bus ends the way the body does, failed included.
        let model = llm.generator.model.clone();
        let opts = BusOptions { journal: ctx.journal_policy()?, meta: json!({ "model": model }), ..BusOptions::default() };
        let mut bus = ctx.pending_bus("stream", opts, "llm");

        let mut stream = leaf
            .complete_streaming(&llm.generator, Some(&llm.params))
            .await
            .map_err(|e| llm.call_error(e))?;
        // The call rides the ctx's client, whose body read ends the moment
        // the run is cancelled, so a Stop lands mid-generation.
        while let Some(chunk) = stream.next_chunk().await {
            let chunk = chunk.map_err(|e| llm.call_error(e))?;
            let bus = bus.open().await?;
            if !chunk.delta.is_empty() {
                bus.send("delta", json!(chunk.delta)).node_err("sending a delta on the bus")?;
            }
        }
        let response = stream.collect().await.map_err(|e| llm.call_error(e))?;
        // A whole reply ends the stream for its readers now, not after
        // the conversation file is written; a cut one fails the node
        // here, and the bus ends failed with it.
        llm.ensure_whole(&response)?;
        bus.open().await?.close();
        let output = call::finish(&ctx, llm, &response, call::reply_text).await?;
        ctx.pulse_downstream(output).await
    }
}
