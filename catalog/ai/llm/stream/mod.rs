//! LlmStream: one language-model call whose reply STREAMS. The node
//! opens a Bus on its `stream` output and sends each text delta the
//! moment the model produces it; the bus close is the end-of-stream
//! signal, and the whole reply then pulses on the normal ports
//! (`response`, `history`, `toolCalls`) exactly like LlmInference. A
//! consumer that wants the live text joins the bus; one that only
//! wants the final reply ignores it.
//!
//! Everything up to the transport (provider, params, tools, history
//! assembly, the storage round-trip) is the package's shared `call.rs`
//! plumbing; see LlmInference for the buffered twin.

use async_trait::async_trait;
use serde_json::json;

use weft::bus::BusOptions;
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let llm = call::assemble(&ctx).await?;
        let leaf = call::to_wire(&ctx, &llm).await?;

        // The delta channel: the model name rides the bus metadata so a
        // consumer knows the source before the first delta; the guard
        // closes on EVERY exit, so a failed call still ends the stream
        // for readers.
        let model = llm.generator.model.clone();
        let opts = BusOptions { meta: json!({ "model": model }), ..BusOptions::default() };
        let bus = ctx.open_bus("stream", opts, "llm").await?;

        let mut stream = leaf
            .complete_streaming(&llm.generator, Some(&llm.params))
            .await
            .node_err("llm")?;
        let cancelled = ctx.cancellation();
        loop {
            let chunk = tokio::select! {
                chunk = stream.next_chunk() => chunk,
                err = cancelled.cancelled_err() => return Err(err),
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.node_err("llm stream")?;
            if !chunk.delta.is_empty() {
                bus.send("delta", json!(chunk.delta)).node_err("sending a delta on the bus")?;
            }
        }
        drop(bus); // the close IS the end-of-stream signal

        let response = stream.collect().await.node_err("llm")?;
        let output = call::finish(&ctx, llm, &response, NodeOutput::new())
            .await?
            .set("response", response.content.clone());
        ctx.pulse_downstream(output).await
    }
}
