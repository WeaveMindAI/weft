//! LlmEmbed: texts to embedding vectors, through the wired provider's
//! connection. Speaks the OpenRouter embeddings endpoint, so the
//! provider must be an OpenRouterProvider whose model names an
//! embedding model (voyage-4, text-embedding-3-small, qwen3, ...).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LlmEmbedNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmEmbedNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let (model, account) = super::provider::read_for(&ctx, "openrouter", "embeddings")?;
        let inputs: Vec<String> = ctx.inputs.list("texts")?;
        let dimensions: Option<f64> = ctx.inputs.opt("dimensions")?;

        if inputs.is_empty() {
            weft::node_bail!("texts is empty; nothing to embed");
        }

        let count = inputs.len();
        let mut body = json!({ "model": model, "input": inputs });
        if let Some(d) = dimensions {
            body["dimensions"] = json!(d as u64);
        }

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::post_json(
            &http,
            "https://openrouter.ai/api/v1/embeddings",
            &body,
            "openrouter: embeddings",
        )
        .await?;
        // Each entry names its input's position (`index`); place by it
        // rather than trusting arrival order, and refuse a hole or an
        // out-of-range slot loud (a misplaced vector would silently
        // pair the wrong text with the wrong embedding).
        let mut slots: Vec<Option<Value>> = vec![None; count];
        for d in answer["data"].as_array().into_iter().flatten() {
            let idx = d["index"]
                .as_u64()
                .node_err("openrouter: an embedding entry carries no index")?
                as usize;
            let Some(slot) = slots.get_mut(idx) else {
                weft::node_bail!(
                    "openrouter names embedding index {idx}, past the {count} input texts"
                );
            };
            *slot = Some(d["embedding"].clone());
        }
        let vectors: Vec<Value> = slots
            .into_iter()
            .enumerate()
            .map(|(i, slot)| {
                slot.ok_or_else(|| {
                    weft::node_error(format!(
                        "openrouter answered no embedding for input {i}; the model may be wrong"
                    ))
                })
            })
            .collect::<WeftResult<_>>()?;
        let dims = vectors[0].as_array().map(|v| v.len() as f64).unwrap_or(0.0);
        ctx.pulse_downstream(
            NodeOutput::new().set("embeddings", json!(vectors)).set("dimensions", dims),
        )
        .await
    }
}
