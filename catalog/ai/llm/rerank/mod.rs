//! LlmRerank: reorder documents by relevance to a query, through the
//! wired provider's connection. Speaks the OpenRouter rerank
//! endpoint, so the provider must be an OpenRouterProvider whose
//! model names a rerank model (cohere/rerank-4-pro, ...).

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LlmRerankNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmRerankNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let (model, account) = super::provider::read_for(&ctx, "openrouter", "reranking")?;
        let query: String = ctx.inputs.get("query")?;
        let documents: Vec<String> = ctx.inputs.get("documents")?;
        let top_n: Option<f64> = ctx.inputs.opt("topN")?;

        if documents.is_empty() {
            weft::node_bail!("documents is empty; nothing to rerank");
        }
        let mut body = json!({
            "model": model,
            "query": query,
            "documents": documents,
        });
        if let Some(n) = top_n {
            body["top_n"] = json!(n as u64);
        }

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::post_json(
            &http,
            "https://openrouter.ai/api/v1/rerank",
            &body,
            "openrouter: rerank",
        )
        .await?;

        // Results carry the original index + score, best first; emit
        // the documents re-ordered alongside the raw scoring.
        let mut ranked = Vec::new();
        let mut scores = Vec::new();
        for r in answer["results"].as_array().into_iter().flatten() {
            let Some(idx) = r["index"].as_u64() else {
                // Skipping would silently shrink the answer; a result
                // with no index is a malformed reply, same as one past
                // the list.
                weft::node_bail!("the rerank answer has a result with no `index`");
            };
            let Some(doc) = documents.get(idx as usize) else {
                weft::node_bail!("the rerank answer names index {idx}, past the document list");
            };
            ranked.push(json!(doc));
            scores.push(json!({
                "index": idx,
                "score": r["relevance_score"],
            }));
        }
        if ranked.is_empty() {
            weft::node_bail!("openrouter answered without rerank results; the model may be wrong");
        }
        ctx.pulse_downstream(
            NodeOutput::new().set("documents", json!(ranked)).set("scores", json!(scores)),
        )
        .await
    }
}
