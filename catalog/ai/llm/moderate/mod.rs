//! LlmModerate: content-safety classification through OpenAI's
//! moderation endpoint (free, no usage caps). It takes the OpenAI
//! connection itself on `access` (wired from an OpenAIAccess node, the
//! same one feeding the chat nodes' OpenAI Model nodes): moderation has
//! its own model family, picked on this node's `model`.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LlmModerateNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmModerateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // An OpenAI connection, by the input's type (`Access[openai]`).
        let account: Access = ctx.inputs.get("access")?;
        let model: String = ctx.inputs.get("model")?;
        let text: String = ctx.inputs.get("text")?;

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::post_json(
            &http,
            "https://api.openai.com/v1/moderations",
            &json!({ "model": model, "input": text }),
            "openai: moderation",
        )
        .await?;
        let result = answer
            .pointer("/results/0")
            .cloned()
            .node_err("openai: moderation answered without results")?;
        // Strict: a verdict this node cannot read must NEVER pass as
        // safe, so a missing or mistyped field fails loud instead.
        let flagged = result["flagged"]
            .as_bool()
            .node_err("openai: the moderation result carries no boolean 'flagged'")?;
        let scores = result["category_scores"]
            .as_object()
            .node_err("openai: the moderation result carries no 'category_scores' object")?;
        // The categories that actually flagged, as a plain list.
        let flagged_categories: Vec<Value> = result["categories"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, v)| v.as_bool() == Some(true))
            .map(|(k, _)| json!(k))
            .collect();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("flagged", flagged)
                .set("categories", json!(flagged_categories))
                .set("scores", json!(scores)),
        )
        .await
    }
}
