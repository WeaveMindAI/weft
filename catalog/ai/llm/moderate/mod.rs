//! LlmModerate: content-safety classification through OpenAI's
//! moderation endpoint (free, no usage caps). The provider must be an
//! OpenAIProvider; its model is ignored here (moderation has its own
//! model family) unless it names a moderation model explicitly.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let (model, account) = super::provider::read_for(&ctx, "openai", "moderation")?;
        let text: String = ctx.inputs.get("text")?;

        // A chat model on the provider is the normal case (the same
        // provider node often feeds an inference step); moderation
        // then uses its own current default.
        let model = if model.contains("moderation") {
            model
        } else {
            "omni-moderation-latest".to_string()
        };

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
