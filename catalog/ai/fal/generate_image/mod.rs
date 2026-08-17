//! FalGenerateImage: text to image through fal's queue. The declared
//! knobs target the blessed default model; `params` carries any
//! model-specific extras verbatim, so every fal image model works
//! through the same node.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::{KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{merge_params, run_queued};

#[derive(NodeManifest)]
pub struct FalGenerateImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalGenerateImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let size: String = ctx.inputs.get("imageSize")?;
        let count: f64 = ctx.inputs.get("count")?;
        let seed: Option<f64> = ctx.inputs.opt("seed")?;
        let params = ctx.inputs.raw("params").cloned();

        let mut payload = json!({
            "prompt": prompt,
            "image_size": size,
            "num_images": (count as u64).clamp(1, 8),
        });
        if let Some(s) = seed {
            payload["seed"] = json!(s as u64);
        }
        merge_params(&mut payload, params.as_ref())?;

        let http = ctx.client(&account).await?;
        let answer = run_queued(&ctx, &http, &model, &payload, "fal: generate the image").await?;

        let urls: Vec<Value> = answer["images"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|i| i["url"].as_str().map(|u| json!(u)))
            .collect();
        if urls.is_empty() {
            weft::node_bail!("fal answered no images for this generation");
        }
        let ty = ctx.output_type("images").node_err("the images port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            // A generated image is the run's product: keep it past the
            // run (default 30-day access-bumped TTL) so the reference
            // stays readable after the execution ends.
            .internalize(&json!(urls), &ty, Some(KeepTtl::Default))
            .await?;
        let first = stored.as_array().and_then(|a| a.first()).cloned().expect("checked non-empty");
        ctx.pulse_downstream(
            NodeOutput::new().set("images", stored).set("image", first),
        )
        .await
    }
}
