//! FalUpscaleImage: enlarge an image through fal's queue. The knobs
//! target the blessed upscaler; `params` carries model-specific
//! extras.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{media_url, merge_params, run_queued};

#[derive(NodeManifest)]
pub struct FalUpscaleImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalUpscaleImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let scale: f64 = ctx.inputs.get("scale")?;
        let params = ctx.inputs.raw("params").cloned();

        let mut payload = json!({
            "image_url": media_url(&ctx, &image).await?,
            "scale": scale,
        });
        merge_params(&mut payload, params.as_ref())?;

        let http = ctx.client(&account).await?;
        let answer = run_queued(&ctx, &http, &model, &payload, "fal: upscale the image").await?;

        let url = answer["image"]["url"]
            .as_str()
            .or_else(|| {
                answer["images"].as_array().and_then(|a| a.first()).and_then(|i| i["url"].as_str())
            })
            .node_err("fal answered no image for this upscale")?;
        let ty = ctx.output_type("image").node_err("the image port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            // The upscaled image is the run's product: keep it past the
            // run (default 30-day access-bumped TTL).
            .internalize(&Value::String(url.to_string()), &ty, Some(KeepTtl::Default))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", stored)).await
    }
}
