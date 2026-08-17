//! FalEditImage: change an existing image with a text instruction
//! (edit, restyle, inpaint with an optional mask) through fal's
//! queue. The knobs target the blessed edit model; `params` carries
//! model-specific extras.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{media_url, merge_params, run_queued};

#[derive(NodeManifest)]
pub struct FalEditImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalEditImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let mask: Option<FileHandle> = ctx.inputs.opt("mask")?;
        let params = ctx.inputs.raw("params").cloned();

        let mut payload = json!({
            "prompt": prompt,
            "image_url": media_url(&ctx, &image).await?,
        });
        if let Some(mask) = &mask {
            payload["mask_url"] = json!(media_url(&ctx, mask).await?);
        }
        merge_params(&mut payload, params.as_ref())?;

        let http = ctx.client(&account).await?;
        let answer = run_queued(&ctx, &http, &model, &payload, "fal: edit the image").await?;

        // Edit models answer `images`; a single-image model answers
        // `image`. Take whichever arrived.
        let url: Option<&str> = answer["images"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|i| i["url"].as_str())
            .or_else(|| answer["image"]["url"].as_str());
        let url = url.node_err("fal answered no image for this edit")?;

        let ty = ctx.output_type("image").node_err("the image port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            // The edited image is the run's product: keep it past the
            // run (default 30-day access-bumped TTL).
            .internalize(&Value::String(url.to_string()), &ty, Some(KeepTtl::Default))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", stored)).await
    }
}
