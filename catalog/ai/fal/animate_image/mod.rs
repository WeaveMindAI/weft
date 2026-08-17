//! FalAnimateImage: image to video (bring a still to life) through
//! fal's queue: the image is the first frame, an optional tail image
//! pins the last frame on models that take one. `params` carries
//! model-specific extras.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, KeepTtl, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{media_url, merge_params, run_queued, video_url};

#[derive(NodeManifest)]
pub struct FalAnimateImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalAnimateImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let tail: Option<FileHandle> = ctx.inputs.opt("tailImage")?;
        let params = ctx.inputs.raw("params").cloned();

        let mut payload = json!({
            "prompt": prompt,
            "image_url": media_url(&ctx, &image).await?,
        });
        if let Some(tail) = &tail {
            payload["tail_image_url"] = json!(media_url(&ctx, tail).await?);
        }
        merge_params(&mut payload, params.as_ref())?;

        let http = ctx.client(&account).await?;
        let answer = run_queued(&ctx, &http, &model, &payload, "fal: animate the image").await?;

        let url = video_url(&answer).node_err("fal answered no video for this animation")?;
        let ty = ctx.output_type("video").node_err("the video port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            // The generated video is the run's product: keep it past the
            // run (default 30-day access-bumped TTL).
            .internalize(&Value::String(url.to_string()), &ty, Some(KeepTtl::Default))
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("video", stored)).await
    }
}
