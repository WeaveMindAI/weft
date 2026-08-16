//! FalGenerateVideo: text to video through fal's queue. The knobs
//! target the blessed default model; `params` carries model-specific
//! extras (durations, resolutions, and audio toggles differ per
//! model family).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{merge_params, run_queued, video_url};

#[derive(NodeManifest)]
pub struct FalGenerateVideoNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for FalGenerateVideoNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let aspect: String = ctx.inputs.get("aspectRatio")?;
        let params = ctx.inputs.raw("params").cloned();

        let mut payload = json!({ "prompt": prompt, "aspect_ratio": aspect });
        merge_params(&mut payload, params.as_ref())?;

        let http = ctx.client(&account).await?;
        let answer = run_queued(&ctx, &http, &model, &payload, "fal: generate the video").await?;

        let url = video_url(&answer).node_err("fal answered no video for this generation")?;
        let ty = ctx.output_type("video").node_err("the video port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            .internalize(&Value::String(url.to_string()), &ty, None)
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("video", stored)).await
    }
}
