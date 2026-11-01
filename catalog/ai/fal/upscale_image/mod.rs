//! FalUpscaleImage: enlarge an image through fal's queue. The knobs
//! target the blessed upscaler; `params` carries model-specific
//! extras.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::fal::{merge_params, run_queued};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let scale: f64 = ctx.inputs.get("scale")?;
        let params = ctx.inputs.opt::<Map<String, Value>>("params")?;

        let mut payload = json!({
            "image_url": ctx.storage(StorageScope::Run).external_file(&image).await?.url,
            "scale": scale,
        });
        merge_params(&mut payload, params.as_ref());

        let http = ctx.client(&account).await?;
        let answer =
            run_queued(&ctx, &account, &http, &model, &payload, "fal: upscale the image").await?;

        let url = answer["image"]["url"]
            .as_str()
            .or_else(|| {
                answer["images"].as_array().and_then(|a| a.first()).and_then(|i| i["url"].as_str())
            })
            .node_err("fal answered no image for this upscale")?;
        let ty = ctx.output_type("image")?;
        let stored = ctx
            .storage(StorageScope::Run)
            .internalize(&Value::String(url.to_string()), &ty, None)
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", stored)).await
    }
}
