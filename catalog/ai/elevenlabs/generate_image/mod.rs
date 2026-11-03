//! ElevenLabsGenerateImage: a prompt, and any images to edit or draw
//! from, to one image through the Image & Video API.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::flows::{generate, set_some, Job, Media, References};

#[derive(NodeManifest)]
pub struct ElevenLabsGenerateImageNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsGenerateImageNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let model: String = ctx.inputs.get("model")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let images: Vec<FileHandle> = ctx.inputs.list("images")?;
        let mask: Option<FileHandle> = ctx.inputs.opt("mask")?;
        let aspect: Option<String> = ctx.inputs.opt("aspectRatio")?;
        let resolution: Option<String> = ctx.inputs.opt("resolution")?;
        let quality: Option<String> = ctx.inputs.opt("quality")?;
        let background: Option<String> = ctx.inputs.opt("background")?;
        let seed: Option<u64> = ctx.inputs.opt("seed")?;

        let http = ctx.client(&account).await?;
        let refs = References::new(&ctx, &http, &account);
        let mut job = ctx.keep(Job::default()).await?;
        let image = generate(&ctx, &mut job, &account, &http, Media::Image, || async {
            let mut body = json!({ "model_id": model, "prompt": prompt });
            if !images.is_empty() {
                body["images"] = json!(refs.of_each(&images).await?);
            }
            if let Some(mask) = &mask {
                body["mask"] = refs.of(mask).await?;
            }
            set_some(&mut body, "aspect_ratio", aspect);
            set_some(&mut body, "resolution", resolution);
            set_some(&mut body, "quality", quality);
            set_some(&mut body, "background", background);
            set_some(&mut body, "seed", seed);
            Ok(body)
        })
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", image)).await
    }
}
