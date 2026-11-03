//! ElevenLabsLipSync: a still picture of someone and an audio track to
//! one video of them speaking it, through the Image & Video API.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::flows::{generate, set_some, Job, Media, References};

#[derive(NodeManifest)]
pub struct ElevenLabsLipSyncNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsLipSyncNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let image: FileHandle = ctx.inputs.get("image")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let model: String = ctx.inputs.get("model")?;
        let resolution: Option<String> = ctx.inputs.opt("resolution")?;
        let guidance: Option<f64> = ctx.inputs.opt("guidanceScale")?;
        let audio_guidance: Option<f64> = ctx.inputs.opt("audioGuidanceScale")?;

        let http = ctx.client(&account).await?;
        let refs = References::new(&ctx, &http, &account);
        let mut job = ctx.keep(Job::default()).await?;
        let video = generate(&ctx, &mut job, &account, &http, Media::Video, || async {
            let picture = refs.of(&image).await?;
            let speech = refs.of(&audio).await?;
            let mut body = json!({ "model_id": model, "image": picture, "audio": speech });
            set_some(&mut body, "resolution", resolution);
            set_some(&mut body, "guidance_scale", guidance);
            set_some(&mut body, "audio_guidance_scale", audio_guidance);
            Ok(body)
        })
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("video", video)).await
    }
}
