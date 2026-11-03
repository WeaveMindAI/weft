//! ElevenLabsSoundEffect: a text prompt to a generated sound effect
//! (a whoosh, rain, an engine): POST the prompt to the
//! sound-generation route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::url;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio, API};
use super::flows::set_some;

#[derive(NodeManifest)]
pub struct ElevenLabsSoundEffectNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsSoundEffectNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let text: String = ctx.inputs.get("text")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let duration: Option<f64> = ctx.inputs.opt("durationSecs")?;
        let looped: bool = ctx.inputs.get("loop")?;
        let influence: Option<f64> = ctx.inputs.opt("promptInfluence")?;

        let mut body = json!({ "text": text, "model_id": model, "loop": looped });
        set_some(&mut body, "duration_seconds", duration);
        set_some(&mut body, "prompt_influence", influence);
        let (extension, mime) = audio_file_type(&output_format)?;
        let filename = format!("sound_effect.{extension}");

        let http = ctx.client(&account).await?;
        let url = url(API, &["sound-generation"])?.query("output_format", &output_format);
        emit_audio(&ctx, http.post(url.as_str()).json(&body), "generate the sound effect on ElevenLabs", &filename, mime).await
    }
}
