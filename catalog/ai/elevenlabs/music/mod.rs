//! ElevenLabsMusic: a text prompt to a full music track: POST the
//! prompt to the music route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{emit_audio, API};

#[derive(NodeManifest)]
pub struct ElevenLabsMusicNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsMusicNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let model: String = ctx.inputs.get("model")?;
        let length: Option<f64> = ctx.inputs.opt("lengthSecs")?;
        let instrumental: bool = ctx.inputs.get("forceInstrumental")?;

        let mut body = json!({ "prompt": prompt, "model_id": model });
        if let Some(secs) = length {
            body["music_length_ms"] = json!((secs * 1000.0).round() as u64);
        }
        if instrumental {
            body["force_instrumental"] = json!(true);
        }

        let http = ctx.client(&account).await?;
        emit_audio(
            &ctx,
            http.post(format!("{API}/music")).json(&body),
            "elevenlabs: compose the music",
            "music.mp3",
        )
        .await
    }
}
