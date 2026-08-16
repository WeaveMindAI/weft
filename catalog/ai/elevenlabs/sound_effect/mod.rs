//! ElevenLabsSoundEffect: a text prompt to a generated sound effect
//! (a whoosh, rain, an engine): POST the prompt to the
//! sound-generation route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{emit_audio, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let text: String = ctx.inputs.get("text")?;
        let duration: Option<f64> = ctx.inputs.opt("durationSecs")?;
        let looped: bool = ctx.inputs.get("loop")?;
        let influence: Option<f64> = ctx.inputs.opt("promptInfluence")?;

        let mut body = json!({ "text": text, "loop": looped });
        if let Some(secs) = duration {
            body["duration_seconds"] = json!(secs);
        }
        if let Some(v) = influence {
            body["prompt_influence"] = json!(v);
        }

        let http = ctx.client(&account).await?;
        emit_audio(
            &ctx,
            http.post(format!("{API}/sound-generation")).json(&body),
            "elevenlabs: generate the sound effect",
            "sound_effect.mp3",
        )
        .await
    }
}
