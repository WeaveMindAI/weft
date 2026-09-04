//! ElevenLabsSpeakFile: batch text-to-speech. Text in, one audio file
//! out (the non-streaming sibling of the realtime voice loop): POST
//! the text to the voice's TTS route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio, API};

#[derive(NodeManifest)]
pub struct ElevenLabsSpeakFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsSpeakFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let text: String = ctx.inputs.get("text")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let stability: Option<f64> = ctx.inputs.opt("stability")?;
        let similarity: Option<f64> = ctx.inputs.opt("similarityBoost")?;
        let speed: Option<f64> = ctx.inputs.opt("speed")?;

        let mut body = json!({ "text": text, "model_id": model });
        let mut settings = serde_json::Map::new();
        if let Some(v) = stability {
            settings.insert("stability".into(), json!(v));
        }
        if let Some(v) = similarity {
            settings.insert("similarity_boost".into(), json!(v));
        }
        if let Some(v) = speed {
            settings.insert("speed".into(), json!(v));
        }
        if !settings.is_empty() {
            body["voice_settings"] = settings.into();
        }

        let http = ctx.client(&account).await?;
        let url = format!(
            "{API}/text-to-speech/{voice}?output_format={}",
            urlencoding::encode(&output_format)
        );
        let (extension, mime) = audio_file_type(&output_format)?;
        let filename = format!("speech.{extension}");
        emit_audio(&ctx, http.post(url).json(&body), "elevenlabs: text to speech", &filename, mime)
            .await
    }
}
