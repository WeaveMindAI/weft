//! ElevenLabsSpeakFile: batch text-to-speech. Text in, one audio file
//! out (the non-streaming sibling of the realtime voice loop): POST
//! the text to the voice's TTS route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio_with_id};
use super::flows::set_some;
use super::speech::{shared_settings, speech_url, voice_settings};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let text: String = ctx.inputs.get("text")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let previous: Option<String> = ctx.inputs.opt("previousText")?;
        let next: Option<String> = ctx.inputs.opt("nextText")?;
        let japanese: bool = ctx.inputs.get("languageTextNormalization")?;

        let mut body = json!({ "text": text, "model_id": model });
        set_some(&mut body, "voice_settings", voice_settings(&ctx)?);
        set_some(&mut body, "previous_text", previous);
        set_some(&mut body, "next_text", next);
        if japanese {
            body["apply_language_text_normalization"] = json!(true);
        }
        shared_settings(&ctx, &mut body)?;

        let http = ctx.client(&account).await?;
        // The voice id is typed text that becomes a path segment:
        // encoded, it can only ever name a voice.
        let url = speech_url(&ctx, &["text-to-speech", &voice], &output_format)?;
        let (extension, mime) = audio_file_type(&output_format)?;
        let filename = format!("speech.{extension}");
        let what = "turn the text into speech on ElevenLabs";
        emit_audio_with_id(&ctx, http.post(url.as_str()).json(&body), what, &filename, mime, ("request-id", "requestId")).await
    }
}
