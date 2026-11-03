//! ElevenLabsChangeVoice: re-voice a recording (speech-to-speech):
//! the words, timing, and emotion stay, the voice becomes the picked
//! one. POST the audio to the voice's speech-to-speech route and
//! store the answer.

use async_trait::async_trait;

use weft::access::client::{Multipart, StoredBodyExt};
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio, input_file_format};
use super::speech::{speech_url, voice_settings};

#[derive(NodeManifest)]
pub struct ElevenLabsChangeVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsChangeVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let voice: String = ctx.inputs.get("voice")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let remove_noise: bool = ctx.inputs.get("removeBackgroundNoise")?;
        // A whole number: the widget's step holds a wired value to whole
        // numbers too.
        let seed: Option<u64> = ctx.inputs.opt("seed")?;
        let settings = voice_settings(&ctx)?;
        // Resolved before anything is sent: a format this node cannot
        // store must refuse before the paid conversion, not after.
        let (extension, mime) = audio_file_type(&output_format)?;

        let recording = ctx.storage(StorageScope::Run).body(&audio).await?;
        let file_format = input_file_format(&ctx, recording.mime_type())?;
        let mut form = Multipart::form_data().text("model_id", &model).stored("audio", recording);
        // The form carries the settings as one JSON string.
        if let Some(settings) = settings {
            form = form.text("voice_settings", &settings.to_string());
        }
        if let Some(seed) = seed {
            form = form.text("seed", &seed.to_string());
        }
        if remove_noise {
            form = form.text("remove_background_noise", "true");
        }
        if let Some(file_format) = file_format {
            form = form.text("file_format", file_format);
        }

        let http = ctx.client(&account).await?;
        // The voice id is typed text that becomes a path segment:
        // encoded, it can only ever name a voice.
        let url = speech_url(&ctx, &["speech-to-speech", &voice], &output_format)?;
        let filename = format!("revoiced.{extension}");
        emit_audio(
            &ctx,
            http.post(url.as_str()).multipart(form),
            "change the voice on ElevenLabs",
            &filename,
            mime,
        )
        .await
    }
}
