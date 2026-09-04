//! ElevenLabsChangeVoice: re-voice a recording (speech-to-speech):
//! the words, timing, and emotion stay, the voice becomes the picked
//! one. POST the audio to the voice's speech-to-speech route and
//! store the answer.

use async_trait::async_trait;

use weft::access::client::Multipart;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio, API};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let voice: String = ctx.inputs.get("voice")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let remove_noise: bool = ctx.inputs.get("removeBackgroundNoise")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&audio).await?;
        let mut form = Multipart::form_data()
            .text("model_id", &model)
            .file("audio", &meta.filename, &meta.mime_type, bytes);
        if remove_noise {
            form = form.text("remove_background_noise", "true");
        }
        let (content_type, body) = form.build();

        let http = ctx.client(&account).await?;
        let url = format!(
            "{API}/speech-to-speech/{voice}?output_format={}",
            urlencoding::encode(&output_format)
        );
        let (extension, mime) = audio_file_type(&output_format)?;
        let filename = format!("revoiced.{extension}");
        emit_audio(
            &ctx,
            http.post(url).header("content-type", content_type).body(body),
            "elevenlabs: change the voice",
            &filename,
            mime,
        )
        .await
    }
}
