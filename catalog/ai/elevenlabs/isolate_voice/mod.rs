//! ElevenLabsIsolateVoice: strip background noise from a recording,
//! keeping only the voice: POST the audio to the audio-isolation
//! route and store the cleaned answer.

use async_trait::async_trait;

use weft::access::client::Multipart;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{emit_audio, API};

#[derive(NodeManifest)]
pub struct ElevenLabsIsolateVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsIsolateVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&audio).await?;
        let (content_type, body) = Multipart::form_data()
            .file("audio", &meta.filename, &meta.mime_type, bytes)
            .build();

        let http = ctx.client(&account).await?;
        let filename = format!("isolated_{}", meta.filename);
        emit_audio(
            &ctx,
            http.post(format!("{API}/audio-isolation"))
                .header("content-type", content_type)
                .body(body),
            "elevenlabs: isolate the voice",
            &filename,
        )
        .await
    }
}
