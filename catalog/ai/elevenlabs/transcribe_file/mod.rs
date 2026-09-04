//! ElevenLabsTranscribeFile: batch speech-to-text for a stored audio
//! file. POST the bytes to the speech-to-text route as a multipart
//! form and emit the transcript; the realtime sibling
//! (`ElevenLabsTranscribe`) is for a live audio bus, this one is for
//! a file that already exists (a voice note, a recording).

use async_trait::async_trait;
use serde_json::Value;

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsTranscribeFileNode;

#[cfg(feature = "node-tests")]
mod tests;

/// What the transcript answer carries onto the ports: the text, and
/// the language when the model reported one. Pure, so the provider's
/// answer shape is testable without a socket; a missing `text` is a
/// loud refusal naming the answer rather than an empty transcript.
pub fn transcript_output(answer: &Value) -> WeftResult<NodeOutput> {
    let text = answer["text"]
        .as_str()
        .node_err(format!("elevenlabs: the transcript answer carries no text: {answer}"))?;
    let mut out = NodeOutput::new().set("text", text);
    if let Some(language) = answer["language_code"].as_str() {
        out = out.set("language", language);
    }
    Ok(out)
}

#[async_trait]
impl Node for ElevenLabsTranscribeFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let language: Option<String> = ctx.inputs.opt("language")?;
        let model: String = ctx.inputs.get("model")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&audio).await?;
        let mut form = Multipart::form_data()
            .file("file", &meta.filename, &meta.mime_type, bytes)
            .text("model_id", &model);
        if let Some(language) = language.filter(|l| !l.trim().is_empty()) {
            form = form.text("language_code", language.trim());
        }
        let (content_type, body) = form.build();

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(format!("{API}/speech-to-text"))
                .header("content-type", content_type)
                .body(body),
            "elevenlabs: transcribe the file",
        )
        .await?;
        ctx.pulse_downstream(transcript_output(&answer)?).await
    }
}
