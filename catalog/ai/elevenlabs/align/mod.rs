//! ElevenLabsAlign: forced alignment: an audio file plus its known
//! transcript to per-word timestamps. POST both to the
//! forced-alignment route and emit the timed words.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsAlignNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsAlignNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let transcript: String = ctx.inputs.get("transcript")?;

        let (meta, bytes) = ctx.storage(StorageScope::Execution).get_bytes(&audio).await?;
        let (content_type, body) = Multipart::form_data()
            .text("text", &transcript)
            .file("file", &meta.filename, &meta.mime_type, bytes)
            .build();

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(format!("{API}/forced-alignment"))
                .header("content-type", content_type)
                .body(body),
            "elevenlabs: align the transcript",
        )
        .await?;

        // The answer's word list interleaves the spacing between words
        // as its own tokens; the port carries the words alone.
        let words: Vec<Value> = answer["words"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|w| w["text"].as_str().is_some_and(|t| !t.trim().is_empty()))
            .map(|w| json!({ "text": w["text"], "start": w["start"], "end": w["end"] }))
            .collect();
        let mut out = NodeOutput::new().set("words", json!(words));
        if let Some(loss) = answer["loss"].as_f64() {
            out = out.set("loss", loss);
        }
        ctx.pulse_downstream(out).await
    }
}
