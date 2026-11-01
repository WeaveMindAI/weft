//! ElevenLabsAlign: forced alignment: an audio file plus its known
//! transcript to per-word and per-character timestamps. POST both to
//! the forced-alignment route and emit the timed words and characters
//! as one stored JSON file, with the overall loss beside it.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{store_json, API};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;
        let transcript: String = ctx.inputs.get("transcript")?;

        let storage = ctx.storage(StorageScope::Run);
        let (meta, bytes) = storage.get_bytes(&audio).await?;
        let (content_type, body) = Multipart::form_data()
            .text("text", &transcript)
            .file("file", &meta.filename, &meta.mime_type, bytes)
            .build();

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(format!("{API}/forced-alignment"))
                .header("content-type", content_type)
                .body(body),
            "align the transcript on ElevenLabs",
        )
        .await?;

        // The answer's word list interleaves the spacing between words
        // as its own tokens; the file keeps the words alone.
        let words: Vec<Value> = answer["words"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|w| w["text"].as_str().is_some_and(|t| !t.trim().is_empty()))
            .map(|w| json!({ "text": w["text"], "start": w["start"], "end": w["end"], "loss": w["loss"] }))
            .collect();
        let characters: Vec<Value> = answer["characters"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|c| json!({ "text": c["text"], "start": c["start"], "end": c["end"] }))
            .collect();
        let mut alignment = json!({ "words": words, "characters": characters });
        let mut out = NodeOutput::new();
        if let Some(loss) = answer["loss"].as_f64() {
            alignment["loss"] = json!(loss);
            out = out.set("loss", loss);
        }

        // Each timed character weighs about 40 bytes as JSON, so a few
        // minutes of speech outgrow the 100 KB a wire carries: the
        // alignment always travels as a file.
        let file = store_json(&ctx, &alignment, "alignment.json").await?;
        ctx.pulse_downstream(out.set("alignment", file)).await
    }
}
