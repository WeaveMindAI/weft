//! ElevenLabsDesignVoice: generate voice PREVIEWS from a text
//! description. Each preview carries its audio and a generated voice
//! id; nothing lands on the account until Create Voice mints the
//! preferred one into a durable voice.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::storage::StorageScope;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsDesignVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsDesignVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let description: String = ctx.inputs.get("description")?;
        let model: String = ctx.inputs.get("model")?;
        let text: Option<String> = ctx.inputs.opt::<String>("text")?.filter(|t| !t.trim().is_empty());

        let mut body = json!({ "voice_description": description, "model_id": model });
        match &text {
            Some(t) => body["text"] = json!(t),
            None => body["auto_generate_text"] = json!(true),
        }

        let http = ctx.client(&account).await?;
        let answer = post_json(
            &http,
            &format!("{API}/text-to-voice/design"),
            &body,
            "elevenlabs: design the voice",
        )
        .await?;

        // Each preview's audio arrives base64; a data: URL in the
        // typed value lets `internalize` store it and hand the slot
        // back as a normal stored file.
        let previews: Vec<Value> = answer["previews"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| {
                let mime = p["media_type"].as_str().unwrap_or("audio/mpeg");
                let b64 = p["audio_base_64"].as_str().unwrap_or_default();
                json!({
                    "voiceId": p["generated_voice_id"],
                    "audio": format!("data:{mime};base64,{b64}"),
                    "durationSecs": p["duration_secs"],
                    "language": p["language"],
                })
            })
            .collect();
        if previews.is_empty() {
            weft::node_bail!("elevenlabs answered no previews for this description");
        }

        let ty = ctx
            .output_type("previews")
            .node_err("the previews port declares no type")?;
        let stored = ctx
            .storage(StorageScope::Execution)
            .internalize(&json!(previews), &ty, None)
            .await?;
        ctx.pulse_downstream(NodeOutput::new().set("previews", stored)).await
    }
}
