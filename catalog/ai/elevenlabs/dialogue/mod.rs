//! ElevenLabsDialogue: a script of lines, each said by its own voice,
//! to one audio file of them talking together, through ElevenLabs's
//! text to dialogue route.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio_with_id};
use super::flows::set_some;
use super::speech::{shared_settings, speech_url};

#[derive(NodeManifest)]
pub struct ElevenLabsDialogueNode;

#[cfg(feature = "node-tests")]
mod tests;

/// One line of the script, as `lines` declares it.
#[derive(Deserialize)]
pub struct DialogueLine {
    pub text: String,
    pub voice: String,
}

#[async_trait]
impl Node for ElevenLabsDialogueNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let lines: Vec<DialogueLine> = ctx.inputs.get("lines")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let stability: Option<f64> = ctx.inputs.opt("stability")?;
        let similarity: Option<f64> = ctx.inputs.opt("similarity")?;
        let previous: Option<String> = ctx.inputs.opt("previousText")?;
        let next: Option<String> = ctx.inputs.opt("nextText")?;

        let inputs: Vec<_> = lines.iter().map(|l| json!({ "text": l.text, "voice_id": l.voice })).collect();
        let mut body = json!({ "inputs": inputs, "model_id": model });
        let mut settings = serde_json::Map::new();
        if let Some(v) = stability {
            settings.insert("stability".into(), json!(v));
        }
        if let Some(v) = similarity {
            settings.insert("similarity".into(), json!(v));
        }
        if !settings.is_empty() {
            body["settings"] = settings.into();
        }
        set_some(&mut body, "previous_text", previous);
        set_some(&mut body, "future_text", next);
        shared_settings(&ctx, &mut body)?;

        let http = ctx.client(&account).await?;
        let url = speech_url(&ctx, &["text-to-dialogue"], &output_format)?;
        let (extension, mime) = audio_file_type(&output_format)?;
        emit_audio_with_id(
            &ctx,
            http.post(url.as_str()).json(&body),
            "turn the script into dialogue on ElevenLabs",
            &format!("dialogue.{extension}"),
            mime,
            ("request-id", "requestId"),
        )
        .await
    }
}
