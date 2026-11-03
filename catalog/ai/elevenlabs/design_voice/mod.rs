//! ElevenLabsDesignVoice: generate voice PREVIEWS from a text
//! description (and, on eleven_ttv_v3, a reference recording). Each
//! preview carries its audio and a generated voice id; nothing lands on
//! the account until Create Voice mints the preferred one into a
//! durable voice.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::json;

use weft::storage::{FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::flows::set_some;
use super::voice_previews::{emit_previews, preview_request};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let description: String = ctx.inputs.get("description")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let text: Option<String> = ctx.inputs.opt("text")?;
        let reference: Option<FileHandle> = ctx.inputs.opt("referenceAudio")?;
        let enhance: bool = ctx.inputs.get("shouldEnhance")?;

        // The fields both preview routes take, then the ones only
        // designing takes.
        let mut body = preview_request(&ctx, &description, text.as_deref())?;
        body["model_id"] = json!(model);
        set_some(&mut body, "quality", ctx.inputs.opt::<f64>("quality")?);
        if enhance {
            body["should_enhance"] = json!(true);
        }
        // The recording goes base64 inside the JSON, so its bytes are read
        // whole.
        if let Some(file) = &reference {
            let (_, bytes) = ctx.storage(StorageScope::Run).get_bytes(file).await?;
            body["reference_audio_base64"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
        }
        emit_previews(&ctx, &account, &["design"], &body, &output_format, "design the voice on ElevenLabs").await
    }
}
