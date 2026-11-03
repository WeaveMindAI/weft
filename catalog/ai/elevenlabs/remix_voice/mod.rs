//! ElevenLabsRemixVoice: generate voice PREVIEWS from a voice the
//! account can already use, changed by a description. Each preview
//! carries its audio and a generated voice id; nothing lands on the
//! account until Create Voice mints the preferred one, and the voice
//! that was remixed stays as it was.

use async_trait::async_trait;

use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::voice_previews::{emit_previews, preview_request};

#[derive(NodeManifest)]
pub struct ElevenLabsRemixVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsRemixVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let description: String = ctx.inputs.get("description")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let text: Option<String> = ctx.inputs.opt("text")?;

        let body = preview_request(&ctx, &description, text.as_deref())?;
        emit_previews(&ctx, &account, &[&voice, "remix"], &body, &output_format, "remix the voice on ElevenLabs").await
    }
}
