//! ElevenLabsCreateVoice: mint a designed preview (from Design Voice)
//! into a durable voice on the account. The minted voice_id is an
//! account asset usable in every speak node from then on.

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsCreateVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsCreateVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let description: String = ctx.inputs.get("description")?;
        let preview_id: String = ctx.inputs.get("previewId")?;

        let http = ctx.client(&account).await?;
        let answer = post_json(
            &http,
            &format!("{API}/text-to-voice"),
            &json!({
                "voice_name": name,
                "voice_description": description,
                "generated_voice_id": preview_id,
            }),
            "elevenlabs: create the voice",
        )
        .await?;
        let voice_id = answer["voice_id"]
            .as_str()
            .node_err("elevenlabs: the create answered no voice_id")?;
        ctx.pulse_downstream(NodeOutput::new().set("voiceId", voice_id)).await
    }
}
