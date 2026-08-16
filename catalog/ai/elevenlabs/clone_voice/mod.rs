//! ElevenLabsCloneVoice: mint a durable voice on the account from
//! sample recordings (instant voice clone). The minted voice_id is an
//! ACCOUNT ASSET: it outlives this execution and works in every speak
//! node from then on.

use async_trait::async_trait;

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsCloneVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsCloneVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let samples: Vec<FileHandle> = ctx.inputs.list("samples")?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        let remove_noise: bool = ctx.inputs.get("removeBackgroundNoise")?;

        if samples.is_empty() {
            weft::node_bail!("cloning a voice needs at least one sample recording");
        }

        let storage = ctx.storage(StorageScope::Execution);
        let mut form = Multipart::form_data().text("name", &name);
        for sample in &samples {
            let (meta, bytes) = storage.get_bytes(sample).await?;
            form = form.file("files", &meta.filename, &meta.mime_type, bytes);
        }
        if let Some(d) = description.as_deref().filter(|d| !d.trim().is_empty()) {
            form = form.text("description", d);
        }
        if remove_noise {
            form = form.text("remove_background_noise", "true");
        }
        let (content_type, body) = form.build();

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(format!("{API}/voices/add"))
                .header("content-type", content_type)
                .body(body),
            "elevenlabs: clone the voice",
        )
        .await?;
        let voice_id = answer["voice_id"]
            .as_str()
            .node_err("elevenlabs: the clone answered no voice_id")?;
        ctx.pulse_downstream(NodeOutput::new().set("voiceId", voice_id)).await
    }
}
