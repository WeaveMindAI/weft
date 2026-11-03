//! ElevenLabsCreateVoice: mint a designed preview (from Design Voice)
//! into a durable voice on the account. The minted voice_id is an
//! account asset usable in every speak node from then on.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::{post_json, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;
use super::flows::set_some;

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let description: String = ctx.inputs.get("description")?;
        let preview_id: String = ctx.inputs.get("previewId")?;
        let labels: Option<BTreeMap<String, String>> = ctx.inputs.opt("labels")?;

        let mut body = json!({
            "voice_name": name,
            "voice_description": description,
            "generated_voice_id": preview_id,
        });
        set_some(&mut body, "labels", labels.filter(|l| !l.is_empty()));

        let http = ctx.client(&account).await?;
        let answer = post_json(&http, url(API, &["text-to-voice"])?.as_str(), &body, "create the voice on ElevenLabs").await?;
        let voice_id = answer["voice_id"]
            .as_str()
            .node_err("ElevenLabs created the voice but its answer has no voice_id")?;

        // The voice exists on the account once ElevenLabs answers, so a
        // part of the answer it left out closes its port rather than
        // failing a run whose rerun would mint a second voice.
        let mut out = NodeOutput::new().set("voiceId", voice_id);
        if let Some(url) = answer["preview_url"].as_str() {
            out = out.set("previewUrl", url);
        }
        if answer["labels"].is_object() {
            out = out.set("labels", answer["labels"].clone());
        }
        ctx.pulse_downstream(out).await
    }
}
