//! ElevenLabsCreateAgent: mint a voice agent on the account (an
//! ACCOUNT ASSET, like a cloned voice): name, system prompt, first
//! message, voice, language. The minted agent_id then works in Agent:
//! Place Call, and the agent is manageable at elevenlabs.io/app/agents.

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsCreateAgentNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsCreateAgentNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let prompt: String = ctx.inputs.get("prompt")?;
        let first_message: Option<String> = ctx.inputs.opt("firstMessage")?;
        let voice: Option<String> = ctx.inputs.opt("voice")?;
        let language: String = ctx.inputs.get("language")?;

        let mut agent = json!({
            "prompt": { "prompt": prompt },
            "language": language,
        });
        if let Some(m) = first_message.as_deref().filter(|m| !m.trim().is_empty()) {
            agent["first_message"] = json!(m);
        }
        let mut config = json!({ "agent": agent });
        if let Some(v) = voice.as_deref().filter(|v| !v.trim().is_empty()) {
            config["tts"] = json!({ "voice_id": v });
        }

        let http = ctx.client(&account).await?;
        let answer = post_json(
            &http,
            &format!("{API}/convai/agents/create"),
            &json!({ "name": name, "conversation_config": config }),
            "elevenlabs: create the agent",
        )
        .await?;
        let id = answer["agent_id"]
            .as_str()
            .node_err("elevenlabs: the create answered no agent_id")?;
        ctx.pulse_downstream(NodeOutput::new().set("agentId", id)).await
    }
}
