//! ElevenLabsAgentCall: place an outbound phone call with a
//! configured voice agent. The phone number's own provider record
//! (Twilio or a SIP trunk) decides which call route is used, so there
//! is no provider knob; the agent then holds the conversation on its
//! own and the node emits the handles to follow it (Get Conversation).

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::{get_json, post_json};
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsAgentCallNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsAgentCallNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let agent: String = ctx.inputs.get("agent")?;
        let phone_number: String = ctx.inputs.get("phoneNumber")?;
        let to_number: String = ctx.inputs.get("toNumber")?;
        let variables = ctx.inputs.raw("dynamicVariables").cloned();

        let http = ctx.client(&account).await?;
        // The number's record says how it is wired; that decides the
        // call route.
        let number = get_json(
            &http,
            &format!("{API}/convai/phone-numbers/{phone_number}"),
            "elevenlabs: read the phone number",
        )
        .await?;
        let route = match number["provider"].as_str().unwrap_or_default() {
            "twilio" => "twilio",
            "sip_trunk" => "sip-trunk",
            other => weft::node_bail!(
                "phone number {phone_number} has provider '{other}', which this node does \
                 not know how to dial through"
            ),
        };

        let mut body = json!({
            "agent_id": agent,
            "agent_phone_number_id": phone_number,
            "to_number": to_number,
        });
        if let Some(vars) = &variables {
            if !vars.as_object().is_some_and(|o| o.is_empty()) {
                body["conversation_initiation_client_data"] =
                    json!({ "dynamic_variables": vars });
            }
        }

        let answer = post_json(
            &http,
            &format!("{API}/convai/{route}/outbound-call"),
            &body,
            "elevenlabs: place the call",
        )
        .await?;
        if answer["success"].as_bool() == Some(false) {
            weft::node_bail!(
                "elevenlabs refused the call: {}",
                answer["message"].as_str().unwrap_or("no detail")
            );
        }
        let conversation: Option<&str> = answer["conversation_id"].as_str();
        let mut out = NodeOutput::new().set(
            "conversationId",
            conversation.node_err("elevenlabs: the call answered no conversation_id")?,
        );
        if let Some(sid) = answer["callSid"].as_str() {
            out = out.set("callSid", sid);
        }
        ctx.pulse_downstream(out).await
    }
}
