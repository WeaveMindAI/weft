//! ElevenLabsAgentCall: place an outbound phone call with a
//! configured voice agent. The phone number's own provider record
//! (Twilio, Exotel or a SIP trunk) decides which call route is used,
//! so there is no provider knob; the agent then holds the conversation
//! on its own and the node emits the handles to follow it (Get
//! Conversation).

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::access::client::{get_json, post_json, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::{merge_json, API};
use super::flows::set_some;

#[derive(NodeManifest)]
pub struct ElevenLabsAgentCallNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The providers a number can be imported from, each with the route
/// that dials through it and how a refusal names it ("an Exotel").
const PROVIDERS: [(&str, &str, &str); 3] =
    [("twilio", "twilio", "a Twilio"), ("exotel", "exotel", "an Exotel"), ("sip_trunk", "sip-trunk", "a SIP trunk")];

#[async_trait]
impl Node for ElevenLabsAgentCallNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let agent: String = ctx.inputs.get("agent")?;
        let phone_number: String = ctx.inputs.get("phoneNumber")?;
        let to_number: String = ctx.inputs.get("toNumber")?;
        let client_data = client_data(&ctx)?;
        let telephony = Telephony::read(&ctx)?;

        let http = ctx.client(&account).await?;
        // The number's record says how it is wired; that decides the
        // call route.
        let number =
            get_json(&http, url(API, &["convai", "phone-numbers", &phone_number])?.as_str(), "read the ElevenLabs phone number")
                .await?;
        let provider = number["provider"].as_str().unwrap_or_default();
        // The number is the program's own choice, so a number this node
        // cannot dial, or a Twilio setting on a number that is not
        // Twilio's, is the program's mistake: an input error, which
        // `_error` never catches.
        let Some(&(_, route, a_provider)) = PROVIDERS.iter().find(|(name, _, _)| *name == provider) else {
            weft::input_bail!(
                "phone number {phone_number} has provider '{provider}', which this node does not know how to \
                 dial through; pick a Twilio, Exotel or SIP trunk number"
            );
        };
        if let Some(setting) = telephony.twilio_only().filter(|_| provider != "twilio") {
            weft::input_bail!(
                "phone number {phone_number} is {a_provider} number, and `{setting}` works only on a Twilio \
                 number (ElevenLabs ignores it on any other): pick a Twilio number, or leave `{setting}` off"
            );
        }

        let mut body = json!({
            "agent_id": agent,
            "agent_phone_number_id": phone_number,
            "to_number": to_number,
        });
        if has_keys(&client_data) {
            body["conversation_initiation_client_data"] = client_data;
        }
        let call_config = telephony.config();
        if has_keys(&call_config) {
            body["telephony_call_config"] = call_config;
        }

        let answer =
            post_json(&http, url(API, &["convai", route, "outbound-call"])?.as_str(), &body, "place the call through ElevenLabs")
                .await?;
        if answer["success"].as_bool() == Some(false) {
            weft::node_bail!("elevenlabs refused the call: {}", answer["message"].as_str().unwrap_or("no detail"));
        }
        let conversation = answer["conversation_id"].as_str().node_err("ElevenLabs placed the call but its answer has no conversation_id")?;
        let mut out = NodeOutput::new().set("conversationId", conversation);
        // Twilio and Exotel answer the call's CallSid, a SIP trunk its
        // SIP call id.
        if let Some(sid) = answer["callSid"].as_str() {
            out = out.set("callSid", sid);
        }
        if let Some(sip) = answer["sip_call_id"].as_str() {
            out = out.set("sipCallId", sip);
        }
        ctx.pulse_downstream(out).await
    }
}

/// Whether `value` is an object holding at least one key.
fn has_keys(value: &Value) -> bool {
    value.as_object().is_some_and(|o| !o.is_empty())
}

/// `conversation_initiation_client_data`: `clientData` with the typed
/// inputs merged on top, holding only what was set.
fn client_data(ctx: &RunContext) -> WeftResult<Value> {
    let mut agent = json!({});
    set_some(&mut agent, "first_message", ctx.inputs.opt::<String>("firstMessage")?);
    set_some(&mut agent, "language", ctx.inputs.opt::<String>("language")?);
    if let Some(prompt) = ctx.inputs.opt::<String>("prompt")? {
        agent["prompt"] = json!({ "prompt": prompt });
    }
    let mut tts = json!({});
    set_some(&mut tts, "voice_id", ctx.inputs.opt::<String>("voice")?);
    let mut override_config = json!({});
    for (key, section) in [("agent", agent), ("tts", tts)] {
        if has_keys(&section) {
            override_config[key] = section;
        }
    }

    let mut typed = json!({});
    if has_keys(&override_config) {
        typed["conversation_config_override"] = override_config;
    }
    if let Some(variables) = ctx.inputs.opt::<Map<String, Value>>("dynamicVariables")?.filter(|v| !v.is_empty()) {
        typed["dynamic_variables"] = Value::Object(variables);
    }
    set_some(&mut typed, "user_id", ctx.inputs.opt::<String>("userId")?);
    set_some(&mut typed, "branch_id", ctx.inputs.opt::<String>("branchId")?);
    set_some(&mut typed, "environment", ctx.inputs.opt::<String>("environment")?);

    let mut data = Value::Object(ctx.inputs.opt::<Map<String, Value>>("clientData")?.unwrap_or_default());
    merge_json(&mut data, typed);
    Ok(data)
}

/// The call's telephony settings, as the node was given them.
struct Telephony {
    ringing_timeout_secs: Option<u64>,
    record: bool,
    machine_detection: Option<String>,
}

impl Telephony {
    fn read(ctx: &RunContext) -> WeftResult<Self> {
        Ok(Self {
            ringing_timeout_secs: ctx.inputs.opt("ringingTimeoutSecs")?,
            record: ctx.inputs.get("recordCall")?,
            machine_detection: ctx.inputs.opt("answeringMachineDetection")?,
        })
    }

    /// The first setting that only a Twilio number honours, by its
    /// input's name, or `None` when none is set.
    fn twilio_only(&self) -> Option<&'static str> {
        if self.record {
            Some("recordCall")
        } else if self.machine_detection.is_some() {
            Some("answeringMachineDetection")
        } else {
            None
        }
    }

    /// `telephony_call_config`, holding only what was set.
    fn config(self) -> Value {
        let mut config = json!({});
        set_some(&mut config, "ringing_timeout_secs", self.ringing_timeout_secs);
        if self.record {
            config["twilio_call_recording_enabled"] = json!(true);
        }
        if let Some(mode) = self.machine_detection {
            config["twilio_machine_detection"] = json!({ "mode": mode });
        }
        config
    }
}
