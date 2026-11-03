//! ElevenLabsCreateAgent: make a voice agent on the account (an
//! ACCOUNT ASSET, like a cloned voice). The typed inputs cover what
//! most agents set; three JSON inputs (`conversationConfig`,
//! `platformSettings`, `workflow`) carry every other ElevenLabs
//! setting, with the typed inputs merged on top. The agent_id then
//! works in Agent: Place Call, and the agent is manageable at
//! elevenlabs.io/app/agents.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use weft::access::client::{post_json, url};
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::{merge_json, API};
use super::flows::set_some;

#[derive(NodeManifest)]
pub struct ElevenLabsCreateAgentNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The longest goal one evaluation criterion takes, in characters.
const MAX_GOAL_CHARS: usize = 2000;
/// The `allowOverrides` names that are not a conversation setting:
/// each allows one field of a call's client data, through its own flag
/// on `platform_settings.overrides`. Every other name is a path under
/// `conversation_config_override`.
const CLIENT_DATA_FLAGS: [(&str, &str); 3] = [
    ("starting_workflow_node_id", "enable_starting_workflow_node_id_from_client"),
    ("procedure_ids", "enable_procedure_ids_from_client"),
    ("custom_llm_extra_body", "custom_llm_extra_body"),
];

#[async_trait]
impl Node for ElevenLabsCreateAgentNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        // Built (and refused) in full before the account is opened.
        let body = request(&ctx)?;
        let http = ctx.client(&account).await?;
        let answer = post_json(&http, url(API, &["convai", "agents", "create"])?.as_str(), &body, "create the ElevenLabs agent").await?;
        let id = answer["agent_id"].as_str().node_err("ElevenLabs created the agent but its answer has no agent_id")?;
        ctx.pulse_downstream(NodeOutput::new().set("agentId", id)).await
    }
}

/// The JSON object on `port`, or an empty one when it is not set.
fn json_input(ctx: &RunContext, port: &str) -> WeftResult<Value> {
    Ok(Value::Object(ctx.inputs.opt::<Map<String, Value>>(port)?.unwrap_or_default()))
}

/// Whether `value` is an object holding at least one key.
fn has_keys(value: &Value) -> bool {
    value.as_object().is_some_and(|o| !o.is_empty())
}

/// The whole create request: each JSON input with the typed inputs
/// merged on top, refusing before anything is sent what ElevenLabs
/// would refuse.
fn request(ctx: &RunContext) -> WeftResult<Value> {
    let mut config = json_input(ctx, "conversationConfig")?;
    merge_json(&mut config, conversation_config(ctx)?);
    // The rule refuses `custom-llm` with no `conversationConfig` at all;
    // what `conversationConfig` itself holds is read here.
    let prompt = &config["agent"]["prompt"];
    if prompt["llm"] == json!("custom-llm") && prompt["custom_llm"].is_null() {
        weft::input_bail!(
            "the agent's model is `custom-llm`, and ElevenLabs needs that model's definition: put it in \
             `conversationConfig` under `agent.prompt.custom_llm` (its `url` at least), or pick another `llm`"
        );
    }
    // ElevenLabs refuses an English agent on any voice model but a turbo
    // one or eleven_flash_v2 ("English Agents must use turbo or flash
    // v2", answered live on 2026-10-11), and an agent with no language
    // is English. The rule refuses the typed pair; what
    // `conversationConfig` itself sets is read here.
    let language = config["agent"]["language"].as_str().unwrap_or("en");
    if let Some(model) = config["tts"]["model_id"].as_str() {
        if language == "en" && !(model.contains("turbo") || model == "eleven_flash_v2") {
            weft::input_bail!(
                "the agent speaks English, and ElevenLabs takes an English agent only on a turbo voice model or \
                 eleven_flash_v2, not '{model}': set `ttsModel` to eleven_v4_turbo or eleven_flash_v2, or leave it empty"
            );
        }
    }
    let mut body = json!({ "name": ctx.inputs.get::<String>("name")?, "conversation_config": config });

    let mut platform = json_input(ctx, "platformSettings")?;
    merge_json(&mut platform, platform_settings(ctx)?);
    if has_keys(&platform) {
        body["platform_settings"] = platform;
    }
    if let Some(workflow) = ctx.inputs.opt::<Map<String, Value>>("workflow")? {
        body["workflow"] = Value::Object(workflow);
    }
    let tags: Vec<String> = ctx.inputs.list("tags")?;
    if !tags.is_empty() {
        body["tags"] = json!(tags);
    }
    Ok(body)
}

/// `conversation_config` from the typed inputs, holding only what was
/// set, so the merge leaves every other key of `conversationConfig`
/// as it was written.
fn conversation_config(ctx: &RunContext) -> WeftResult<Value> {
    let mut agent = json!({ "prompt": prompt_config(ctx)? });
    set_some(&mut agent, "first_message", ctx.inputs.opt::<String>("firstMessage")?);
    set_some(&mut agent, "language", ctx.inputs.opt::<String>("language")?);
    if let Some(placeholders) = ctx.inputs.opt::<Map<String, Value>>("dynamicVariables")? {
        agent["dynamic_variables"] = json!({ "dynamic_variable_placeholders": placeholders });
    }

    let mut tts = json!({});
    set_some(&mut tts, "voice_id", ctx.inputs.opt::<String>("voice")?);
    set_some(&mut tts, "model_id", ctx.inputs.opt::<String>("ttsModel")?);
    set_some(&mut tts, "stability", ctx.inputs.opt::<f64>("stability")?);
    set_some(&mut tts, "similarity_boost", ctx.inputs.opt::<f64>("similarityBoost")?);
    set_some(&mut tts, "speed", ctx.inputs.opt::<f64>("speed")?);
    set_some(&mut tts, "agent_output_audio_format", ctx.inputs.opt::<String>("outputAudioFormat")?);

    let mut asr = json!({});
    let keywords: Vec<String> = ctx.inputs.list("asrKeywords")?;
    if !keywords.is_empty() {
        asr["keywords"] = json!(keywords);
    }

    let mut turn = json!({});
    set_some(&mut turn, "turn_timeout", ctx.inputs.opt::<f64>("turnTimeout")?);
    set_some(&mut turn, "silence_end_call_timeout", ctx.inputs.opt::<f64>("silenceEndCallTimeout")?);

    let mut conversation = json!({});
    set_some(&mut conversation, "max_duration_seconds", ctx.inputs.opt::<u64>("maxDurationSeconds")?);

    let mut config = json!({ "agent": agent });
    for (key, section) in [("tts", tts), ("asr", asr), ("turn", turn), ("conversation", conversation)] {
        if has_keys(&section) {
            config[key] = section;
        }
    }
    Ok(config)
}

/// `conversation_config.agent.prompt` from the typed inputs.
fn prompt_config(ctx: &RunContext) -> WeftResult<Value> {
    let mut prompt = json!({ "prompt": ctx.inputs.get::<String>("prompt")? });
    set_some(&mut prompt, "llm", ctx.inputs.opt::<String>("llm")?);
    set_some(&mut prompt, "temperature", ctx.inputs.opt::<f64>("temperature")?);
    set_some(&mut prompt, "max_tokens", ctx.inputs.opt::<u64>("maxTokens")?);
    set_some(&mut prompt, "reasoning_effort", ctx.inputs.opt::<String>("reasoningEffort")?);
    set_some(&mut prompt, "thinking_budget", ctx.inputs.opt::<u64>("thinkingBudget")?);
    set_some(&mut prompt, "timezone", ctx.inputs.opt::<String>("timezone")?);

    let tools: Vec<String> = ctx.inputs.list("toolIds")?;
    if !tools.is_empty() {
        prompt["tool_ids"] = json!(tools);
    }
    let servers: Vec<String> = ctx.inputs.list("mcpServerIds")?;
    if !servers.is_empty() {
        prompt["mcp_server_ids"] = json!(servers);
    }

    let built_in: Vec<String> = ctx.inputs.list("builtInTools")?;
    if !built_in.is_empty() {
        let mut system_tools = json!({});
        for tool in built_in {
            // A system tool's minimal config: ElevenLabs's own
            // description, timeout and behaviour for that tool.
            system_tools[tool.as_str()] = json!({ "type": "system", "name": tool, "params": { "system_tool_type": tool } });
        }
        prompt["built_in_tools"] = system_tools;
    }

    let documents: Vec<Document> = ctx.inputs.list("knowledgeBase")?;
    if !documents.is_empty() {
        prompt["knowledge_base"] = documents.into_iter().map(Document::locator).collect();
    }
    Ok(prompt)
}

/// One `knowledgeBase` entry, as the port types it.
#[derive(Deserialize)]
struct Document {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "usageMode")]
    usage_mode: Option<String>,
}

impl Document {
    /// The document as ElevenLabs's knowledge base locator.
    fn locator(self) -> Value {
        let mut locator = json!({ "type": self.kind, "name": self.name, "id": self.id });
        set_some(&mut locator, "usage_mode", self.usage_mode);
        locator
    }
}

/// `platform_settings` from the typed inputs, holding only what was
/// set.
fn platform_settings(ctx: &RunContext) -> WeftResult<Value> {
    let mut platform = json!({});
    if let Some(items) = ctx.inputs.opt::<BTreeMap<String, CollectionItem>>("dataCollection")? {
        let mut collection = Map::new();
        for (id, item) in items {
            let item = item.into_item(&id)?;
            collection.insert(id, item);
        }
        platform["data_collection"] = Value::Object(collection);
    }
    if let Some(goals) = ctx.inputs.opt::<BTreeMap<String, String>>("evaluationCriteria")? {
        let criteria = goals.into_iter().map(|(id, goal)| criterion(id, goal)).collect::<WeftResult<Vec<_>>>()?;
        platform["evaluation"] = json!({ "criteria": criteria });
    }
    let allowed: Vec<String> = ctx.inputs.list("allowOverrides")?;
    if !allowed.is_empty() {
        platform["overrides"] = permissions(&allowed);
    }
    Ok(platform)
}

/// One `dataCollection` entry, as the port types it: a text is the
/// description of a string item, an object is the item as written.
#[derive(Deserialize)]
#[serde(untagged)]
enum CollectionItem {
    Description(String),
    Item {
        #[serde(rename = "type")]
        kind: String,
        description: String,
        #[serde(rename = "enum")]
        values: Option<Vec<String>>,
    },
}

/// The value types a data collection item can have.
const DATA_TYPES: [&str; 4] = ["string", "boolean", "integer", "number"];

impl CollectionItem {
    /// The entry as ElevenLabs's item. A rule cannot look inside a dict's
    /// values, so what ElevenLabs refuses in one is refused here: a type
    /// it does not know, and allowed values (`enum`) on anything but a
    /// `string` item.
    fn into_item(self, id: &str) -> WeftResult<Value> {
        match self {
            CollectionItem::Description(description) => Ok(json!({ "type": "string", "description": description })),
            CollectionItem::Item { kind, .. } if !DATA_TYPES.contains(&kind.as_str()) => weft::input_bail!(
                "data collection item '{id}' has type '{kind}', and ElevenLabs takes one of {}",
                DATA_TYPES.join(", ")
            ),
            CollectionItem::Item { kind, values: Some(_), .. } if kind != "string" => weft::input_bail!(
                "data collection item '{id}' lists allowed values in `enum`, and ElevenLabs takes those only on a \
                 `string` item: make it a `string`, or leave `enum` out"
            ),
            CollectionItem::Item { kind, description, values } => {
                let mut item = json!({ "type": kind, "description": description });
                set_some(&mut item, "enum", values);
                Ok(item)
            }
        }
    }
}

/// One `evaluationCriteria` entry as ElevenLabs's criterion, its id
/// doubling as its name.
fn criterion(id: String, goal: String) -> WeftResult<Value> {
    // A rule cannot measure a dict's values, so the length is checked
    // here.
    let length = goal.chars().count();
    if length > MAX_GOAL_CHARS {
        weft::input_bail!(
            "evaluation criterion '{id}' has a goal of {length} characters, and ElevenLabs takes at most {MAX_GOAL_CHARS}"
        );
    }
    Ok(json!({ "id": id, "name": id, "conversation_goal_prompt": goal }))
}

/// `platform_settings.overrides` allowing each named override.
fn permissions(allowed: &[String]) -> Value {
    let mut overrides = json!({});
    for name in allowed {
        if let Some((_, flag)) = CLIENT_DATA_FLAGS.iter().find(|(field, _)| *field == name.as_str()) {
            overrides[*flag] = json!(true);
            continue;
        }
        let mut slot = &mut overrides["conversation_config_override"];
        for key in name.split('.') {
            slot = &mut slot[key];
        }
        *slot = json!(true);
    }
    overrides
}
