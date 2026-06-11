//! LlmInference: call an LLM via OpenRouter and return the
//! completion text. Settings come from (in priority order):
//!   1. the `config` input port (an upstream LlmConfig node)
//!   2. this node's own config fields
//!
//! When `parseJson` is set, the response is JSON-repaired + parsed.
//! Any top-level keys in the parsed object that match declared
//! output ports on this node get copied to their own port so users
//! can extract structured data directly.

use async_trait::async_trait;
use minillmlib::{
    json_repair, ChatNode, CompletionParameters, GeneratorInfo, NodeCompletionParameters,
    ReasoningConfig,
};
use std::collections::HashMap;

use serde_json::{Map, Value};

use weft_core::context::ConfigBag;
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct LlmInferenceNode;

const METADATA_JSON: &str = include_str!("metadata.json");

/// Primary output port. Set once with the model's full response, and
/// also used as the "skip me when fanning the JSON object onto its own
/// output ports" exclusion key. Single source so a metadata rename
/// surfaces both call sites together.
const PRIMARY_OUTPUT_PORT: &str = "response";

#[async_trait]
impl Node for LlmInferenceNode {
    fn node_type(&self) -> &'static str {
        "LlmInference"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("LlmInference metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Merge config precedence: input.config wins over node config,
        // then read every field through the SAME language-level typed
        // accessor (`ConfigBag::get_optional`) every other node uses:
        // absent/null -> None (caller's default), present-but-wrong-
        // type -> loud error. No node-local typed-read reinvention.
        let from_config_input: Map<String, Value> = ctx
            .input
            .raw("config")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let mut effective: HashMap<String, Value> = ctx.config.values.clone();
        for (k, v) in from_config_input {
            effective.insert(k, v);
        }
        let cfg = ConfigBag { values: effective };

        // systemPrompt priority: input port wins over everything.
        let system_prompt = ctx
            .input
            .raw("systemPrompt")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or(cfg.get_optional::<String>("systemPrompt")?)
            .unwrap_or_default();

        let prompt: String = ctx.input.get("prompt")?;

        let model = cfg
            .get_optional::<String>("model")?
            .unwrap_or_else(|| "anthropic/claude-sonnet-4.6".to_string());

        let parse_json = cfg.get_optional::<bool>("parseJson")?.unwrap_or(false);

        // Build the generator. If an apiKey is set in effective
        // config AND it isn't the BYOK sentinel, use it; otherwise
        // fall back to env ($OPENROUTER_API_KEY) which the platform
        // sets when running on credits.
        let mut generator = GeneratorInfo::openrouter(model.clone());
        if let Some(key) = cfg.get_optional::<String>("apiKey")? {
            if !key.is_empty() && key != "__BYOK__" {
                generator = generator.with_api_key(key);
            }
        }

        let mut cp = CompletionParameters::new();
        if let Some(t) = cfg.get_optional::<f64>("temperature")? {
            cp = cp.with_temperature(t as f32);
        }
        if let Some(m) = cfg.get_optional::<u64>("maxTokens")? {
            cp = cp.with_max_tokens(m as u32);
        }
        if let Some(tp) = cfg.get_optional::<f64>("topP")? {
            cp = cp.with_top_p(tp as f32);
        }
        if let Some(fp) = cfg.get_optional::<f64>("frequencyPenalty")? {
            cp.frequency_penalty = Some(fp as f32);
        }
        if let Some(pp) = cfg.get_optional::<f64>("presencePenalty")? {
            cp.presence_penalty = Some(pp as f32);
        }
        if let Some(s) = cfg.get_optional::<u64>("seed")? {
            cp = cp.with_seed(s);
        }
        if cfg.get_optional::<bool>("reasoning")?.unwrap_or(false) {
            let effort = cfg
                .get_optional::<String>("reasoningEffort")?
                .unwrap_or_else(|| "none".to_string());
            cp = cp.with_reasoning(ReasoningConfig {
                effort: Some(effort),
                max_tokens: None,
                exclude: None,
            });
        }

        let params = NodeCompletionParameters::new()
            .with_parse_json(parse_json)
            .with_params(cp);

        let root = ChatNode::root(system_prompt);
        let user = root.add_user(prompt);

        let response = user
            .complete(&generator, Some(&params))
            .await
            .map_err(|e| WeftError::NodeExecution(format!("llm: {e}")))?;

        let text = response
            .text()
            .ok_or_else(|| {
                WeftError::NodeExecution(
                    "llm: provider returned no text content (function-call only or empty response)"
                        .into(),
                )
            })?
            .to_string();

        let response_value = if parse_json {
            repair_and_parse(&text)?
        } else {
            Value::String(text)
        };

        let mut output = NodeOutput::empty().set(PRIMARY_OUTPUT_PORT, response_value.clone());
        // parseJson: fan each declared key of the response onto its
        // port. Intersect with declared ports so an extra model field
        // doesn't trip the undeclared-port error after the paid call.
        if parse_json {
            output = output.extend_from_declared(
                &response_value,
                ctx.declared_output_ports(),
                &[PRIMARY_OUTPUT_PORT],
            );
        }

        ctx.pulse_downstream(output).await
    }
}

fn repair_and_parse(text: &str) -> WeftResult<Value> {
    // Plain parse first; fall back to minillmlib's repair_json
    // (returns a String of normalized JSON) then re-parse. If both
    // fail with parseJson=true, the caller asked for JSON and the
    // provider returned something we can't honor; fail loud rather
    // than hand back a String pretending to be the JSON output.
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        return Ok(v);
    }
    if let Ok(fixed) = json_repair::repair_json(text, &Default::default()) {
        if let Ok(v) = serde_json::from_str::<Value>(&fixed) {
            return Ok(v);
        }
    }
    Err(WeftError::NodeExecution(format!(
        "llm: parseJson=true but provider response is not parseable JSON, even after repair: {}",
        text.chars().take(200).collect::<String>()
    )))
}
