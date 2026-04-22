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
use serde_json::{Map, Value};

use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct LlmInferenceNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for LlmInferenceNode {
    fn node_type(&self) -> &'static str {
        "LlmInference"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("LlmInference metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // Merge config precedence: input.config wins over node config.
        let from_config_input: Map<String, Value> = ctx
            .input
            .raw("config")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let mut effective: Map<String, Value> = ctx.config.values.clone().into_iter().collect();
        for (k, v) in from_config_input {
            effective.insert(k, v);
        }

        // systemPrompt priority: input port wins over everything.
        let system_prompt = ctx
            .input
            .raw("systemPrompt")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                effective
                    .get("systemPrompt")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();

        let prompt: String = ctx.input.get("prompt")?;

        let model = effective
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("anthropic/claude-sonnet-4.6")
            .to_string();

        let parse_json = ctx
            .config
            .values
            .get("parseJson")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Build the generator. If an apiKey is set in effective
        // config AND it isn't the BYOK sentinel, use it; otherwise
        // fall back to env ($OPENROUTER_API_KEY) which the platform
        // sets when running on credits.
        let mut generator = GeneratorInfo::openrouter(model.clone());
        if let Some(key) = effective.get("apiKey").and_then(|v| v.as_str()) {
            if !key.is_empty() && key != "__BYOK__" {
                generator = generator.with_api_key(key.to_string());
            }
        }

        let mut cp = CompletionParameters::new();
        if let Some(t) = effective.get("temperature").and_then(|v| v.as_f64()) {
            cp = cp.with_temperature(t as f32);
        }
        if let Some(m) = effective.get("maxTokens").and_then(|v| v.as_u64()) {
            cp = cp.with_max_tokens(m as u32);
        }
        if let Some(tp) = effective.get("topP").and_then(|v| v.as_f64()) {
            cp = cp.with_top_p(tp as f32);
        }
        if let Some(fp) = effective.get("frequencyPenalty").and_then(|v| v.as_f64()) {
            cp.frequency_penalty = Some(fp as f32);
        }
        if let Some(pp) = effective.get("presencePenalty").and_then(|v| v.as_f64()) {
            cp.presence_penalty = Some(pp as f32);
        }
        if let Some(s) = effective.get("seed").and_then(|v| v.as_u64()) {
            cp = cp.with_seed(s);
        }
        let reasoning_on = effective
            .get("reasoning")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if reasoning_on {
            let effort = effective
                .get("reasoningEffort")
                .and_then(|v| v.as_str())
                .unwrap_or("medium")
                .to_string();
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

        let text = response.text().unwrap_or_default().to_string();

        let response_value = if parse_json {
            repair_and_parse(&text)
        } else {
            Value::String(text)
        };

        let mut output = NodeOutput::empty().set("response", response_value.clone());

        // parseJson + object response: also emit each top-level key
        // as its own output port so downstream nodes can bind
        // specific fields directly.
        if parse_json {
            if let Value::Object(obj) = &response_value {
                for (key, val) in obj {
                    if key == "response" {
                        continue;
                    }
                    output = output.set(key.clone(), val.clone());
                }
            }
        }

        Ok(output)
    }
}

fn repair_and_parse(text: &str) -> Value {
    // Plain parse first; fall back to minillmlib's repair_json
    // (returns a String of normalized JSON) → serde_json::from_str;
    // fall back to the raw string as a last resort.
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        return v;
    }
    if let Ok(fixed) = json_repair::repair_json(text, &Default::default()) {
        if let Ok(v) = serde_json::from_str::<Value>(&fixed) {
            return v;
        }
    }
    Value::String(text.to_string())
}
