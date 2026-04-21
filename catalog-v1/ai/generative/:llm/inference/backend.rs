//! LLM Node - AI language model completion

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// LLM node for AI language model completions.
#[derive(Default)]
pub struct LlmNode;

#[async_trait]
impl Node for LlmNode {
    fn node_type(&self) -> &'static str {
        "LlmInference"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "LLM",
            inputs: vec![
                PortDef::new("prompt", "String", true),
                PortDef::new("systemPrompt", "String", false),
                PortDef::wired_only("config", "Dict[String, String | Number | Boolean]", false),
            ],
            outputs: vec![
                PortDef::new("response", "MustOverride", false),
            ],
            features: NodeFeatures {
                canAddOutputPorts: true,
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("model"),
                FieldDef::textarea("systemPrompt"),
                FieldDef::number("maxTokens"),
                FieldDef::number("temperature"),
                FieldDef::number("topP"),
                FieldDef::number("frequencyPenalty"),
                FieldDef::number("presencePenalty"),
                FieldDef::checkbox("reasoning"),
                FieldDef::select("reasoningEffort", vec!["low", "medium", "high"]),
                FieldDef::number("seed"),
                FieldDef::checkbox("parseJson"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        use minillmlib::{ChatNode, NodeCompletionParameters};

        // Get config from input port (from LlmConfig node) or fall back to node's own config
        let llm_config = ctx.input.get("config")
            .and_then(|v| v.as_object())
            .map(|o| serde_json::Value::Object(o.clone()));
        
        let config_source = llm_config.as_ref().unwrap_or(&ctx.config);

        let model = config_source.get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("anthropic/claude-sonnet-4.6");
        
        // systemPrompt priority: input port (wired edge or config-fill via A2)
        // -> LlmConfig's config output -> this node's own config field -> "".
        let system_prompt = ctx.input.get("systemPrompt")
            .and_then(|v| v.as_str())
            .or_else(|| config_source.get("systemPrompt").and_then(|v| v.as_str()))
            .unwrap_or("");

        // parseJson is on the LLM node itself (not LLM Config) since it changes the output type
        let parse_json = ctx.config.get("parseJson")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let temperature = config_source.get("temperature")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);

        let max_tokens = config_source.get("maxTokens")
            .and_then(|v| v.as_u64())
            .map(|t| t as u32);

        let top_p = config_source.get("topP")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);

        let frequency_penalty = config_source.get("frequencyPenalty")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);

        let presence_penalty = config_source.get("presencePenalty")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);

        let seed = config_source.get("seed")
            .and_then(|v| v.as_u64());

        let reasoning_enabled = config_source.get("reasoning")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let reasoning_effort = config_source.get("reasoningEffort")
            .and_then(|v| v.as_str())
            .unwrap_or("medium");

        let prompt = ctx.input.get("prompt")
            .or_else(|| ctx.input.get("data"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let completion_ctx = match ctx.tracked_ai_context("openrouter", model, llm_config.as_ref()).await {
            Ok(c) => c,
            Err(e) => return NodeResult::failed(&e),
        };

        tracing::info!("LLM request: model={}, prompt_len={}, parse_json={}, reasoning={}",
            model, prompt.len(), parse_json, reasoning_enabled);

        let root = ChatNode::root(system_prompt);
        let user_node = root.add_user(prompt);

        // Build completion parameters
        let mut params = NodeCompletionParameters::new()
            .with_parse_json(parse_json);

        let mut completion_params = minillmlib::CompletionParameters::new();
        if let Some(t) = temperature {
            completion_params = completion_params.with_temperature(t);
        }
        if let Some(m) = max_tokens {
            completion_params = completion_params.with_max_tokens(m);
        }
        if let Some(p) = top_p {
            completion_params = completion_params.with_top_p(p);
        }
        if let Some(fp) = frequency_penalty {
            completion_params.frequency_penalty = Some(fp);
        }
        if let Some(pp) = presence_penalty {
            completion_params.presence_penalty = Some(pp);
        }
        if let Some(s) = seed {
            completion_params = completion_params.with_seed(s);
        }
        if reasoning_enabled {
            completion_params = completion_params.with_reasoning(minillmlib::ReasoningConfig {
                effort: Some(reasoning_effort.to_string()),
                max_tokens: None,
                exclude: None,
            });
        }
        params = params.with_params(completion_params);
        
        match user_node.complete_tracked(&completion_ctx, Some(&params)).await {
            Ok(response) => {
                let text = response.text().unwrap_or_default().to_string();
                
                // If parse_json was enabled, the text is already valid JSON - parse it to avoid double-encoding
                let response_value = if parse_json {
                    serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::String(text.clone()))
                } else {
                    serde_json::Value::String(text)
                };

                let mut output = serde_json::Map::new();
                output.insert("response".to_string(), response_value.clone());

                // When parseJson is true and result is a JSON object, also output
                // each top-level key as a separate port for direct extraction
                if parse_json {
                    if let serde_json::Value::Object(obj) = &response_value {
                        for (key, val) in obj {
                            if key != "response" {
                                output.insert(key.clone(), val.clone());
                            }
                        }
                    }
                }

                NodeResult::completed(serde_json::Value::Object(output))
            }
            Err(e) => {
                tracing::error!("LLM error: {}", e);
                NodeResult::failed(&e.to_string())
            }
        }
    }
}

register_node!(LlmNode);
