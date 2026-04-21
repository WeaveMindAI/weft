//! LLM: call a large language model via minillmlib. Supports
//! OpenRouter (default) and direct OpenAI/Anthropic through
//! provider routing.

use async_trait::async_trait;
use minillmlib::{ChatNode, GeneratorInfo};
use serde_json::Value;

use weft_core::error::WeftError;
use weft_core::node::{Diagnostic, NodeOutput, Severity};
use weft_core::project::{NodeDefinition, ProjectDefinition};
use weft_core::{CostReport, ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct LlmNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for LlmNode {
    fn node_type(&self) -> &'static str {
        "Llm"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("Llm metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        let prompt: String = ctx.input.get("prompt")?;
        let context: Option<String> = ctx.input.get_optional("context")?;
        let model: String = ctx.config.get("model")?;
        let temperature: Option<f64> = ctx.config.get_optional("temperature")?;
        let max_tokens: Option<u32> = ctx.config.get_optional("maxTokens")?;
        let api_key: Option<String> = ctx.config.get_optional("apiKey")?;

        // Build the generator. Default: OpenRouter (handles most
        // providers via one API key). Users can paste any model id
        // their OpenRouter account supports.
        let mut generator = GeneratorInfo::openrouter(&model);
        if let Some(key) = api_key {
            generator = generator.with_api_key(key);
        }

        // Build the conversation: optional system via `context`,
        // user message with the prompt.
        let system = context.as_deref().unwrap_or("");
        let root = ChatNode::root(system);
        let user_node = root.add_user(prompt.as_str());

        // Call the model.
        let response_node = user_node
            .complete(&generator, None)
            .await
            .map_err(|e| WeftError::NodeExecution(format!("llm: {e}")))?;
        let response_text = response_node
            .text()
            .ok_or_else(|| WeftError::NodeExecution("llm: empty response".into()))?
            .to_string();

        // Cost report. minillmlib carries token counts on the
        // response node's metadata (see tracking module); wiring the
        // precise cost math requires model price tables. For now
        // emit a zero cost with token-count metadata so the
        // dispatcher's ledger accepts it.
        ctx.report_cost(CostReport {
            service: "openrouter".into(),
            model: Some(model.clone()),
            amount_usd: 0.0,
            metadata: serde_json::json!({
                "note": "cost math lands with provider price tables",
            }),
        });

        let _ = temperature;
        let _ = max_tokens;

        Ok(NodeOutput::empty()
            .set("response", Value::String(response_text))
            .set("inputTokens", Value::from(0))
            .set("outputTokens", Value::from(0)))
    }

    fn validate(&self, node: &NodeDefinition, _project: &ProjectDefinition) -> Vec<Diagnostic> {
        let mut d = Vec::new();
        let line = node.header_span.map(|s| s.start_line).unwrap_or(0);
        // Model must be set (required in the catalog, but catch the
        // empty-string and missing cases for clarity).
        let model = node.config.get("model").and_then(|v| v.as_str()).unwrap_or("");
        if model.trim().is_empty() {
            d.push(Diagnostic {
                line,
                column: 0,
                severity: Severity::Error,
                message: format!("Llm '{}' is missing a model name.", node.id),
                code: Some("llm-model-required".into()),
            });
        }
        // API key must come from somewhere: config or OPENROUTER_API_KEY
        // env var (resolved at run time). Warn (not error) when both
        // config and env look absent at author time.
        let cfg_has_key = node
            .config
            .get("apiKey")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if !cfg_has_key && std::env::var("OPENROUTER_API_KEY").is_err() {
            d.push(Diagnostic {
                line,
                column: 0,
                severity: Severity::Warning,
                message: format!(
                    "Llm '{}' has no apiKey in config and OPENROUTER_API_KEY is not set. The call will fail at run time.",
                    node.id
                ),
                code: Some("llm-api-key-missing".into()),
            });
        }
        d
    }
}
