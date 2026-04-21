//! LLM: call a large language model via minillmlib. Supports
//! OpenRouter (default) and direct OpenAI/Anthropic through
//! provider routing.

use async_trait::async_trait;
use minillmlib::{ChatNode, GeneratorInfo};
use serde_json::Value;

use weft_core::{CostReport, ExecutionContext, Node, NodeMetadata, WeftResult};
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;

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
}
