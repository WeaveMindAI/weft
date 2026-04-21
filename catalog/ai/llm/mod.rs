//! LLM: call a large language model. Scaffold placeholder that
//! demonstrates the `report_cost` primitive.
//!
//! Phase A2 port: wire the real minillmlib client (model routing, cost
//! tracking, streaming). For now, body returns a stub response so
//! `cargo check` succeeds.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::{CostReport, ExecutionContext, Node, NodeMetadata, WeftResult};
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
        let model: String = ctx.config.get("model")?;
        let _temperature: Option<f64> = ctx.config.get_optional("temperature")?;
        let _max_tokens: Option<u32> = ctx.config.get_optional("maxTokens")?;
        let _api_key: Option<String> = ctx.config.get_optional("apiKey")?;

        // Phase A2: wire minillmlib here. For now emit a placeholder
        // so the primitive surface and cost-report path exercise end
        // to end.
        let response = format!("[stub response to: {}]", prompt);

        ctx.report_cost(CostReport {
            service: "openrouter".into(),
            model: Some(model),
            amount_usd: 0.0,
            metadata: serde_json::json!({ "stub": true }),
        });

        Ok(NodeOutput::empty()
            .set("response", Value::String(response))
            .set("inputTokens", Value::from(0))
            .set("outputTokens", Value::from(0)))
    }
}
