//! LlmConfig Node - LLM provider and parameters configuration

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// LlmConfig node for LLM provider and parameters configuration.
#[derive(Default)]
pub struct LlmConfigNode;

#[async_trait]
impl Node for LlmConfigNode {
    fn node_type(&self) -> &'static str {
        "LlmConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "LLM Config",
            inputs: vec![
                PortDef::new("systemPrompt", "String", false),
            ],
            outputs: vec![
                PortDef::new("config", "Dict[String, String | Number | Boolean]", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![
                FieldDef::api_key("apiKey", "openrouter"),
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
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        // Merge input-port values into the config output so wired fields
        // (like an edge into systemPrompt) override literal config values.
        let mut merged = match ctx.config.as_object() {
            Some(o) => o.clone(),
            None => serde_json::Map::new(),
        };
        if let Some(inputs) = ctx.input.as_object() {
            for (k, v) in inputs {
                if !v.is_null() {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        NodeResult::completed(serde_json::json!({ "config": serde_json::Value::Object(merged) }))
    }
}

register_node!(LlmConfigNode);
