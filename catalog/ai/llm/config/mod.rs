//! LlmConfig: declarative LLM parameters. No inference happens here.
//! Users wire this node's `config` output into one or more
//! LlmInference nodes to share settings. The node's config map is
//! emitted on the `config` output, with the input `systemPrompt`
//! (if wired) overriding the config field of the same name.

use async_trait::async_trait;
use serde_json::{Map, Value};

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeMetadata, WeftResult};

pub struct LlmConfigNode;

const METADATA_JSON: &str = include_str!("metadata.json");

#[async_trait]
impl Node for LlmConfigNode {
    fn node_type(&self) -> &'static str {
        "LlmConfig"
    }

    fn metadata(&self) -> NodeMetadata {
        serde_json::from_str(METADATA_JSON).expect("LlmConfig metadata.json must be valid")
    }

    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<NodeOutput> {
        // Base from config fields (the user's design-time settings).
        let mut out: Map<String, Value> = ctx.config.values.clone().into_iter().collect();

        // Live override: input.systemPrompt wins over config.systemPrompt
        // when present. Matches v1 wiring semantics.
        if let Some(sp) = ctx.input.raw("systemPrompt").cloned() {
            out.insert("systemPrompt".into(), sp);
        }

        Ok(NodeOutput::empty().set("config", Value::Object(out)))
    }
}
