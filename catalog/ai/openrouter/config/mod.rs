//! OpenRouterConfig: declarative OpenRouter parameters. No inference happens
//! here. Users wire this node's `config` output into one or more
//! OpenRouterInference nodes to share settings. The node's config map is
//! emitted on the `config` output, with the input `systemPrompt`
//! (if wired) overriding the config field of the same name.

use async_trait::async_trait;
use serde_json::{Map, Value};

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterConfigNode;

#[async_trait]
impl Node for OpenRouterConfigNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // Base from config fields (the user's design-time settings).
        let mut out: Map<String, Value> = ctx.config.values.clone().into_iter().collect();

        // Live override: input.systemPrompt wins over config.systemPrompt
        // when present.
        if let Some(sp) = ctx.input.raw("systemPrompt").cloned() {
            out.insert("systemPrompt".into(), sp);
        }

        ctx.pulse_downstream(NodeOutput::empty().set("config", Value::Object(out))).await
    }
}
