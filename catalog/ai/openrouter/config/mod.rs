//! OpenRouterConfig: declarative OpenRouter parameters. No inference happens
//! here. Users wire this node's `config` output into one or more
//! OpenRouterInference nodes to share settings. The node's config map
//! (model, system prompt, sampling knobs, key) is emitted whole on the
//! `config` output; the system prompt lives HERE, as the `systemPrompt`
//! field, and nowhere else.

use async_trait::async_trait;
use serde_json::Value;

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterConfigNode;

#[async_trait]
impl Node for OpenRouterConfigNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The node's whole job is forwarding its config fields as one object.
        let out = ctx.config.object()?.clone();
        ctx.pulse_downstream(NodeOutput::new().set("config", Value::Object(out))).await
    }
}
