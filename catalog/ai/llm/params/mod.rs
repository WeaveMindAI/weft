//! LlmParams: declarative completion parameters. No inference happens
//! here. Users wire this node's `params` output into one or more LLM
//! nodes to share settings; the fields are provider-agnostic (the same
//! params object drives OpenRouter, Anthropic, OpenAI, or a custom
//! endpoint). The system prompt lives HERE, as the `systemPrompt`
//! field, and nowhere else.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct LlmParamsNode;

#[async_trait]
impl Node for LlmParamsNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The node's whole job is forwarding its config fields as one object.
        let out = ctx.inputs.object()?.clone();
        ctx.pulse_downstream(NodeOutput::new().set("params", Value::Object(out))).await
    }
}
