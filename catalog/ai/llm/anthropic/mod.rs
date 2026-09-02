//! AnthropicProvider: the native-Anthropic half of an LLM call, as one
//! value. Owns the Anthropic connection pick (the service recipe lives
//! in this node's metadata) and the model choice, and emits both as
//! one `LlmProvider` object any inference node consumes. The call
//! itself speaks Anthropic's own `/v1/messages` wire (minillmlib's
//! Anthropic dialect), so model names are Anthropic's, not
//! OpenRouter's.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct AnthropicProviderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AnthropicProviderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        provider::emit(&ctx, "anthropic").await
    }
}
