//! CustomProvider: any OpenAI-compatible endpoint as an LLM provider.
//! Owns the endpoint's base URL, the model name, and (optionally) a
//! pasted-key connection, and emits them as one `LlmProvider` object
//! any inference node consumes. The connection is optional on purpose:
//! a local or otherwise unauthenticated compatible server (an Ollama,
//! a vLLM behind your own wall) takes bare calls.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct CustomProviderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CustomProviderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        provider::emit(&ctx, "custom").await
    }
}
