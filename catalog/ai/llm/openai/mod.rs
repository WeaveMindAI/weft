//! OpenAIProvider: the direct-OpenAI half of an LLM call, as one
//! value. Owns the OpenAI connection pick (the service recipe lives in
//! this node's metadata) and the model choice, and emits both as one
//! `LlmProvider` object any inference node consumes.

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct OpenAIProviderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for OpenAIProviderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        provider::emit(&ctx, "openai").await
    }
}
