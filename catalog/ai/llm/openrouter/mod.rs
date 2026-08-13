//! OpenRouterProvider: the OpenRouter half of an LLM call, as one
//! value. Owns the project's OpenRouter connection pick (the service
//! recipe lives in this node's metadata; the `connection` input's
//! stored handle becomes the full `Access` marker when the bag is
//! built) and the model choice, and emits both as one `LlmProvider`
//! object any inference node consumes. Consuming nodes open the
//! connection at call time, inside their own firing (which is what
//! keeps a runtime-supplied credential's per-firing lifecycle intact).

use async_trait::async_trait;

use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct OpenRouterProviderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for OpenRouterProviderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        provider::emit(&ctx, "openrouter", true).await
    }
}
