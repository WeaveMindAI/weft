//! OpenRouterDecisionModel: the OpenRouter half of a decision call, as
//! one value. Names a decision model (and OpenRouter's routing block)
//! and emits it with the connection wired in from an OpenRouterAccess
//! node as one `DecisionProvider` object the decision nodes consume.
//! The consuming node opens the connection inside its own firing.

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::decision;

#[derive(NodeManifest)]
pub struct OpenRouterDecisionModelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for OpenRouterDecisionModelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // The typed read fails HERE on a malformed marker, at the node
        // that names the model, not later inside a decision node.
        let account: Access = ctx.inputs.get("access")?;
        let mut provider = Map::new();
        provider.insert("kind".into(), json!("openrouter"));
        provider.insert("model".into(), json!(ctx.inputs.get::<String>("model")?));
        provider.insert("account".into(), account.to_value());
        // OpenRouter's routing block, built here and sent as it is, so a
        // routing setting is written in this one place.
        let mut routing = Map::new();
        if let Some(pin) = ctx.inputs.opt::<String>("servingProvider")? {
            routing.insert("order".into(), json!([pin]));
        }
        if let Some(fallbacks) = ctx.inputs.opt::<bool>("providerFallbacks")? {
            routing.insert("allow_fallbacks".into(), json!(fallbacks));
        }
        if !routing.is_empty() {
            provider.insert(decision::ROUTING.into(), Value::Object(routing));
        }
        ctx.pulse_downstream(NodeOutput::new().set("provider", Value::Object(provider))).await
    }
}
