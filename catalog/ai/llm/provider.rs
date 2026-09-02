//! The shared body of every provider node.
//!
//! A provider node is the ONE place a service's connection is picked
//! (its metadata carries the service recipe, its `connection` config
//! input the picker) and the one place its model is named. Its output
//! is a single `LlmProvider` object the inference nodes interpret:
//! the service kind, the model, the picked connection's Access marker,
//! and any service-specific knobs. The body is the same for every
//! service, so it lives here once.

use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, WeftResult};

/// Emit the `LlmProvider` object: `kind` + `model` + the picked
/// connection + every OTHER declared input that holds a value,
/// forwarded verbatim under its own name. Reading the declared inputs
/// off the bag keeps the metadata the single source of truth: a
/// service-specific knob added there rides the object with no code
/// change, and none can silently desync. Whether the connection is
/// required comes from the same place: the recipe's
/// `connection_optional` (a custom endpoint may be unauthenticated),
/// read through the bag's access accessor.
pub async fn emit(ctx: &ExecutionContext, kind: &str) -> WeftResult<()> {
    let mut provider = Map::new();
    // Service-specific knobs first; the fields this function OWNS
    // (`kind`, `model`, `account`) are inserted after, so no declared
    // input can ever overwrite the object's identity.
    for (name, value) in ctx.inputs.declared() {
        if name == "connection" || name == "model" || value.is_null() {
            continue;
        }
        provider.insert(name.clone(), value.clone());
    }
    provider.insert("kind".into(), json!(kind));
    provider.insert("model".into(), json!(ctx.inputs.get::<String>("model")?));
    // Typed read: a malformed marker fails HERE, at the node that owns
    // the pick, not later inside the inference node that consumes it.
    // An unpicked-and-optional connection simply omits `account`.
    if let Some(marker) = ctx.inputs.access("connection")? {
        provider.insert("account".into(), marker.to_value());
    }
    ctx.pulse_downstream(NodeOutput::new().set("provider", Value::Object(provider))).await
}

/// The wired `LlmProvider` object decoded for a single-service
/// endpoint node (moderation, rerank, embeddings): refuses a provider
/// of any other kind, and hands back the model and the picked
/// connection. The one reader beside `call::assemble`, so the object
/// has exactly two decoding paths (the multi-provider inference one
/// and this single-service one), never a per-node copy. `what` names
/// the capability ("moderation", "reranking", ...).
pub fn read_for(
    ctx: &ExecutionContext,
    expected_kind: &str,
    what: &str,
) -> WeftResult<(String, Access)> {
    let provider = ctx.inputs.nested("provider")?;
    let kind: String = provider.get("kind")?;
    if kind != expected_kind {
        // Name the NODE the user has to wire, not the wire-level kind.
        let (api, node) = match expected_kind {
            "openai" => ("OpenAI", "OpenAIProvider"),
            "openrouter" => ("OpenRouter", "OpenRouterProvider"),
            "anthropic" => ("Anthropic", "AnthropicProvider"),
            other => (other, other),
        };
        weft::node_bail!("{what} speaks the {api} API; wire an {node} (got '{kind}')");
    }
    let model: String = provider.get("model")?;
    let account: Access = match provider.opt("account")? {
        Some(a) => a,
        None => weft::node_bail!(
            "the wired LlmProvider object carries no connection; pick one on the provider node"
        ),
    };
    Ok((model, account))
}
