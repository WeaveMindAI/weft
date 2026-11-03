//! The shared body of every provider node, and the one reading of what
//! it emits.
//!
//! A provider node (a service's Model node, or CustomProvider) is the
//! one place a model is named. Its output is a single `LlmProvider`
//! record the inference nodes interpret: the service kind, the model,
//! the connection's Access marker, and any service-specific knobs. The
//! connection comes from where the node keeps it: a Model node takes
//! it on its `access` wire from the service's Access node (the one
//! place that connection is picked, so many models share one pick),
//! CustomProvider picks its own (an endpoint whose key belongs to it
//! alone, and which may need none). The body is the same for every
//! service, so it lives here once.

use serde::Deserialize;
use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, WeftResult};

/// OpenRouter's API base, for the single-service nodes (embeddings,
/// rerank) that call it directly.
pub const OPENROUTER_API: &str = "https://openrouter.ai/api/v1";

/// The input a Model node takes its service's connection on, wired
/// from that service's Access node.
pub const ACCESS_INPUT: &str = "access";

/// The connection picker of a provider node that keeps its own pick
/// (CustomProvider).
pub const CONNECTION_INPUT: &str = "connection";

/// A Model node's body: its `LlmProvider` record, with the connection
/// wired in on [`ACCESS_INPUT`]. The typed read fails HERE on a
/// malformed marker, at the node that names the model, not later
/// inside the inference node that consumes it.
pub async fn emit_model(ctx: &RunContext, kind: &str) -> WeftResult<()> {
    let account: Access = ctx.inputs.get(ACCESS_INPUT)?;
    emit(ctx, kind, Some(account)).await
}

/// Emit the `LlmProvider` record: `kind` + `model` + the connection
/// (`account`, omitted when there is none) + every OTHER declared
/// input that holds a value, forwarded under its own name. The record
/// is closed, so a knob a Model node declares and `LlmProvider` does
/// not is refused when the output is held to its type (the node's own
/// tests set every knob, so they catch it): a new knob is one line in
/// the type, never one riding along unchecked.
pub async fn emit(ctx: &RunContext, kind: &str, account: Option<Access>) -> WeftResult<()> {
    let mut provider = Map::new();
    // Service-specific knobs first; the fields this function OWNS
    // (`kind`, `model`, `account`) are inserted after, so no declared
    // input can ever overwrite the record's identity.
    for (name, value) in ctx.inputs.declared() {
        if name == ACCESS_INPUT || name == CONNECTION_INPUT || name == "model" || value.is_null() {
            continue;
        }
        provider.insert(name.clone(), value.clone());
    }
    provider.insert("kind".into(), json!(kind));
    provider.insert("model".into(), json!(ctx.inputs.get::<String>("model")?));
    if let Some(account) = account {
        provider.insert("account".into(), account.to_value());
    }
    ctx.pulse_downstream(NodeOutput::new().set("provider", Value::Object(provider))).await
}

/// The wired `LlmProvider` record, read by its `kind`: each service's
/// fields, so the connection a named service always carries is a field
/// of its variant, and only a custom endpoint's may be missing.
// SYNC: Provider <-> types.LlmProvider (catalog/ai/llm/metadata.json)
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Provider {
    Openrouter {
        model: String,
        account: Access,
        #[serde(rename = "servingProvider")]
        serving_provider: Option<String>,
        #[serde(rename = "providerFallbacks")]
        provider_fallbacks: Option<bool>,
    },
    Anthropic {
        model: String,
        account: Access,
    },
    Openai {
        model: String,
        account: Access,
    },
    Custom {
        model: String,
        account: Option<Access>,
        #[serde(rename = "baseUrl")]
        base_url: String,
        name: Option<String>,
    },
}

/// The `provider` input of an inference node.
pub fn read(ctx: &RunContext) -> WeftResult<Provider> {
    ctx.inputs.get("provider")
}

/// The model and connection of the wired provider, for a node that
/// speaks the OpenRouter API itself (embeddings, rerank): any other
/// service is refused, naming the node to wire. `what` names the
/// capability ("embeddings", "reranking").
pub fn openrouter(ctx: &RunContext, what: &str) -> WeftResult<(String, Access)> {
    match read(ctx)? {
        Provider::Openrouter { model, account, .. } => Ok((model, account)),
        _ => weft::input_bail!("{what} speaks the OpenRouter API; wire an OpenRouterModel"),
    }
}
