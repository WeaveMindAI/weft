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
/// connection (required unless `connection_required` is false: a
/// custom endpoint may be unauthenticated) + every OTHER declared
/// input that holds a value, forwarded verbatim under its own name.
/// Reading the declared inputs off the bag keeps the metadata the
/// single source of truth: a service-specific knob added there rides
/// the object with no code change, and none can silently desync.
pub async fn emit(ctx: &ExecutionContext, kind: &str, connection_required: bool) -> WeftResult<()> {
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
    match ctx.inputs.opt::<Access>("connection")? {
        Some(marker) => {
            provider.insert("account".into(), marker.to_value());
        }
        None if !connection_required => {}
        None => weft::node_bail!("no connection picked; pick one on the node"),
    }
    ctx.pulse_downstream(NodeOutput::new().set("provider", Value::Object(provider))).await
}
