//! Shared plumbing for talking to the project's WhatsApp bridge.
//!
//! `endpointUrl` (the WhatsAppBridge node's output, the bridge's
//! `ctx.endpoint("api")` URL) is BARE service DNS with no path; that
//! contract lives here once. `route` appends a route to it, and
//! `action` speaks the bridge's `/action` door with its envelope: a
//! non-2xx is a thrown bridge error (json_call quotes it), and a 200
//! whose `result.error` is set is a SOFT refusal (e.g. "WhatsApp not
//! connected" while the phone is unpaired) surfaced just as loudly.

use serde_json::{json, Value};

use weft::access::client::json_call;
use weft::{ExecutionContext, WeftResult};

/// `endpointUrl` + a route (`/events`, `/media/<id>`, ...).
pub fn route(endpoint_url: &str, path: &str) -> String {
    format!("{}{path}", endpoint_url.trim_end_matches('/'))
}

/// POST one bridge action and hand back its `result` object.
pub async fn action(
    ctx: &ExecutionContext,
    endpoint_url: &str,
    action: &str,
    payload: Value,
) -> WeftResult<Value> {
    let url = route(endpoint_url, "/action");
    let answer = json_call(
        ctx.http().post(&url).json(&json!({ "action": action, "payload": payload })),
        &format!("run the bridge action {action}"),
    )
    .await?;
    let result = answer["result"].clone();
    if let Some(err) = result["error"].as_str() {
        weft::node_bail!("bridge: {err}");
    }
    Ok(result)
}
