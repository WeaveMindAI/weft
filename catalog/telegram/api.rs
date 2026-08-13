//! Shared Telegram Bot API plumbing for every node in this package.
//!
//! One helper, one contract: `call` POSTs a method with a JSON body on
//! the connection's authenticated client (whose PathPrefix step aims
//! the generic URL at `/bot<token>/<method>`), checks Telegram's `ok`
//! envelope, and returns the parsed body; `send_raw` does the same for
//! a pre-framed body (the multipart media upload). Both fail loudly
//! with Telegram's own `description` so the user sees the API's exact
//! refusal, not a generic failure.

use serde_json::Value;

use weft::{Access, ExecutionContext, NodeErrExt, WeftResult};

/// POST a Bot API method with a JSON payload.
pub async fn call(
    ctx: &ExecutionContext,
    access: &Access,
    method: &str,
    payload: Value,
) -> WeftResult<Value> {
    let client = ctx.client(access).await?;
    let resp = client
        .post(format!("https://api.telegram.org/{method}"))
        .json(&payload)
        .send()
        .await
        .node_err(format!("telegram: call {method}"))?;
    check(method, resp.json().await.node_err(format!("telegram: read {method} response"))?)
}

/// POST a Bot API method with a pre-framed raw body (multipart media).
pub async fn send_raw(
    ctx: &ExecutionContext,
    access: &Access,
    method: &str,
    content_type: &str,
    body: Vec<u8>,
) -> WeftResult<Value> {
    let client = ctx.client(access).await?;
    let resp = client
        .post(format!("https://api.telegram.org/{method}"))
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .node_err(format!("telegram: call {method}"))?;
    check(method, resp.json().await.node_err(format!("telegram: read {method} response"))?)
}

/// Telegram's `ok` envelope check: every Bot API response says
/// `ok: true` or carries a `description` naming what was refused.
fn check(method: &str, answer: Value) -> WeftResult<Value> {
    if answer.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(answer);
    }
    Err(weft::node_error(format!(
        "telegram refused {method}: {}",
        answer.get("description").and_then(Value::as_str).unwrap_or("no detail")
    )))
}

/// A REQUIRED i64 field of a checked response's `result`.
pub fn result_message_id(answer: &Value, method: &str) -> WeftResult<i64> {
    answer
        .pointer("/result/message_id")
        .and_then(Value::as_i64)
        .node_err(format!("telegram: {method} response carries no result.message_id"))
}
