//! Shared Telegram Bot API plumbing for every node in this package.
//!
//! One helper, one contract: `call` POSTs a method on the connection's
//! authenticated client (opened once per run by the node and passed
//! in; its PathPrefix step aims the generic URL at
//! `/bot<token>/<method>`), with the body the caller's `prepare` puts
//! on the request (a JSON payload, or the pre-framed multipart media
//! upload). The transport (send, refuse a non-success status, parse) is
//! the core `json_call`, and Telegram's `ok` envelope is checked through
//! the core `require_ok_flag`, so a refusal fails loudly with
//! Telegram's own `description`, the API's exact words.

use serde_json::Value;

use weft::access::client::{json_call, require_ok_flag};
use weft::reqwest_middleware::{ClientWithMiddleware, RequestBuilder};
use weft::{NodeErrExt, WeftResult};

/// POST a Bot API method, its body set by `prepare`
/// (`|req| req.json(&payload)`, or a raw multipart body with its
/// content type).
pub async fn call(
    client: &ClientWithMiddleware,
    method: &str,
    prepare: impl FnOnce(RequestBuilder) -> RequestBuilder,
) -> WeftResult<Value> {
    let req = prepare(client.post(format!("https://api.telegram.org/{method}")));
    let what = format!("call telegram's {method}");
    require_ok_flag(json_call(req, &what).await?, "ok", "description", &what)
}

/// A REQUIRED i64 field of a checked response's `result`.
pub fn result_message_id(answer: &Value, method: &str) -> WeftResult<i64> {
    answer
        .pointer("/result/message_id")
        .and_then(Value::as_i64)
        .node_err(format!("telegram: {method} response carries no result.message_id"))
}
