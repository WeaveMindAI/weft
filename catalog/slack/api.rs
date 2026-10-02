//! Shared Slack Web API plumbing for every node in this package.
//!
//! `call` POSTs a method with a JSON body on the connection's
//! authenticated client and returns the parsed body; `get` covers the
//! read shape (GET with query params) and `paged` the cursor-paged
//! reads. Every one checks Slack's `ok` envelope through the core
//! `require_ok_flag`, so a refusal fails loudly with Slack's own
//! `error` token (`channel_not_found`). The transport (send, refuse a
//! non-success status, parse) is the core `json_call`.

use serde_json::Value;

use weft::access::client::{cursor_paged, json_call, require_ok_flag, CursorPaging};
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::WeftResult;

/// POST a Web API method (`chat.postMessage`, ...) with a JSON payload,
/// on the connection's authenticated client. A node opens that client
/// once per run (`ctx.client`) and passes it to every call it makes.
pub async fn call(client: &ClientWithMiddleware, method: &str, payload: Value) -> WeftResult<Value> {
    let req = client.post(format!("https://slack.com/api/{method}")).json(&payload);
    let what = format!("call {method}");
    require_ok_flag(json_call(req, &what).await?, "ok", "error", &what)
}

/// GET a Web API method with query parameters (the read methods).
pub async fn get(
    client: &ClientWithMiddleware,
    method: &str,
    query: &[(&str, String)],
) -> WeftResult<Value> {
    let req = client.get(format!("https://slack.com/api/{method}")).query(query);
    let what = format!("call {method}");
    require_ok_flag(json_call(req, &what).await?, "ok", "error", &what)
}

/// Page a cursor-paged read method (`conversations.list`,
/// `conversations.replies`) through the core `cursor_paged`, in Slack's
/// shape: GET with `query` plus `limit=200`, the `ok` envelope checked
/// on every page, the cursor at `response_metadata.next_cursor` sent
/// back as `cursor`. `items` points at the page's array (`/channels`);
/// `visit` returns `Some(t)` to stop early with that answer;
/// `past_cap_hint` is what the user is told to do instead when the
/// listing runs past the page cap.
pub async fn paged<T>(
    client: &ClientWithMiddleware,
    method: &str,
    query: &[(&str, String)],
    items: &str,
    past_cap_hint: &str,
    visit: impl FnMut(&[Value]) -> WeftResult<Option<T>>,
) -> WeftResult<Option<T>> {
    let what = format!("call {method}");
    let url = format!("https://slack.com/api/{method}");
    let paging = CursorPaging {
        items,
        next: "/response_metadata/next_cursor",
        param: "cursor",
        items_may_be_absent: false,
        past_cap_hint,
    };
    cursor_paged(
        paging,
        &what,
        || client.get(&url).query(query).query(&[("limit", "200")]),
        |answer| require_ok_flag(answer, "ok", "error", &what),
        visit,
    )
    .await
}

/// The `text`/`blocks` message-content pair, applied onto a payload:
/// Slack takes text, blocks, or both (text doubles as the notification
/// fallback when blocks are present). One Slack rule, stated once for
/// the post and update nodes; NEITHER never reaches here, because both
/// nodes declare the pair `oneOfRequired` and are skipped without it.
pub fn set_content(payload: &mut Value, text: Option<String>, blocks: Option<Value>) {
    if let Some(t) = text {
        payload["text"] = Value::String(t);
    }
    if let Some(b) = blocks {
        payload["blocks"] = b;
    }
}
