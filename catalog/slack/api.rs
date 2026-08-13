//! Shared Slack Web API plumbing for every node in this package.
//!
//! One helper, one contract: `call` POSTs a method with a JSON body on
//! the connection's authenticated client, checks Slack's `ok` envelope,
//! and returns the parsed body. Slack's Web API accepts JSON POST for
//! every write method and GET with query params for reads; `get` covers
//! the read shape. Both fail loudly with Slack's own `error` token so
//! the user sees `channel_not_found`, not a generic failure. The
//! transport (send, refuse a non-success status, parse) is the core
//! `json_call`; only the `ok` envelope is Slack's own.

use serde_json::Value;

use weft::access::client::json_call;
use weft::{node_error, Access, ExecutionContext, WeftResult};

/// POST a Web API method (`chat.postMessage`, ...) with a JSON payload.
pub async fn call(
    ctx: &ExecutionContext,
    access: &Access,
    method: &str,
    payload: Value,
) -> WeftResult<Value> {
    call_on(&ctx.client(access).await?, method, payload).await
}

/// [`call`] on an already-opened authenticated client, for callers
/// that hold the connection rather than a ctx (test rigs cleaning up
/// through the opened connection).
pub async fn call_on(
    client: &weft::reqwest_middleware::ClientWithMiddleware,
    method: &str,
    payload: Value,
) -> WeftResult<Value> {
    let req = client.post(format!("https://slack.com/api/{method}")).json(&payload);
    check(method, json_call(req, &format!("call {method}")).await?)
}

/// GET a Web API method with query parameters (the read methods).
pub async fn get(
    ctx: &ExecutionContext,
    access: &Access,
    method: &str,
    query: &[(&str, String)],
) -> WeftResult<Value> {
    let client = ctx.client(access).await?;
    let req = client.get(format!("https://slack.com/api/{method}")).query(query);
    check(method, json_call(req, &format!("call {method}")).await?)
}

/// Page a cursor-paged read method (`conversations.list`,
/// `conversations.replies`): GET with `query` plus `limit=200`, follow
/// `response_metadata.next_cursor`, and hand each page's `items` array
/// to `visit`. `visit` returns `Some(t)` to stop early with that
/// answer; a listing that ends without one returns `Ok(None)`. The
/// page cap bounds a degenerate listing loudly instead of consuming
/// the whole rate budget; `over_cap` words that error for the caller's
/// domain ("the thread exceeds ...").
pub async fn paged<T>(
    ctx: &ExecutionContext,
    access: &Access,
    method: &str,
    query: &[(&str, String)],
    items: &str,
    mut visit: impl FnMut(&[Value]) -> WeftResult<Option<T>>,
    over_cap: impl Fn(usize) -> String,
) -> WeftResult<Option<T>> {
    const MAX_PAGES: usize = 50;
    const PAGE_LIMIT: usize = 200;
    let mut cursor: Option<String> = None;
    for _page in 0..MAX_PAGES {
        let mut query: Vec<(&str, String)> = query.to_vec();
        query.push(("limit", PAGE_LIMIT.to_string()));
        if let Some(c) = &cursor {
            query.push(("cursor", c.clone()));
        }
        let answer = get(ctx, access, method, &query).await?;
        let page = answer[items]
            .as_array()
            .ok_or_else(|| node_error(format!("slack: {method} returned no {items} array")))?;
        if let Some(found) = visit(page)? {
            return Ok(Some(found));
        }
        cursor = answer
            .pointer("/response_metadata/next_cursor")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
            .map(str::to_string);
        if cursor.is_none() {
            return Ok(None);
        }
    }
    Err(node_error(over_cap(MAX_PAGES * PAGE_LIMIT)))
}

/// Slack's `ok` envelope check: every Web API response says `ok: true`
/// or carries an `error` token naming exactly what was refused.
fn check(method: &str, answer: Value) -> WeftResult<Value> {
    if answer.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(answer);
    }
    Err(node_error(format!(
        "slack refused {method}: {}",
        answer.get("error").and_then(Value::as_str).unwrap_or("no detail")
    )))
}

/// A REQUIRED string field of a checked response: absent or non-string
/// fails the node loudly naming the method and field.
pub fn required_str<'a>(answer: &'a Value, method: &str, name: &str) -> WeftResult<&'a str> {
    weft::access::client::required_str(answer, method, name)
}

/// The `text`/`blocks` message-content pair, applied onto a payload:
/// Slack takes text, blocks, or both (text doubles as the notification
/// fallback when blocks are present), and NEITHER is a loud error.
/// One Slack rule, stated once for the post and update nodes; `doing`
/// words the refusal ("send", "update with").
pub fn set_content(
    payload: &mut Value,
    text: Option<String>,
    blocks: Option<Value>,
    doing: &str,
) -> WeftResult<()> {
    if text.is_none() && blocks.is_none() {
        return Err(node_error(format!("nothing to {doing}: provide text, blocks, or both")));
    }
    if let Some(t) = text {
        payload["text"] = Value::String(t);
    }
    if let Some(b) = blocks {
        payload["blocks"] = b;
    }
    Ok(())
}
