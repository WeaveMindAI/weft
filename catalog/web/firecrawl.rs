//! Shared Firecrawl plumbing: one POST helper owning the API base and
//! Firecrawl's `success` envelope (a 200 can still say
//! `success: false` with an `error` string), so every Firecrawl node
//! states only its body and its field extraction.

use serde_json::Value;

use weft::access::client::post_json;
use weft::WeftResult;

pub const API: &str = "https://api.firecrawl.dev/v2";

/// POST `body` to `{API}/{path}`, refuse a failed status or a
/// `success: false` envelope loudly with Firecrawl's own words, and
/// hand back the parsed answer.
pub async fn call(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    path: &str,
    body: &Value,
    what: &str,
) -> WeftResult<Value> {
    let answer = post_json(http, &format!("{API}/{path}"), body, what).await?;
    if answer["success"].as_bool() != Some(true) {
        weft::node_bail!(
            "{what}: firecrawl refused: {}",
            answer["error"].as_str().unwrap_or("no detail")
        );
    }
    Ok(answer)
}
