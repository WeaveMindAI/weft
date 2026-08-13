//! Shared plumbing for the whole google package, across its API
//! families: the `nextPageToken` paging convention every Google list
//! endpoint speaks (Drive, Calendar, Gmail).

use serde_json::Value;

use weft::access::client::get_json;
use weft::WeftResult;

/// Page a Google list endpoint to exhaustion: GET `base` (which
/// already carries its per-page size and every filter), collect the
/// `items` array of each page, follow `nextPageToken` until the
/// listing ends, `stop` says enough (it sees the collected list after
/// each page), or the page cap trips loudly. The cap bounds a
/// degenerate query or a token loop; past it the node fails instead of
/// growing without bound.
pub async fn paged(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    base: &str,
    items: &str,
    what: &str,
    stop: impl Fn(&[Value]) -> bool,
) -> WeftResult<Vec<Value>> {
    const MAX_PAGES: usize = 50;
    let mut collected: Vec<Value> = Vec::new();
    let mut page_token: Option<String> = None;
    for _page in 0..MAX_PAGES {
        let url = match &page_token {
            Some(t) => format!("{base}&pageToken={}", urlencoding::encode(t)),
            None => base.to_string(),
        };
        let answer: Value = get_json(http, &url, what).await?;
        collected.extend(answer[items].as_array().into_iter().flatten().cloned());
        page_token = answer["nextPageToken"].as_str().map(str::to_string);
        if page_token.is_none() || stop(&collected) {
            return Ok(collected);
        }
    }
    weft::node_bail!(
        "{what}: more than {MAX_PAGES} pages of results; narrow the query"
    )
}
