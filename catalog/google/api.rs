//! Shared plumbing for the whole google package, across its API
//! families: one page of a list endpoint, paged by the `nextPageToken`
//! Drive and Calendar speak, and the Calendar API's base.

use serde_json::Value;

use weft::access::client::{get_json, Url};
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::storage::over_wire;
use weft::WeftResult;

/// The Calendar API, which both calendar nodes address.
pub const CALENDAR_API: &str = "https://www.googleapis.com/calendar/v3";

/// One page of a Google listing, and Google's token for the next.
pub struct ListPage {
    pub items: Vec<Value>,
    /// Google's `nextPageToken`, `None` after the last page.
    pub next_page_token: Option<String>,
}

/// Read ONE page of a Google list endpoint (Drive, Calendar). `base` is
/// the list URL with its query, the page size included; `items` names
/// the answer's array (`files`, `items`); `shape` turns each item into
/// what the node emits. Paging is Google's own and nothing else:
/// `page_token` is the `nextPageToken` an earlier page handed out, and
/// a page is never read twice, since Google does not promise the same
/// token gives the same page again. A page heavier than a wire carries
/// fails, naming `size_setting`, the node's page-size setting that
/// makes pages lighter.
pub async fn list_page(
    http: &ClientWithMiddleware,
    base: Url,
    items: &str,
    page_token: Option<&str>,
    shape: impl Fn(&Value) -> Value,
    what: &str,
    size_setting: &str,
) -> WeftResult<ListPage> {
    let url = base.query_opt("pageToken", page_token);
    let answer = get_json(http, url.as_str(), what).await?;
    // A page with no hits leaves the array out.
    let items: Vec<Value> = answer[items].as_array().into_iter().flatten().map(shape).collect();
    if let Some(weight) = over_wire(&items) {
        weft::node_bail!("{what}: this page weighs {weight}; lower `{size_setting}` so a page holds fewer");
    }
    let next_page_token = answer["nextPageToken"].as_str().filter(|t| !t.is_empty()).map(str::to_string);
    Ok(ListPage { items, next_page_token })
}
