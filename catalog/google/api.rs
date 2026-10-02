//! Shared plumbing for the whole google package, across its API
//! families: the `nextPageToken` paging convention every Google list
//! endpoint speaks (Drive, Calendar, Gmail), and path-segment encoding.

use weft::access::client::CursorPaging;

/// How every Google list endpoint pages: `items` points at the page's
/// array (`/files`, `/items`), the token sits top-level as
/// `nextPageToken` and goes back as `pageToken`, and a page with no
/// hits leaves the array out. `past_cap_hint` is what the node tells
/// the user to do when the listing runs past the page cap. Pass it to
/// `weft::access::client::cursor_paged`.
pub fn paging<'a>(items: &'a str, past_cap_hint: &'a str) -> CursorPaging<'a> {
    CursorPaging {
        items,
        next: "/nextPageToken",
        param: "pageToken",
        items_may_be_absent: true,
        past_cap_hint,
    }
}

/// One URL path segment from an id the caller supplied (a file, doc,
/// message, spreadsheet, label or calendar id), percent-encoded so a
/// `/`, `?`, `#` or `%` in it addresses that id instead of reshaping
/// the request path.
pub fn segment(raw: &str) -> String {
    urlencoding::encode(raw).into_owned()
}
