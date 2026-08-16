//! Shared Airtable plumbing: the API base and the base/table id
//! checks every node repeats (both ids ride URLs the node builds).

use weft::WeftResult;

pub const API: &str = "https://api.airtable.com/v0";

/// Refuse an id that is not a plain Airtable id (`app...`, `tbl...`,
/// `rec...`, or a table NAME without URL-hostile characters), so
/// nothing can traverse out of the API path the node builds.
pub fn checked_id<'a>(id: &'a str, what: &str) -> WeftResult<&'a str> {
    let clean = !id.is_empty()
        && !id.contains(['/', '?', '#', '\\'])
        && !id.starts_with('.');
    if !clean {
        weft::node_bail!("'{id}' is not a usable Airtable {what}");
    }
    Ok(id)
}
