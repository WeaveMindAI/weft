//! Shared live-test plumbing for the notion package: the fixture
//! names every live test targets and the page archival that cleans
//! up what a test created. Notion's API bills nothing; the tests
//! archive their own pages so the workspace stays clean across runs.
#![cfg(feature = "node-tests")]

use weft::access::OpenedConnection;
use weft::{NodeErrExt, WeftResult};

use super::notion::API;

/// A page the signed-in connection can write under (test pages are
/// created as its children, then archived).
pub const PARENT_PAGE_FIXTURE: &str = "NOTION_PARENT_PAGE";
/// A database the connection can write, whose title property is
/// named `Name` (test rows filter and update on it).
pub const DATABASE_FIXTURE: &str = "NOTION_DATABASE";

pub const PARENT_PAGE_LABEL: (&str, &str) = (
    "Test parent page id",
    "A Notion page the connection can write; test pages are created under it and archived.",
);
pub const DATABASE_LABEL: (&str, &str) = (
    "Test database id",
    "A Notion database the connection can write, with its title property named 'Name'.",
);

/// Archive one page/row the test created (Notion's delete). A failed
/// archive is a loud test failure: leftovers would skew later runs.
pub async fn archive_page(conn: &OpenedConnection, page_id: &str) -> WeftResult<()> {
    let resp = conn
        .client()
        .patch(format!("{API}/pages/{page_id}"))
        .json(&serde_json::json!({ "archived": true }))
        .send()
        .await
        .node_err("notion: archive the test page")?;
    if !resp.status().is_success() {
        weft::node_bail!(
            "notion refused to archive test page {page_id} ({}); archive it by hand",
            resp.status()
        );
    }
    Ok(())
}

/// A title no earlier run could have left behind, so a filter matches
/// exactly what this run created.
pub fn unique_title(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_nanos();
    format!("{prefix} {nanos}")
}
