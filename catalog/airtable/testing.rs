//! Shared live-test plumbing for the airtable package: the test
//! base/table fixture pair every live test targets, and the record
//! cleanup that keeps the table empty across runs. Airtable's API
//! bills nothing, so the live tier costs only the records it creates,
//! and it deletes those itself.
#![cfg(feature = "node-tests")]

use weft::access::OpenedConnection;
use weft::{NodeErrExt, WeftResult};

use super::airtable::API;

/// The fixture names: a base the signed-in connection can reach and a
/// table in it with a plain text field named `Name`. Every airtable
/// live test declares both.
pub const BASE_FIXTURE: &str = "AIRTABLE_BASE";
pub const TABLE_FIXTURE: &str = "AIRTABLE_TABLE";

pub const BASE_LABEL: (&str, &str) =
    ("Test base id", "An Airtable base (app...) the connection can write.");
pub const TABLE_LABEL: (&str, &str) =
    ("Test table", "A table in the test base with a text field named 'Name'.");

/// Seed one record directly through the API (the tests that exercise
/// update/search need an existing row that is not minted by the node
/// under test). Answers the new record's id.
pub async fn seed_record(
    conn: &OpenedConnection,
    base: &str,
    table: &str,
    name: &str,
) -> WeftResult<String> {
    let created = weft::access::client::post_json(
        conn.client(),
        &format!("{API}/{base}/{table}"),
        &serde_json::json!({ "fields": { "Name": name } }),
        "airtable: seed the test record",
    )
    .await?;
    created["id"]
        .as_str()
        .map(str::to_string)
        .node_err("airtable answered no id for the seeded record")
}

/// A name no earlier run could have left behind, so a search matches
/// exactly the row this run seeded.
pub fn unique_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_nanos();
    format!("{prefix} {nanos}")
}

/// Delete one record the test created, so the table stays empty
/// across runs. A failed delete is a loud test failure: leftover rows
/// would silently skew the next search test.
pub async fn delete_record(
    conn: &OpenedConnection,
    base: &str,
    table: &str,
    record_id: &str,
) -> WeftResult<()> {
    let resp = conn
        .client()
        .delete(format!("{API}/{base}/{table}/{record_id}"))
        .send()
        .await
        .node_err("airtable: delete the test record")?;
    if !resp.status().is_success() {
        weft::node_bail!(
            "airtable refused to delete test record {record_id} ({}); remove it by hand",
            resp.status()
        );
    }
    Ok(())
}
