//! GoogleDriveListFiles self-tests: the one list GET.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveListFilesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_with_the_default_page_size", lists),
        NodeTest::live("one_real_listing", "google", live_list),
    ]
}

async fn lists(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files?pageSize=25&fields=nextPageToken,files(id,name,mimeType)&q=name contains 'report'",
        json!({ "files": [{ "id": "f1", "name": "report.pdf", "mimeType": "application/pdf" }] }),
    );
    let outcome = rig
        .run(
            &GoogleDriveListFilesNode,
            json!({ "account": rig.access("google"), "query": "name contains 'report'" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["files"][0]["name"], json!("report.pdf"));
    Ok(())
}

async fn live_list(rig: LiveRig) -> WeftResult<()> {
    // Read-only: every listed file carries the projected fields.
    let outcome = rig
        .run(
            &GoogleDriveListFilesNode,
            json!({ "account": rig.access("google"), "pageSize": 5 }),
        )
        .await
        .ok()?;
    let files = outcome.output("files")?.as_array().expect("files list").clone();
    for file in &files {
        assert!(file["id"].is_string(), "every file carries its id: {file}");
        assert!(file["name"].is_string(), "every file carries its name: {file}");
    }
    Ok(())
}
