//! GoogleDriveListFiles self-tests: the one list GET.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveListFilesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("lists_with_the_default_page_size", lists),
        NodeTest::fake("a_page_heavier_than_a_wire_names_page_size", heavy_listing),
        NodeTest::fake("a_whole_drive_page_moves_on_to_the_next_token", next_drive_page),
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
    assert_eq!(outcome.outputs["hasMore"], json!(false));
    Ok(())
}

const PAGE: &str = "/drive/v3/files?pageSize=3&fields=nextPageToken,files(id,name,mimeType)";

/// Three files of 40 KB weigh more than a wire: the page fails, naming
/// the setting that makes pages lighter.
async fn heavy_listing(rig: FakeRig) -> WeftResult<()> {
    let files: Vec<_> = (0..3)
        .map(|i| json!({ "id": format!("f{i}"), "name": "n".repeat(40 * 1024), "mimeType": "text/plain" }))
        .collect();
    rig.respond("GET", PAGE, json!({ "files": files }));
    let err = rig
        .run(&GoogleDriveListFilesNode, json!({ "account": rig.access("google"), "pageSize": 3 }))
        .await
        .failure()?;
    assert!(err.contains("at most 100 KB") && err.contains("lower `pageSize`"), "{err}");
    Ok(())
}

/// Each call reads one Drive page, and the next one is read with the
/// token Drive handed out for it.
async fn next_drive_page(rig: FakeRig) -> WeftResult<()> {
    rig.respond("GET", PAGE, json!({ "files": [{ "id": "a" }], "nextPageToken": "t2" }));
    rig.respond("GET", &format!("{PAGE}&pageToken=t2"), json!({ "files": [{ "id": "b" }] }));
    let list = |token: &str| {
        rig.run(
            &GoogleDriveListFilesNode,
            json!({ "account": rig.access("google"), "pageSize": 3, "pageToken": token }),
        )
    };
    let first = list("").await.ok()?;
    assert_eq!(first.outputs["files"], json!([{ "id": "a" }]));
    assert_eq!(first.outputs["nextPageToken"], json!("t2"));
    assert_eq!(first.outputs["hasMore"], json!(true), "Drive said there is more");
    let second = list("t2").await.ok()?;
    assert_eq!(second.outputs["files"], json!([{ "id": "b" }]));
    assert_eq!(second.outputs["nextPageToken"], json!(""));
    assert_eq!(second.outputs["hasMore"], json!(false));
    assert_eq!(rig.requests().len(), 2, "no page is read twice");
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
