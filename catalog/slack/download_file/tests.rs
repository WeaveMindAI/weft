//! SlackDownloadFile self-tests: info -> authenticated fetch ->
//! storage.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::SlackDownloadFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("downloads_the_named_file_into_storage", downloads)]
}

async fn downloads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/api/files.info?file=F1",
        json!({ "ok": true, "file": {
            "name": "notes.txt",
            "mimetype": "text/plain",
            "url_private": "https://files.slack.example/private/notes.txt",
        }}),
    );
    rig.respond_raw("GET", "/private/notes.txt", 200, "text/plain", b"hello".to_vec());
    let outcome = rig
        .run(
            &SlackDownloadFileNode,
            json!({ "account": rig.access("slack"), "fileId": "F1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("notes.txt"));
    assert_eq!(outcome.outputs["mimeType"], json!("text/plain"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(5));
    Ok(())
}
