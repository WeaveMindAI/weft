//! GoogleDriveUpload self-tests: the one-shot multipart upload.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveUploadNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("uploads_the_stored_bytes_with_their_metadata", uploads),
        NodeTest::live("one_real_upload_then_deleted", "google", live_upload),
    ]
}

/// Upload a tiny text file into the Drive root, then take it back out
/// through the test's own connection so repeated runs never pile
/// files onto the account.
async fn live_upload(rig: LiveRig) -> WeftResult<()> {
    let file = rig
        .store_file("weft-node-tests.txt", "text/plain", b"weft node test: upload".to_vec())
        .await?;
    let outcome = rig
        .run(
            &GoogleDriveUploadNode,
            json!({ "account": rig.access("google"), "file": file }),
        )
        .await
        .ok()?;
    // No assert between the node's create and the delete: the id IS
    // the delete key, so an empty one makes drive_delete fail loudly on
    // its own. A guard here would only risk leaking the file it names.
    let id = outcome.output("fileId")?.as_str().expect("file id").to_string();
    let conn = rig.connect().await?;
    crate::testing::drive_delete(&conn, &id).await
}

async fn uploads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink",
        json!({ "id": "f1", "webViewLink": "https://drive.google.com/f1" }),
    );
    let file = rig.store_file("notes.txt", "text/plain", b"hello".to_vec());
    let outcome = rig
        .run(
            &GoogleDriveUploadNode,
            json!({ "account": rig.access("google"), "file": file, "folder": "dest" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileId"], json!("f1"));
    let sent = &rig.requests()[0];
    let body = sent.body_text.as_ref().expect("multipart body is buffered text");
    assert!(body.contains(r#""parents":["dest"]"#), "folder rides the metadata part: {body}");
    assert!(body.contains("hello"), "the stored bytes ride the media part");
    Ok(())
}
