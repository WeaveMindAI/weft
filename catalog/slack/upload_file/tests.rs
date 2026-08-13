//! SlackUploadFile self-tests: the three-step external-upload dance.

use serde_json::json;

use weft::{fixture_spec, FakeRig, LiveRig, NodeTest, WeftResult};

use crate::slack_download_file::SlackDownloadFileNode;

use super::SlackUploadFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("mints_uploads_and_completes_the_share", uploads),
        NodeTest::live("one_real_upload_read_back", "slack", live_upload).with_fixture(
            fixture_spec(
                "SLACK_CHANNEL_ID",
                "Channel id",
                "The channel the test posts in; invite the connected bot to it first.",
            ),
        ),
    ]
}

/// Upload a tiny text file into the fixture channel, read it back
/// through SlackDownloadFile and compare the byte count, then delete
/// the file through the test's own connection so repeated runs never
/// pile shared files into the channel.
async fn live_upload(rig: LiveRig) -> WeftResult<()> {
    let channel = rig.fixture("SLACK_CHANNEL_ID")?;
    let content = b"weft node test: upload payload".to_vec();
    let file = rig.store_file("weft-node-tests.txt", "text/plain", content.clone()).await?;
    let uploaded = rig
        .run(
            &SlackUploadFileNode,
            json!({
                "account": rig.access("slack"),
                "file": file,
                "channel": channel,
                "comment": "weft node test: upload",
            }),
        )
        .await
        .ok()?;
    let file_id = uploaded.output("fileId")?.as_str().expect("file id").to_string();
    let permalink = uploaded.output("permalink")?.as_str().expect("permalink").to_string();
    // The download + its assert sit between the upload and the delete,
    // so they run inside a cleanup guard: the file is deleted however
    // this body exits (a `?` on the download, or a failed assert).
    let conn = rig.connect().await?;
    weft::with_cleanup(
        || async {
            let downloaded = rig
                .run(
                    &SlackDownloadFileNode,
                    json!({ "account": rig.access("slack"), "fileId": file_id.clone() }),
                )
                .await
                .ok()?;
            let size = downloaded.output("sizeBytes")?.as_f64().expect("size");
            assert!(permalink.starts_with("https://"));
            assert_eq!(size, content.len() as f64, "the bytes round-tripped whole");
            Ok(())
        },
        || async {
            crate::api::call_on(conn.client(), "files.delete", json!({ "file": file_id.clone() }))
                .await
                .map(|_| ())
        },
    )
    .await
}

async fn uploads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/api/files.getUploadURLExternal?filename=report.pdf&length=4",
        json!({ "ok": true, "upload_url": "https://files.slack.example/up/abc", "file_id": "F1" }),
    );
    rig.respond("POST", "/up/abc", json!({}));
    rig.respond(
        "POST",
        "/api/files.completeUploadExternal",
        json!({ "ok": true, "files": [{ "id": "F1", "permalink": "https://slack.example/F1" }] }),
    );
    let file = rig.store_file("report.pdf", "application/pdf", b"%PDF".to_vec());
    let outcome = rig
        .run(
            &SlackUploadFileNode,
            json!({
                "account": rig.access("slack"),
                "file": file,
                "channel": "C1",
                "comment": "here you go",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileId"], json!("F1"));
    assert_eq!(outcome.outputs["permalink"], json!("https://slack.example/F1"));

    let sent = rig.requests();
    assert_eq!(sent.len(), 3, "mint, bytes, complete");
    assert_eq!(sent[1].path, "/up/abc", "the bytes go to the pre-signed URL");
    let complete = sent[2].body.as_ref().expect("complete body");
    assert_eq!(complete["channel_id"], json!("C1"));
    assert_eq!(complete["initial_comment"], json!("here you go"));
    Ok(())
}
