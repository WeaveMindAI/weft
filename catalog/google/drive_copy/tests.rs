//! GoogleDriveCopy self-tests: the copy call and its optional
//! rename/retarget body.

use serde_json::json;

use weft::access::client::post_json;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveCopyNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("copies_with_rename_and_folder", copies),
        NodeTest::live("one_real_copy_then_both_deleted", "google", live_copy),
    ]
}

/// Create a scratch source file through the test's own connection,
/// copy it via the node, then delete both files so repeated runs
/// never pile files onto the account.
async fn live_copy(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let scope = crate::testing::DriveScope::new();
    weft::with_cleanup(
        || async {
            let source = post_json(
                conn.client(),
                "https://www.googleapis.com/drive/v3/files",
                &json!({ "name": "weft-node-tests-copy-source" }),
                "create the copy's source file",
            )
            .await?;
            let source_id = scope.track_created(&source, "the copy's source create")?;

            let outcome = rig
                .run(
                    &GoogleDriveCopyNode,
                    json!({
                        "account": rig.access("google"),
                        "fileId": source_id.clone(),
                        "name": "weft-node-tests-copy",
                    }),
                )
                .await
                .ok()?;
            let copy_id = outcome.output("fileId")?.as_str().expect("copy id").to_string();
            scope.track(&copy_id);
            assert_ne!(copy_id, source_id, "the copy is a new file");
            Ok(())
        },
        || scope.delete_all(&conn),
    )
    .await
}

async fn copies(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/drive/v3/files/f1/copy?fields=id,webViewLink",
        json!({ "id": "f2", "webViewLink": "https://drive.google.com/f2" }),
    );
    let outcome = rig
        .run(
            &GoogleDriveCopyNode,
            json!({
                "account": rig.access("google"),
                "fileId": "f1",
                "name": "copy of report",
                "folder": "folder9",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileId"], json!("f2"));
    let body = rig.requests()[0].body.clone().expect("copy body");
    assert_eq!(body, json!({ "name": "copy of report", "parents": ["folder9"] }));
    Ok(())
}
