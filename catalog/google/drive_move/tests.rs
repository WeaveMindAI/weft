//! GoogleDriveMove self-tests: current parents are read then replaced
//! in one PATCH.

use serde_json::json;

use weft::access::client::{get_json, post_json};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleDriveMoveNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("replaces_the_current_parents", moves),
        NodeTest::live("one_real_move_then_both_deleted", "google", live_move),
    ]
}

/// Create a scratch folder and file through the test's own
/// connection, move the file into the folder via the node, verify the
/// parent changed, then delete both so repeated runs never rearrange
/// or pile onto the account's Drive.
async fn live_move(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let scope = crate::testing::DriveScope::new();
    weft::with_cleanup(
        || async {
            let folder = post_json(
                conn.client(),
                "https://www.googleapis.com/drive/v3/files",
                &json!({
                    "name": "weft-node-tests-move-dest",
                    "mimeType": "application/vnd.google-apps.folder",
                }),
                "create the move's destination folder",
            )
            .await?;
            let folder_id = scope.track_created(&folder, "the move's folder create")?;
            let file = post_json(
                conn.client(),
                "https://www.googleapis.com/drive/v3/files",
                &json!({ "name": "weft-node-tests-move-me" }),
                "create the file to move",
            )
            .await?;
            let file_id = scope.track_created(&file, "the move's file create")?;

            let outcome = rig
                .run(
                    &GoogleDriveMoveNode,
                    json!({
                        "account": rig.access("google"),
                        "fileId": file_id.clone(),
                        "folder": folder_id.clone(),
                    }),
                )
                .await
                .ok()?;
            assert_eq!(outcome.output("done")?, &json!(true));

            let meta = get_json(
                conn.client(),
                &format!("https://www.googleapis.com/drive/v3/files/{file_id}?fields=parents"),
                "read the moved file's parents",
            )
            .await?;
            assert_eq!(
                meta["parents"],
                json!([folder_id]),
                "the folder is the file's one parent after the move"
            );
            Ok(())
        },
        || scope.delete_all(&conn),
    )
    .await
}

async fn moves(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files/f1?fields=parents&supportsAllDrives=true",
        json!({ "parents": ["old1", "old2"] }),
    );
    rig.respond(
        "PATCH",
        "/drive/v3/files/f1?addParents=dest&fields=id&removeParents=old1,old2",
        json!({ "id": "f1" }),
    );
    let outcome = rig
        .run(
            &GoogleDriveMoveNode,
            json!({ "account": rig.access("google"), "fileId": "f1", "folder": "dest" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["done"], json!(true));
    assert_eq!(rig.requests().len(), 2, "read parents + patch");
    Ok(())
}
