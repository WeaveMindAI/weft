//! GoogleDriveWatch self-tests: the registered event subscription
//! (with its sync-suppression filter) and a fire's follow-up read.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::GoogleDriveWatchNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_subscribes_and_suppresses_the_sync_ping", setup_registers),
        NodeTest::fake("a_fire_reads_the_changed_file", fire_reads),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(
        &GoogleDriveWatchNode,
        json!({ "account": rig.access("google"), "target": "f1", "changes": "content" }),
    )
    .await
    .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1);
    let spec = serde_json::to_value(&registered[0].0).expect("spec serializes").to_string();
    assert!(spec.contains("drive_changes"), "{spec}");
    assert!(spec.contains("sync"), "the channel-live ping is filtered out: {spec}");
    assert!(spec.contains("content"), "the changes narrowing rides as a filter: {spec}");
    Ok(())
}

async fn fire_reads(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "GET",
        "/drive/v3/files/f1?fields=id,name,mimeType,modifiedTime,lastModifyingUser(displayName)&supportsAllDrives=true",
        json!({
            "id": "f1",
            "name": "notes.txt",
            "mimeType": "text/plain",
            "modifiedTime": "2026-08-09T10:00:00Z",
            "lastModifyingUser": { "displayName": "Ada" },
        }),
    );
    rig.wake(json!({ "state": "update", "changed": "content" }));
    let outcome = rig
        .run(
            &GoogleDriveWatchNode,
            json!({ "account": rig.access("google"), "target": "f1" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["fileId"], json!("f1"));
    assert_eq!(outcome.outputs["name"], json!("notes.txt"));
    assert_eq!(outcome.outputs["modifiedBy"], json!("Ada"));
    assert_eq!(outcome.outputs["state"], json!("update"), "the push headers fan out");
    Ok(())
}
