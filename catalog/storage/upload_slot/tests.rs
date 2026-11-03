//! UploadSlot self-tests: the slot asked for carries the node's settings,
//! and a setting no slot can have is refused before anything is asked.

use serde_json::json;

use weft::storage::{KeepTtl, StorageScope};
use weft::{FakeRig, NodeTest, WeftResult};

use super::UploadSlotNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_slot_carries_the_settings", settings),
        NodeTest::fake("a_slot_no_upload_could_use_is_refused", refused),
    ]
}

async fn settings(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&UploadSlotNode, json!({ "maxMb": 2, "types": "Video", "minutes": 30, "scope": "run", "ttlDays": 2 }))
        .await
        .ok()?;
    assert!(outcome.outputs["slot"].as_str().is_some_and(weft::storage::slot::is_slot), "{}", outcome.outputs["slot"]);
    let asked = rig.upload_slots();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].max_bytes, 2_097_152);
    assert_eq!((asked[0].accepts.as_str(), asked[0].minutes), ("Video", 30));
    assert_eq!(asked[0].scope, StorageScope::Run);
    assert_eq!(asked[0].keep, Some(KeepTtl::Secs { secs: 2 * 24 * 3600 }));
    rig.run(&UploadSlotNode, json!({ "maxMb": 10, "types": "File", "minutes": 60, "scope": "project" })).await.ok()?;
    assert_eq!(rig.upload_slots()[1].keep, None, "no ttlDays, the scope's own lifetime");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    for (settings, says) in [
        (json!({ "maxMb": 1, "types": "String", "minutes": 60, "scope": "project" }), "file type"),
        (json!({ "maxMb": 1, "types": "File", "minutes": 0, "scope": "project" }), "minutes"),
    ] {
        let err = rig.run(&UploadSlotNode, settings).await.failure()?;
        assert!(err.contains(says), "{says}: {err}");
    }
    assert!(rig.upload_slots().is_empty(), "nothing was asked");
    Ok(())
}
