//! TextToFile self-tests: the text lands as the file's bytes under the
//! chosen name, type, scope and lifetime.

use serde_json::json;

use weft::storage::{StorageScope, StoredFile};
use weft::{FakeRig, NodeTest, WeftResult};

use super::TextToFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("stores_the_text_as_a_plain_text_file", plain_text),
        NodeTest::fake("names_the_type_scope_and_lifetime", chosen),
        NodeTest::fake("an_unknown_scope_is_refused", unknown_scope),
    ]
}

async fn plain_text(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&TextToFileNode, json!({ "text": "héllo" })).await.ok()?;
    let file = StoredFile::from_value(outcome.output("file")?)?;
    assert_eq!(file.filename, "text.txt");
    assert_eq!(file.mime_type, "text/plain");
    assert_eq!(rig.stored_bytes(&file.key)?.as_ref(), "héllo".as_bytes(), "saved as UTF-8");
    let meta = rig.stored_meta(&file.key)?;
    assert_eq!((meta.keep, meta.keep_ttl_secs), (false, None), "an execution file goes with its run");
    Ok(())
}

async fn chosen(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &TextToFileNode,
            json!({
                "text": "[]",
                "filename": "chat.json",
                "mimeType": "application/json",
                "scope": "project",
                "ttl_days": 3,
            }),
        )
        .await
        .ok()?;
    let file = StoredFile::from_value(outcome.output("file")?)?;
    assert_eq!((file.filename.as_str(), file.mime_type.as_str()), ("chat.json", "application/json"));
    assert_eq!(rig.stored_meta(&file.key)?.keep_ttl_secs, Some(3 * 24 * 3600));
    let project = rig.stored_files(&StorageScope::Project)?;
    assert_eq!(project.len(), 1, "stored in the project's scope");
    Ok(())
}

async fn unknown_scope(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&TextToFileNode, json!({ "text": "x", "scope": "forever" })).await.failure()?;
    assert!(err.contains("`scope` is 'forever'"), "{err}");
    Ok(())
}
