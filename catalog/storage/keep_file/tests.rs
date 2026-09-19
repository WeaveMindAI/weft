//! KeepFile self-tests: the keep flag lands and the exact marker
//! passes through; a project copy is a new file with the same shape;
//! a ttl with the project scope is refused before anything moves.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::KeepFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("keeps_and_passes_the_marker_through", keeps),
        NodeTest::fake("project_scope_copies_the_file_and_emits_the_copy", copies_into_project),
        NodeTest::fake("ttl_days_with_project_scope_is_refused", ttl_in_project),
    ]
}

async fn keeps(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file.clone() }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["file"], file, "the marker passes through untouched");
    let key = StoredFile::from_value(&file)?.key;
    assert!(rig.stored_meta(&key)?.keep, "the keep flag landed on the run's file");
    Ok(())
}

/// The copy is a NEW stored file carrying the source's filename, mime
/// type, size and bytes; the source stays where it was, unkept.
async fn copies_into_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file.clone(), "scope": "project" }))
        .await
        .ok()?;
    let source = StoredFile::from_value(&file)?;
    let copy = StoredFile::from_value(&outcome.outputs["file"])?;
    assert_ne!(copy.key, source.key, "the emitted reference is the copy, not the source");
    assert_eq!(copy.filename, "upload.png");
    assert_eq!(copy.mime_type, "image/png");
    assert_eq!(copy.size_bytes, 4);
    let copy_meta = rig.stored_meta(&copy.key)?;
    assert_eq!(copy_meta.size_bytes, 4, "the bytes were copied");
    assert!(!copy_meta.keep, "a project file carries no keep flag");
    assert!(!rig.stored_meta(&source.key)?.keep, "the source is left as it was");
    Ok(())
}

/// A project file already outlives the run, so a ttl there is a
/// contradiction named before anything is copied.
async fn ttl_in_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let err = rig
        .run(&KeepFileNode, json!({ "file": file.clone(), "scope": "project", "ttl_days": 7 }))
        .await
        .result
        .expect_err("a ttl in project scope refuses")
        .to_string();
    assert!(err.contains("`ttl_days`") && err.contains("scope: project"), "{err}");
    let source = StoredFile::from_value(&file)?.key;
    assert!(!rig.stored_meta(&source)?.keep, "nothing was kept");
    Ok(())
}
