//! KeepFile self-tests: a project copy is a new file with the same
//! shape, with no deadline unless `ttlDays` gives it one; `run` leaves the
//! file where it is, the exact marker passing through, and sets or clears
//! its deadline.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::KeepFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("run_leaves_the_file_and_passes_the_marker_through", in_place),
        NodeTest::fake("project_scope_copies_the_file_and_emits_the_copy", copies_into_project),
        NodeTest::fake("ttl_days_gives_the_project_copy_a_deadline", ttl_in_project),
        NodeTest::fake("ttl_days_sets_or_clears_a_files_deadline_in_place", ttl_in_place),
        NodeTest::fake("a_video_stays_a_video_whichever_scope", video_stays_video),
    ]
}

/// The output is the input's own kind (`T: File`): a Video kept or
/// copied comes out as a Video, which a port typed `Video` downstream
/// takes. Held to `Video` here as such a program would declare it.
async fn video_stays_video(rig: FakeRig) -> WeftResult<()> {
    let video = rig.store_file("clip.mp4", "video/mp4", b"\x00\x00\x00\x18ftypmp42".to_vec());
    rig.output_type("file", weft::WeftType::parse("Video").expect("parses"));
    for scope in ["run", "project"] {
        let outcome = rig.run(&KeepFileNode, json!({ "file": video.clone(), "scope": scope })).await.ok()?;
        let out = &outcome.outputs["file"];
        assert!(out.get("__weft_video__").is_some(), "{scope}: still a video: {out}");
    }
    Ok(())
}

async fn in_place(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
    let outcome = rig.run(&KeepFileNode, json!({ "file": file.clone(), "scope": "run" })).await.ok()?;
    assert_eq!(outcome.outputs["file"], file, "the marker passes through untouched");
    Ok(())
}

/// The copy is a NEW stored file carrying the source's filename, mime
/// type, size and bytes; the source stays where it was, its deadline
/// untouched. The default scope is the project's.
async fn copies_into_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let outcome = rig.run(&KeepFileNode, json!({ "file": file.clone() })).await.ok()?;
    let source = StoredFile::from_value(&file)?;
    let copy = StoredFile::from_value(&outcome.outputs["file"])?;
    assert_ne!(copy.key, source.key, "the emitted reference is the copy, not the source");
    assert!(copy.key.contains("/project/"), "copied into the project's files: {}", copy.key);
    assert_eq!(copy.filename, "upload.png");
    assert_eq!(copy.mime_type, "image/png");
    assert_eq!(copy.size_bytes, 4);
    assert_eq!(rig.stored_meta(&copy.key)?.size_bytes, 4, "the bytes were copied");
    assert_eq!(rig.stored_meta(&source.key)?.keep_ttl_secs, None, "the source is left as it was");
    Ok(())
}

/// A project copy with `ttlDays` is deleted once idle that long; without
/// one it has no deadline and lives as long as the project.
async fn ttl_in_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file, "scope": "project", "ttlDays": 7 }))
        .await
        .ok()?;
    let copy = rig.stored_meta(&StoredFile::from_value(&outcome.outputs["file"])?.key)?;
    assert_eq!(copy.keep_ttl_secs, Some(7 * 24 * 3600), "the copy goes after 7 idle days");
    let forever = rig.store_file("b.png", "image/png", b"PNG!".to_vec());
    let outcome = rig.run(&KeepFileNode, json!({ "file": forever, "scope": "project" })).await.ok()?;
    let copy = rig.stored_meta(&StoredFile::from_value(&outcome.outputs["file"])?.key)?;
    assert_eq!(copy.keep_ttl_secs, None, "no ttlDays: the project copy has no deadline");
    Ok(())
}

/// In place (`run`): a number is that many idle days, empty or 0 is no
/// deadline at all.
async fn ttl_in_place(rig: FakeRig) -> WeftResult<()> {
    for (ttl_days, expected) in [(None, None), (Some(0), None), (Some(2), Some(2 * 24 * 3600))] {
        let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
        let mut inputs = json!({ "file": file.clone(), "scope": "run" });
        if let Some(days) = ttl_days {
            inputs["ttlDays"] = json!(days);
        }
        rig.run(&KeepFileNode, inputs).await.ok()?;
        let meta = rig.stored_meta(&StoredFile::from_value(&file)?.key)?;
        assert_eq!(meta.keep_ttl_secs, expected, "ttlDays {ttl_days:?}");
    }
    Ok(())
}
