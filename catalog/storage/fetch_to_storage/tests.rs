//! FetchToStorage self-tests: a URL streams into storage and the
//! reference flows on.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FetchToStorageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("fetches_into_storage_and_emits_the_reference", fetches),
        NodeTest::fake("an_identity_makes_the_fetch_once_per_project", once),
        NodeTest::fake("ttl_days_keeps_a_run_file_that_long", ttl_keeps_a_run_file),
        NodeTest::fake("a_refused_download_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_download_comes_out_on_error_when_it_is_wired", refused_wired),
        NodeTest::fake("a_contradictory_setting_fails_the_run_even_with_error_wired", mistake_wired),
    ]
}

fn refuse_the_download(rig: &FakeRig) {
    rig.respond_raw("GET", "/files/gone.pdf", 404, "text/plain", b"not found".to_vec());
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_download(&rig);
    let err = rig
        .run(&FetchToStorageNode, json!({ "url": "https://cdn.example/files/gone.pdf" }))
        .await
        .failure()?;
    assert!(err.contains("404"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    refuse_the_download(&rig);
    rig.wire_output("error");
    let outcome = rig
        .run(&FetchToStorageNode, json!({ "url": "https://cdn.example/files/gone.pdf" }))
        .await
        .ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("404"), "{error}");
    for port in ["file", "sizeBytes", "mimeType"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}

async fn mistake_wired(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig
        .run(
            &FetchToStorageNode,
            json!({ "url": "https://cdn.example/files/logo.png", "scope": "everywhere" }),
        )
        .await
        .failure()?;
    assert!(err.starts_with("input error") && err.contains("`scope`"), "{err}");
    Ok(())
}

/// `ttl_days` reads as on every storage node: an execution file given
/// one outlives its run for that long.
async fn ttl_keeps_a_run_file(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/files/logo.png", 200, "image/png", b"PNG!".to_vec());
    let outcome = rig
        .run(&FetchToStorageNode, json!({ "url": "https://cdn.example/files/logo.png", "ttl_days": 2 }))
        .await
        .ok()?;
    let key = weft::storage::StoredFile::from_value(&outcome.outputs["file"])?.key;
    let meta = rig.stored_meta(&key)?;
    assert!(meta.keep, "kept past the run: {meta:?}");
    assert_eq!(meta.keep_ttl_secs, Some(2 * 24 * 3600));
    assert_eq!(outcome.outputs["filename"], json!("logo.png"), "the four ports a stored file travels as");
    Ok(())
}

/// The same identity in the project scope is one file: the second run
/// gets the first run's reference, and the URL is not fetched again.
async fn once(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/files/logo.png", 200, "image/png", b"PNG!".to_vec());
    let input = json!({
        "url": "https://cdn.example/files/logo.png",
        "scope": "project",
        "identity": "cdn:logo",
    });
    let first = rig.run(&FetchToStorageNode, input.clone()).await.ok()?;
    let second = rig.run(&FetchToStorageNode, input).await.ok()?;
    assert_eq!(first.outputs["file"], second.outputs["file"], "one file for one identity");
    assert_eq!(rig.requests().len(), 1, "the second fetch downloaded nothing");
    Ok(())
}

async fn fetches(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("GET", "/files/report.pdf", 200, "application/pdf", b"%PDF".to_vec());
    let outcome = rig
        .run(
            &FetchToStorageNode,
            json!({ "url": "https://cdn.example/files/report.pdf" }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["mimeType"], json!("application/pdf"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(4));
    let blob = &outcome.outputs["file"]["__weft_blob__"];
    assert_eq!(blob["filename"], json!("report.pdf"), "the URL's last segment names the file");
    assert!(blob["key"].is_string(), "a stored-file reference flows on");
    Ok(())
}
