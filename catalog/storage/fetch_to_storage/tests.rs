//! FetchToStorage self-tests: a URL streams into storage and the
//! reference flows on.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FetchToStorageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("fetches_into_storage_and_emits_the_reference", fetches),
        NodeTest::fake("an_identity_makes_the_fetch_once_per_project", once),
        NodeTest::fake("keep_with_project_scope_is_refused", keep_in_project),
    ]
}

/// A project file already outlives the run, so `keep` there is a
/// contradiction named before anything is fetched.
async fn keep_in_project(rig: FakeRig) -> WeftResult<()> {
    let err = rig
        .run(
            &FetchToStorageNode,
            json!({ "url": "https://cdn.example/files/logo.png", "scope": "project", "keep": true }),
        )
        .await
        .result
        .expect_err("keep in project scope refuses")
        .to_string();
    assert!(err.contains("`keep`") && err.contains("scope: project"), "{err}");
    assert!(rig.requests().is_empty(), "refused before fetching");
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
