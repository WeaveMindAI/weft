//! FetchToStorage self-tests: a URL streams into storage and the
//! reference flows on.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FetchToStorageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("fetches_into_storage_and_emits_the_reference", fetches)]
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
