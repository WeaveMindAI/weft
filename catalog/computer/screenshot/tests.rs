//! Screenshot self-tests: the image stored and kept, and a computer
//! answering without one failing loud.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use base64::Engine;

use super::super::testing::{answers, asked, computer, PNG_BASE64};
use super::ScreenshotNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("stores_the_screenshot_kept_past_the_run", stores),
        NodeTest::fake("an_answer_with_no_image_or_size_fails_loud", no_image),
    ]
}

async fn stores(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "png": PNG_BASE64, "width": 1280, "height": 800 }));
    let outcome = rig.run(&ScreenshotNode, json!({ "computer": computer })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Took a screenshot of the 1280x800 screen."));
    let file = StoredFile::from_value(&outcome.outputs["image"])?;
    assert_eq!(file.mime_type, "image/png");
    let png = base64::engine::general_purpose::STANDARD.decode(PNG_BASE64).expect("base64");
    assert_eq!(rig.stored_bytes(&file.key)?.as_ref(), png.as_slice(), "the bytes are the computer's PNG");
    assert_eq!(rig.stored_meta(&file.key)?.keep_ttl_secs, None, "no deadline: it reads for as long as the run is kept");
    assert_eq!(asked(&rig), vec![("screenshot".to_string(), json!({}))]);
    Ok(())
}

async fn no_image(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "width": 1280, "height": 800 }));
    let outcome = rig.run(&ScreenshotNode, json!({ "computer": computer })).await;
    let error = outcome.result.expect_err("no image, no screenshot").to_string();
    assert!(error.contains("answered no image"), "{error}");
    let rig = FakeRig::new();
    let computer = super::super::testing::computer(&rig);
    answers(&rig, json!({ "png": PNG_BASE64, "width": 1280 }));
    let outcome = rig.run(&ScreenshotNode, json!({ "computer": computer })).await;
    let error = outcome.result.expect_err("no height").to_string();
    assert!(error.contains("no height"), "{error}");
    Ok(())
}
