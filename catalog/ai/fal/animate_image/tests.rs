//! FalAnimateImage self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::FalAnimateImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("animates_and_stores_the_video", animates)]
}

async fn animates(rig: FakeRig) -> WeftResult<()> {
    let model = "fal-ai/kling-video/v2.1/standard/image-to-video";
    rig.respond("POST", &format!("/{model}"), json!({ "request_id": "req-5" }));
    // The request routes address the app (`owner/name`), variant dropped.
    rig.respond(
        "GET",
        "/fal-ai/kling-video/requests/req-5/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/kling-video/requests/req-5",
        json!({ "video": { "url": "data:video/mp4;base64,YW5pbQ==" } }),
    );
    rig.output_type("video", WeftType::parse("Video").expect("parses"));
    let first = rig.store_file("first.png", "image/png", b"first".to_vec());
    let last = rig.store_file("last.png", "image/png", b"last".to_vec());
    let outcome = rig
        .run(
            &FalAnimateImageNode,
            json!({
                "account": rig.access("fal"),
                "image": first,
                "tailImage": last,
                "prompt": "the fox turns and runs",
                "model": model,
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["video"].is_object(), "the video lands as a stored file");
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert!(body["image_url"].as_str().is_some());
    assert!(body["tail_image_url"].as_str().is_some());
    Ok(())
}
