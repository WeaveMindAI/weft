//! FalEditImage self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::FalEditImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("edits_and_stores_the_image", edits)]
}

async fn edits(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux-pro/kontext", json!({ "request_id": "req-2" }));
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-2/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/flux-pro/requests/req-2",
        json!({ "images": [{ "url": "data:image/png;base64,ZWRpdA==" }] }),
    );
    rig.output_type("image", WeftType::parse("Image").expect("parses"));
    let image = rig.store_file("photo.png", "image/png", b"png-bytes".to_vec());
    let outcome = rig
        .run(
            &FalEditImageNode,
            json!({
                "account": rig.access("fal"),
                "image": image,
                "prompt": "make it night",
                "model": "fal-ai/flux-pro/kontext",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["image"].is_object(), "the edit lands as a stored file");

    let sent = &rig.requests()[0];
    let body = sent.body.as_ref().expect("json payload");
    assert_eq!(body["prompt"], json!("make it night"));
    assert!(
        body["image_url"].as_str().is_some_and(|u| u.starts_with("data:image/png")),
        "the input image rides inline when no public link exists: {}",
        body["image_url"]
    );
    assert!(body.get("mask_url").is_none(), "no mask key without a mask");
    Ok(())
}
