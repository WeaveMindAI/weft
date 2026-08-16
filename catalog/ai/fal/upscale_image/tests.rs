//! FalUpscaleImage self-tests.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult, WeftType};

use super::FalUpscaleImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("upscales_and_stores_the_image", upscales)]
}

async fn upscales(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/esrgan", json!({ "request_id": "req-3" }));
    rig.respond(
        "GET",
        "/fal-ai/esrgan/requests/req-3/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/esrgan/requests/req-3",
        json!({ "image": { "url": "data:image/png;base64,Ymln" } }),
    );
    rig.output_type("image", WeftType::parse("Image").expect("parses"));
    let image = rig.store_file("small.png", "image/png", b"small".to_vec());
    let outcome = rig
        .run(
            &FalUpscaleImageNode,
            json!({
                "account": rig.access("fal"),
                "image": image,
                "scale": 2,
                "model": "fal-ai/esrgan",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["image"].is_object(), "the upscale lands as a stored file");
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["scale"], json!(2.0));
    Ok(())
}
