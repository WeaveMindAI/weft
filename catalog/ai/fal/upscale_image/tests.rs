//! FalUpscaleImage self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::FalUpscaleImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("upscales_and_stores_the_image", upscales),
        NodeTest::live("one_real_upscale", "fal", live_upscale),
    ]
}

/// One real 2x upscale of a freshly minted sample image (esrgan bills
/// GPU seconds; a small square costs a fraction of a cent). fal stores
/// nothing on the account, so there is nothing to clean.
async fn live_upscale(rig: LiveRig) -> WeftResult<()> {
    let conn = rig.connect().await?;
    let (mime, bytes) = crate::testing::sample_image(&conn).await?;
    let image = rig.store_file("seed.png", &mime, bytes).await?;
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
    assert!(outcome.output("image")?.is_object(), "the upscale lands as a stored file");
    Ok(())
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
