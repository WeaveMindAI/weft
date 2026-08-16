//! FalGenerateImage self-tests: the queue dance (submit, status,
//! result) and the internalized image outputs.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::FalGenerateImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("queues_polls_and_stores_the_images", generates),
        NodeTest::fake("a_failed_generation_surfaces_fals_words", failed_generation),
        NodeTest::fake("a_traversal_model_id_refuses", bad_model),
        NodeTest::live("one_real_small_generation", "fal", live_generate),
    ]
}

/// One real image through the whole queue dance (submit, poll,
/// result, internalize), on the cheapest settings (one small flux
/// image). fal stores nothing on the account, so there is nothing to
/// clean.
async fn live_generate(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FalGenerateImageNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "a single red circle on a white background",
                "model": "fal-ai/flux/dev",
                "imageSize": "square",
                "count": 1,
                "params": { "num_inference_steps": 4 },
            }),
        )
        .await
        .ok()?;
    let image = outcome.output("image")?;
    assert!(image.is_object(), "the image lands as a stored file: {image}");
    assert_eq!(outcome.output("images")?.as_array().map(Vec::len), Some(1));
    Ok(())
}

async fn generates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux/dev", json!({ "request_id": "req-1" }));
    rig.respond(
        "GET",
        "/fal-ai/flux/requests/req-1/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/flux/requests/req-1",
        json!({ "images": [{ "url": "data:image/png;base64,aWpn" }] }),
    );
    rig.output_type("images", WeftType::parse("List[Image]").expect("parses"));
    let outcome = rig
        .run(
            &FalGenerateImageNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "a red fox",
                "model": "fal-ai/flux/dev",
                "imageSize": "square",
                "count": 2,
                "params": { "guidance_scale": 3.5 },
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["image"].is_object(), "the first image is a stored file");
    assert_eq!(outcome.outputs["images"].as_array().map(Vec::len), Some(1));

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "prompt": "a red fox",
            "image_size": "square",
            "num_images": 2,
            "guidance_scale": 3.5,
        })
    );
    Ok(())
}

/// fal's queue has no failed status: a failed generation COMPLETES
/// and carries `error` / `error_type` on the result body. The node
/// must surface fal's own words, not report success or a shapeless
/// "no images" refusal.
async fn failed_generation(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux/dev", json!({ "request_id": "req-9" }));
    rig.respond(
        "GET",
        "/fal-ai/flux/requests/req-9/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/flux/requests/req-9",
        json!({ "error": "content policy violation", "error_type": "ContentPolicyViolation" }),
    );
    let outcome = rig
        .run(
            &FalGenerateImageNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "a red fox",
                "model": "fal-ai/flux/dev",
                "imageSize": "square",
                "count": 1,
            }),
        )
        .await;
    let err = outcome.result.expect_err("a failed generation must refuse").to_string();
    assert!(err.contains("content policy violation"), "{err}");
    Ok(())
}

async fn bad_model(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FalGenerateImageNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "x",
                "model": "../../etc",
                "imageSize": "square",
                "count": 1,
            }),
        )
        .await;
    let err = outcome.result.expect_err("a traversal id must refuse").to_string();
    assert!(err.contains("not a fal model id"), "{err}");
    Ok(())
}
