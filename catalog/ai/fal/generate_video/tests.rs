//! FalGenerateVideo self-tests.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::FalGenerateVideoNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("queues_polls_and_stores_the_video", generates),
        NodeTest::live("one_real_short_video", "fal", live_generate),
    ]
}

/// One real text-to-video on the cheapest priced model fal carries
/// (ltx, a flat two cents per video). fal stores nothing on the
/// account, so there is nothing to clean.
async fn live_generate(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FalGenerateVideoNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "a single blue square slowly rotating on white",
                "model": "fal-ai/ltx-video",
                "aspectRatio": "16:9",
            }),
        )
        .await
        .ok()?;
    assert!(outcome.output("video")?.is_object(), "the video lands as a stored file");
    Ok(())
}

async fn generates(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/veo3/fast", json!({ "request_id": "req-4" }));
    rig.respond(
        "GET",
        "/fal-ai/veo3/requests/req-4/status",
        json!({ "status": "COMPLETED" }),
    );
    rig.respond(
        "GET",
        "/fal-ai/veo3/requests/req-4",
        json!({ "video": { "url": "data:video/mp4;base64,dmlk" } }),
    );
    rig.output_type("video", WeftType::parse("Video").expect("parses"));
    let outcome = rig
        .run(
            &FalGenerateVideoNode,
            json!({
                "account": rig.access("fal"),
                "prompt": "a fox running through snow",
                "model": "fal-ai/veo3/fast",
                "aspectRatio": "16:9",
                "params": { "duration": "8s", "generate_audio": true },
            }),
        )
        .await
        .ok()?;
    assert!(outcome.outputs["video"].is_object(), "the video lands as a stored file");
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert_eq!(body["aspect_ratio"], json!("16:9"));
    assert_eq!(body["duration"], json!("8s"));
    Ok(())
}
