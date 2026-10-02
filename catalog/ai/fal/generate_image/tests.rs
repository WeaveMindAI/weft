//! FalGenerateImage self-tests: the queue dance (submit, the parked
//! wait on the status, result) and the internalized image outputs.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult, WeftType};

use super::FalGenerateImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("queues_waits_and_stores_the_images", generates),
        NodeTest::fake("a_resumed_run_never_submits_twice", resumed),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::fake("a_failed_generation_surfaces_fals_words", failed_generation),
        NodeTest::fake("a_traversal_model_id_fails_the_run_even_when_error_is_wired", bad_model),
        NodeTest::fake("a_refused_submit_fails_the_run_when_error_is_unwired", refused_unwired),
        NodeTest::fake("a_refused_submit_comes_out_on_error_when_it_is_wired", refused_wired),
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
    rig.signal(json!({ "status": "COMPLETED" }));
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

    // The wait parks on the app's status route (never the variant
    // subpath), signed by the node's connection, until the request
    // leaves the queue.
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(
        awaited[0].config["url"],
        json!("https://queue.fal.run/fal-ai/flux/requests/req-1/status")
    );
    assert!(awaited[0].access.is_some(), "each poll is signed by the fal connection");
    assert_eq!(
        awaited[0].match_predicates,
        vec![
            weft::signal::Predicate::neq("status", "IN_QUEUE"),
            weft::signal::Predicate::neq("status", "IN_PROGRESS"),
        ]
    );
    Ok(())
}

fn fox(rig: &FakeRig) -> serde_json::Value {
    json!({
        "account": rig.access("fal"),
        "prompt": "a red fox",
        "model": "fal-ai/flux/dev",
        "imageSize": "square",
        "count": 1,
    })
}

/// The body replays from the top when the wait resumes. The paid
/// submit is journaled, so the second pass reads the request id back
/// instead of queueing a second generation.
async fn resumed(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux/dev", json!({ "request_id": "req-7" }));
    rig.signal(json!({ "status": "COMPLETED" }));
    rig.respond(
        "GET",
        "/fal-ai/flux/requests/req-7",
        json!({ "images": [{ "url": "data:image/png;base64,aWpn" }] }),
    );
    rig.run(&FalGenerateImageNode, fox(&rig)).await.ok()?;
    rig.run(&FalGenerateImageNode, fox(&rig)).await.ok()?;
    let submits = rig.requests().iter().filter(|r| r.method == "POST").count();
    assert_eq!(submits, 1, "the replay reads the journaled submit back");
    Ok(())
}

/// A status outside fal's queue vocabulary ends the wait and refuses
/// loudly, never parks forever on an answer the node does not know.
async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux/dev", json!({ "request_id": "req-8" }));
    rig.signal(json!({ "status": "CANCELLED" }));
    let outcome = rig.run(&FalGenerateImageNode, fox(&rig)).await;
    let err = outcome.result.expect_err("an unknown status is loud").to_string();
    assert!(err.contains("unexpected status 'CANCELLED'"), "{err}");
    Ok(())
}

/// fal's queue has no failed status: a failed generation COMPLETES
/// and carries `error` / `error_type` on the result body. The node
/// must surface fal's own words, not report success or a shapeless
/// "no images" refusal.
async fn failed_generation(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/fal-ai/flux/dev", json!({ "request_id": "req-9" }));
    rig.signal(json!({ "status": "COMPLETED" }));
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

/// A malformed model id is a mistake in the program, not fal's answer:
/// a wired `error` must not swallow it.
async fn bad_model(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    let err = rig
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
        .await
        .failure()?;
    assert!(err.starts_with("input error"), "{err}");
    assert!(err.contains("not a fal model id"), "{err}");
    Ok(())
}

/// The submit refused by fal, and the inputs of the run that sends it.
fn refuse_the_submit(rig: &FakeRig) -> serde_json::Value {
    rig.respond_status("POST", "/fal-ai/flux/dev", 401, json!({ "detail": "bad key" }));
    json!({
        "account": rig.access("fal"),
        "prompt": "a red fox",
        "model": "fal-ai/flux/dev",
        "imageSize": "square",
        "count": 1,
    })
}

async fn refused_unwired(rig: FakeRig) -> WeftResult<()> {
    let inputs = refuse_the_submit(&rig);
    let err = rig.run(&FalGenerateImageNode, inputs).await.failure()?;
    assert!(err.contains("bad key"), "{err}");
    Ok(())
}

async fn refused_wired(rig: FakeRig) -> WeftResult<()> {
    let inputs = refuse_the_submit(&rig);
    rig.wire_output("error");
    let outcome = rig.run(&FalGenerateImageNode, inputs).await.ok()?;
    let error = outcome.output("error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("bad key"), "{error}");
    for port in ["images", "image"] {
        assert!(!outcome.outputs.contains_key(port), "a caught failure emits nothing on {port}");
    }
    Ok(())
}
