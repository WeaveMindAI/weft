//! ElevenLabsGenerateImage self-tests: the request each setting makes,
//! how files become references (inline, or a temporary asset that is
//! deleted again), the parked wait, and the stored image.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::flows::{inline_mime, output_filename};
use crate::testing::{completes, create_body, emitted_file, fails, inline};

use super::ElevenLabsGenerateImageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("only_small_files_of_an_accepted_type_go_inline", inline_rule),
        NodeTest::basic("an_output_is_named_for_its_kind_and_type", filenames),
        NodeTest::fake("a_prompt_alone_sends_only_the_model_and_the_prompt", prompt_alone),
        NodeTest::fake("every_setting_and_file_reaches_the_request", every_setting),
        NodeTest::fake("a_single_wired_image_goes_as_a_one_item_list", single_image),
        NodeTest::fake("a_file_that_cannot_go_inline_is_a_temporary_asset_deleted_after", temporary_asset),
        NodeTest::fake("a_refused_create_deletes_its_temporary_asset", refused_create_cleans_up),
        NodeTest::fake("a_resumed_run_never_starts_a_second_generation", resumed),
        NodeTest::fake("a_failed_generation_fails_with_elevenlabs_reason", failed),
        NodeTest::fake("a_failed_generation_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_refused_create_quotes_elevenlabs_and_never_waits", refused),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::fake("the_image_is_stored_as_the_type_elevenlabs_reported", reported_type_wins),
        NodeTest::fake("an_output_reported_as_no_image_is_refused_before_any_download", reported_non_image_refused),
        NodeTest::fake("a_temporary_asset_already_gone_counts_as_removed", asset_already_gone),
        NodeTest::fake("an_asset_that_cannot_be_removed_is_logged_and_the_image_still_lands", asset_left_behind),
        NodeTest::fake("a_wait_given_up_removes_its_temporary_asset", given_up_wait_cleans_up),
        NodeTest::live("one_real_image_then_an_edit_of_it", "elevenlabs", live_generate_then_edit),
    ]
}

fn inline_rule() -> WeftResult<()> {
    assert_eq!(inline_mime("image/png", 10), Some("image/png"));
    assert_eq!(inline_mime("image/jpg", 10), Some("image/jpeg"), "a common alias");
    assert_eq!(inline_mime("audio/x-wav", 10), Some("audio/wav"), "a common alias");
    assert_eq!(inline_mime("audio/mpeg; charset=binary", 10), Some("audio/mpeg"), "parameters do not count");
    assert_eq!(inline_mime("video/mp4", 25_000_000), Some("video/mp4"), "25 MB still fits");
    assert_eq!(inline_mime("video/mp4", 25_000_001), None, "past 25 MB it is an asset");
    assert_eq!(inline_mime("image/gif", 10), None, "a type ElevenLabs does not take inline");
    assert_eq!(inline_mime("audio/ogg; codecs=opus", 10), None);
    Ok(())
}

fn filenames() -> WeftResult<()> {
    assert_eq!(output_filename("image", "image/png"), "image.png");
    assert_eq!(output_filename("image", "image/jpeg"), "image.jpg");
    assert_eq!(output_filename("video", "video/mp4"), "video.mp4");
    assert_eq!(output_filename("video", "video/quicktime"), "video.mov");
    assert_eq!(output_filename("audio", "audio/mpeg"), "audio.mp3");
    assert_eq!(output_filename("image", "image/svg+xml"), "image.svg");
    Ok(())
}

fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "prompt": "a corgi on a beach",
        "model": "gemini-3.1-flash-image",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn prompt_alone(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g1", "image/png", b"png-bytes");
    let outcome = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;

    assert_eq!(
        create_body(&rig),
        json!({ "model_id": "gemini-3.1-flash-image", "prompt": "a corgi on a beach" }),
        "a setting nobody gave never travels: every model refuses a field it does not take"
    );
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "image")?;
    assert_eq!(bytes, b"png-bytes");
    assert_eq!(filename, "image.png");
    assert_eq!(mime, "image/png");

    // The wait parks on the generation, signed by the connection, looks
    // every 5 seconds (weft's floor; ElevenLabs asks for 2 at most),
    // ends on anything not in flight, and carries only the status.
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(awaited[0].config["url"], json!("https://api.elevenlabs.io/v1/flows/image/g1"));
    assert_eq!(awaited[0].config["interval_secs"], json!(5));
    assert_eq!(awaited[0].config["carry"], json!(["status"]));
    assert!(awaited[0].access.is_some(), "each look is signed by the ElevenLabs connection");
    assert_eq!(
        awaited[0].match_predicates,
        vec![
            weft::signal::Predicate::neq("status", "pending"),
            weft::signal::Predicate::neq("status", "generating"),
        ]
    );
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g2", "image/png", b"edited");
    let photo = rig.store_file("photo.png", "image/png", b"photo".to_vec());
    let style = rig.store_file("style.jpg", "image/jpeg", b"style".to_vec());
    let mask = rig.store_file("mask.png", "image/png", b"mask".to_vec());
    rig.run(
        &ElevenLabsGenerateImageNode,
        inputs(
            &rig,
            json!({
                "model": "gpt-image-2.5-flare",
                "images": [photo, style],
                "mask": mask,
                "aspectRatio": "3:2",
                "resolution": "2K",
                "quality": "max",
                "background": "transparent",
                "seed": 42,
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "gpt-image-2.5-flare",
            "prompt": "a corgi on a beach",
            "images": [inline("image/png", b"photo"), inline("image/jpeg", b"style")],
            "mask": inline("image/png", b"mask"),
            "aspect_ratio": "3:2",
            "resolution": "2K",
            "quality": "max",
            "background": "transparent",
            "seed": 42,
        })
    );
    Ok(())
}

async fn single_image(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g3", "image/png", b"edited");
    let photo = rig.store_file("photo.webp", "image/webp", b"photo".to_vec());
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": photo }))).await.ok()?;
    assert_eq!(create_body(&rig)["images"], json!([inline("image/webp", b"photo")]));
    Ok(())
}

async fn temporary_asset(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g4", "image/png", b"edited");
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a1", "name": "spin.gif", "mime_type": "image/gif" }));
    rig.respond_status("DELETE", "/v1/assets/a1", 204, Value::Null);
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": [gif] }))).await.ok()?;

    assert_eq!(create_body(&rig)["images"], json!([{ "type": "asset", "asset_id": "a1" }]));
    let sent = rig.requests();
    let upload = sent.iter().find(|r| r.path == "/v1/assets").expect("the gif went up as an asset");
    let form = upload.body_text.as_deref().expect("a buffered multipart body");
    assert!(form.contains("name=\"asset\"; filename=\"spin.gif\""), "{form}");
    assert!(form.contains("GIF89a"), "the file's bytes are in the upload");
    let order: Vec<(&str, &str)> = sent.iter().map(|r| (r.method.as_str(), r.path.as_str())).collect();
    let create = order.iter().position(|r| *r == ("POST", "/v1/flows/image")).expect("a create");
    let delete = order.iter().position(|r| *r == ("DELETE", "/v1/assets/a1")).expect("the asset is deleted");
    assert!(delete > create, "the asset outlives the create it was uploaded for: {order:?}");
    Ok(())
}

async fn refused_create_cleans_up(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a2" }));
    rig.respond_status("DELETE", "/v1/assets/a2", 204, Value::Null);
    rig.respond_status(
        "POST",
        "/v1/flows/image",
        422,
        json!({ "detail": [{ "loc": ["body", "gemini-3.1-flash-image", "images", 0], "msg": "Unsupported image type", "type": "value_error" }] }),
    );
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": [gif] }))).await.failure()?;
    assert!(err.contains("Unsupported image type"), "{err}");
    rig.assert_sent("DELETE", "/v1/assets/a2");
    Ok(())
}

async fn resumed(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g5", "image/png", b"png");
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    let creates = rig.requests().iter().filter(|r| r.method == "POST" && r.path == "/v1/flows/image").count();
    assert_eq!(creates, 1, "the resumed body finds the job in its state instead of paying twice");
    assert_eq!(rig.awaited_signals().len(), 1, "and the wait it already got an answer to is not made again");
    assert_eq!(rig.saved_state(&ElevenLabsGenerateImageNode).expect("a saved state")["submitted"]["id"], json!("g5"));
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "image", "g6", "moderated", "The prompt was rejected by content moderation. You were not charged for this generation.");
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("moderated"), "{err}");
    assert!(err.contains("You were not charged"), "{err}");
    assert!(err.contains("g6"), "the error names the generation: {err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "image", "g7", "timeout", "Timed out while processing.");
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("timeout"), "{error}");
    assert!(!outcome.outputs.contains_key("image"), "a caught failure emits no image");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v1/flows/image",
        402,
        json!({ "detail": { "type": "payment_required", "code": "paid_plan_required", "message": "The Image & Video API requires a Pro plan or above." } }),
    );
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("402"), "{err}");
    assert!(err.contains("Pro plan"), "{err}");
    assert!(rig.awaited_signals().is_empty(), "a refused create never parks");
    Ok(())
}

async fn unknown_status(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/flows/image", json!({ "id": "g8", "status": "pending" }));
    rig.signal(json!({ "status": "cancelled" }));
    rig.respond("GET", "/v1/flows/image/g8", json!({ "id": "g8", "status": "cancelled" }));
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("unexpected status 'cancelled'"), "{err}");
    Ok(())
}

async fn reported_type_wins(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/flows/image", json!({ "id": "g9", "status": "pending" }));
    rig.signal(json!({ "status": "completed" }));
    rig.respond(
        "GET",
        "/v1/flows/image/g9",
        json!({
            "id": "g9",
            "status": "completed",
            "content_url": "https://storage.googleapis.com/generations/g9",
            "content_mime_type": "image/webp",
        }),
    );
    rig.respond_raw("GET", "/generations/g9", 200, "application/octet-stream", b"webp".to_vec());
    let outcome = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "image")?;
    assert_eq!(bytes, b"webp");
    assert_eq!(mime, "image/webp", "the type ElevenLabs reported, never the download's header");
    assert_eq!(filename, "image.webp");
    Ok(())
}

async fn reported_non_image_refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/flows/image", json!({ "id": "g10", "status": "pending" }));
    rig.signal(json!({ "status": "completed" }));
    rig.respond(
        "GET",
        "/v1/flows/image/g10",
        json!({
            "id": "g10",
            "status": "completed",
            "content_url": "https://storage.googleapis.com/generations/g10",
            "content_mime_type": "application/zip",
        }),
    );
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("reported the finished image as application/zip"), "{err}");
    assert!(!rig.requests().iter().any(|r| r.path == "/generations/g10"), "nothing is downloaded");
    Ok(())
}

/// A gif goes up as a temporary asset; the canned answers for its
/// upload and its delete (answering `delete_status`), and the inputs
/// that send it.
fn gif_through_an_asset(rig: &FakeRig, delete_status: u16) -> Value {
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a3" }));
    rig.respond_status("DELETE", "/v1/assets/a3", delete_status, json!({ "detail": { "message": "storage hiccup" } }));
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    inputs(rig, json!({ "images": [gif] }))
}

async fn asset_already_gone(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g11", "image/png", b"png");
    let given = gif_through_an_asset(&rig, 404);
    rig.run(&ElevenLabsGenerateImageNode, given).await.ok()?;
    assert!(rig.logs().is_empty(), "an asset already gone is removed, nothing to say: {:?}", rig.logs());
    Ok(())
}

async fn asset_left_behind(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g12", "image/png", b"png");
    let given = gif_through_an_asset(&rig, 500);
    let outcome = rig.run(&ElevenLabsGenerateImageNode, given).await.ok()?;
    let (bytes, _, _) = emitted_file(&rig, &outcome.outputs, "image")?;
    assert_eq!(bytes, b"png", "the paid image still lands");
    let logs = rig.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].0, weft::context::LogLevel::Warn);
    assert!(logs[0].1.contains("a3") && logs[0].1.contains("Assets section"), "{}", logs[0].1);
    Ok(())
}

async fn given_up_wait_cleans_up(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/flows/image", json!({ "id": "g13", "status": "pending" }));
    rig.signal_given_up();
    let given = gif_through_an_asset(&rig, 204);
    let err = rig.run(&ElevenLabsGenerateImageNode, given).await.failure()?;
    assert!(err.contains("given up"), "{err}");
    rig.assert_sent("DELETE", "/v1/assets/a3");
    Ok(())
}

/// One real image on the cheapest model at its smallest size, then an
/// edit of that image, which sends it back inline. Nothing is left on
/// the account: generations are listed by the API but hold no storage.
async fn live_generate_then_edit(rig: LiveRig) -> WeftResult<()> {
    let made = rig
        .run(
            &ElevenLabsGenerateImageNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a single red circle on a white background",
                "model": "gemini-3.1-flash-lite-image",
                "aspectRatio": "1:1",
            }),
        )
        .await
        .ok()?;
    let image = made.output("image")?.clone();
    let edited = rig
        .run(
            &ElevenLabsGenerateImageNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "make the circle blue",
                "model": "gemini-3.1-flash-lite-image",
                "images": image,
            }),
        )
        .await
        .ok()?;
    let file = weft::storage::StoredFile::from_value(edited.output("image")?)?;
    assert!(file.mime_type.starts_with("image/"), "{}", file.mime_type);
    assert!(file.size_bytes > 1_000, "a real image is more than a kilobyte: {}", file.size_bytes);
    Ok(())
}
