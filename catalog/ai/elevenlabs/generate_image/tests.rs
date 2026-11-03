//! ElevenLabsGenerateImage self-tests: the request each setting makes,
//! how files become references (inline, or a temporary asset whose
//! delete weft owes once the node ends), the parked wait, and the stored
//! image.

use serde_json::{json, Value};

use weft::signal::{CancelMethod, SendWhen};
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
        NodeTest::fake("a_file_that_cannot_go_inline_is_a_temporary_asset_weft_deletes_after", temporary_asset),
        NodeTest::fake("a_refused_create_still_owes_its_temporary_asset_delete", refused_create_cleans_up),
        NodeTest::fake("a_resumed_run_never_starts_a_second_generation", resumed),
        NodeTest::fake("a_failed_generation_fails_with_elevenlabs_reason", failed),
        NodeTest::fake("a_failed_generation_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_refused_create_quotes_elevenlabs_and_never_waits", refused),
        NodeTest::fake("an_unknown_status_fails_loud", unknown_status),
        NodeTest::fake("the_image_is_stored_as_the_type_elevenlabs_reported", reported_type_wins),
        NodeTest::fake("an_output_reported_as_no_image_is_refused_before_any_download", reported_non_image_refused),
        NodeTest::fake("a_wait_given_up_still_owes_its_temporary_asset_delete", given_up_wait_cleans_up),
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
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": [gif] }))).await.ok()?;

    assert_eq!(create_body(&rig)["images"], json!([{ "type": "asset", "asset_id": "a1" }]));
    let sent = rig.requests();
    let upload = sent.iter().find(|r| r.path == "/v1/assets").expect("the gif went up as an asset");
    assert!(upload.body_streamed, "the asset streams out of storage, never held whole");
    assert!(!sent.iter().any(|r| r.method == "DELETE"), "the node deletes nothing itself");
    owes_asset_delete(&rig, "a1");
    Ok(())
}

/// The node left weft the delete of the temporary asset `id`, sent
/// however the node ends.
fn owes_asset_delete(rig: &FakeRig, id: &str) {
    let owed = rig.owed_calls();
    assert_eq!(owed.len(), 1, "{owed:?}");
    let (call, access) = &owed[0];
    assert!(call.url.ends_with(&format!("/v1/assets/{id}")), "{}", call.url);
    assert!(matches!(call.method, CancelMethod::Delete));
    assert_eq!(call.when, SendWhen::Always);
    assert!(access.is_some(), "the delete goes through the node's own access");
}

async fn refused_create_cleans_up(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a2" }));
    rig.respond_status(
        "POST",
        "/v1/flows/image",
        422,
        json!({ "detail": [{ "loc": ["body", "gemini-3.1-flash-image", "images", 0], "msg": "Unsupported image type", "type": "value_error" }] }),
    );
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": [gif] }))).await.failure()?;
    assert!(err.contains("Unsupported image type"), "{err}");
    owes_asset_delete(&rig, "a2");
    Ok(())
}

async fn resumed(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "image", "g5", "image/png", b"png");
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({}))).await.ok()?;
    let creates = rig.requests().iter().filter(|r| r.method == "POST" && r.path == "/v1/flows/image").count();
    assert_eq!(creates, 1, "the resumed body finds the job in its state instead of paying twice");
    assert_eq!(rig.awaited_signals().len(), 1, "and the wait it already got an answer to is not made again");
    assert_eq!(rig.saved_state(&ElevenLabsGenerateImageNode).expect("a saved state")["submitted"], json!("g5"));
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

async fn given_up_wait_cleans_up(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v1/flows/image", json!({ "id": "g13", "status": "pending" }));
    rig.signal_given_up();
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a3" }));
    let gif = rig.store_file("spin.gif", "image/gif", b"GIF89a".to_vec());
    let err = rig.run(&ElevenLabsGenerateImageNode, inputs(&rig, json!({ "images": [gif] }))).await.failure()?;
    assert!(err.contains("given up"), "{err}");
    owes_asset_delete(&rig, "a3");
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
