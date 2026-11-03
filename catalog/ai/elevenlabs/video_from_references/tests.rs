//! ElevenLabsVideoFromReferences self-tests: how each model takes the
//! reference material (Veo with a role per image, Seedance plain), the
//! stored video, and the two ways a generation fails. Style images on a
//! model with no roles are refused by a metadata rule, which this rig
//! does not run.

use serde_json::{json, Value};

use weft::signal::SendWhen;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{completes, create_body, emitted_file, fails, inline};

use super::ElevenLabsVideoFromReferencesNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("veo_sends_subjects_and_styles_with_their_roles", veo_roles),
        NodeTest::fake("seedance_sends_subjects_clips_and_sounds_plain", seedance_plain),
        NodeTest::fake("a_clip_that_cannot_go_inline_is_a_temporary_asset_weft_deletes_after", clip_through_an_asset),
        NodeTest::fake("a_failed_generation_fails_with_elevenlabs_reason", failed),
        NodeTest::fake("a_failed_generation_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_refused_create_quotes_elevenlabs_and_never_waits", refused),
        NodeTest::live("one_real_video_of_a_generated_subject", "elevenlabs", live_from_subject),
    ]
}

/// The node's inputs with one subject image, plus `extra` on top.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "prompt": "the mug turns slowly on the table",
        "subjects": rig.store_file("mug.png", "image/png", b"mug".to_vec()),
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn veo_roles(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "r1", "video/mp4", b"mp4-bytes");
    let mug = rig.store_file("mug.png", "image/png", b"mug".to_vec());
    let dog = rig.store_file("dog.jpg", "image/jpeg", b"dog".to_vec());
    let paint = rig.store_file("watercolor.webp", "image/webp", b"watercolor".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsVideoFromReferencesNode,
            inputs(&rig, json!({ "subjects": [mug, dog], "styles": paint, "durationSecs": 8 })),
        )
        .await
        .ok()?;

    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "veo-3.1-fast-generate-001",
            "prompt": "the mug turns slowly on the table",
            "generate_audio": true,
            "duration_secs": 8,
            "images": [
                { "image": inline("image/png", b"mug"), "role": "subject" },
                { "image": inline("image/jpeg", b"dog"), "role": "subject" },
                { "image": inline("image/webp", b"watercolor"), "role": "style" },
            ],
        }),
        "every subject, then every style, each carrying its role"
    );
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "video")?;
    assert_eq!(bytes, b"mp4-bytes");
    assert_eq!(filename, "video.mp4");
    assert_eq!(mime, "video/mp4");
    Ok(())
}

async fn seedance_plain(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "r2", "video/mp4", b"mp4");
    let clip = rig.store_file("dance.mp4", "video/mp4", b"clip".to_vec());
    let sound = rig.store_file("line.mp3", "audio/mpeg", b"speech".to_vec());
    rig.run(
        &ElevenLabsVideoFromReferencesNode,
        inputs(
            &rig,
            json!({ "model": "bytedance-seedance-v2.5", "videos": [clip], "audios": [sound] }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "bytedance-seedance-v2.5",
            "prompt": "the mug turns slowly on the table",
            "generate_audio": true,
            "images": [inline("image/png", b"mug")],
            "videos": [inline("video/mp4", b"clip")],
            "audios": [inline("audio/mpeg", b"speech")],
        }),
        "the one subject wired as a single image still goes as a one-item list"
    );
    Ok(())
}

/// A clip in a container ElevenLabs does not take inline (Matroska)
/// goes up as a temporary asset, whose delete weft owes once the node ends.
async fn clip_through_an_asset(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "r5", "video/mp4", b"mp4");
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a9" }));
    let clip = rig.store_file("dance.mkv", "video/x-matroska", b"mkv".to_vec());
    rig.run(
        &ElevenLabsVideoFromReferencesNode,
        inputs(&rig, json!({ "model": "bytedance-seedance-v2", "videos": clip })),
    )
    .await
    .ok()?;
    assert_eq!(create_body(&rig)["videos"], json!([{ "type": "asset", "asset_id": "a9" }]));
    let owed = rig.owed_calls();
    assert_eq!(owed.len(), 1, "{owed:?}");
    assert!(owed[0].0.url.ends_with("/v1/assets/a9"), "{}", owed[0].0.url);
    assert_eq!(owed[0].0.when, SendWhen::Always, "weft deletes the asset however the node ends");
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "r3", "moderated", "The prompt was rejected by content moderation. You were not charged for this generation.");
    let err = rig.run(&ElevenLabsVideoFromReferencesNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("moderated"), "{err}");
    assert!(err.contains("You were not charged"), "{err}");
    assert!(err.contains("r3"), "the error names the generation: {err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "r4", "timeout", "Timed out while processing.");
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsVideoFromReferencesNode, inputs(&rig, json!({}))).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("timeout"), "{error}");
    assert!(error.contains("Timed out while processing."), "{error}");
    assert!(!outcome.outputs.contains_key("video"), "a caught failure emits no video");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v1/flows/video",
        422,
        json!({ "detail": [{ "loc": ["body", "veo-3.1-fast-generate-001", "videos"], "msg": "Extra inputs are not permitted", "type": "extra_forbidden" }] }),
    );
    let clip = rig.store_file("dance.mp4", "video/mp4", b"clip".to_vec());
    let err = rig
        .run(&ElevenLabsVideoFromReferencesNode, inputs(&rig, json!({ "videos": clip })))
        .await
        .failure()?;
    assert!(err.contains("422"), "{err}");
    assert!(err.contains("body.veo-3.1-fast-generate-001.videos: Extra inputs are not permitted"), "{err}");
    assert!(rig.awaited_signals().is_empty(), "a refused create never parks");
    Ok(())
}

/// One real image on the cheapest image model, then one real 8-second
/// 720p video with no sound on Veo 3.1 Fast that shows it: it bills
/// that image and that video to the account's credits. The image goes
/// inline, so no temporary asset is made, and nothing is left on the
/// account: generations are listed by the API but hold no storage.
async fn live_from_subject(rig: LiveRig) -> WeftResult<()> {
    let made = rig
        .run(
            &crate::eleven_labs_generate_image::ElevenLabsGenerateImageNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a plain red ceramic mug on a wooden table, studio light",
                "model": "gemini-3.1-flash-lite-image",
            }),
        )
        .await
        .ok()?;
    let mug = made.output("image")?.clone();
    let outcome = rig
        .run(
            &ElevenLabsVideoFromReferencesNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "the mug turns slowly on the table",
                "subjects": mug,
                "model": "veo-3.1-fast-generate-001",
                "durationSecs": 8,
                "resolution": "720p",
                "generateAudio": false,
            }),
        )
        .await
        .ok()?;
    let file = weft::storage::StoredFile::from_value(outcome.output("video")?)?;
    assert!(file.mime_type.starts_with("video/"), "{}", file.mime_type);
    assert!(file.size_bytes > 10_000, "a real video is more than 10 KB: {}", file.size_bytes);
    Ok(())
}
