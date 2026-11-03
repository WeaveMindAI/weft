//! ElevenLabsLipSync self-tests: the request each setting makes, a
//! sound that cannot go inline going up as a temporary asset deleted
//! after, the stored video, and the two ways a generation fails.

use serde_json::{json, Value};

use weft::signal::SendWhen;
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::testing::{completes, create_body, emitted_file, fails, inline};

use super::ElevenLabsLipSyncNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("sends_the_picture_and_the_speech_and_stores_the_video", picture_and_speech),
        NodeTest::fake("every_setting_reaches_the_request", every_setting),
        NodeTest::fake("an_ogg_goes_up_as_a_temporary_asset_deleted_after", ogg_asset),
        NodeTest::fake("a_failed_generation_fails_with_elevenlabs_reason", failed),
        NodeTest::fake("a_failed_generation_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_refused_create_quotes_elevenlabs_and_never_waits", refused),
        NodeTest::live("one_real_generated_face_speaking_generated_speech", "elevenlabs", live_lip_sync),
    ]
}

/// The node's inputs with a picture and an mp3 of speech, plus `extra`
/// on top.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "image": rig.store_file("face.png", "image/png", b"face".to_vec()),
        "audio": rig.store_file("hello.mp3", "audio/mpeg", b"hello".to_vec()),
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn picture_and_speech(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "l1", "video/mp4", b"mp4-bytes");
    let outcome = rig.run(&ElevenLabsLipSyncNode, inputs(&rig, json!({}))).await.ok()?;

    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "creatify-aurora",
            "image": inline("image/png", b"face"),
            "audio": inline("audio/mpeg", b"hello"),
        }),
        "a setting nobody gave never travels"
    );
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "video")?;
    assert_eq!(bytes, b"mp4-bytes");
    assert_eq!(filename, "video.mp4");
    assert_eq!(mime, "video/mp4");
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "l2", "video/mp4", b"mp4");
    rig.run(
        &ElevenLabsLipSyncNode,
        inputs(&rig, json!({ "resolution": "480p", "guidanceScale": 2.5, "audioGuidanceScale": 1.5 })),
    )
    .await
    .ok()?;
    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "creatify-aurora",
            "image": inline("image/png", b"face"),
            "audio": inline("audio/mpeg", b"hello"),
            "resolution": "480p",
            "guidance_scale": 2.5,
            "audio_guidance_scale": 1.5,
        })
    );
    Ok(())
}

async fn ogg_asset(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "l3", "video/mp4", b"mp4");
    rig.respond("POST", "/v1/assets", json!({ "asset_id": "a1" }));
    let note = rig.store_file("note.ogg", "audio/ogg", b"OggS".to_vec());
    rig.run(&ElevenLabsLipSyncNode, inputs(&rig, json!({ "audio": note }))).await.ok()?;

    assert_eq!(create_body(&rig)["audio"], json!({ "type": "asset", "asset_id": "a1" }));
    assert_eq!(create_body(&rig)["image"], inline("image/png", b"face"), "the png still goes inline");
    let owed = rig.owed_calls();
    assert_eq!(owed.len(), 1, "{owed:?}");
    assert!(owed[0].0.url.ends_with("/v1/assets/a1"), "{}", owed[0].0.url);
    assert_eq!(owed[0].0.when, SendWhen::Always, "weft deletes the asset however the node ends");
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "l4", "moderated", "The image was rejected by content moderation. You were not charged for this generation.");
    let err = rig.run(&ElevenLabsLipSyncNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("moderated"), "{err}");
    assert!(err.contains("You were not charged"), "{err}");
    assert!(err.contains("l4"), "the error names the generation: {err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "l5", "timeout", "Timed out while processing.");
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsLipSyncNode, inputs(&rig, json!({}))).await.ok()?;
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
        json!({ "detail": [{ "loc": ["body", "creatify-aurora", "resolution"], "msg": "Input should be '480p' or '720p'", "type": "literal_error" }] }),
    );
    let err = rig
        .run(&ElevenLabsLipSyncNode, inputs(&rig, json!({ "resolution": "1080p" })))
        .await
        .failure()?;
    assert!(err.contains("422"), "{err}");
    assert!(err.contains("body.creatify-aurora.resolution: Input should be '480p' or '720p'"), "{err}");
    assert!(rig.awaited_signals().is_empty(), "a refused create never parks");
    Ok(())
}

/// One real face on the cheapest image model, one short line of real
/// speech, then one real 480p lip-sync of the two on Creatify Aurora: it
/// bills that image, that line and that video to the account's credits.
/// The picture and the mp3 go inline, so no temporary asset is made, and
/// nothing is left on the account: generations are listed by the API but
/// hold no storage.
async fn live_lip_sync(rig: LiveRig) -> WeftResult<()> {
    let made = rig
        .run(
            &crate::eleven_labs_generate_image::ElevenLabsGenerateImageNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a photo of a smiling woman looking straight at the camera, head and shoulders, plain grey background",
                "model": "gemini-3.1-flash-lite-image",
                "aspectRatio": "1:1",
            }),
        )
        .await
        .ok()?;
    let face = made.output("image")?.clone();
    let conn = rig.connect().await?;
    let voice = crate::testing::first_voice_id(&conn).await?;
    let bytes = crate::testing::speech_sample(&conn, &voice, "Hello from weft.").await?;
    let speech = rig.store_file("speech.mp3", "audio/mpeg", bytes).await?;
    let outcome = rig
        .run(
            &ElevenLabsLipSyncNode,
            json!({
                "account": rig.access("elevenlabs"),
                "image": face,
                "audio": speech,
                "model": "creatify-aurora",
                "resolution": "480p",
            }),
        )
        .await
        .ok()?;
    let file = weft::storage::StoredFile::from_value(outcome.output("video")?)?;
    assert!(file.mime_type.starts_with("video/"), "{}", file.mime_type);
    assert!(file.size_bytes > 10_000, "a real video is more than 10 KB: {}", file.size_bytes);
    Ok(())
}
